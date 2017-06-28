package com.pzw.streaming.flink.plan.builder;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.huawei.streaming.api.opereators.AggregateOperator;
import com.huawei.streaming.api.opereators.Window;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.executor.operatorinfocreater.ExpressionGetterStrategy;
import com.huawei.streaming.cql.executor.operatorinfocreater.ExpressionsWalker;
import com.huawei.streaming.cql.executor.operatorviewscreater.FilterViewExpressionCreator;
import com.huawei.streaming.cql.executor.operatorviewscreater.SelectViewExpressionCreator;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.AggregateExpression;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.expression.MethodExpression;
import com.huawei.streaming.expression.OperatorBasedExpression;
import com.huawei.streaming.expression.PropertyValueExpression;
import com.huawei.streaming.process.agg.aggregator.IAggregate;
import com.huawei.streaming.process.agg.aggregator.IAggregateClone;
import com.pzw.streaming.flink.graph.ApplicationGraph;
import com.pzw.streaming.flink.plan.AbstractPlan;
import com.pzw.streaming.flink.plan.AggregatePlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/6/23 下午4:02
 */
public class AggregatePlanBuilder extends AbstractPlanBuilder<AggregateOperator> {

    private static Logger LOG = LoggerFactory.getLogger(AggregatePlanBuilder.class);

    public AggregatePlanBuilder(ApplicationGraph graph) {
        super(graph);
    }

    @Override
    public AbstractPlan build(AggregateOperator operator,
                              Schema inputSchema,
                              Schema outputSchema) throws StreamingException {

        //生成AggregatePlan计划
        AggregatePlan aggregatePlan = new AggregatePlan();
        aggregatePlan.setParallelNumber(operator.getParallelNumber());

        //设置窗口
        Window window = operator.getWindow();
        aggregatePlan.setWindow(window);

        //生成select计划
        String selectExprStr = operator.getOutputExpression();
        IExpression[] selectExpressions = new SelectViewExpressionCreator().create(Lists.newArrayList(inputSchema),
                selectExprStr, getGraph().getConfs());

        //找到所有聚合表达式
        List<AggregateExpression> aggExpressions = findAggregateExpression(selectExpressions);

        //建立聚合表达式和要替换的PropertyValueExpression的映射关系
        Map<AggregateExpression, PropertyValueExpression> aggMapProperty = Maps.newHashMap();

        for (int i = 0; i < aggExpressions.size(); i++) {

            AggregateExpression aggExpression = aggExpressions.get(i);
            PropertyValueExpression pv = new PropertyValueExpression("agg" + i, aggExpression.getType());

            aggMapProperty.put(aggExpression, pv);
        }

        //替换select表达式中的聚合运算
        replaceAggregateInSelect(selectExpressions, aggMapProperty);

        //替换所有聚合表达式为AggregateExpressionWithId
        List<AggregateExpressionWithId> aggregateExpressionWithIds = wrapAggregateExpressionWithId(aggExpressions);

        //
        //替换aggMapProperty中的AggregateExpression为AggregateExpressionWithId对象
        Map<AggregateExpression, PropertyValueExpression> aggWithIdMapProperty = Maps.newHashMap();

        for(int i=0;i<aggExpressions.size();i++) {

            PropertyValueExpression propertyValueExpression = aggMapProperty.get(aggExpressions.get(i));

            aggWithIdMapProperty.put(aggregateExpressionWithIds.get(i),propertyValueExpression);
        }


        aggregatePlan.getAggregates().addAll(aggregateExpressionWithIds);
        aggregatePlan.getSelectExpressions().addAll(Lists.newArrayList(selectExpressions));
        aggregatePlan.setAggMapProperty(aggWithIdMapProperty);


        //生成filter after groupby
        String filterAfterExprStr = operator.getFilterAfterAggregate();

        if (!Strings.isNullOrEmpty(filterAfterExprStr)) {

            IExpression filterAfterExp = new FilterViewExpressionCreator().create(Lists.newArrayList(outputSchema),
                    filterAfterExprStr, getGraph().getConfs());

            aggregatePlan.setFilterExpr(filterAfterExp);
        }

        String orderByExprStr = operator.getOrderBy();

        if (!Strings.isNullOrEmpty(orderByExprStr)) {
            throw new StreamingException("order by is not support currently for flink");
        }

        Integer limit = operator.getLimit();
        if (limit != null) {
            throw new StreamingException("limit is not support currently for flink");
        }

        return aggregatePlan;
    }

    /**
     * 找出所有聚合函数
     *
     * @param exprs
     * @return
     */
    private List<AggregateExpression> findAggregateExpression(IExpression[] exprs) {

        List<IExpression> aggExpressions = Lists.newArrayList();

        ExpressionsWalker walker = new ExpressionsWalker(new ExpressionGetterStrategy() {
            @Override
            public boolean isEqual(IExpression exp) {
                if (exp instanceof AggregateExpression) {
                    return true;
                }
                return false;
            }
        });

        try {
            for (IExpression expr : exprs) {
                walker.found(expr, aggExpressions);
            }
        } catch (ExecutorException e) {
            LOG.error("error in find aggreate expression", e);
        }

        List<AggregateExpression> aggregates = Lists.newArrayList();

        for (int i=0;i<aggExpressions.size();i++) {

            AggregateExpression aggExpr = (AggregateExpression) aggExpressions.get(i);

            aggregates.add(aggExpr);
        }

        return aggregates;
    }

    private void replaceAggregateInSelect(IExpression[] selects,
                                          Map<AggregateExpression, PropertyValueExpression> aggMapProperty) {

        for (int i = 0; i < selects.length; i++) {
            selects[i] = replaceAggregate(selects[i], aggMapProperty);
        }
    }

    private IExpression replaceAggregate(IExpression expression,
                                         Map<AggregateExpression, PropertyValueExpression> aggMapProperty) {

        if (expression instanceof MethodExpression) {
            MethodExpression method = (MethodExpression) expression;
            IExpression[] args = method.getExpr();

            if (args != null) {
                for (int i = 0; i < args.length; i++) {
                    args[i] = replaceAggregate(args[i], aggMapProperty);
                }
            }
        } else if (expression instanceof OperatorBasedExpression) {

            OperatorBasedExpression operator = (OperatorBasedExpression) expression;
            operator.setLeftExpr(replaceAggregate(operator.getLeftExpr(), aggMapProperty));
            operator.setRightExpr(replaceAggregate(operator.getRightExpr(), aggMapProperty));

        } else if (expression instanceof AggregateExpression) {

            expression = aggMapProperty.get(expression);
        }

        return expression;
    }


    private List<AggregateExpressionWithId> wrapAggregateExpressionWithId(List<AggregateExpression> aggregateExpressions) {

        List<AggregateExpressionWithId> aggregateExpressionWithIds = Lists.newArrayList();

        for(int i=0;i<aggregateExpressions.size();i++) {
            AggregateExpressionWithId aggregateExpressionWithId = new AggregateExpressionWithId(aggregateExpressions.get(i),i);
            aggregateExpressionWithIds.add(aggregateExpressionWithId);
        }

        return aggregateExpressionWithIds;
    }
    /**
     * 由于AggregateExpression没有实现equal和hashCode方法,这里进行一次封装,添加id字段实现equals和hashCode方法
     */
    public static class AggregateExpressionWithId extends AggregateExpression {

        private AggregateExpression base;
        private int id;

        public AggregateExpressionWithId(AggregateExpression base,int id) {
            super(base.getAggegator(), base.isDistinct());
            this.base = base;
            this.id = id;
        }

        @Override
        public Object evaluate(IEvent theEvent) {
            return base.evaluate(theEvent);
        }

        @Override
        public Object evaluate(IEvent[] eventsPerStream) {
            return base.evaluate(eventsPerStream);
        }

        public boolean isDistinct() {
            return base.isDistinct();
        }

        @Override
        public Class<?> getType() {
            return base.getType();
        }

        public IExpression getAggArgExpression() {
            return base.getAggArgExpression();
        }

        public void setAggArgExpression(IExpression aggArgExpression) {
            base.setAggArgExpression(aggArgExpression);
        }

        public IAggregate getAggegator() {
            return base.getAggegator();
        }

        public IExpression getAggArgFilterExpression() {
            return base.getAggArgFilterExpression();
        }

        public void setAggArgFilterExpression(IExpression aggArgFilterExpression) {
            base.setAggArgFilterExpression(aggArgFilterExpression);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            AggregateExpressionWithId that = (AggregateExpressionWithId) o;

            return id == that.id;

        }

        @Override
        public int hashCode() {
            return id;
        }

        public AggregateExpressionWithId clone() {

            AggregateExpression baseClone = new AggregateExpression(((IAggregateClone)base.getAggegator()).cloneAggregate(),base.isDistinct());
            baseClone.setAggArgExpression(base.getAggArgExpression());
            baseClone.setAggArgFilterExpression(base.getAggArgFilterExpression());

            AggregateExpressionWithId instance = new AggregateExpressionWithId(baseClone,id);
            return instance;
        }
    }

}
