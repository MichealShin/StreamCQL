package com.pzw.streaming.flink.plan.builder;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.huawei.streaming.api.opereators.*;
import com.huawei.streaming.api.streams.Column;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.application.DistributeType;
import com.huawei.streaming.cql.executor.operatorviewscreater.GroupByViewCreator;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.IExpression;
import com.pzw.streaming.flink.graph.ApplicationGraph;
import com.pzw.streaming.flink.graph.ApplicationVertex;
import com.pzw.streaming.flink.graph.GroupbyKeyInSelectReplacer;
import com.pzw.streaming.flink.plan.AbstractPlan;
import com.pzw.streaming.flink.plan.PlanEdge;
import com.pzw.streaming.flink.plan.PlanEdge.ShuffleType;
import com.pzw.streaming.flink.util.PlanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * 执行计划生成器
 *
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/15 下午5:33
 */
public class ExecutePlanBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutePlanBuilder.class);

    private static Map<Class<? extends Operator>,Class<? extends AbstractPlanBuilder>> builders = Maps.newHashMap();

    static {
        builders.put(InputStreamOperator.class,InputPlanBuilder.class);
        builders.put(FunctorOperator.class,FunctorPlanBuilder.class);
        builders.put(FilterOperator.class,FilterPlanBuilder.class);
        builders.put(AggregateOperator.class,AggregatePlanBuilder.class);
        builders.put(OutputStreamOperator.class,OutputPlanBuilder.class);
    }


    private ApplicationGraph graph;

    public ExecutePlanBuilder(ApplicationGraph graph) {
        this.graph = graph;
    }

    public List<AbstractPlan> build() throws StreamingException {

        List<AbstractPlan> tops = Lists.newArrayList();

        for (ApplicationVertex vertex : graph.getApplicationVertices()) {
            AbstractPlan operator = buildPlan(vertex, null);
            tops.add(operator);
        }
        return tops;
    }

    private AbstractPlan buildPlan(ApplicationVertex current, OperatorTransition inTransition)
            throws StreamingException {

        Operator operator = current.getOperator();

        AbstractPlanBuilder planBuilder = createPlanBuilder(operator);

        Schema inputSchema = inTransition!=null ? getSchemaByName(inTransition.getSchemaName()) :null;

        Schema outputSchema = current.getEdges().size() > 0 ?
                getSchemaByName(current.getEdges().get(0).getSchemaName()) : getSchemaByName(inTransition.getSchemaName());

        if(inTransition!=null) {

            IExpression[] shuffleExpressions = inTransition.getDistributedType() == DistributeType.FIELDS ?
                    new GroupByViewCreator().create(Lists.newArrayList(inputSchema), inTransition.getDistributedFields(), graph.getConfs()) : null;

            //Shuffle表达式字段添加到input schema中
            if (shuffleExpressions != null) {
                for (int i = 0; i < shuffleExpressions.length; i++) {
                    Column column = new Column(GroupbyKeyInSelectReplacer.getGroupByExpressionReplacedName(i), shuffleExpressions[i].getType());
                    inputSchema.addCol(column);
                }
            }
        }

        //构建执行计划
        AbstractPlan plan = planBuilder.build(operator,inputSchema,outputSchema);

        //获取当前节点链路的最后一个节点,使用该节点连接下游子节点
        AbstractPlan currentLastPlan = PlanUtils.getLastPlan(plan);

        //构建下游节点的执行计划
        for (int i = 0; i < current.getChildren().size(); i++) {

            ApplicationVertex childVertex = current.getChildren().get(i);
            OperatorTransition childTransition = current.getEdges().get(i);

            Schema childInputSchema = getSchemaByName(childTransition.getSchemaName());

            //构建下游执行计划
            AbstractPlan childPlan = buildPlan(childVertex, childTransition);

            //建立和下游的连接
            PlanEdge edge = new PlanEdge();
            edge.setEventType(PlanUtils.createEventType(childInputSchema));

            switch (childTransition.getDistributedType()) {
                case FIELDS:
                    edge.setShuffleType(ShuffleType.SHUFFLE_EXPRESSION);

                    if (!Strings.isNullOrEmpty(childTransition.getDistributedFields())) { //包含group by的情况

                        IExpression[] shuffleExprs = new GroupByViewCreator().create(Lists.newArrayList(childInputSchema),
                                childTransition.getDistributedFields(), graph.getConfs());

                        edge.setShuffleExpressions(shuffleExprs);
                        edge.setShuffleType(ShuffleType.SHUFFLE_EXPRESSION);
                    }
                    break;
                case SHUFFLE:
                    edge.setShuffleType(ShuffleType.SHUFFLE_RANDOM);
                    break;
            }
            currentLastPlan.addChild(childPlan, edge);
        }

        return plan;
    }

    private AbstractPlanBuilder createPlanBuilder(Operator operator) {

        Class<? extends AbstractPlanBuilder> planBuilderClass = builders.get(operator.getClass());

        if(planBuilderClass==null) {
            throw new IllegalStateException("cannot find AbstractPlanBuilder class for "+operator.getClass());
        }

        try {
            return planBuilderClass.
                    getDeclaredConstructor(ApplicationGraph.class).newInstance(graph);
        } catch (Exception e) {
            LOG.warn("error in create AbstractPlanBuilder instance",e);
            throw new IllegalStateException("error in create AbstractPlanBuilder instance for "+planBuilderClass);
        }
    }

    private Schema getSchemaByName(String schemaName) throws StreamingException {

        Schema schema = graph.getSchemaByName(schemaName);

        if (schema == null) {
            throw new StreamingException("cannot find schema for " + schemaName);
        }

        return schema;
    }

}
