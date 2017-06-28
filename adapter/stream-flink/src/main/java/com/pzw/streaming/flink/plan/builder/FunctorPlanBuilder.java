package com.pzw.streaming.flink.plan.builder;

import com.google.common.collect.Lists;
import com.huawei.streaming.api.opereators.FunctorOperator;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.executor.operatorviewscreater.SelectViewExpressionCreator;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.IExpression;
import com.pzw.streaming.flink.graph.ApplicationGraph;
import com.pzw.streaming.flink.plan.AbstractPlan;
import com.pzw.streaming.flink.plan.FilterPlan;
import com.pzw.streaming.flink.plan.PlanEdge;
import com.pzw.streaming.flink.plan.PlanEdge.ShuffleType;
import com.pzw.streaming.flink.plan.SelectPlan;
import com.pzw.streaming.flink.util.PlanUtils;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/6/23 下午2:52
 */
public class FunctorPlanBuilder extends AbstractPlanBuilder<FunctorOperator> {


    public FunctorPlanBuilder(ApplicationGraph graph) {
        super(graph);
    }

    @Override
    public AbstractPlan build(FunctorOperator operator,
                              Schema inputSchema,
                              Schema outputSchema) throws StreamingException {
        //生成过滤计划
        String filterExpStr = operator.getFilterExpression();

        FilterPlan filterPlan = buildFilterPlan(filterExpStr, inputSchema, operator.getParallelNumber());

        //生成select计划
        String selectExpStr = operator.getOutputExpression();
        final SelectViewExpressionCreator creater = new SelectViewExpressionCreator();
        IExpression[] exprs = creater.create(Lists.newArrayList(inputSchema), selectExpStr, getGraph().getConfs());

        SelectPlan selectPlan = new SelectPlan();
        for (IExpression expr : exprs) {
            selectPlan.getExpressions().add(expr);
        }
        selectPlan.setParallelNumber(operator.getParallelNumber());

        //连接filter和select
        if (filterPlan != null) {

            PlanEdge edge = new PlanEdge();
            edge.setShuffleType(ShuffleType.SHUFFLE_RANDOM);
            edge.setEventType(PlanUtils.createEventType(inputSchema));

            filterPlan.addChild(selectPlan, edge);
            return filterPlan;
        }

        return selectPlan;
    }
}
