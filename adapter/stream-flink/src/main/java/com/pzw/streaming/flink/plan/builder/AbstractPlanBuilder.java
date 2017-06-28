package com.pzw.streaming.flink.plan.builder;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.executor.operatorviewscreater.FilterViewExpressionCreator;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.IExpression;
import com.pzw.streaming.flink.graph.ApplicationGraph;
import com.pzw.streaming.flink.plan.AbstractPlan;
import com.pzw.streaming.flink.plan.FilterPlan;

/** 执行计划生成器基础类
 * @author pengzhiwei
 * @version V1.0
 * @date 17/6/23 下午2:40
 */
public abstract class AbstractPlanBuilder<O extends Operator> {

    private ApplicationGraph graph;

    public AbstractPlanBuilder(ApplicationGraph graph) {
        this.graph = graph;
    }

    /**
     * 将算子构建成Flink执行计划
     * @param operator
     * @param inputSchema
     * @param outputSchema
     * @return
     */
    public abstract AbstractPlan build(O operator,
                                       Schema inputSchema,
                                       Schema outputSchema) throws StreamingException;



    protected FilterPlan buildFilterPlan(String filterExpStr, Schema inputSchema, int parallelNumber) throws StreamingException {

        if (Strings.isNullOrEmpty(filterExpStr))
            return null;

        IExpression filterExp = new FilterViewExpressionCreator().create(Lists.newArrayList(inputSchema), filterExpStr, graph.getConfs());

        FilterPlan filterPlan = new FilterPlan();
        filterPlan.getExpressions().add(filterExp);
        filterPlan.setParallelNumber(parallelNumber);
        return filterPlan;
    }

    public ApplicationGraph getGraph() {
        return graph;
    }
}
