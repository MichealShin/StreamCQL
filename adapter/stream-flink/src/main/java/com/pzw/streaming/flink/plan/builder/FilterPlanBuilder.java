package com.pzw.streaming.flink.plan.builder;

import com.huawei.streaming.api.opereators.FilterOperator;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.IExpression;
import com.pzw.streaming.flink.graph.ApplicationGraph;
import com.pzw.streaming.flink.plan.AbstractPlan;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/6/23 下午3:19
 */
public class FilterPlanBuilder extends AbstractPlanBuilder<FilterOperator> {

    public FilterPlanBuilder(ApplicationGraph graph) {
        super(graph);
    }

    @Override
    public AbstractPlan build(FilterOperator operator,
                              Schema inputSchema,
                              Schema outputSchema) throws StreamingException {

        String filterExpStr = operator.getFilterExpression();

        return buildFilterPlan(filterExpStr, inputSchema, operator.getParallelNumber());
    }
}
