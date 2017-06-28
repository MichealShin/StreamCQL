package com.pzw.streaming.flink.plan.builder;

import com.huawei.streaming.api.opereators.OutputStreamOperator;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.mapping.SimpleLexer;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.serde.StreamSerDe;
import com.pzw.streaming.flink.graph.ApplicationGraph;
import com.pzw.streaming.flink.plan.AbstractPlan;
import com.pzw.streaming.flink.plan.output.ISinkFunction;
import com.pzw.streaming.flink.plan.output.SinkPlan;
import com.pzw.streaming.flink.plan.output.SinkPlanRegistry;
import com.pzw.streaming.flink.util.PlanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/6/23 下午3:43
 */
public class OutputPlanBuilder extends AbstractPlanBuilder<OutputStreamOperator> {

    private Logger LOG = LoggerFactory.getLogger(OutputPlanBuilder.class);

    public OutputPlanBuilder(ApplicationGraph graph) {
        super(graph);
    }

    @Override
    public AbstractPlan build(OutputStreamOperator operator,
                              Schema inputSchema,
                              Schema outputSchema) throws StreamingException {

        StreamingConfig config = new StreamingConfig();

        if (operator.getArgs() != null) {
            config.putAll(operator.getArgs());
        }
        config.putAll(getGraph().getConfs());

        StreamSerDe serde = PlanUtils.createSerdeInstance(operator.getSerializerClassName(), config);
        serde.setSchema(PlanUtils.createEventType(outputSchema));

        try {
            serde.initialize();

            Class<? extends ISinkFunction> sinkFunctionClass =
                    SinkPlanRegistry.get(SimpleLexer.OUTPUT.getSimpleName(operator.getRecordWriterClassName()));

            if (sinkFunctionClass == null) {
                sinkFunctionClass = SinkPlanRegistry.get(operator.getRecordWriterClassName());
            }

            ISinkFunction sinkFunction = sinkFunctionClass.newInstance();
            sinkFunction.setSerDe(serde);

            SinkPlan sinkPlan = new SinkPlan(sinkFunction);
            sinkPlan.setParallelNumber(operator.getParallelNumber());
            sinkPlan.setConfig(config);

            return sinkPlan;
        } catch (Exception e) {
            LOG.warn("failed to build plan for output operator:"+operator,e);
            throw new StreamingException(e.getMessage(), e);
        }
    }
}
