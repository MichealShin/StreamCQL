package com.pzw.streaming.flink.plan.builder;

import com.huawei.streaming.api.opereators.InputStreamOperator;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.mapping.SimpleLexer;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.operator.InputOperator;
import com.huawei.streaming.serde.StreamSerDe;
import com.pzw.streaming.flink.graph.ApplicationGraph;
import com.pzw.streaming.flink.plan.AbstractPlan;
import com.pzw.streaming.flink.plan.input.ISourceFunction;
import com.pzw.streaming.flink.plan.input.SourcePlan;
import com.pzw.streaming.flink.plan.input.SourcePlanRegistry;
import com.pzw.streaming.flink.util.PlanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/6/23 下午3:28
 */
public class InputPlanBuilder extends AbstractPlanBuilder<InputStreamOperator> {

    private static Logger LOG = LoggerFactory.getLogger(InputPlanBuilder.class);

    public InputPlanBuilder(ApplicationGraph graph) {
        super(graph);
    }

    @Override
    public AbstractPlan build(InputStreamOperator operator,
                              Schema inputSchema,
                              Schema outputSchema) throws StreamingException {

        StreamingConfig config = new StreamingConfig();

        if (operator.getArgs() != null) {
            config.putAll(operator.getArgs());
        }
        config.putAll(getGraph().getConfs());

        StreamSerDe serde = PlanUtils.createSerdeInstance(operator.getDeserializerClassName(), config);
        serde.setSchema(PlanUtils.createEventType(outputSchema));

        try {
            serde.initialize();

            Class<? extends ISourceFunction> sourceFunctionClass = SourcePlanRegistry.get(SimpleLexer.INPUT.getSimpleName(operator.getRecordReaderClassName()));

            if (sourceFunctionClass == null) {
                sourceFunctionClass = SourcePlanRegistry.get(operator.getRecordReaderClassName());
            }

            ISourceFunction sourceFunction = sourceFunctionClass.newInstance();
            sourceFunction.setSerDe(serde);

            SourcePlan sourcePlan = new SourcePlan(sourceFunction);

            sourcePlan.setParallelNumber(operator.getParallelNumber());
            sourcePlan.setConfig(config);

            return sourcePlan;

        } catch (Exception e) {
            LOG.warn("failed to build plan for input operator:"+operator,e);
            throw new StreamingException(e.getMessage(), e);
        }
    }
}
