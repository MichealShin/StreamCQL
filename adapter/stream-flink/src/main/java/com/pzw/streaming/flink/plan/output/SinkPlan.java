package com.pzw.streaming.flink.plan.output;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.serde.StreamSerDe;
import com.pzw.streaming.flink.plan.AbstractPlan;
import com.pzw.streaming.flink.plan.PlanEdge;
import com.pzw.streaming.flink.plan.common.TupleEventSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;

import java.util.List;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/17 下午10:07
 */
public class SinkPlan extends AbstractPlan  {

    private ISinkFunction sinkFunction;

    public SinkPlan(ISinkFunction sinkFunction) {
        this.sinkFunction = sinkFunction;
    }

    public void setConfig(StreamingConfig config) throws StreamingException {
        sinkFunction.setConfig(config);
    }

    @Override
    public DataStream<TupleEvent> translate(StreamExecutionEnvironment env,
                                            List<DataStream<TupleEvent>> inputs,
                                            List<PlanEdge> inputEdges) throws StreamingException {

        for(DataStream<TupleEvent> input:inputs) {
            input.addSink(sinkFunction.getSinkFunction()).setParallelism(getParallelNumber());
        }

        return DataStreamEndpoint.create(env);
    }
}
