package com.pzw.streaming.flink.plan.input;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.serde.StreamSerDe;
import com.pzw.streaming.flink.plan.AbstractPlan;
import com.pzw.streaming.flink.plan.PlanEdge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/16 下午10:27
 */
public  class SourcePlan extends AbstractPlan {

    private ISourceFunction sourceFunction;

    public SourcePlan(ISourceFunction sourceFunction) {
        this.sourceFunction = sourceFunction;
    }

    public void setConfig(StreamingConfig config) throws StreamingException {
        sourceFunction.setConfig(config);
    }

    @Override
    public DataStream<TupleEvent> translate(StreamExecutionEnvironment env,
                                            List<DataStream<TupleEvent>> inputs,
                                            List<PlanEdge> inputEdges) throws StreamingException {
        return env.addSource(sourceFunction.getSourceFunction()).setParallelism(getParallelNumber());
    }
}
