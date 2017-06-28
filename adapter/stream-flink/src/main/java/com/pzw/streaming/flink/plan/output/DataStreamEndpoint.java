package com.pzw.streaming.flink.plan.output;

import com.huawei.streaming.event.TupleEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.StreamTransformation;

import java.util.Collection;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/24 下午8:50
 */
public class DataStreamEndpoint extends DataStream<TupleEvent> {

    private DataStreamEndpoint(StreamExecutionEnvironment env) {
        super(env, new EndpointStreamTransformation());
    }

    public static DataStreamEndpoint create(StreamExecutionEnvironment env) {
        return new DataStreamEndpoint(env);
    }

    private static class EndpointStreamTransformation extends StreamTransformation<TupleEvent> {

        public EndpointStreamTransformation() {
            super("endpoint", TypeInformation.of(TupleEvent.class), 1);
        }

        @Override
        public void setChainingStrategy(ChainingStrategy chainingStrategy) {

        }

        @Override
        public Collection<StreamTransformation<?>> getTransitivePredecessors() {
            return null;
        }
    }
}
