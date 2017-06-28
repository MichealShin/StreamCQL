package com.pzw.streaming.flink.plan.output;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.serde.StreamSerDe;
import com.pzw.streaming.flink.plan.common.TupleEventSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/25 下午9:28
 */
public class ConsoleSink implements ISinkFunction,SinkFunction<TupleEvent> {

    private static Logger LOG = LoggerFactory.getLogger(ConsoleSink.class);

    private TupleEventSchema schema;

    @Override
    public void setSerDe(StreamSerDe serde) {
        schema = new TupleEventSchema(serde);
    }

    @Override
    public void setConfig(StreamingConfig config) throws StreamingException {

    }

    @Override
    public SinkFunction<TupleEvent> getSinkFunction() {
        return this;
    }

    @Override
    public void invoke(TupleEvent event) throws Exception {
        byte[] bytes = schema.serialize(event);
        LOG.info(new String(bytes));
    }
}
