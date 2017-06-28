package com.pzw.streaming.flink.plan.common;

import com.google.common.collect.Lists;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.event.TupleEventType;
import com.huawei.streaming.exception.StreamSerDeException;
import com.huawei.streaming.serde.StreamSerDe;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/17 下午9:33
 */
public class TupleEventSchema implements DeserializationSchema<TupleEvent>, SerializationSchema<TupleEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(TupleEventSchema.class);

    private StreamSerDe serde;

    public TupleEventSchema(StreamSerDe serde) {
        this.serde = serde;
    }

    @Override
    public TupleEvent deserialize(byte[] bytes) throws IOException {
        try {
            List<Object[]> values=serde.deSerialize(new String(bytes,"UTF-8"));

            if(values!=null) {
                Object[] value=values.get(0); //序列化结果只考虑只有一组记录的情况
                //TODO streamName获取
                TupleEvent event=new TupleEvent(serde.getSchema().getEventTypeName(),serde.getSchema(),value);

                return event;
            }
        } catch (StreamSerDeException e) {
            LOG.warn("error to deserialize:"+new String(bytes,"UTF-8"),e);
            throw new IOException(e.getMessage(),e.getCause());
        }
        return null;
    }

    @Override
    public boolean isEndOfStream(TupleEvent tupleEvent) {
        return false;
    }

    @Override
    public TypeInformation<TupleEvent> getProducedType() {
        return TypeInformation.of(TupleEvent.class);
    }

    @Override
    public byte[] serialize(TupleEvent event) {
        List<Object[]> values=Lists.newArrayList();
        values.add(event.getAllValues());

        try {
            Object obj=serde.serialize(values);
            return obj.toString().getBytes(Charset.forName("UTF-8"));

        } catch (StreamSerDeException e) {
            LOG.warn("error in serialize:"+event,e);
        }
        return new byte[0];
    }
}
