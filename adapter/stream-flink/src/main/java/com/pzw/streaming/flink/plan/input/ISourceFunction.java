package com.pzw.streaming.flink.plan.input;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.serde.StreamSerDe;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/25 下午8:44
 */
public interface ISourceFunction  {

    void setSerDe(StreamSerDe serde);

    void setConfig(StreamingConfig config) throws StreamingException;

    SourceFunction<TupleEvent> getSourceFunction();
}
