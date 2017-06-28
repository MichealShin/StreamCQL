package com.pzw.streaming.flink.plan.input;

import com.huawei.streaming.config.KafkaConfig;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.serde.StreamSerDe;
import com.pzw.streaming.flink.plan.PlanEdge;
import com.pzw.streaming.flink.plan.common.TupleEventSchema;
import kafka.api.OffsetRequest;
import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StoppableStreamSource;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;

import java.util.List;
import java.util.Properties;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/16 下午10:28
 */
public class KafkaSource implements ISourceFunction {

    private Properties kafkaProperties;

    private String topic;

    private StreamSerDe serde;

    @Override
    public void setSerDe(StreamSerDe serde) {
        this.serde = serde;
    }

    @Override
    public void setConfig(StreamingConfig conf) throws StreamingException {
        kafkaProperties = new Properties();

        kafkaProperties.put(KafkaConfig.KAFKA_CON_ZK_CONNECT,
                conf.getStringValue(StreamingConfig.OPERATOR_KAFKA_ZOOKEEPERS));
        kafkaProperties.put(KafkaConfig.KAFKA_GROUP_ID,
                conf.getStringValue(StreamingConfig.OPERATOR_KAFKA_GROUPID));
        kafkaProperties.put(KafkaConfig.KAFKA_SERIAL_CLASS,
                conf.getStringValue(StreamingConfig.OPERATOR_KAFKA_MESSAGESERIALIZERCLASS));
        kafkaProperties.put(KafkaConfig.KAFKA_SESSION_TIME,
                conf.getStringValue(StreamingConfig.OPERATOR_KAFKA_ZKSESSIONTIMEOUT));
        kafkaProperties.put(KafkaConfig.KAFKA_SYNC_TIME,
                conf.getStringValue(StreamingConfig.OPERATOR_KAFKA_ZKSYNCTIME));

        if (conf.getBooleanValue(StreamingConfig.OPERATOR_KAFKA_READ_FROMBEGINNING)) {
            kafkaProperties.put(KafkaConfig.KAFKA_OFFSET_RESET, OffsetRequest.SmallestTimeString());
        } else {
            kafkaProperties.put(KafkaConfig.KAFKA_OFFSET_RESET, OffsetRequest.LargestTimeString());
        }

        topic = conf.getStringValue(StreamingConfig.OPERATOR_KAFKA_TOPIC);
    }

    @Override
    public SourceFunction<TupleEvent> getSourceFunction() {
        return new FlinkKafkaConsumer08(topic, new TupleEventSchema(serde), kafkaProperties);
    }


}
