package com.pzw.streaming.flink.plan.output;

import com.huawei.streaming.config.KafkaConfig;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.serde.StreamSerDe;
import com.pzw.streaming.flink.plan.PlanEdge;
import com.pzw.streaming.flink.plan.common.TupleEventSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;

import java.util.List;
import java.util.Properties;

/**
 * @author pengzhiwei
 * @version V1.0
 * @date 17/5/17 下午10:09
 */
public class KafkaSinkPlan implements ISinkFunction {

    private Properties kafkaProperties;

    private String topic;

    private StreamSerDe serde;

    @Override
    public void setConfig(StreamingConfig conf) throws StreamingException {
        kafkaProperties=new Properties();
        topic = conf.getStringValue(StreamingConfig.OPERATOR_KAFKA_TOPIC);

        kafkaProperties.put(KafkaConfig.KAFKA_PRO_ZK_CONNECT,
                conf.getStringValue(StreamingConfig.OPERATOR_KAFKA_ZOOKEEPERS));
        kafkaProperties.put(KafkaConfig.KAFKA_SERIAL_CLASS,
                conf.getStringValue(StreamingConfig.OPERATOR_KAFKA_MESSAGESERIALIZERCLASS));
        kafkaProperties.put(KafkaConfig.KAFKA_SESSION_TIME,
                conf.getStringValue(StreamingConfig.OPERATOR_KAFKA_ZKSESSIONTIMEOUT));
        kafkaProperties.put(KafkaConfig.KAFKA_SYNC_TIME, conf.getStringValue(StreamingConfig.OPERATOR_KAFKA_ZKSYNCTIME));
        kafkaProperties.put(KafkaConfig.KAFKA_BROKER_LIST, conf.getStringValue(StreamingConfig.OPERATOR_KAFKA_BROKERS));
    }


    @Override
    public void setSerDe(StreamSerDe serde) {
        this.serde=serde;
    }

    @Override
    public SinkFunction<TupleEvent> getSinkFunction() {
        return new FlinkKafkaProducer08(topic,new TupleEventSchema(serde),kafkaProperties);
    }

}
