set 'operator.kafka.zookeepers' = 'localhost:2181,192.168.0.2:2181';
set 'operator.kafka.zksessiontimeout'='20000';
set 'operator.kafka.zksynctime'='20000';
set 'operator.kafka.messageserializerclass'='kafka.serializer.StringEncoder';
set "serde.simpleserde.separator"=',';
set "operator.kafka.brokers"="192.168.0.2:9092";

create input stream input_case_when
	(Country String ,sex int ,Population long)
	 SERDE SimpleSerDe
	 SOURCE 'com.huawei.streaming.operator.inputstream.KafkaSourceOp'
		 PROPERTIES ("operator.kafka.groupid" = "gidkpi_1_1","operator.kafka.topic"="case_when_in");

create output stream output_case_when
	(Country String ,MaleP long ,FemaleP long)
	 SERDE SimpleSerDe
	 SINK 'com.huawei.streaming.operator.outputstream.KafkaFunctionOp'
		 PROPERTIES ("operator.kafka.topic"="case_when_out");

insert into stream output_case_when select country,Population,previous(1,Population) as p2 from input_case_when[range today Population];
submit application previous1;