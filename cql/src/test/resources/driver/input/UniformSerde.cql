set 'operator.kafka.zookeepers' = 'localhost:2181,192.168.0.2:2181';
set 'operator.kafka.zksessiontimeout'='20000';
set 'operator.kafka.zksynctime'='20000';
set 'operator.kafka.messageserializerclass'='kafka.serializer.StringEncoder';
set "serde.simpleserde.separator"=',';
set "operator.kafka.brokers"="192.168.0.2:9092";


create input stream S 
(id int ,name String )
	 SERDE 'com.huawei.streaming.serde.KeyValueSerDe'
		 PROPERTIES ("serde.keyvalueserde.separator" = ",")
	 SOURCE 'com.huawei.streaming.operator.inputstream.KafkaSourceOp'
		 PROPERTIES ("operator.kafka.groupid" = "gidkpi_1_1","operator.kafka.topic"="simple_1");
							
	create output stream rs 
		(id string,name string)
	 SERDE 'com.huawei.streaming.serde.KeyValueSerDe'
		 PROPERTIES ("serde.keyvalueserde.separator" = ",")
	 SINK 'com.huawei.streaming.operator.outputstream.KafkaFunctionOp'
		 PROPERTIES ("operator.kafka.topic"="simple_2");
 

insert into rs select * from s;
explain application uniformSerde;