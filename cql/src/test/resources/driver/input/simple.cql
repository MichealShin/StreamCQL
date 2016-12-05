set "operator.kafka.brokers"="192.168.0.2:9092";

create input stream S 
(id int ,name String )
	 SERDE SimpleSerDe
		 PROPERTIES ("serde.simpleserde.separator" = ",")
	 SOURCE 'com.huawei.streaming.operator.inputstream.KafkaSourceOp'
		 PROPERTIES ("operator.kafka.groupid" = "gidkpi_1_1","operator.kafka.topic"="simple_1",
						"operator.kafka.zookeepers"="localhost:2181,192.168.0.2:2181",
						"operator.kafka.zksessiontimeout"="20000","operator.kafka.zksynctime"="20000",
							"operator.kafka.messageserializerclass"="kafka.serializer.StringEncoder");
							
	create output stream rs 
		(id string,name string)
	 SERDE SimpleSerDe
		 PROPERTIES ("serde.simpleserde.separator" = ",")
	 SINK 'com.huawei.streaming.operator.outputstream.KafkaFunctionOp'
		 PROPERTIES ("operator.kafka.topic"="simple_2","operator.kafka.zksessiontimeout"="20000",	"operator.kafka.zookeepers"="192.168.0.2:2181",
						"operator.kafka.zksynctime"="20000","operator.kafka.messageserializerclass"="kafka.serializer.StringEncoder");
							
	insert into stream rs select * from S where id > 5;	
	explain application simple;