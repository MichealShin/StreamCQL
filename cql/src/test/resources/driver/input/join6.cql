set "operator.kafka.brokers"="192.168.0.2:9092";

create input stream S1 
(id int ,name String,type int)
	 SERDE SimpleSerDe
		 PROPERTIES ("serde.simpleserde.separator" = ",")
	 SOURCE 'com.huawei.streaming.operator.inputstream.KafkaSourceOp'
		 PROPERTIES ("operator.kafka.groupid" = "gidkpi_1_1","operator.kafka.topic"="join_1",
						"operator.kafka.zookeepers"="192.168.0.2:2181",
						"operator.kafka.zksessiontimeout"="20000","operator.kafka.zksynctime"="20000",
							"operator.kafka.messageserializerclass"="kafka.serializer.StringEncoder");

create input stream S2 
(id int ,name String,type int)
	 SERDE SimpleSerDe
		 PROPERTIES ("serde.simpleserde.separator" = ",")
	 SOURCE 'com.huawei.streaming.operator.inputstream.KafkaSourceOp'
		 PROPERTIES ("operator.kafka.groupid" = "gidkpi_1_1","operator.kafka.topic"="join_2",
						"operator.kafka.zookeepers"="192.168.0.2:2181",
						"operator.kafka.zksessiontimeout"="20000","operator.kafka.zksynctime"="20000",
							"operator.kafka.messageserializerclass"="kafka.serializer.StringEncoder");
							
	create output stream rs 
		(id int ,name String,type int,id2 int ,name2 String,type2 int)
	 SERDE SimpleSerDe
		 PROPERTIES ("serde.simpleserde.separator" = ",")
	 SINK 'com.huawei.streaming.operator.outputstream.KafkaFunctionOp'
		 PROPERTIES ("operator.kafka.topic"="join_3","operator.kafka.zksessiontimeout"="20000",	"operator.kafka.zookeepers"="192.168.0.2:2181",
						"operator.kafka.zksynctime"="20000","operator.kafka.messageserializerclass"="kafka.serializer.StringEncoder");
						
	-- coross join 转换测试
	insert into stream rs select * from S1[range 20 seconds batch] join S2[range unbounded] where s1.id > 5;	
	explain application jointest;