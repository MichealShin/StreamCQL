set 'operator.kafka.zookeepers' = 'localhost:2181,192.168.0.2:2181';
set 'operator.kafka.zksessiontimeout'='20000';
set 'operator.kafka.zksynctime'='20000';
set 'operator.kafka.messageserializerclass'='kafka.serializer.StringEncoder';
set "serde.simpleserde.separator"=',';
set "operator.kafka.brokers"="192.168.0.2:9092";

create input stream s 
(id int ,name String,type int)
	 SERDE SimpleSerDe
	 SOURCE 'com.huawei.streaming.operator.inputstream.KafkaSourceOp'
		 PROPERTIES ("operator.kafka.groupid" = "chineline_1_1","operator.kafka.topic"="join_1");

create output stream rs 
(id int,name String,type int) 
  SERDE SimpleSerDe
  SINK 'com.huawei.streaming.operator.outputstream.KafkaFunctionOp'
  PROPERTIES ("operator.kafka.topic"="join_3");

insert into stream rs select s.id,s.name,s2.type from S[rows 1 slide] join S[range unbounded] as s2 on s.id = s2.id where s.id > 5;
explain application tt;