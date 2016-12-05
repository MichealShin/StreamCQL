set 'operator.kafka.zookeepers' = 'localhost:2181,192.168.0.2:2181';
set 'operator.kafka.zksessiontimeout'='20000';
set 'operator.kafka.zksynctime'='20000';
set 'operator.kafka.messageserializerclass'='kafka.serializer.StringEncoder';
set "serde.simpleserde.separator"=',';
set "operator.kafka.brokers"="192.168.0.2:9092";

create input stream s1 
	(Country String ,sex int ,Population long) 
	 SERDE SimpleSerDe
	 SOURCE 'com.huawei.streaming.operator.inputstream.KafkaSourceOp'
		 PROPERTIES ("operator.kafka.groupid" = "gidkpi_1_1","operator.kafka.topic"="case_when_in");

create input stream s2 
	(Country String ,sex int ,Population long) 
	 SERDE SimpleSerDe
	 SOURCE 'com.huawei.streaming.operator.inputstream.KafkaSourceOp'
		 PROPERTIES ("operator.kafka.groupid" = "gidkpi_1_1","operator.kafka.topic"="case_when_in");
		
create input stream s3 
	(Country String ,sex int ,Population long) 
	 SERDE SimpleSerDe
	 SOURCE 'com.huawei.streaming.operator.inputstream.KafkaSourceOp'
		 PROPERTIES ("operator.kafka.groupid" = "gidkpi_1_1","operator.kafka.topic"="case_when_in");
		
create output stream output_case_when  
	(Country String ,MaleP long ,FemaleP long) 
	 SERDE SimpleSerDe
	 SINK 'com.huawei.streaming.operator.outputstream.KafkaFunctionOp'
		 PROPERTIES ("operator.kafka.topic"="case_when_out");
			
insert into stream output_case_when select s4.country,s2.Population,s3.Population from s1 as s4,s2,s3 combine(s4.sex,s2.sex,s3.sex);
explain application combinetest;