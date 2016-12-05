set 'operator.kafka.zookeepers' = '192.168.0.2:2181';
set 'operator.kafka.zksessiontimeout'='20000';
set 'operator.kafka.zksynctime'='20000';
set 'operator.kafka.messageserializerclass'='kafka.serializer.StringEncoder';
set "serde.simpleserde.separator"=',';
set "operator.kafka.brokers"="192.168.0.2:9092";

create input stream s1 
	(Country String ,sex int ,Population long) 
	 SERDE SimpleSerDe
	 SOURCE 'com.huawei.streaming.operator.inputstream.HeadStreamSourceOp'
		 PROPERTIES ("operator.headstream.timeunit" = "SECONDS",
						"operator.headstream.period"="1",
						"operator.headstream.eventnumperperiod"="1000",
						"operator.headstream.isschedule"="true",
						"operator.headstream.totalnumber"="10000",
						"operator.headstream.delaytime"="3");

create input stream s2 
	(Country String ,sex int ,Population long) 
	 SERDE SimpleSerDe
	 SOURCE 'com.huawei.streaming.operator.inputstream.HeadStreamSourceOp'
		 PROPERTIES ("operator.headstream.timeunit" = "SECONDS",
						"operator.headstream.period"="1",
						"operator.headstream.eventnumperperiod"="1000",
						"operator.headstream.isschedule"="true",
						"operator.headstream.totalnumber"="10000",
						"operator.headstream.delaytime"="3");
		
create input stream s3 
	(Country String ,sex int ,Population long) 
	 SERDE SimpleSerDe
	 SOURCE 'com.huawei.streaming.operator.inputstream.HeadStreamSourceOp'
		 PROPERTIES ("operator.headstream.timeunit" = "SECONDS",
						"operator.headstream.period"="1",
						"operator.headstream.eventnumperperiod"="1000",
						"operator.headstream.isschedule"="true",
						"operator.headstream.totalnumber"="10000",
						"operator.headstream.delaytime"="3");
		
create output stream output_case_when  
	(Country String ,MaleP long ,FemaleP long) 
	 SERDE SimpleSerDe
	 SINK 'com.huawei.streaming.operator.outputstream.KafkaFunctionOp'
		 PROPERTIES ("operator.kafka.topic"="case_when_out");
			
insert into stream output_case_when select s1.country,s2.Population,s3.Population from s1,s2,s3 combine(s1.sex,s2.sex,s3.sex);
submit application combinetest;