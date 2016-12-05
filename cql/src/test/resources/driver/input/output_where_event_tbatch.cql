set 'operator.kafka.zookeepers' = 'localhost:2181,192.168.0.2:2181';
set 'operator.kafka.zksessiontimeout'='20000';
set 'operator.kafka.zksynctime'='20000';
set 'operator.kafka.messageserializerclass'='kafka.serializer.StringEncoder';
set "serde.simpleserde.separator"=',';
set "operator.kafka.brokers"="192.168.0.2:9092";
create input stream input_where_event_tbatch 
	(O_Id int ,OrderDate String ,OrderPrice long ,Customer String ,tstamp long)
	 SERDE SimpleSerDe
	 SOURCE 'com.huawei.streaming.operator.inputstream.KafkaSourceOp'
		 PROPERTIES ("operator.kafka.groupid" = "gidkpi_1_4","operator.kafka.topic"="where_event_tbatch_in");
							
create output stream output_where_event_tbatch 
	--(Customer String ,orderprice long)
	(sumprice long,avgprice long ,countprice long)
	 SERDE SimpleSerDe
	 SINK 'com.huawei.streaming.operator.outputstream.KafkaFunctionOp'
		 PROPERTIES ("operator.kafka.topic"="where_event_tbatch_out");
		
insert into stream output_where_event_tbatch select sum(OrderPrice),avg(OrderPrice),count(OrderPrice) from input_where_event_tbatch[range 10 seconds batch trigger by tstamp] where OrderPrice=100 or OrderPrice>=700;
explain application app_where_event_tbatch;