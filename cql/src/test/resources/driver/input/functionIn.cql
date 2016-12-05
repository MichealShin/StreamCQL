set 'operator.kafka.zookeepers' = 'localhost:2181,192.168.0.2:2181';
set 'operator.kafka.zksessiontimeout'='20000';
set 'operator.kafka.zksynctime'='20000';
set 'operator.kafka.messageserializerclass'='kafka.serializer.StringEncoder';
set "serde.simpleserde.separator"=',';
set "operator.kafka.brokers"="192.168.0.2:9092";

create input stream input_where 
	(O_Id int ,OrderDate String ,OrderPrice long ,Customer String)
	 SERDE SimpleSerDe
	 SOURCE 'com.huawei.streaming.operator.inputstream.KafkaSourceOp'
		 PROPERTIES ("operator.kafka.groupid" = "gidkpi_1_1","operator.kafka.topic"="where_in");
							
create output stream output_where 
	(Customer String ,sum_orderprice long)
	 SERDE SimpleSerDe
	 SINK 'com.huawei.streaming.operator.outputstream.KafkaFunctionOp'
		 PROPERTIES ("operator.kafka.topic"="where_out");
insert into stream output_where select Customer,OrderPrice from input_where[range 10 seconds batch] where (OrderPrice=100 or OrderPrice<=1000) and Customer in ('Bush','Carter');
explain application tt;