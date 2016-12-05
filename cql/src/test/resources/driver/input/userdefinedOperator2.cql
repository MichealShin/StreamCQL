--自定义算子的两个算子测试

set "operator.kafka.brokers"="192.168.0.2:9092";
set "serde.simpleserde.separator" = ",";

create input stream S
(id int ,name String)
	 SERDE SimpleSerDe
	 SOURCE 'com.huawei.streaming.cql.executor.userdefined.WebSpout'
		 PROPERTIES ("port" = "7999","fixlength"="966");

	create output stream rs
		(id int,name string)
	 SERDE SimpleSerDe
	 SINK 'com.huawei.streaming.cql.executor.userdefined.WebFilterOutputStream';

	insert into stream rs select * from S;
	explain application simple;