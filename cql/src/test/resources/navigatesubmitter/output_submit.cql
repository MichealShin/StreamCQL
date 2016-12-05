create input stream S
(id int ,name String,type int)
	 SOURCE kafkaoutput
		 PROPERTIES ("operator.kafka.groupid" = "gidkpi_1_1","operator.kafka.topic"="agg_1_1");

	create output stream rs
		(id string,name string,cc int)
	 SERDE SimpleSerDe
	 SINK kafkaoutput
		 PROPERTIES ( "operator.kafka.topic"="agg_1_2");
	--最一般的聚合查询。仅仅带了窗口
	insert into stream rs select id,name,count(id) from S[range 200 milliseconds batch] where id > 5 group by type;
	submit application simple;