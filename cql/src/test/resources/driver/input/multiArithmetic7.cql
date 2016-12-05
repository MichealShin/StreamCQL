create input stream S 
(num int)
	 SOURCE KafkaInput
		 PROPERTIES ("operator.kafka.groupid" = "gidkpi_1_1","operator.kafka.topic"="agg_1_1","operator.kafka.zookeepers"="192.168.0.2:2181");
							
	create output stream rs 
		(id int)
	 SINK ConsoleOutput;

--加减乘除混合运算
 insert into stream rs select num+1*num-2/num from S;	
	explain application simple;