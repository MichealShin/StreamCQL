create input stream S1 
(id int, name String)
	 SOURCE kafkaInput
		 PROPERTIES (   "operator.kafka.groupid" = "gidkpi_1_1",
                       "operator.kafka.topic"="join_1",
						"operator.kafka.zookeepers"="192.168.0.2:2181"
                      );

create input stream S2 
(id int, type String)
	 SOURCE kafkaInput
         PROPERTIES (   "operator.kafka.groupid" = "gidkpi_1_1",
                       "operator.kafka.topic"="join_2",
						"operator.kafka.zookeepers"="192.168.0.2:2181"
                      );
							
create output stream rs 
	(id int, name String, id2 int, type2 String)
 SINK consoleOutput;
					
insert into stream rs select * from S1[rows 3 slide sort by id] right outer join S2[rows 3 batch] unidirection on s1.id=s2.id;
submit application force jointest;
