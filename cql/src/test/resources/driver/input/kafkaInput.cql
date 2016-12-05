
create input stream S1
  (
      C1   STRING,
      C2   STRING
   )
	 SOURCE kafkainput
		 PROPERTIES ( "operator.kafka.groupid" = "gidkpi_1_1","operator.kafka.topic"="agg_1_1",
						"operator.kafka.zookeepers"="192.168.0.2:2181");


create output stream rs
	(
    C1 STRING,
	C2  STRING
	)
SINK ConsoleOutput
      PROPERTIES("printFrequence" = "10");


insert into stream rs select * from S1;

explain application read;

