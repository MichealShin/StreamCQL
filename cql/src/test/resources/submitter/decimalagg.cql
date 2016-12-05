
set 'operator.kafka.zookeepers' = '192.168.0.2:24002';
set "serde.simpleserde.separator"=',';
set "operator.kafka.brokers"="192.168.0.2:9092";

Create Input Stream Input_Case_When
	(id decimal)
	 Source  KafkaINput
		 Properties ("operator.kafka.groupid" = "Gidkpi_1_1","operator.kafka.topic"="Case_When_In");

Create Output Stream Output_Case_When
	(id decimal,suma decimal,sumf decimal,avga decimal,avgf decimal)
	 Sink consoleOutput;

Insert Into Stream Output_Case_When Select id,sum(id),sum(id,id > 10),avg(id),avg(id,id > 10) From Input_Case_When[range unbounded] group by id;
submit Application Previous1;
