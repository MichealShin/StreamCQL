create input stream S1
  (
      a STRING,
      b int,
      c long,
      d float,
      e double,
      f boolean,
      g time,
      h date,
      i timestamp,
      j decimal
   )
   SERde keyvalueserde
 SOURCE KafkaInput
      PROPERTIES 
      ("operator.kafka.groupid" = "zhou123",
      "operator.kafka.topic" = "Previous_input_0110_0001",
      "operator.kafka.zookeepers" = "192.168.0.2:2181");



create output stream rs
	(
      a STRING,
      b int,
      c long,
      d float,
      e double,
      f boolean,
      g time,
      h date,
      i timestamp,
      j decimal
	)
    SERDE binarySERde
    properties("timeType" = "String",
    "attributesLength" = "1,4,8,4,8,1,8,10,23,6")
 SINK TCPClientOutput
      PROPERTIES(
 "server" = "192.168.0.2",
 "port" = "8081",
 "sessionTimeout"='2000' );

insert into stream rs select * from S1;
explain application send;