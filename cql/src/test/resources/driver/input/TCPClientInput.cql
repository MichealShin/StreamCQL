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
SERDE binarySERde
    properties("timeType" = "String","decimalType"="String",
    "attributesLength" = "1,4,8,4,8,1,8,10,23,6")
SOURCE TCPClientInput
 PROPERTIES (
	"server" = "192.168.0.2",
	"port" = "8081",
	"packageLength" = "73");


create output stream rs
	(
     a   STRING,
      b   int,
      c long,
      d float,
      e double,
      f boolean,
      g time,
      h date,
      i timestamp,
      j decimal
	)
SINK ConsoleOutput;

insert into stream rs select * from S1;
explain application read;
