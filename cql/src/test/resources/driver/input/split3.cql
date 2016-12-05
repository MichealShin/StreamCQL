CREATE INPUT STREAM Stream_voice_call
(
	CallingNumber 		STRING,
	CalledNumber		STRING,
	InitTime			LONG
)
     SERDE BinarySerDe
         PROPERTIES ("serde.binaryserde.attributeslength"="26,26,26,15,15,15,17,6,6,4,4,4,4,4,4,8,8,1,1,1,8")
     SOURCE 'com.huawei.streaming.cql.toolkits.operators.TCPServerInputOperator'
         PROPERTIES ("operator.tcpserver.port" = "7999","operator.tcpserver.fixedlength"="203");

CREATE OUTPUT STREAM Filter_Voice1
(
	CallingNumber 		STRING
)
     SERDE SimpleSerDe
         PROPERTIES ("serde.simpleserde.separator" = ",")
     SINK 'com.huawei.streaming.operator.outputstream.ConsolePrintOp'
         PROPERTIES("operator.consoleprint.frequence"="1")  parallel 1;

CREATE OUTPUT STREAM Filter_Voice2
(
	CallingNumber 		STRING,
	CalledNumber		STRING
)
     SERDE SimpleSerDe
         PROPERTIES ("serde.simpleserde.separator" = ",")
     SINK 'com.huawei.streaming.operator.outputstream.ConsolePrintOp'
         PROPERTIES("operator.consoleprint.frequence"="1")  parallel 1;

CREATE OUTPUT STREAM Filter_Voice3
(
	CallingNumber 		STRING,
	CalledNumber		STRING,
	InitTime			LONG
)
     SERDE SimpleSerDe
         PROPERTIES ("serde.simpleserde.separator" = ",")
     SINK 'com.huawei.streaming.operator.outputstream.ConsolePrintOp'
         PROPERTIES("operator.consoleprint.frequence"="1")  parallel 1;

insert into tmpStr select count(CallingNumber) as CallingNumber,count(CallingNumber) as CalledNumber,count(CallingNumber) as InitTime from Stream_voice_call[range 10 seconds batch] group by CalledNumber;

FROM (select * from tmpStr where CallingNumber != "10086")
    INSERT INTO stream Filter_Voice1 SELECT CallingNumber
    INSERT INTO stream Filter_Voice2 SELECT CallingNumber,CalledNumber
    INSERT INTO stream Filter_Voice3 SELECT *;



explain application split;
