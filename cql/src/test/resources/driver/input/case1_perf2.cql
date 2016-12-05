CREATE INPUT STREAM Stream_voice_call1
(
 CallingNumber   STRING,
 CalledNumber  STRING,
 ThirdNumber   STRING,
 CallingIMSI   STRING,
 CalledIMSI   STRING,
 ThirdIMSI   STRING,
 CallingIMEI   STRING,
 CallingHome   STRING,
 CalledHome   STRING,
 CallingLac   INT,
 CallingCellID  INT,
 CalledLac   INT,
 CalledCellID  INT,
 MscCode    INT,
 BscCode    INT,
 CallTime   LONG,
 ConnTime   LONG,
 CallingNetType  STRING,
 VipFlag    STRING,
 CalledNetType  STRING,
 InitTime   LONG
)
     SERDE BinarySerDe
         PROPERTIES ("serde.binaryserde.attributeslength"="26,26,26,15,15,15,17,6,6,4,4,4,4,4,4,8,8,1,1,1,8")
     SOURCE 'com.huawei.streaming.cql.toolkits.operators.TCPServerInputOperator'
         PROPERTIES ("operator.tcpserver.port" = "7999","operator.tcpserver.fixedlength"="203");
		
CREATE INPUT STREAM Stream_voice_call2
(
 CallingNumber   STRING,
 CalledNumber  STRING,
 ThirdNumber   STRING,
 CallingIMSI   STRING,
 CalledIMSI   STRING,
 ThirdIMSI   STRING,
 CallingIMEI   STRING,
 CallingHome   STRING,
 CalledHome   STRING,
 CallingLac   INT,
 CallingCellID  INT,
 CalledLac   INT,
 CalledCellID  INT,
 MscCode    INT,
 BscCode    INT,
 CallTime   LONG,
 ConnTime   LONG,
 CallingNetType  STRING,
 VipFlag    STRING,
 CalledNetType  STRING,
 InitTime   LONG
)
     SERDE BinarySerDe
         PROPERTIES ("serde.binaryserde.attributeslength"="26,26,26,15,15,15,17,6,6,4,4,4,4,4,4,8,8,1,1,1,8")
     SOURCE 'com.huawei.streaming.cql.toolkits.operators.TCPServerInputOperator'
         PROPERTIES ("operator.tcpserver.port" = "7998","operator.tcpserver.fixedlength"="203");

CREATE INPUT STREAM Stream_voice_call3
(
 CallingNumber   STRING,
 CalledNumber  STRING,
 ThirdNumber   STRING,
 CallingIMSI   STRING,
 CalledIMSI   STRING,
 ThirdIMSI   STRING,
 CallingIMEI   STRING,
 CallingHome   STRING,
 CalledHome   STRING,
 CallingLac   INT,
 CallingCellID  INT,
 CalledLac   INT,
 CalledCellID  INT,
 MscCode    INT,
 BscCode    INT,
 CallTime   LONG,
 ConnTime   LONG,
 CallingNetType  STRING,
 VipFlag    STRING,
 CalledNetType  STRING,
 InitTime   LONG
)
     SERDE BinarySerDe
         PROPERTIES ("serde.binaryserde.attributeslength"="26,26,26,15,15,15,17,6,6,4,4,4,4,4,4,8,8,1,1,1,8")
     SOURCE 'com.huawei.streaming.cql.toolkits.operators.TCPServerInputOperator'
         PROPERTIES ("operator.tcpserver.port" = "7997","operator.tcpserver.fixedlength"="203");

CREATE INPUT STREAM Stream_voice_call4
(
 CallingNumber   STRING,
 CalledNumber  STRING,
 ThirdNumber   STRING,
 CallingIMSI   STRING,
 CalledIMSI   STRING,
 ThirdIMSI   STRING,
 CallingIMEI   STRING,
 CallingHome   STRING,
 CalledHome   STRING,
 CallingLac   INT,
 CallingCellID  INT,
 CalledLac   INT,
 CalledCellID  INT,
 MscCode    INT,
 BscCode    INT,
 CallTime   LONG,
 ConnTime   LONG,
 CallingNetType  STRING,
 VipFlag    STRING,
 CalledNetType  STRING,
 InitTime   LONG
)
     SERDE BinarySerDe
         PROPERTIES ("serde.binaryserde.attributeslength"="26,26,26,15,15,15,17,6,6,4,4,4,4,4,4,8,8,1,1,1,8")
     SOURCE 'com.huawei.streaming.cql.toolkits.operators.TCPServerInputOperator'
         PROPERTIES ("operator.tcpserver.port" = "7996","operator.tcpserver.fixedlength"="203");
		
CREATE INPUT STREAM Stream_voice_call5
(
 CallingNumber   STRING,
 CalledNumber  STRING,
 ThirdNumber   STRING,
 CallingIMSI   STRING,
 CalledIMSI   STRING,
 ThirdIMSI   STRING,
 CallingIMEI   STRING,
 CallingHome   STRING,
 CalledHome   STRING,
 CallingLac   INT,
 CallingCellID  INT,
 CalledLac   INT,
 CalledCellID  INT,
 MscCode    INT,
 BscCode    INT,
 CallTime   LONG,
 ConnTime   LONG,
 CallingNetType  STRING,
 VipFlag    STRING,
 CalledNetType  STRING,
 InitTime   LONG
)
     SERDE BinarySerDe
         PROPERTIES ("serde.binaryserde.attributeslength"="26,26,26,15,15,15,17,6,6,4,4,4,4,4,4,8,8,1,1,1,8")
     SOURCE 'com.huawei.streaming.cql.toolkits.operators.TCPServerInputOperator'
         PROPERTIES ("operator.tcpserver.port" = "7995","operator.tcpserver.fixedlength"="203");

CREATE OUTPUT STREAM Filter_Voice
(
 CallingNumber   STRING,
 CalledNumber  STRING,
 ThirdNumber   STRING,
 CallingIMSI   STRING,
 CalledIMSI   STRING,
 ThirdIMSI   STRING,
 CallingIMEI   STRING,
 CallingHome   STRING,
 CalledHome   STRING,
 CallingLac   INT,
 CallingCellID  INT,
 CalledLac   INT,
 CalledCellID  INT,
 MscCode    INT,
 BscCode    INT,
 CallTime   LONG,
 ConnTime   LONG,
 CallingNetType  STRING,
 VipFlag    STRING,
 CalledNetType  STRING,
 InitTime   LONG
)
     SERDE SimpleSerDe
         PROPERTIES ("serde.simpleserde.separator" = ",")
     SINK 'com.huawei.streaming.operator.outputstream.ConsolePrintOp'
         PROPERTIES("operator.consoleprint.frequence"="1")  parallel 1;



------use case 1------------------
--过滤号码为10086的事件
INSERT INTO Filter_Voice SELECT * FROM Stream_voice_call1 where CalledNumber =='10086';
INSERT INTO Filter_Voice SELECT * FROM Stream_voice_call2 where CalledNumber =='10086';
INSERT INTO Filter_Voice SELECT * FROM Stream_voice_call3 where CalledNumber =='10086';
INSERT INTO Filter_Voice SELECT * FROM Stream_voice_call4 where CalledNumber =='10086';
INSERT INTO Filter_Voice SELECT * FROM Stream_voice_call5 where CalledNumber =='10086';

explain application  case1_filter;
