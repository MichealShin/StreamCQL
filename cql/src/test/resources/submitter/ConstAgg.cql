CREATE INPUT STREAM Stream_Gn_we_chat
(
 Msisdn   STRING,
 TMSI  STRING,
 IMSI   STRING,
 Lac   INT,
 CellID  INT,
 MscCode   INT,
 BscCode  INT,
 AudioSize    INT,
 UpdateTime   LONG,
 Result  STRING,
 NetType    STRING,
 InitTime   LONG 
)
     SERDE BinarySerDe
         PROPERTIES ("serde.binaryserde.attributeslength"="26,4,15,4,4,4,4,4,8,1,1,8")
     SOURCE 'com.huawei.streaming.cql.toolkits.operators.TCPServerInputOperator'
         PROPERTIES ("operator.tcpserver.port" = "7996","operator.tcpserver.fixedlength"="83");

CREATE OUTPUT STREAM Agg_Chat_min
(
 StreamName STRING,
 AudioSum   LONG,
 AudioAvg  DOUBLE
)
     SERDE SimpleSerDe
         PROPERTIES ("serde.simpleserde.separator" = ",")
     SINK 'com.huawei.streaming.operator.outputstream.ConsolePrintOp'
         PROPERTIES("operator.consoleprint.frequence"="1")  parallel 1;

  
------use case 4------------------
--Stream_Gn_we_chat事件流，连续统计每分钟，每5分钟，每15分钟，每半小时，每1小时微信语音消息的总流量，平均值
INSERT INTO Agg_Chat_min 
SELECT 'Agg_Chat_min' AS StreamName, sum(AudioSize) AS AudioSum, avg(AudioSize) AS AudioAvg FROM Stream_Gn_we_chat[RANGE 1 SECONDS BATCH];

submit application constAgg;
