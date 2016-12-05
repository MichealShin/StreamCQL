create input stream S1 (column1 string)
   SOURCE KafkaInput
        PROPERTIES ("operator.kafka.groupid" = "zhou123", "operator.kafka.topic" = "STRV1R2_CQL_EXP_FUNC_006_0001r", "operator.kafka.zookeepers" = "192.168.0.2:2181");

create output stream S2 (column1 Boolean, column2 int ,column3 long ,column4 float ,column5 double ,column6 string ,column7 Timestamp,column8 Date,column9 time,column10 Decimal)
    SINK KafkaOutput
        PROPERTIES ("operator.kafka.topic" = "STRV1R2_CQL_EXP_FUNC_006_0001t", "operator.kafka.zookeepers" = "192.168.0.2:2181");

insert into stream S2 select CAST (S1.column1 as Boolean ), CAST (S1.column1 as int ), CAST (S1.column1 as long), CAST (S1.column1 as float), CAST (S1.column1 as Double), CAST (S1.column1 as string), CAST (S1.column1 as Timestamp  ), CAST ( S1.column1 as Date), CAST ( S1.column1 as Time ), CAST ( S1.column1 as Decimal) from S1;

submit application force cast2;

