create input stream S1 (c1 int, c2 long)
     SOURCE KafkaInput
         PROPERTIES ("operator.kafka.groupid" = "zhou123", "operator.kafka.topic" = "Previous_input_0110_0001", "operator.kafka.zookeepers" = "192.168.0.2:2181");
                 
create output stream S2  (c1 int)
    SINK consoleOutput;
        
insert into stream S2 select sum(S1.c1) from S1 [range 15 seconds  BATCH  TRIGGER BY S1.c2 ];
submit application force STRV1R2_CQL_WIN_FUNC_0110_0001;
