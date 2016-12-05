create input stream S1 (c1 int,c2 int)
     SOURCE KafkaInput
         PROPERTIES ("operator.kafka.groupid" = "zhou123","operator.kafka.topic" = "Previous_input_0110_0001", "operator.kafka.zookeepers" = "192.168.0.2:2181");
                 
create output stream S2  (c1 String)
    SINK consoleOutput;
        
insert into stream S2 select c2 from S1 [rows 3 slide sort by S1.c2];
submit application force STRV1R2_CQL_WIN_FUNC_0110_0001;
