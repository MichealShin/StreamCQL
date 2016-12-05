create input stream S1 (c1 string)
     SOURCE KafkaInput
         PROPERTIES ("operator.kafka.groupid" = "zhou123","operator.kafka.topic" = "Previous_input_0070_0001", "operator.kafka.zookeepers" = "192.168.0.2:2181");
create output stream S2  (c1 int,c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 string)
    SINK KafkaOutput
        PROPERTIES ("operator.kafka.topic" = "Previous_output_0070_0001", "operator.kafka.zookeepers" = "192.168.0.2:2181");
insert into stream S2 select Year(S1.c1), Month(S1.c1), Day(S1.c1), DayofMonth(S1.c1), Hour(S1.c1), Minute(S1.c1), Second(S1.c1), WeekofYear(S1.c1), ToDate(S1.c1) from S1;

explain application tt;