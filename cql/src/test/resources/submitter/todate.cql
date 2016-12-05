create input stream S1 (c1 string)
     SOURCE KafkaInput
         PROPERTIES ("operator.kafka.groupid" = "zhou123","operator.kafka.topic" = "Previous_input_0070_0001", "operator.kafka.zookeepers" = "192.168.0.2:2181");
               
create output stream S2  (c string, c1 int,c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 string)
    SINK consoleoutput;
       
insert into stream S2 select from_unixtime(1,"yyyy-MM-dd HH:mm:ss") as co1,  
    Year(S1.c1) as co2, Month(S1.c1) as co3, Day(S1.c1) as co4, 
    DayofMonth(S1.c1) as co5, Hour(S1.c1) as co6,
    Minute(S1.c1) as co7, Second(S1.c1) as co8, 
    WeekofYear(S1.c1) as co9, ToDate(S1.c1,"yyyy-MM-dd HH:mm:ss") as co10 from S1;

submit application force tt1;