create input stream S1 (ci int,cl long,cf float,cd double,cde decimal )
     SOURCE KafkaInput
         PROPERTIES ("operator.kafka.groupid" = "zhou123", "operator.kafka.topic" = "demo_sin", "operator.kafka.zookeepers" = "192.168.0.2:24002");
                 
create output stream S2  (cl1 long,cf1 float,cd1 double,cde1 decimal)
    SINK KafkaOutput
        PROPERTIES ("operator.kafka.topic" = "demo_sout", "operator.kafka.zookeepers" = "192.168.0.2:24002");

insert into stream S2 select S1.ci*S1.cl,S1.ci*S1.cl*S1.cf,S1.ci*S1.cl*S1.cf*S1.cd,S1.ci*S1.cl*S1.cf*S1.cd*S1.cde from S1;
submit application  STRV1R2_CQL_BASIC_FUNC_0050_0023;