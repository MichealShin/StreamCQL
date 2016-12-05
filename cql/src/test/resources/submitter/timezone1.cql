set "streaming.operator.timezone" = "GMT+01:00";

create input stream S1 (c1   timestamp)
     SOURCE KafkaInput
         PROPERTIES ("operator.kafka.groupid" = "zhou5", "operator.kafka.topic" = "input_timestamp_keyvalue");

create output stream S2 (c1  timestamp)
     SERDE keyValueSerde
         PROPERTIES ( "separator" = ",", "timezoneForTimestamp" = "GMT+02:00" )
    SINK KafkaOutput
        PROPERTIES ("operator.kafka.topic" = "timestamp_keyvalue" );

create input stream S3 (c1 timestamp)
     SERDE keyValueSerde
         PROPERTIES ( "separator" = "," )
     SOURCE KafkaInput
         PROPERTIES ("operator.kafka.groupid" = "zhou","operator.kafka.topic"  = "timestamp_keyvalue");

create output stream S4  (c1 timestamp)
    SINK KafkaOutput
        PROPERTIES ("operator.kafka.topic" = "output_timestamp_keyvalue");

insert into stream S2 select * from S1;
insert into stream S4 select * from S3;
submit application   115_timestamp_ky;
