--add jar "/opt/a.jar";

create input stream s (id int, name string)
SOURCE KafkaInput  
PROPERTIES ( "operator.kafka.groupid" = "gidkpi_1_1","operator.kafka.topic"="agg_1_1");

create output stream rs 
(id string, name string, type int) 
SINK KafkaOutput properties("operator.kafka.topic"="agg_2");

create operator userOp as "com.huawei.streaming.cql.executor.userdefined.UserOperatorTest" 
		input (id int, name string) 
		output (newID string, name string, type int) 
		properties ("userop.filename" = "/home/omm/kv.properties");


insert into rs using operator userOp from s distribute by id parallel 2;

submit application tt;