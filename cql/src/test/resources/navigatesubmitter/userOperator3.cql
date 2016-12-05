create input stream s (name string, id int)
SOURCE KafkaInput  
PROPERTIES ( "operator.kafka.groupid" = "gidkpi_1_1","operator.kafka.topic"="agg_1_1");

create output stream rs 
(newName string, type1 int, type2 float) 
SINK KafkaOutput properties("operator.kafka.topic"="agg_2");

--输入输出列类型不一致
create operator userOp as "com.huawei.streaming.cql.executor.userdefined.UserOperatorTest" 
		input (name string, mid int) 
		output (newName string, mtype1 int, type2 double) 
		properties ("client.port" = "8080");

insert into rs using operator userOp from s;

submit application tt;