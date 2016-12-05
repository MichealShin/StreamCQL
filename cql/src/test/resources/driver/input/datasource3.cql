--一个数据源，使用两次测试
SET "datasource.rdb.driver" = "org.postgresql.Driver";
set "operator.kafka.brokers" = "192.168.0.2:9092";
set "operator.kafka.zookeepers" = "localhost:2181,192.168.0.2:2181";
set "operator.kafka.zksessiontimeout" = "20000";
set "operator.kafka.zksynctime" = "20000";
set "operator.kafka.messageserializerclass" = "kafka.serializer.StringEncoder";
set "serde.simpleserde.separator" = ",";

create input stream S 
(id int ,name String,type int)
	 SERDE SimpleSerDe
	 SOURCE 'com.huawei.streaming.operator.inputstream.KafkaSourceOp'
		 PROPERTIES ("operator.kafka.groupid" = "gidkpi_1_1","operator.kafka.topic"="agg_1_1");

CREATE DATASOURCE rdbdatasource
 SOURCE 'com.huawei.streaming.datasource.RDBDataSource'
 PROPERTIES (
				"datasource.rdb.url" = "jdbc:postgresql://192.168.0.2:1521/streaming",
				"datasource.rdb.username"="root",
				"datasource.rdb.password"="root"
				);	

create output stream rS 
(rid int ,sid int,countrid int,sumsid int)
		 SERDE SimpleSerDe
	 SINK 'com.huawei.streaming.operator.outputstream.KafkaFunctionOp'
		 PROPERTIES ("operator.kafka.topic"="agg_1_2");
 
insert into ds1 select rdbdatasource.id,rdbdatasource.name,rdbdatasource.type from S,
DATASOURCE rdbdatasource
			[
			SCHEMA (id int,name String,type int),
			QUERY("select rid as id,rname,rtype from rdbtable where id = ''{0}'' and name like '%{1}%' ", s.id, s.name)
			] rdb;

insert into rs select rdb.id,s.id,count(rdb.id),sum(s.id) from ds1[rows 10 slide] as s,
DATASOURCE rdbdatasource
			[
			SCHEMA (id int,name String,type int),
			QUERY("select rid as id,rname,rtype from rdbtable where id = ''{0}'' ",ds1.id)
			] rdb 
where rdb.name like '%hdd%'
group by rdb.id,s.id;

explain application datasourcetest;