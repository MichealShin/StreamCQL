--2014.02.14 讨论结果
--数据源查询中不带窗口。不允许进行CQL的聚合运算以及having，sort等过滤条件

--全局参数定义
--允许定义全局参数，也可以在datasource内部定义私有参数，私有参数会覆盖全局参数
SET "datasource.rdb.driver" = "org.postgresql.Driver";

--定义数据源
--数据库用户名和密码暂时采用明文，后期统一对所有的用户名和密码进行加密处理
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
 SOURCE RDBDataSource
 PROPERTIES (
				"url" = "jdbc:postgresql://192.168.0.2:1521/streaming",
                 "userName" = "root",
                 "password" = "root"
				);	


create output stream rS 
(rid int ,sid int,countrid int,sumsid int)
		 SERDE SimpleSerDe
	 SINK 'com.huawei.streaming.operator.outputstream.KafkaFunctionOp'
		 PROPERTIES ("operator.kafka.topic"="agg_1_2");
				
--示例1：RDB数据读取，支持多行数据读取，同时支持CQL UDF以及窗口和聚合运算
--QUERY内部的参数顺序固定，不同的数据源，有不同的参数。
--RDB的SQL中，如果不包含Where，就会一次查询出多行记录。和原始流做了Join之后，最终输出多条结果。
insert into rs select rdb.id,s.id,count(rdb.id),sum(s.id) from S[rows 10 slide],
DATASOURCE rdbdatasource
			[
			SCHEMA (id int,name String,type int),
			QUERY("select rid as id,rname,rtype from rdbtable where id = ''{0}'' ",s.id)
			] rdb 
where rdb.name like '%hdd%'
group by rdb.id,s.id;

submit application datasourcetest;