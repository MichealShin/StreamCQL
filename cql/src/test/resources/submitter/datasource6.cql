set "datasource.rdb.driver" = "org.postgresql.Driver";
set "datasource.rdb.url" = "jdbc:postgresql://192.168.0.2:21600/omm";
set "datasource.rdb.username" = "omm";
set "datasource.rdb.password" = "omm";

create input stream S 
(id int,name String)
	SOURCE KafkaInput
	PROPERTIES (
        "operator.kafka.groupid" = "gidkpi_1_1",
       "operator.kafka.topic" = "0912",
        "operator.kafka.zookeepers" = "192.168.0.2:2181"
        );
                     

CREATE DATASOURCE ds1
 SOURCE RDBDataSource;	
               
create output stream rS1
(type String, id String, tag String)
	SINK consoleOutput;

insert into rS1 select "role", rdb.id, rdb.name from S,
DATASOURCE ds1
			[
			SCHEMA (id int,name String),
			QUERY("select roleid,rolename from OMM_1.tbl_iam_role where roleid = ? ",s.id)
			] rdb;
            

submit application datasourcetest;