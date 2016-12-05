-- add Files
add file 'hdfs:///home/user/test.jar';
add file '/home/user/test.jar';
-- add jar
add jar '/home/user/test.jar';
-- show 
show functions tostring;
show functions extended tostring;
show applications testapp;
-- load
load application '/opt/test.xml';
load application 'hdfs:///opt/test.xml';
-- explain
explain application testapp 'hdfs:///opt/test.xml';
explain application testapp '/opt/test.xml';
-- get/set
set "x" = "1";
set "bls" = "true";
set "x" = 'abc';
set "x"="1";
set "x"="1";
get "x";
-- create function
create function test as 'com.huawei.streaming.cql.udf.testudf';
-- drop function
drop function if exists test;
-- submit application
submit application force testapp;
submit application testapp;
-- drop application
drop application if exists testapp;
drop application testapp;

--deactive application
deactive application app1;
