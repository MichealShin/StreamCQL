# StreamCQL: Continuous Query Language on RealTime Computation System

------

Continuous Query Language (CQL) is a query language used for data streams. Compared with traditional SQL, CQL introduces the concept of window. Data is stored in memory so that in-memory computing can be quickly implemented. CQL query results are computing results at a specific moment of data streams.
CQL is a Storm-based SQL query language. It resolves the problem that original Storm APIs are complex and difficult to use and some basic functions are unavailable. CQL improves usability of the Storm component. 
During CQL syntax design, it is found that syntax of existing CEP products includes not only SQL statements but requires client code. This forces users to learn client APIs, which improves complexity and difficulty. 
The CQL design objective is to use SQL statements and certain commands to execute and release all tasks so that the tasks can be distributed by using SQL interfaces. In this way, client interfaces are unified. Users who are familiar with SQL statements can develop operational CQL programs only by learning certain special CQL syntax, such as syntax of window or stream definitions. This reduces difficulty. 

------
## Key Concepts Definitions

 - Stream: A set of (infinite) elements, each of which belongs to a same schema. Each element is related to logic time. That is, streams have the tuple and time attributes. Any elements can be expressed in the format of Element<tuple,Time>, in which tuple includes data structures and content, and Time is the logic time of data.
 - Window: A way for processing unbounded and streaming events. A window sets an event stream to a static view at a moment for various query operations on database tables. Two types of window are defined on a stream, that is, time-based and row-based windows. Both types of window support slide and tumble. 
 - Expression: A set of symbols and operators. The CQL parsing engine processes an expression to obtain single values. A simple expression can be a constant, variable, or function. Two or more simple expressions can be combined as a complex expression by using operators.

## Requirements

Storm 0.10.0/Storm 1.0.2/JStorm 2.0.0 : Required

Kafka_2.10 0.10.1.2 : Optional

## Building StreamCQL
StreamCQL is built using [Apache Maven](http://maven.apache.org/).

 1. Clone HuaweiBigData/StreamCQL from github.
```shell
$ git clone https://github.com/HuaweiBigData/StreamCQL
```

 2. Go to the root of source directory.
```shell
$ cd StreamCQL
```

 3. build project
```shell
$ mvn clean install
```
------
## Install StreamCQL
 1. Copy stream-cql-bianry-2.0.tar.gz from

    ${StreamCQL_Source_Dir}/cql-binary/target to Storm Client node.
    
 2. unCompresse stream-cql-bianry-2.0.tar.gz
```shell
$ tar xvf stream-cql-bianry-2.0.tar.gz
```

 3. Go to Stream CQL bin directory
```shell
$ cd stream-cql-bianry-2.0/bin
```

## StartUp Apache Storm 
Before testing StreamCQL you should deploy a storm on your machine 

First , startup Zookeeper instance :

```
wget http://mirror.bit.edu.cn/apache/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz

tar xvf zookeeper-3.4.6.tar.gz ；cd zookeeper-3.4.6

bin/zkServer.sh start

```

```
wget http://apache.fayea.com/storm/apache-storm-1.0.2/apache-storm-1.0.2.zip

unzip apache-storm-1.0.2.zip

cd apache-storm-1.0.2
```
StartUp Storm in Local Modle:

```
nohup bin/storm nimbus &

nohup bin/storm ui &

nohup bin/storm supervisor &
```
------

## Submit CQL application to Storm
 1. Open CQL Client Shell.
```shell
$ ./cql
```
 2. Execute CQL in CQL client shell.
this is a simple cql example.

```
CREATE INPUT STREAM s
    (id INT, name STRING, type INT)
SOURCE randomgen
    PROPERTIES ( "timeUnit" = "SECONDS", "period" = "1",
        "eventNumPerperiod" = "1", "isSchedule" = "true" );

CREATE OUTPUT STREAM rs
    (type INT, cc INT)
SINK consoleOutput;

INSERT INTO STREAM rs SELECT type, COUNT(id) as cc
    FROM s[RANGE 20 SECONDS BATCH]
    WHERE id > 5 GROUP BY type;

SUBMIT APPLICATION example;    
```

------

## Another CQL example with [Apache Kafka](http://kafka.apache.org/)

```sql
CREATE INPUT STREAM s1
    (name STRING)
SOURCE RANDOMGEN
    PROPERTIES ( "timeUnit" = "SECONDS", "period" = "1",
        "eventNumPerperiod" = "1", "isSchedule" = "true" );

CREATE OUTPUT STREAM s2 
    (c1 STRING)
SINK kafkaOutput
    PROPERTIES ( "topic" = "cqlOut", "zookeepers" = "127.0.0.1:2181",
        "brokers" = "127.0.0.1:9092" );

CREATE INPUT STREAM s3
    ( c1 STRING)
SOURCE KafkaInput
    PROPERTIES ("groupId" = "cqlClient", "topic" = "cqlOut",
        "zookeepers" = "127.0.0.1:2181", "brokers" = "127.0.0.1:9092" );

CREATE OUTPUT STREAM s4
    (c1 STRING)
SINK consoleOutput;

INSERT INTO STREAM s2 SELECT * FROM s1;
INSERT INTO STREAM s4 SELECT * FROM s3;

SUBMIT APPLICATION cql_kafka_example;
```
