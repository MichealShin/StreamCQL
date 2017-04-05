# StreamCQL RoadMap

------
# StreamCQL 2.0
1. support Storm1.0.2，Kafka 0.10.1.0
2. used log4j2 in client log.
3. support in、like、case-when、cast、between expression.
4. multi timezone support.
5. support dateadd，datediff，datesub，upper，lower，currenttimemillis functions.
6. support event driven window.(EventSlideWindow, EventBatchWindow, GroupEventSlideWindow, GroupEventBatchWindow, NaturalWindow,GroupNaturalWindow.)
7. support stream split and merge. and support window index funciton previous.
8. support datasoruce operator, DBMS can be used in CQL.
9. modify all configs , key and value must be string type now.

# StreamCQL 1.1
## New Feature
> * support group window. support group rows slide window, group rows batch window, group time slide window, group time batch window. window.
> * support Active, Deactive、rebalance in StormAdapter.
> * support in、like、case、between expression
> * add RDBDatasource，read data  from data base using JDBC.
> * [#13][1] support jstorm(uncertain)

## Improvement
> * [#15][2] support storm local model in StreamCQL.

## BUG
> * [#19][3] Please add setting JAVA_HOME before launching cql to documentation


[1]: https://github.com/HuaweiBigData/StreamCQL/issues/13
[2]: https://github.com/HuaweiBigData/StreamCQL/issues/15
[3]: https://github.com/HuaweiBigData/StreamCQL/issues/19
