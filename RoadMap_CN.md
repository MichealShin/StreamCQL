# StreamCQL RoadMap

------

# 2.0

1. 支持Storm1.0.2，Kafka 0.10.1.0
2. 客户端日志统一使用log4j2,和Storm保持一致
3. 支持in、like、case-when、cast、between表达式
4. 支持多时区，timestamp类型输入支持时区，输出默认包含时区，时区设置以客户端参数为准，默认为客户端所在时区
5. 支持dateadd，datediff，datesub，upper，lower，currenttimemillis等函数
6. 支持事件驱动窗口（包含事件驱动滑动窗，事件驱动跳动窗，事件驱动分组滑动窗，事件驱动分组跳动窗，时间排序窗），以及自然天窗口和分组自然天窗口
7. 支持流拆分和流合并，以及支持窗口索引函数(previous)
8. 支持数据源算子，可以在流处理中访问数据库。
9. 修改所有输入和输入算子，序列化反序列化参数，参数名称和值只支持字符串类型，并且区分大小写。

注意：
1. 第9点需要注意，和上个版本有少许差异。基本上参数名称加上双引号或者单引号即可。
2. 如果要使用老版本storm，可以使用cql之前版本的storm adapter重新编译，或者自己修改包应用重新编译，这个主要是由于storm新版本大幅度修改了客户端包路径导致的。


# 1.1 --未发布
## 特性列表
> * 分组窗支持，支持在窗口中，按照指定字段进行分组。功能包含分组长度滑动窗，分组长度跳动窗，分组时间滑动窗，分组时间跳动窗。
> * 支持Active, Deactive、rebalance功能，功能作用同Storm，rebanance只支持worker数量设置。
> * 支持in、like、case、between表达式
> * 添加RDBDatasource数据源，支持在算子中，通过JDBC从关系型数据库中读取数据。
> * [#13][1] 添加对jstorm的适配支持(待定)

## 改进列表
> * [#15][2] 解决CQL中不能执行local模式的问题

## 问题解决
> * [#19][3] Please add setting JAVA_HOME before launching cql to documentation


[1]: https://github.com/HuaweiBigData/StreamCQL/issues/13
[2]: https://github.com/HuaweiBigData/StreamCQL/issues/15
[3]: https://github.com/HuaweiBigData/StreamCQL/issues/19
