--测试可选部分
CREATE INPUT STREAM S (id INT,name STRING) 
SERDE 'com.huawei.streaming.sql.SerDe.CSVSerDe'
SOURCE 'com.huawei.streaming.spout.csvReader' PROPERTIES ("path" = "/local");

--测试完整语法
CREATE INPUT STREAM S (id INT,name STRING) 
SERDE 'com.huawei.streaming.sql.SerDe.CSVSerDe'  properties ("ip" = "localhost")
SOURCE 'com.huawei.streaming.spout.csvReader' PROPERTIES ("path" = "/local");

--测试大小写
CREATE INPUT STREAM S (id INT,name String) 
SERDE 'com.huawei.streaming.sql.SerDe.CSVSerDe'  pRoperties ("ip" = "localhost")
SOURCE 'com.huawei.streaming.spout.csvReader' PROPERTIES ("path" = "/local");

--测试精简写法
CREATE INPUT STREAM S (id INT,name String) 
SERDE 'com.huawei.streaming.sql.SerDe.CSVSerDe'
SOURCE 'com.huawei.streaming.spout.csvReader';

--注释测试
CREATE INPUT STREAM S (id INT comment 'user id',name String comment 'user name') 
COMMENT 'this is stream comment'
SERDE 'com.huawei.streaming.sql.SerDe.CSVSerDe'
SOURCE 'com.huawei.streaming.spout.csvReader';