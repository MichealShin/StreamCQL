--测试可选部分
CREATE OUTPUT STREAM S (id INT,name STRING) 
SERDE 'com.huawei.streaming.sql.SerDe.CSVSerDe'
SINK 'com.huawei.streaming.output.csvWriter' PROPERTIES ("path" = "/local");

--测试完整语法
CREATE OUTPUT STREAM S (id INT,name STRING) 
SERDE 'com.huawei.streaming.sql.SerDe.CSVSerDe'  properties ("ip" = "localhost")
SINK 'com.huawei.streaming.output.csvWriter' PROPERTIES ("path" = "/local");

--测试大小写
CREATE OUTPUT STREAM S (id INT,name String) 
SERDE 'com.huawei.streaming.sql.SerDe.CSVSerDe'  pRoperties ("ip" = "localhost")
SINK 'com.huawei.streaming.output.csvWriter' PROPERTIES ("path" = "/local");

--测试精简写法
CREATE OUTPUT STREAM S (id INT,name String) 
SERDE 'com.huawei.streaming.sql.SerDe.CSVSerDe'
SINK 'com.huawei.streaming.output.csvWriter';

--注释测试
CREATE OUTPUT STREAM S (id INT comment 'user id',name String comment 'user name') 
COMMENT 'this is stream comment'
SERDE 'com.huawei.streaming.sql.SerDe.CSVSerDe'
SINK 'com.huawei.streaming.output.csvWriter';