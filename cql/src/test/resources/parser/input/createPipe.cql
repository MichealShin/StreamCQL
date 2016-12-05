--测试完整语法
CREATE STREAM S (id INT,name STRING);

--测试大小写
CREATE STREAM S (id INT,name String);

--注释测试
CREATE STREAM S (id INT comment 'user id',name String comment 'user name') 
COMMENT 'this is stream comment';