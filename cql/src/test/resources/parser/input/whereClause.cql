--条件表达式测试
a is null;
a <>1;
a >=1;
a<1;
a+b=0;
(a+b) >1.0F;
(a-b) =0;
a==null;
--udf函数测试
udf(a+b) is null;
udf(a+b)>5;
udf(a+b);


a.id in ('1','2','3');
id + 100 between (150 + -50) AND (150 + 50);
(key + 100) between (150 + -50) AND (150 + 50);
a.id like '%xx%';
a.id like 'xx%';
a.id like 'xx?';