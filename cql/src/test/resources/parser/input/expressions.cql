--最简单的模式测试
a;
--函数测试
udf(a);
udf(a,b);
udf(a+b);
udf(a-b);
udf(a*b);
udf(a/b);
udf('0');
udf(0);
--表达式测试
a+b;
c+(e*f);
c+(e+f);
c+(e-f);
c+(e/f);
c+(e%f);
(c+(e%f));
(c+(e%f));
-- case when测试
case when b>2 then 10 when b<=2 then 11 else 0 end; 
case when key<'90' then 2 else 4 end;
CASE a.key WHEN '27' THEN TRUE WHEN '38' THEN FALSE ELSE NULL END;
CASE WHEN (value LIKE 'aaa%' OR value LIKE 'vvv%') THEN 1 ELSE 0 END;

case when key<'90' then 2 when key = '90' then 3 when key > '90'  then 5 else 4 end;
case key when '1' then 1 when '2' then 2 when '3' then 3 else 99 end;

--like 测试

a like 'b';
a like 'b%';
a like '%b%';
a like '%b_';
a like 'a[1-9][1-9][1-9]';
a not like 'a[1-9][1-9][1-9]';

-- in 测试
a in ('1','2','3');
a not in ('1','2','3');
-- between
a between 1 and 1000;
a not between 1 and 1000;

udf(distinct a,b);
udf(distinct *);
--函数中 加入表达式测试
--函数中distinct测试
--函数中*号测试

--其他测试
udf(null);
udf();
udf(*);
udf(a is null);
udf(a == 1);
udf(a = 1);
udf(a <> 1);
udf(a != 1);
udf(a >= 1);
udf(a <= 1);
udf(toint(a <= 1));
udf(toint(a <= 1));
S1.a;

'1';
1;
1L;
1d;
1f;
true;
false;