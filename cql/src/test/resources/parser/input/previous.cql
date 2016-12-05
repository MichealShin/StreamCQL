previous(1,id,name,type);
--previous(1,*);

--previous表达式暂时不支持多个列一起的别名设置。
--previous(1,id as aid,name as aname,type as atype);
--previous函数在select子句、where、having以及聚合表达式中都可以使用。
