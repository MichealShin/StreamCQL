--select
select * from teststream;
select a from teststream;
select id,name from testStream;
--join
select * from testStream S1 join S2;
select * from testStream as S1 join S2 on S1.id=S2.id;
select * from testStream as S1 inner join S2 on S1.id=S2.id;
select * from testStream as S1 cross join S2 on S1.id=S2.id;
select * from testStream as S1 natural join S2;
select * from testStream as S1 left outer join S2 on S1.id=S2.id;
select * from testStream as S1 right outer join S2 on S1.id=S2.id;
select * from testStream as S1 full outer join S2 on S1.id=S2.id;
--groupby
select count(*) from S1 group by id;
select count(*) from S1 group by id,type;
--having
select count(*) from S1 group by id having count(*) > 10;
select count(*)  a from S1 group by id having a > 10;
select count(*)  as a from S1 group by id having a > 10;
select count(*)  as a from S1 having a > 10;
--orderby
select * from S1 order by id;
select * from S1 order by id desc;
select * from S1 order by id asc;
--limit
select * from S1 limit 10;
--window
select * from S1[range unbounded];
select * from S[range 10 milliseconds batch];
select * from S1[range 10 milliseconds batch partition by a];
select * from S1[range 10 milliseconds sort by a];
--insert into
insert into stream S1 select * from S2;
--UDTF
select hbasescan("hb","rowkey","f1:c1")  as a from S1 having a > 10;
select hbasescan("hb","rowkey","f1:c1,f1:c2")  as (col1,col2) from S1 having col1 is not null;