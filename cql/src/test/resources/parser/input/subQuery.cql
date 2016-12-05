select * from S1 order by id asc;
select * from S1[range 10 milliseconds batch] order by id asc;
select * from S1[range 10 milliseconds batch]  SS order by id asc;
select * from (select * from S1[range 10 milliseconds batch] )[range 10 milliseconds batch]  SS order by id asc;
select * from (select * from S1[range 10 milliseconds batch] )[range 10 milliseconds batch]  order by id asc;

select * from S1[range 10 milliseconds batch]  as a inner join S2[range unbounded] order by id asc;
select * from (select * from s1)[range 10 milliseconds batch]  as a inner join (select * from S2)[range unbounded] order by id asc;

select * from S1(s1.id>10)[range 10 milliseconds batch]  as a inner join S2[range unbounded] order by id asc;
select * from (select * from s1)(s1.id>10)[range 10 milliseconds batch]  as a inner join (select * from S2)[range unbounded] order by id asc;
