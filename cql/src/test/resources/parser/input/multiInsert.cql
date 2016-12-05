from teststream
    insert into stream s1 select *
    insert into stream s2 select a
    insert into stream s3 select id,name  where id > 10;

from (select * from teststream)
    insert into stream s1 select *
    insert into stream s2 select a
    insert into stream s3 select id,name  where id > 10;


from (select * from teststream[range 1 seconds slide])
    insert into stream s1 select *
    insert into stream s2 select a
    insert into stream s3 select id,name  where id > 10;

from (select count(id),'sss' from teststream(id >5 )[range 1 seconds slide] group by ss)
    insert into stream s1 select *
    insert into stream s2 select a
    insert into stream s3 select id,name  where id > 10;
