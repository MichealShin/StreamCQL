Select * from transformEvent[rows 10 slide];
Select * from transformEvent[rows 10 slide partition by type];
Select * from transformEvent[range 1 day 1 hour 1 minute 1 second 1 millisecond slide partition by id];
Select * from transformEvent[range 1000 milliseconds batch sort by id];
Select * from transformEvent[range unbounded] as s1 inner join transformBak  s2 on S1.id=S2.id;
Select * from transformEvent[range 10 seconds  slide];
Select * from transformEvent[range 1 day 6 hour slide];
Select * from transformEvent[range 10 seconds  slide partition by id];
Select * from transformEvent[range 1000 milliseconds batch];
Select * from transformEvent[range 1000 milliseconds batch partition by id];
Select * from transformEvent[range 1000 milliseconds sort by dte];

insert into stream output_where_event_tbatch select sum(OrderPrice),avg(OrderPrice),count(OrderPrice) 
from input_where_event_tbatch[range 10 seconds  slide trigger by TS exclude now] where OrderPrice=100 or OrderPrice>=701;

insert into stream output_where_event_tbatch select sum(OrderPrice),avg(OrderPrice),count(OrderPrice) 
from input_where_event_tbatch[range 10 seconds  slide partition by id trigger by TS exclude now] where OrderPrice=100 or OrderPrice>=700;

insert into stream output_where_event_tbatch select sum(OrderPrice),avg(OrderPrice),count(OrderPrice) 
from input_where_event_tbatch[range 10 seconds  batch trigger by TS exclude now] where OrderPrice=100 or OrderPrice>=700;

insert into stream output_where_event_tbatch select sum(OrderPrice),avg(OrderPrice),count(OrderPrice) 
from input_where_event_tbatch[range 10 seconds  batch partition by type trigger by TS exclude now] where OrderPrice=100 or OrderPrice>=700;

insert into stream rs select id,name,count(id) 
from transformEvent[range today ts] where id > 5 group by type having id>10;

insert into stream rs select id,name,count(id) 
from transformEvent[range today ts partition by type] where id > 5 group by type having id>10;
