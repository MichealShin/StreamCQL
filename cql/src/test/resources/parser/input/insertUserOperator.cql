insert into rs using operator userOp from s1 distribute by type parallel 2;
insert into rs using operator userOp from s1 distribute by type;
insert into rs using operator userOp from s1;