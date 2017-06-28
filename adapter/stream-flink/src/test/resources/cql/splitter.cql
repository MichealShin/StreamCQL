create input stream itcp
   (id int,name string)
   serde simpleSerde
   properties ('separator' = "," )
   source tcpClientInput
   properties ('server'="localhost",'port' ="9000")
   ;

create output stream c1
   (tag int,id int,name string)
   serde simpleSerde
   properties ('separator' = "\t" )
   sink consoleOutput
   ;

create output stream c2
   (tag int,id int,name string)
   serde simpleSerde
   properties ('separator' = "\t" )
   sink consoleOutput
   ;

from (
select id,concat('-',name,'1','2') as nb from itcp
)
insert into c1 select 'c1',id,nb where id%2=0
insert into c2 select 'c2',id,nb where id%2=1
;

submit application flink_splitter;