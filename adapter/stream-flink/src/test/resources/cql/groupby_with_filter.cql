create input stream itcp
   (id int,name string)
   serde simpleSerde
   properties ('separator' = "," )
   source tcpClientInput
   properties ('server'="localhost",'port' ="9000")
   ;

create output stream console
   (id int,cnt int)
   serde simpleSerde
   properties ('separator' = "\t" )
   sink consoleOutput
   ;

 insert into console
 select id,count(1) from itcp[range 10 second batch] group by id having count(1)>=2
 ;

 submit application flink_groupby_with_filter;