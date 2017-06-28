create input stream itcp
   (id int,name string)
   serde simpleSerde
   properties ('separator' = "," )
   source tcpClientInput
   properties ('server'="localhost",'port' ="9000")
   ;

create output stream console
   (id double)
   serde simpleSerde
   properties ('separator' = "\t" )
   sink consoleOutput
   ;

 insert into console
 select avg(todouble(id)) from itcp;

 submit application flink_no_window_no_key_groupby;