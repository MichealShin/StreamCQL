create input stream itcp
   (id int,name string)
   serde simpleSerde
   properties ('separator' = "," )
   source tcpClientInput
   properties ('server'="localhost",'port' ="9000")
   ;


create output stream console
   (id int,name string)
   serde simpleSerde
   properties ('separator' = "\t" )
   sink consoleOutput
   ;

 insert into console
 select id,concat('%',name) from itcp where id%2 = 1
 ;

 submit application flink_filter;