create input stream a
    (id int, name string)
source randomgen
    properties ( 'timeUnit' = "SECONDS", 'period' = "1",
        'eventNumPerPeriod' = "1000")
        ;

create output stream b
(id int,name string)
serde simpleSerde
   properties('separator' ="\t")
sink consoleOutput
;

insert into b
select * from a
;

submit application flink_random_input;
