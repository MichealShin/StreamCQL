CREATE INPUT STREAM Stream_Voice_call
(
    CallingNumber String,
    CalledNumber     String,
    InitTime long
)
SOURCE randomgen
    PROPERTIES ( "timeUnit" = "SECONDS", "period" = "1",
        "eventNumPerPeriod" = "1", "isSchedule" = "true" );


CREATE OUTPUT STREAM Filter_Voice1
 (callingnumber string)
SINK consoleOutput;

CREATE OUTPUT STREAM Filter_Voice2
 (callingnumber string,CalledNumer string)
SINK consoleOutput;


CREATE OUTPUT STREAM Filter_Voice3
 (callingnumber string,CalledNumer string,InitTime long)
SINK consoleOutput;


from (select * from stream_voice_call where callingnumber != "10086")
    insert into stream filter_voice1 select callingNumber
    Insert into stream Filter_voice2 select callingnumber,calledNumber
    insert into stream filter_voice3 select *;

 submit application tt;