CREATE INPUT STREAM HISPACE
	(
	 HISPACE_APP_ID  	STRING,
	 LOGON_ID  STRING,
	 OPER_TIME  TimeStamp,
	 OPER_TYPE  STRING,
	 BIZ_CHANNEL_ID  STRING,
	 NET_SYSTEM_ID  STRING,
	 IP_ADDRESS  STRING,
	 REQSOURCE  STRING,
	 SUBSOURCE  STRING,
	 LISTID  STRING,
	 POSITION  STRING,
	 EXPAND  STRING
	)
	 SERDE simpleSerde
		PROPERTIES ( "separator" = "|" )
	 SOURCE  kafkaInput
		PROPERTIES (
			"operator.kafka.groupid" = "group1",
		"operator.kafka.topic" = "ods",
			"operator.kafka.zookeepers" = "192.168.0.2:2181"
			);

	CREATE OUTPUT STREAM OPEN_COUNT
	(
	opencount  	long
	)
	 SINK ConsoleOutput
		  PROPERTIES("isPrintCount" = "true", "printFrequence" = "10") ;

	INSERT INTO timetrans
	select LOGON_ID,  CurrentTimeMillis() as tp, OPER_TYPE from HISPACE  WHERE OPER_TYPE ='0';

	INSERT INTO OPEN_COUNT
	SELECT COUNT(*) FROM timetrans[RANGE 1 SECONDS BATCH TRIGGER BY tp ];

 explain application test;
