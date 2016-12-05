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
			"operator.kafka.groupid" = "group2",
		"operator.kafka.topic" = "ods"
			);

	CREATE OUTPUT STREAM OPEN_COUNT
	(
	opencount  	long
	)
	 SINK ConsoleOutput
		  PROPERTIES("isPrintCount" = "true", "printFrequence" = "1") ;

	INSERT INTO timetrans
	select LOGON_ID as ID,  CurrentTimeMillis() as tp from HISPACE  WHERE OPER_TYPE in ('11', '12');

	INSERT INTO OPEN_COUNT
	--SELECT COUNT(Distinct ID) FROM timetrans[RANGE 1 SECONDS BATCH TRIGGER BY tp ];
	SELECT COUNT(Distinct concat(trim(ID), 'hispace')) FROM timetrans[RANGE 1 SECONDS BATCH TRIGGER BY tp ];
	explain application userCount;