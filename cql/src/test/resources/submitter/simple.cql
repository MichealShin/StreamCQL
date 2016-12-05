set "operator.kafka.brokers"="192.168.0.2:9092";
set "serde.simpleserde.separator" = ",";
create input stream S 
(id int ,name String )
	 SERDE SimpleSerDe
	 SOURCE 'com.huawei.streaming.operator.inputstream.HeadStreamSourceOp'
		 PROPERTIES ("operator.headstream.timeunit" = "SECONDS",
						"operator.headstream.period"="1",
						"operator.headstream.eventnumperperiod"="1000",
						"operator.headstream.isschedule"="true",
						"operator.headstream.totalnumber"="10000",
						"operator.headstream.delaytime"="3");
							
	create output stream rs 
		(id string,name string)
	 SERDE 'com.huawei.streaming.serde.KeyValueSerDe'
 SINK 'com.huawei.streaming.operator.outputstream.ConsolePrintOp';

	insert into stream rs select * from S where id > 5;	
	submit application simple;