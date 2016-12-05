set "streaming.storm.submit.islocal" = "true";
set "streaming.localtask.alivetime.ms" = "2000";
set "operator.kafka.brokers"="192.168.0.2:9092";
set "serde.simpleserde.separator" = ",";
create input stream S 
(id int ,name String,type int)
	 SERDE SimpleSerDe
	 SOURCE 'com.huawei.streaming.operator.inputstream.HeadStreamSourceOp'
		 PROPERTIES ("operator.headstream.timeunit" = "SECONDS",
						"operator.headstream.period"="1",
						"operator.headstream.eventnumperperiod"="1000",
						"operator.headstream.isschedule"="true",
						"operator.headstream.totalnumber"="10000",
						"operator.headstream.delaytime"="3");
							
create output stream rs 
	(id string,name string,cc int)
	 SERDE SimpleSerDe
 SINK 'com.huawei.streaming.operator.outputstream.ConsolePrintOp';
	
	--最一般的聚合查询。仅仅带了窗口							
insert into stream rs select id,name,count(id)as cc from S[range today type] where id > 5 group by type having count(id) > 10 order by cc desc;
explain application having1;