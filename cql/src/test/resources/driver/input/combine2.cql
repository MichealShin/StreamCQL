set "operator.kafka.messageserializerclass"="kafka.serializer.StringEncoder";
set "operator.kafka.zookeepers"="192.168.0.2:2181";
set "operator.kafka.brokers"="192.168.0.2:9092";
set "operator.kafka.zksessiontimeout"="20000";
set "operator.kafka.zksynctime"="20000";
set "serde.simpleserde.separator" = ",";

--输入流定义
create input stream transaction 
  (  
   SEQ_NO LONG,
   SM_USER_ID STRING,
   DD_APDATE LONG,
   SD_SEQ_NUM STRING,
   SD_KEY STRING,
   SD_RETL_ID STRING,
   SD_TERM_ID STRING,
   MGROUP_ID           	STRING,
   SD_TERM_NAME_LOC     STRING,
   SD_ACQ_INST_ID_NUM   STRING,
   SD_TERM_CNTRY        STRING,
   SD_TERM_ST           STRING,
   SD_TERM_CITY         STRING,
   SD_RETL_SIC_CDE      STRING,
   MD_TRAN_AMT1         DOUBLE,
   MD_TRAN_AMT3         DOUBLE,
   SD_ORIG_CRNCY_CDE    STRING,
   MD_TRAN_AMT2         DOUBLE,
   DD_TRAN_DAT_TIM      LONG,
   DD_TRAN_DAT          STRING,
   DD_TRAN_TIM          STRING,
   SD_PIN_IND           STRING,
   SD_PIN_TRIES         STRING,
   SD_TRAN_TYP          STRING,
   SD_TRAN_CDE          STRING,
   SD_RESP_CDE          STRING,
   SD_MSG_TYP           STRING,
   SD_FILLER_FIELD      STRING,
   SD_AUTH_DAT          STRING,
   SD_AUTH_TIM          STRING,
   SD_PT_SRV_ENTRY_MDE  STRING,
   SD_TRAN_MEDIA        STRING,
   SD_APPRV_CDE         STRING,
   SD_PT_SRV_COND_CDE   STRING,
   TERM_ENTRY_CAPA      STRING,
   TERM_VERIF_RSUT      STRING,
   TERM_CAPABIT         STRING,
   CADHLD_VERIF_METH_RSUT STRING,
   TRAN_CHANNEL_TYPE    STRING,
   ISSU_SCRIPT_RSUT     STRING,
   CHIP_COND_CDE        STRING,
   SM_USER_MOB_PHN      STRING,
   SM_USER_MOB_PHN1     STRING,
   SM_USER_PRI_GOVT_ID  STRING,
   SM_USER_POSTAL_CDE   STRING,
   SM_EXP_DAT           STRING,
   SM_CRD_BLK_CDE       STRING,
   MM_OPEN_TO_BUY       DOUBLE,
   SCORE                INT,
   OBJECT_NO            STRING,
   OBJECT_LEVEL         STRING,
   IS_ICE               STRING,
   IS_WATCH             STRING,
   IS_MARK             	STRING,
   FRAUD_TYPE           STRING,
   FRAUD_DATE           LONG,
   RULE_COUNT           INT,
   SD_IP_ADDR           STRING,
   SD_MAC_ADDR          STRING,
   SD_ACCEPT_OUTSID_VOID_CCK STRING,
   SD_AUTH_SRC          STRING,
   SD_CUP_COUN          INT,
   SD_CVD_PRESENT_FLG   STRING,
   E_CASH_BALANCE       DOUBLE,
   SD_E_COM_FLG         STRING,
   INTERACT_FLAG        STRING,
   TRAN_LAUNCH_WAY      STRING,
   SD_ACCT_NUM          STRING
   )
	 SERDE SimpleSerDe
	 SOURCE 'com.huawei.streaming.operator.inputstream.KafkaSourceOp'
	 PROPERTIES ("operator.kafka.groupid" = "trans_1","operator.kafka.topic"="trans");

--输出流定义	
create output stream realresult 
	(
	SEQ_NO LONG,
	DD_TRAN_TIM STRING,
	SD_KEY STRING,
	REAL_CARD_M20_scont LONG,
	REAL_CARD_M20_samt DOUBLE,
	--REAL_CARD_M20_fcont LONG,
	--REAL_CARD_M20_famt DOUBLE,
	REAL_CARD_M20_cont LONG,
    REAL_CARD_M20_amt  DOUBLE,
	REAL_CARD_M20_avgamt DOUBLE,
	REAL_CARD_M20_maxamt DOUBLE,
	REAL_CARD_M20_minamt DOUBLE
	)
	 SERDE SimpleSerDe
	 SINK 'com.huawei.streaming.operator.outputstream.KafkaFunctionOp'
	 PROPERTIES ("operator.kafka.topic"="real");

--卡片20分钟窗口变量定义	
create stream REAL_CARDM20
	(
	SEQ_NO               LONG,
	DD_TRAN_TIM STRING,
	SD_KEY           	STRING,
	REAL_CARD_M20_scont LONG,
	REAL_CARD_M20_samt DOUBLE,
	--REAL_CARD_M20_fcont LONG,
    --REAL_CARD_M20_famt DOUBLE,
    REAL_CARD_M20_cont LONG,
    REAL_CARD_M20_amt  DOUBLE,
    REAL_CARD_M20_avgamt DOUBLE,
    REAL_CARD_M20_maxamt DOUBLE,
    REAL_CARD_M20_minamt DOUBLE
 );
 
--卡片20分钟窗口计算 
insert into stream REAL_CARDM20
 select SEQ_NO, DD_TRAN_TIM, SD_KEY, 
 count(1,sd_resp_cde in('001','000') and  sd_filler_field in('000','0000')) as REAL_CARD_M20_scont, 
 (case when sum(md_tran_amt1,sd_resp_cde in('001','000') and  sd_filler_field in('000','0000')) is null then 2000 else sum(md_tran_amt1,sd_resp_cde in('001','000') and  sd_filler_field in('000','0000')) end ) as REAL_CARD_M20_samt,
 --count(1,(sd_resp_cde in('001','000') and  sd_filler_field not in('000','0000'))=false) as REAL_CARD_M20_fcont,
 --sum(md_tran_amt1, (sd_resp_cde in('001','000') and  sd_filler_field in('000','0000'))=false) as REAL_CARD_M20_famt,
 count(1,sd_tran_cde='10' and sd_pt_srv_entry_mde like '90%' and  sd_term_cntry='CN' and sd_tran_typ!='7' and sd_msg_typ='0210' and sd_resp_cde in('001','000') and sd_filler_field in('000','0000')) as REAL_CARD_M20_cont,
 sum(md_tran_amt1, sd_tran_cde='10' and sd_pt_srv_entry_mde like '90%' and  sd_term_cntry='CN' and sd_tran_typ!='7' and sd_msg_typ='0210' and sd_resp_cde in('001','000') and sd_filler_field in('000','0000')) as  REAL_CARD_M20_amt, 
	 (case when avg(md_tran_amt1,sd_resp_cde in('001','000') and  sd_filler_field in('000','0000')) is null then 2000 else avg(md_tran_amt1,sd_resp_cde in('001','000')  and  sd_filler_field in('000','0000')) end) as REAL_CARD_M20_avgamt,
	 max(md_tran_amt1,sd_resp_cde in('001','000') and  sd_filler_field in('000','0000')) as REAL_CARD_M20_maxamt,
	 min(md_tran_amt1,sd_resp_cde in('001','000') and  sd_filler_field in('000','0000')) as REAL_CARD_M20_minamt
	 from transaction[range 20 minutes partition by sd_key trigger by DD_TRAN_DAT_TIM] 
	 group by SD_KEY;
 
--汇聚输出流
insert into stream realresult 
 select transaction.SEQ_NO, transaction.DD_TRAN_TIM, transaction.SD_KEY, REAL_CARDM20.REAL_CARD_M20_scont, REAL_CARDM20.REAL_CARD_M20_samt, REAL_CARDM20.REAL_CARD_M20_cont, REAL_CARDM20.REAL_CARD_M20_amt, REAL_CARDM20.REAL_CARD_M20_avgamt, REAL_CARDM20.REAL_CARD_M20_maxamt  ,REAL_CARDM20.REAL_CARD_M20_minamt  
 from transaction, REAL_CARDM20 combine(transaction.SEQ_NO,REAL_CARDM20.SEQ_NO);

 --insert into stream realresult select * from REAL_CARDM20;
explain application prm;