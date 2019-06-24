-- DROP TABLE IF ALREADY EXISTS --
DROP TABLE IF EXISTS ORDERS_DETAILS;
DROP TABLE IF EXISTS staging_orders_details;

-- CREATE COLUMN TABLE ORDERS_DETAILS --
CREATE EXTERNAL TABLE staging_orders_details USING com.databricks.spark.csv
             OPTIONS (path ':dataLocation/ORDERS_DETAILS.dat', header 'true', inferSchema 'false',
              nullValue 'NULL', maxCharsPerColumn '4096');

CREATE TABLE ORDERS_DETAILS
             (SINGLE_ORDER_DID BIGINT ,SYS_ORDER_ID VARCHAR(64) ,SYS_ORDER_VER INTEGER ,DATA_SNDG_SYS_NM VARCHAR(128) ,
             SRC_SYS VARCHAR(20) ,SYS_PARENT_ORDER_ID VARCHAR(64) ,SYS_PARENT_ORDER_VER SMALLINT ,PARENT_ORDER_TRD_DATE VARCHAR(20),
             PARENT_ORDER_SYS_NM VARCHAR(128) ,SYS_ALT_ORDER_ID VARCHAR(64) ,TRD_DATE VARCHAR(20),GIVE_UP_BROKER VARCHAR(20) ,
             EVENT_RCV_TS TIMESTAMP ,SYS_ROOT_ORDER_ID VARCHAR(64) ,GLB_ROOT_ORDER_ID VARCHAR(64) ,GLB_ROOT_ORDER_SYS_NM VARCHAR(128) ,
             GLB_ROOT_ORDER_RCV_TS TIMESTAMP ,SYS_ORDER_STAT_CD VARCHAR(20) ,SYS_ORDER_STAT_DESC_TXT VARCHAR(120) ,DW_STAT_CD VARCHAR(20) ,
             EVENT_TS TIMESTAMP,ORDER_OWNER_FIRM_ID VARCHAR(20),RCVD_ORDER_ID VARCHAR(64) ,EVENT_INITIATOR_ID VARCHAR(64),
             TRDR_SYS_LOGON_ID VARCHAR(64),SOLICITED_FG  VARCHAR(1),RCVD_FROM_FIRMID_CD VARCHAR(20),RCV_DESK VARCHAR(20),
             SYS_ACCT_ID_SRC VARCHAR(64) ,CUST_ACCT_MNEMONIC VARCHAR(128),CUST_SLANG VARCHAR(20) ,SYS_ACCT_TYPE VARCHAR(20) ,
             CUST_EXCH_ACCT_ID VARCHAR(64) ,SYS_SECURITY_ALT_ID VARCHAR(64) ,TICKER_SYMBOL VARCHAR(32) ,TICKER_SYMBOL_SUFFIX VARCHAR(20) ,
             PRODUCT_CAT_CD VARCHAR(20) ,SIDE VARCHAR(20) ,LIMIT_PRICE DECIMAL(28, 8),STOP_PRICE DECIMAL(28, 8),ORDER_QTY DECIMAL(28, 4) ,
             TOTAL_EXECUTED_QTY DECIMAL(28, 4) ,AVG_PRICE DECIMAL(28, 8) ,DAY_EXECUTED_QTY DECIMAL(28, 4) ,DAY_AVG_PRICE DECIMAL(28, 8) ,
             REMNG_QTY DECIMAL(28, 4) ,CNCL_QTY DECIMAL(28, 4) ,CNCL_BY_FG  VARCHAR(1) ,EXPIRE_TS TIMESTAMP ,EXEC_INSTR VARCHAR(64) ,
             TIME_IN_FORCE VARCHAR(20) ,RULE80AF  VARCHAR(1) ,DEST_FIRMID_CD VARCHAR(20) ,SENT_TO_CONDUIT VARCHAR(20) ,SENT_TO_MPID VARCHAR(20) ,
             RCV_METHOD_CD VARCHAR(20) ,LIMIT_ORDER_DISP_IND  VARCHAR(1) ,MERGED_ORDER_FG  VARCHAR(1) ,MERGED_TO_ORDER_ID VARCHAR(64) ,
             RCV_DEPT_ID VARCHAR(20) ,ROUTE_METHOD_CD VARCHAR(20) ,LOCATE_ID VARCHAR(256) ,LOCATE_TS TIMESTAMP ,LOCATE_OVERRIDE_REASON VARCHAR(2000) ,
             LOCATE_BROKER VARCHAR(256) ,ORDER_BRCH_SEQ_TXT VARCHAR(20) ,IGNORE_CD VARCHAR(20) ,CLIENT_ORDER_REFID VARCHAR(64) ,
             CLIENT_ORDER_ORIG_REFID VARCHAR(64) ,ORDER_TYPE_CD VARCHAR(20) ,SENT_TO_ORDER_ID VARCHAR(64) ,ASK_PRICE DECIMAL(28, 8) ,
             ASK_QTY DECIMAL(28, 4) ,BID_PRICE DECIMAL(28, 10) ,BID_QTY DECIMAL(28, 4) ,REG_NMS_EXCEP_CD VARCHAR(20) ,REG_NMS_EXCEP_TXT VARCHAR(2000) ,
             REG_NMS_LINK_ID VARCHAR(64) ,REG_NMS_PRINTS  VARCHAR(1) ,REG_NMS_STOP_TIME TIMESTAMP ,SENT_TS TIMESTAMP ,RULE92  VARCHAR(1) ,
             RULE92_OVERRIDE_TXT VARCHAR(2000) ,RULE92_RATIO DECIMAL(25, 10) ,EXMPT_STGY_BEGIN_TIME TIMESTAMP ,EXMPT_STGY_END_TIME TIMESTAMP ,
             EXMPT_STGY_PRICE_INST VARCHAR(2000) ,EXMPT_STGY_QTY DECIMAL(28, 4) ,CAPACITY VARCHAR(20) ,DISCRETION_QTY DECIMAL(28, 4) ,
             DISCRETION_PRICE VARCHAR(64) ,BRCHID_CD VARCHAR(20) ,BASKET_ORDER_ID VARCHAR(64) ,PT_STRTGY_CD VARCHAR(20) ,
             SETL_DATE VARCHAR(20),SETL_TYPE VARCHAR(20) ,SETL_CURR_CD VARCHAR(20) ,SETL_INSTRS VARCHAR(2000) ,COMMENT_TXT VARCHAR(2000) ,
             CHANNEL_NM VARCHAR(128) ,FLOW_CAT VARCHAR(20) ,FLOW_CLASS VARCHAR(20) ,FLOW_TGT VARCHAR(20) ,ORDER_FLOW_ENTRY VARCHAR(20) ,
             ORDER_FLOW_CHANNEL VARCHAR(20) ,ORDER_FLOW_DESK VARCHAR(20) ,FLOW_SUB_CAT VARCHAR(20) ,STRTGY_CD VARCHAR(20) ,RCVD_FROM_VENDOR VARCHAR(20) ,
             RCVD_FROM_CONDUIT VARCHAR(20) ,SLS_PERSON_ID VARCHAR(64) ,SYNTHETIC_FG  VARCHAR(1) ,SYNTHETIC_TYPE VARCHAR(20) ,FXRT DECIMAL(25, 8) ,
             PARENT_CLREFID VARCHAR(64) ,REF_TIME_ID INTEGER ,OPT_CONTRACT_QTY DECIMAL(28, 4) ,OCEAN_PRODUCT_ID BIGINT ,CREATED_BY VARCHAR(64) ,
             CREATED_DATE TIMESTAMP ,FIRM_ACCT_ID BIGINT ,DEST VARCHAR(20) ,CNTRY_CD VARCHAR(20) ,DW_SINGLE_ORDER_CAT VARCHAR(20) ,CLIENT_ACCT_ID BIGINT ,
             EXTERNAL_TRDR_ID VARCHAR(64) ,ANONYMOUS_ORDER_FG  VARCHAR(1) ,SYS_SECURITY_ALT_SRC VARCHAR(20) ,CURR_CD VARCHAR(20) ,
             EVENT_TYPE_CD VARCHAR(20) ,SYS_CLIENT_ACCT_ID VARCHAR(64) ,SYS_FIRM_ACCT_ID VARCHAR(20) ,SYS_TRDR_ID VARCHAR(64) ,DEST_ID INTEGER ,
             OPT_PUT_OR_CALL VARCHAR(20) ,SRC_FEED_REF_CD VARCHAR(64) ,DIGEST_KEY VARCHAR(128) ,EFF_TS TIMESTAMP ,ENTRY_TS TIMESTAMP ,
             OPT_STRIKE_PRICE DECIMAL(28, 8) ,OPT_MATURITY_DATE VARCHAR(20) ,ORDER_RESTR VARCHAR(4) ,SHORT_SELL_EXEMPT_CD VARCHAR(4) ,
             QUOTE_TIME TIMESTAMP ,SLS_CREDIT VARCHAR(20) ,SYS_SECURITY_ID VARCHAR(64) ,SYS_SECURITY_ID_SRC VARCHAR(20) ,SYS_SRC_SYS_ID VARCHAR(20) ,
             SYS_ORDER_ID_UNIQUE_SUFFIX VARCHAR(20) ,DEST_ID_SRC VARCHAR(4) ,GLB_ROOT_SRC_SYS_ID VARCHAR(20) ,GLB_ROOT_ORDER_ID_SUFFIX VARCHAR(64) ,
             SYS_ROOT_ORDER_ID_SUFFIX VARCHAR(20) ,SYS_PARENT_ORDER_ID_SUFFIX VARCHAR(20) ,CREDIT_BREACH_PERCENT DECIMAL(25, 10) ,
             CREDIT_BREACH_OVERRIDE VARCHAR(256) ,INFO_BARRIER_ID VARCHAR(256) ,EXCH_PARTICIPANT_ID VARCHAR(64) ,REJECT_REASON_CD VARCHAR(4) ,
             DIRECTED_DEST VARCHAR(20) ,REG_NMS_LINK_TYPE VARCHAR(20) ,CONVER_RATIO DECIMAL(28, 9) ,STOCK_REF_PRICE DECIMAL(28, 8) ,
             CB_SWAP_ORDER_FG  VARCHAR(1) ,EV DECIMAL(28, 8) ,SYS_DATA_MODIFIED_TS TIMESTAMP ,CMSN_TYPE VARCHAR(20) ,SYS_CREDIT_TRDR_ID VARCHAR(20) ,
             SYS_ENTRY_USER_ID VARCHAR(20) ,OPEN_CLOSE_CD VARCHAR(20) ,AS_OF_TRD_FG  VARCHAR(1) ,HANDLING_INSTR VARCHAR(20) ,SECURITY_DESC VARCHAR(512) ,
             MINIMUM_QTY DECIMAL(21, 6) ,CUST_OR_FIRM VARCHAR(20) ,MAXIMUM_SHOW DECIMAL(21, 6) ,SECURITY_SUB_TYPE VARCHAR(20) ,MULTILEG_RPT_TYPE VARCHAR(4) ,
             ORDER_ACTION_TYPE VARCHAR(4) ,BARRIER_STYLE VARCHAR(4) ,AUTO_IOI_REF_TYPE VARCHAR(4) ,PEG_OFFSET_VAL DECIMAL(10, 2) ,AUTO_IOI_OFFSET DECIMAL(28, 10) ,
             IOI_PRICE DECIMAL(28, 10) ,TGT_PRICE DECIMAL(28, 10) ,IOI_QTY VARCHAR(64) ,IOI_ORDER_QTY DECIMAL(28, 4) ,CMSN VARCHAR(64) ,SYS_LEG_REF_ID VARCHAR(64) ,
             TRADING_TYPE VARCHAR(4) ,EXCH_ORDER_ID VARCHAR(64) ,DEAL_ID VARCHAR(64) ,ORDER_TRD_TYPE VARCHAR(4) ,CXL_REASON VARCHAR(64))
             USING column OPTIONS (partition_by 'SINGLE_ORDER_DID', redundancy ':redundancy');

INSERT INTO ORDERS_DETAILS SELECT * FROM staging_orders_details;

-- DROP TABLE IF ALREADY EXISTS --
DROP TABLE IF EXISTS EXEC_DETAILS;
DROP TABLE IF EXISTS staging_exec_details;

-- CREATE COLUMN TABLE EXEC_DETAILS --
CREATE EXTERNAL TABLE staging_exec_details USING com.databricks.spark.csv
             OPTIONS (path ':dataLocation/EXEC_DETAILS.dat', header 'true', inferSchema 'false', nullValue 'NULL', maxCharsPerColumn '4096');

CREATE TABLE EXEC_DETAILS(
             EXEC_DID BIGINT,SYS_EXEC_VER INTEGER,SYS_EXEC_ID VARCHAR(64),TRD_DATE VARCHAR(20),ALT_EXEC_ID VARCHAR(64),SYS_EXEC_STAT VARCHAR(20),
             DW_EXEC_STAT VARCHAR(20),ORDER_OWNER_FIRM_ID VARCHAR(20),TRDR_SYS_LOGON_ID VARCHAR(64),CONTRA_BROKER_MNEMONIC VARCHAR(20),SIDE VARCHAR(20),
             TICKER_SYMBOL VARCHAR(32),SYS_SECURITY_ALT_ID VARCHAR(64),PRODUCT_CAT_CD VARCHAR(20),LAST_MKT VARCHAR(20),EXECUTED_QTY DECIMAL(18, 4),
             EXEC_PRICE DECIMAL( 18, 8),EXEC_PRICE_CURR_CD VARCHAR(20),EXEC_CAPACITY VARCHAR(20),CLIENT_ACCT_ID BIGINT,FIRM_ACCT_ID BIGINT,
             AVG_PRICE_ACCT_ID BIGINT,OCEAN_ACCT_ID BIGINT,EXEC_CNTRY_CD VARCHAR(20),CMSN VARCHAR(20),COMMENT_TXT VARCHAR(2000),
             ACT_BRCH_SEQ_TXT VARCHAR(20),IGNORE_CD VARCHAR(20),SRC_SYS VARCHAR(20),EXEC_TYPE_CD VARCHAR(20),LIQUIDITY_CD VARCHAR(20),
             ASK_PRICE DECIMAL( 18, 8),ASK_QTY DECIMAL(18, 4),TRD_REPORT_ASOF_DATE VARCHAR(20),BID_PRICE DECIMAL( 18, 8),BID_QTY DECIMAL(18, 4),
             CROSS_ID VARCHAR(64),NYSE_SUBREPORT_TYPE VARCHAR(20),QUOTE_COORDINATOR VARCHAR(20),QUOTE_TIME TIMESTAMP,REG_NMS_EXCEPT_CD VARCHAR(20),
             REG_NMS_EXCEPT_TXT VARCHAR(2000),REG_NMS_LINK_ID VARCHAR(64),REG_NMS_MKT_CENTER_ID VARCHAR(64),REG_NMS_OVERRIDE VARCHAR(20),REG_NMS_PRINTS  VARCHAR(1),
             EXECUTED_BY VARCHAR(20),TICKER_SYMBOL_SUFFIX VARCHAR(20),PREREGNMS_TRD_MOD1  VARCHAR(1),PREREGNMS_TRD_MOD2  VARCHAR(1),PREREGNMS_TRD_MOD3  VARCHAR(1),
             PREREGNMS_TRD_MOD4  VARCHAR(1),NMS_FG  VARCHAR(1),GIVEUP_BROKER VARCHAR(20),CHANNEL_NM VARCHAR(128),ORDER_FLOW_ENTRY VARCHAR(20),FLOW_CAT VARCHAR(20),
             FLOW_CLASS VARCHAR(20),FLOW_TGT VARCHAR(20),ORDER_FLOW_CHANNEL VARCHAR(20),FLOW_SUBCAT VARCHAR(20),SYS_ACCT_ID_SRC VARCHAR(64),STRTGY_CD VARCHAR(20),
             EXECUTING_BROKER_CD VARCHAR(20),LEAF_EXEC_FG  VARCHAR(1),RCVD_EXEC_ID VARCHAR(64),RCVD_EXEC_VER INTEGER,ORDER_FLOW_DESK VARCHAR(20),
             SYS_ROOT_ORDER_ID VARCHAR(64),SYS_ROOT_ORDER_VER INTEGER,GLB_ROOT_ORDER_ID VARCHAR(64),TOTAL_EXECUTED_QTY DECIMAL(18, 4),AVG_PRICE DECIMAL( 18, 8),
             DEST_CD VARCHAR(20),CLIENT_ORDER_REFID VARCHAR(64),CLIENT_ORDER_ORIG_REFID VARCHAR(64),CROSS_EXEC_FG  VARCHAR(1),OCEAN_PRODUCT_ID BIGINT,
             TRDR_ID BIGINT,REF_TIME_ID INTEGER,CREATED_BY VARCHAR(64),CREATED_DATE TIMESTAMP,FIX_EXEC_ID VARCHAR(64),FIX_ORIGINAL_EXEC_ID VARCHAR(64),
             RELATED_MKT_CENTER VARCHAR(20),TRANS_TS TIMESTAMP,SYS_SECURITY_ALT_SRC VARCHAR(20),EVENT_TYPE_CD VARCHAR(20),SYS_CLIENT_ACCT_ID VARCHAR(64),
             SYS_FIRM_ACCT_ID VARCHAR(20),SYS_AVG_PRICE_ACCT_ID VARCHAR(20),SYS_TRDR_ID VARCHAR(64),ACT_BRCH_SEQ VARCHAR(20),SYS_ORDER_ID VARCHAR(64),
             SYS_ORDER_VER INTEGER,SRC_FEED_REF_CD VARCHAR(64),DIGEST_KEY VARCHAR(128),TRUE_LAST_MKT VARCHAR(20),ENTRY_TS TIMESTAMP,OPT_STRIKE_PRICE DECIMAL( 18, 8),
             OPT_MATURITY_DATE VARCHAR(20),EXPIRE_TS TIMESTAMP,OPT_PUT_OR_CALL VARCHAR(20),SYS_ORDER_STAT_CD VARCHAR(20),CONTRA_ACCT VARCHAR(64),CONTRA_ACCT_SRC VARCHAR(20),
             CONTRA_BROKER_SRC VARCHAR(20),SYS_SECURITY_ID VARCHAR(64),SYS_SECURITY_ID_SRC VARCHAR(20),SYS_SRC_SYS_ID VARCHAR(20),SYS_ORDER_ID_UNIQUE_SUFFIX VARCHAR(20),
             DEST VARCHAR(20),DEST_ID_SRC VARCHAR(4),CONVER_RATIO DECIMAL(18, 9),STOCK_REF_PRICE DECIMAL( 18, 8),AS_OF_TRD_FG  VARCHAR(1),MULTILEG_RPT_TYPE VARCHAR(4),
             REG_NMS_LINK_TYPE VARCHAR(20),EXEC_SUB_TYPE VARCHAR(4),CMSN_TYPE VARCHAR(20),QUOTE_CONDITION_IND VARCHAR(20),TRD_THROUGH_FG  VARCHAR(1),
             REGNMS_ORDER_LINK_ID VARCHAR(64),REGNMS_ORDER_LINK_TYPE VARCHAR(20),DK_IND VARCHAR(20),NBBO_QUOTE_TIME VARCHAR(20),GLB_ROOT_SRC_SYS_ID VARCHAR(20),
             TRD_REPORT_TYPE VARCHAR(20),REPORT_TO_EXCH_FG VARCHAR(1),CMPLN_COMMENT VARCHAR(256),DEAL_TYPE VARCHAR(4),EXEC_COMMENTS VARCHAR(256),
             OPTAL_FIELDS VARCHAR(120),SPOT_REF_PRICE VARCHAR(20),DELTA_OVERRIDE VARCHAR(20),UNDERLYING_PRICE VARCHAR(20),PRICE_DELTA VARCHAR(20),
             NORMALIZED_LIQUIDITY_IND VARCHAR(4),USER_AVG_PRICE VARCHAR(20),LAST_EXEC_TS TIMESTAMP,LULD_LOWER_PRICE_BAND VARCHAR(20),LULD_UPPER_PRICE_BAND VARCHAR(20),
             LULD_PRICE_BAND_TS TIMESTAMP,REMNG_QTY DECIMAL(18, 4),ORDER_QTY DECIMAL(18, 4),AMD_TS TIMESTAMP,SETL_CODE VARCHAR(50),SETL_DATE VARCHAR(20),
             CUST_NM VARCHAR(50),EXEC_TYPE VARCHAR(50),TRDR_KEY VARCHAR(50),TRDR_NM VARCHAR(50),FX_RATE VARCHAR(50),CUST_FX_RATE VARCHAR(50),
             PARENT_ORDER_SYS_NM VARCHAR(10),CNC_TYPE VARCHAR(50),FEE_AMT DECIMAL(20, 2),FEE_CCY VARCHAR(10),BRKG_AMT DECIMAL(20, 2),BRKG_CCY VARCHAR(10),
             CLEAR VARCHAR(50),PMT_FIX_DATE VARCHAR(20),FOLLOW_ON_FG  VARCHAR(1),FX_RATE_CCY_TO VARCHAR(10),FX_RATE_CCY_FROM VARCHAR(10),CUST_FX_RATE_CCY_TO VARCHAR(10),
             CUST_FX_RATE_CCY_FROM VARCHAR(10),SYS_GFCID VARCHAR(20),CONTRA_SIDE VARCHAR(20),OPT_CONTRACT_MULTIPLIER DECIMAL(10, 2),PRIOR_REF_PRICE_TS TIMESTAMP,
             SECURITY_SUB_TYPE VARCHAR(20),MSG_DIRECTION VARCHAR(20),LEAF_SYS_EXEC_ID VARCHAR(64),LEAF_SRC_SYS VARCHAR(20),FIX_LAST_MKT VARCHAR(20),
             FIX_CONTRA_BROKER_MNEMONIC VARCHAR(20),RIO_MSG_SRC VARCHAR(64),SNAPSHOT_TS TIMESTAMP,EXTERNAL_TRANS_TS TIMESTAMP,PRICE_CATEGORY VARCHAR(32),
             UNDERLYING_FX_RATE DECIMAL(18, 8),CONVERSION_RATE DECIMAL(18, 8),TRANS_COMMENT VARCHAR(256),AGGRESSOR_FLAG VARCHAR(1))
             USING column OPTIONS (partition_by 'EXEC_DID', redundancy ':redundancy');

INSERT INTO EXEC_DETAILS SELECT * FROM staging_exec_details;

