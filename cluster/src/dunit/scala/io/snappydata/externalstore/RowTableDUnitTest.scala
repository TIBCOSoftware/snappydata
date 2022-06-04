/*
 * Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package io.snappydata.externalstore

import io.snappydata.cluster.ClusterManagerTestBase

import org.apache.spark.sql.SaveMode

class RowTableDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  def testTableCreation(): Unit = {
    startSparkJob()
  }

  def testCreateInsertAndDropOfTable(): Unit = {
    startSparkJob2()
  }


  private val tableName: String = "RowTable"

  val props = Map.empty[String, String]

  def startSparkJob(): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new RowData(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable(tableName, "row", dataDF.schema, props)
    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect()
    assert(r.length == 0)

    snc.dropTable(tableName, ifExists = true)
    getLogWriter.info("Successful")
  }

  def startSparkJob2(): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new RowData(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable(tableName, "row", dataDF.schema, props)

    dataDF.write.format("row").mode(SaveMode.Append)
        .options(props).saveAsTable(tableName)

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect()
    assert(r.length == 5)

    snc.dropTable(tableName, ifExists = true)
    getLogWriter.info("Successful")
  }

  def testJobSNAP1224(): Unit = {


    val query: String = "select '5-CTFIX_ORDER' as SrcFl, * from ORDER_DETAILS " +
        "where trd_date>='20160413' and glb_root_order_id in " +
        "( select glb_root_order_id from ORDER_DETAILS where trd_date>='20160413' and src_sys='CRIO' ) " +
        "order by glb_root_order_id, trd_datE"
    val order_details_create_ddl =
      "create table order_details" +
          "(SINGLE_ORDER_DID BIGINT ,SYS_ORDER_ID VARCHAR(64) ,SYS_ORDER_VER INTEGER , " +
          "DATA_SNDG_SYS_NM VARCHAR(128) ,SRC_SYS VARCHAR(20) ,SYS_PARENT_ORDER_ID VARCHAR(64) ," +
          "SYS_PARENT_ORDER_VER SMALLINT ,PARENT_ORDER_TRD_DATE VARCHAR(20),PARENT_ORDER_SYS_NM  " +
          "VARCHAR(128) ,SYS_ALT_ORDER_ID VARCHAR(64) ,TRD_DATE VARCHAR(20),GIVE_UP_BROKER  " +
          "VARCHAR(20) ,EVENT_RCV_TS TIMESTAMP ,SYS_ROOT_ORDER_ID VARCHAR(64) ,GLB_ROOT_ORDER_ID " +
          " VARCHAR(64) ,GLB_ROOT_ORDER_SYS_NM VARCHAR(128) ,GLB_ROOT_ORDER_RCV_TS TIMESTAMP , " +
          "SYS_ORDER_STAT_CD VARCHAR(20) ,SYS_ORDER_STAT_DESC_TXT VARCHAR(120) ,DW_STAT_CD  " +
          "VARCHAR(20) ,EVENT_TS TIMESTAMP,ORDER_OWNER_FIRM_ID VARCHAR(20),RCVD_ORDER_ID VARCHAR" +
          "(64)  ,EVENT_INITIATOR_ID VARCHAR(64),TRDR_SYS_LOGON_ID VARCHAR(64),SOLICITED_FG " +
          "VARCHAR(1),RCVD_FROM_FIRMID_CD VARCHAR(20),RCV_DESK VARCHAR(20),SYS_ACCT_ID_SRC  " +
          "VARCHAR(64) ,CUST_ACCT_MNEMONIC VARCHAR(128),CUST_SLANG VARCHAR(20) ,SYS_ACCT_TYPE  " +
          "VARCHAR(20) ,CUST_EXCH_ACCT_ID VARCHAR(64) ,SYS_SECURITY_ALT_ID VARCHAR(64) ," +
          "TICKER_SYMBOL VARCHAR(32) ,TICKER_SYMBOL_SUFFIX VARCHAR(20) ,PRODUCT_CAT_CD VARCHAR" +
          "(20)  ,SIDE VARCHAR(20) ,LIMIT_PRICE DECIMAL(38, 18),STOP_PRICE DECIMAL(38, 18), " +
          "ORDER_QTY DECIMAL(18, 4) ,TOTAL_EXECUTED_QTY DECIMAL(18, 4) , AVG_PRICE DECIMAL(38, " +
          "18) ,DAY_EXECUTED_QTY DECIMAL(18, 4) ,DAY_AVG_PRICE DECIMAL(38, 18) ,REMNG_QTY DECIMAL(18, 4) ," +
          "CNCL_QTY DECIMAL(18, 4) ,CNCL_BY_FG  VARCHAR(1) ,EXPIRE_TS TIMESTAMP ,EXEC_INSTR VARCHAR(64) ,TIME_IN_FORCE VARCHAR(20) ," +
          "RULE80AF  VARCHAR(1) ,DEST_FIRMID_CD VARCHAR(20) ,SENT_TO_CONDUIT VARCHAR(20) ,SENT_TO_MPID VARCHAR(20) ," +
          "RCV_METHOD_CD VARCHAR(20) ,LIMIT_ORDER_DISP_IND  VARCHAR(1) ,MERGED_ORDER_FG  VARCHAR(1) ,MERGED_TO_ORDER_ID VARCHAR(64) ," +
          "RCV_DEPT_ID VARCHAR(20) ,ROUTE_METHOD_CD VARCHAR(20) ,LOCATE_ID VARCHAR(256) ,LOCATE_TS TIMESTAMP ,LOCATE_OVERRIDE_REASON VARCHAR(2000) ," +
          "LOCATE_BROKER VARCHAR(256) ,ORDER_BRCH_SEQ_TXT VARCHAR(20) ,IGNORE_CD VARCHAR(20) ,CLIENT_ORDER_REFID VARCHAR(64) ," +
          "CLIENT_ORDER_ORIG_REFID VARCHAR(64) ,ORDER_TYPE_CD VARCHAR(20) ,SENT_TO_ORDER_ID VARCHAR(64) ,ASK_PRICE DECIMAL(38, 18) ," +
          "ASK_QTY DECIMAL(18, 4) ,BID_PRICE DECIMAL(38, 18) ,BID_QTY DECIMAL(18, 4) ,REG_NMS_EXCEP_CD VARCHAR(20) ," +
          "REG_NMS_EXCEP_TXT VARCHAR(2000) ,REG_NMS_LINK_ID VARCHAR(64) ,REG_NMS_PRINTS  VARCHAR(1) ,REG_NMS_STOP_TIME TIMESTAMP ," +
          "SENT_TS TIMESTAMP ,RULE92  VARCHAR(1) ,RULE92_OVERRIDE_TXT VARCHAR(2000) ,RULE92_RATIO DECIMAL(25, 10) ," +
          "EXMPT_STGY_BEGIN_TIME TIMESTAMP ,EXMPT_STGY_END_TIME TIMESTAMP ,EXMPT_STGY_PRICE_INST VARCHAR(2000) ," +
          "EXMPT_STGY_QTY DECIMAL(18, 4) ,CAPACITY VARCHAR(20) ,DISCRETION_QTY DECIMAL(18, 4) ,DISCRETION_PRICE VARCHAR(64) ," +
          "BRCHID_CD VARCHAR(20) ,BASKET_ORDER_ID VARCHAR(64) ,PT_STRTGY_CD VARCHAR(20) ,SETL_DATE VARCHAR(20),SETL_TYPE VARCHAR(20) ," +
          "SETL_CURR_CD VARCHAR(20) ,SETL_INSTRS VARCHAR(2000) ,COMMENT_TXT VARCHAR(2000) ,CHANNEL_NM VARCHAR(128) ," +
          "FLOW_CAT VARCHAR(20) ,FLOW_CLASS VARCHAR(20) ,FLOW_TGT VARCHAR(20) ,ORDER_FLOW_ENTRY VARCHAR(20) ,ORDER_FLOW_CHANNEL VARCHAR(20) ," +
          "ORDER_FLOW_DESK VARCHAR(20) ,FLOW_SUB_CAT VARCHAR(20) ,STRTGY_CD VARCHAR(20) ,RCVD_FROM_VENDOR VARCHAR(20) ," +
          "RCVD_FROM_CONDUIT VARCHAR(20) ,SLS_PERSON_ID VARCHAR(64) ,SYNTHETIC_FG  VARCHAR(1) ,SYNTHETIC_TYPE VARCHAR(20) ," +
          "FXRT DECIMAL(25, 8) ,PARENT_CLREFID VARCHAR(64) ,REF_TIME_ID INTEGER ,OPT_CONTRACT_QTY DECIMAL(18, 4) ," +
          "OCEAN_PRODUCT_ID BIGINT ,CREATED_BY VARCHAR(64) ,CREATED_DATE TIMESTAMP ,FIRM_ACCT_ID BIGINT ,DEST VARCHAR(20) ," +
          "CNTRY_CD VARCHAR(20) ,DW_SINGLE_ORDER_CAT VARCHAR(20) ,CLIENT_ACCT_ID BIGINT ," +
          "EXTERNAL_TRDR_ID VARCHAR(64) ,ANONYMOUS_ORDER_FG  VARCHAR(1) ,SYS_SECURITY_ALT_SRC VARCHAR(20) ,CURR_CD VARCHAR(20) ," +
          "EVENT_TYPE_CD VARCHAR(20) ,SYS_CLIENT_ACCT_ID VARCHAR(64) ,SYS_FIRM_ACCT_ID VARCHAR(20) ,SYS_TRDR_ID VARCHAR(64) ," +
          "DEST_ID INTEGER ,OPT_PUT_OR_CALL VARCHAR(20) ,SRC_FEED_REF_CD VARCHAR(64) ,DIGEST_KEY VARCHAR(128) ,EFF_TS TIMESTAMP ," +
          "ENTRY_TS TIMESTAMP ,OPT_STRIKE_PRICE DECIMAL(38, 18) ,OPT_MATURITY_DATE VARCHAR(20) ,ORDER_RESTR VARCHAR(4) ," +
          "SHORT_SELL_EXEMPT_CD VARCHAR(4) ,QUOTE_TIME TIMESTAMP ,SLS_CREDIT VARCHAR(20) ,SYS_SECURITY_ID VARCHAR(64) ," +
          "SYS_SECURITY_ID_SRC VARCHAR(20) ,SYS_SRC_SYS_ID VARCHAR(20) ,SYS_ORDER_ID_UNIQUE_SUFFIX VARCHAR(20) ," +
          "DEST_ID_SRC VARCHAR(4) ,GLB_ROOT_SRC_SYS_ID VARCHAR(20) ,GLB_ROOT_ORDER_ID_SUFFIX VARCHAR(64) ,SYS_ROOT_ORDER_ID_SUFFIX VARCHAR(20) ," +
          "SYS_PARENT_ORDER_ID_SUFFIX VARCHAR(20) ,CREDIT_BREACH_PERCENT DECIMAL(25, 10) ,CREDIT_BREACH_OVERRIDE VARCHAR(256) ," +
          "INFO_BARRIER_ID VARCHAR(256) ,EXCH_PARTICIPANT_ID VARCHAR(64) ,REJECT_REASON_CD VARCHAR(4) ,DIRECTED_DEST VARCHAR(20) ," +
          "REG_NMS_LINK_TYPE VARCHAR(20) ,CONVER_RATIO DECIMAL(18, 9) ,STOCK_REF_PRICE DECIMAL(38, 18) ,CB_SWAP_ORDER_FG  VARCHAR(1) ," +
          "EV DECIMAL(38, 18) ,SYS_DATA_MODIFIED_TS TIMESTAMP ,CMSN_TYPE VARCHAR(20), " +
          "SYS_CREDIT_TRDR_ID VARCHAR(20) ,SYS_ENTRY_USER_ID VARCHAR(20) ,OPEN_CLOSE_CD VARCHAR" +
          "(20) ,AS_OF_TRD_FG  VARCHAR(1),HANDLING_INSTR VARCHAR(20),SECURITY_DESC VARCHAR(512) ," +
          "MINIMUM_QTY DECIMAL(21, 6) ,CUST_OR_FIRM VARCHAR(20) ,MAXIMUM_SHOW DECIMAL(21, 6) ,SECURITY_SUB_TYPE VARCHAR(20) ," +
          "MULTILEG_RPT_TYPE VARCHAR(4) ,ORDER_ACTION_TYPE VARCHAR(4) ,BARRIER_STYLE VARCHAR(4) ," +
          " AUTO_IOI_REF_TYPE VARCHAR(4) ,PEG_OFFSET_VAL DECIMAL(10, 2) , AUTO_IOI_OFFSET DECIMAL" +
          "(28, 12) ,IOI_PRICE DECIMAL(28, 12) ,TGT_PRICE DECIMAL(28, 12) ,IOI_QTY VARCHAR(64) , " +
          "IOI_ORDER_QTY DECIMAL(18, 4) ,CMSN VARCHAR(64) ,SYS_LEG_REF_ID VARCHAR(64) , " +
          "TRADING_TYPE VARCHAR(4) ,EXCH_ORDER_ID VARCHAR(64) ,DEAL_ID VARCHAR(64) , " +
          "ORDER_TRD_TYPE VARCHAR(4) ,CXL_REASON VARCHAR(64))"

    val snc = org.apache.spark.sql.SnappyContext(sc)
    snc.sql("drop table if exists order_details")
    snc.sql(order_details_create_ddl)
    // This test is only added for functional test hence no data and assertion is added
    snc.sql(query).show
    snc.sql("drop table order_details")

  }
}

case class RowData(col1: Int, col2: Int, col3: Int)
