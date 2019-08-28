/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

package io.snappydata.hydra.ct

import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SQLContext, SnappyContext}

object CTQueries {

  var snc: SnappyContext = _

  val query1: String = "select count(*) from ORDERS_DETAILS"

  val query2: String = "select max(single_order_did) from ORDERS_DETAILS"

  val query3: String = "select min(single_order_did) from ORDERS_DETAILS"

  val query4: String = "select AVG(single_order_did) from ORDERS_DETAILS"

  val query5: String = "select avg(single_order_did),min(single_order_did),max(single_order_did) " +
      "from ORDERS_DETAILS"

  val query6: String = "select SRC_SYS,count(*) from ORDERS_DETAILS group by SRC_SYS"

  val query7: String = "select AVG(BID_PRICE),SRC_SYS from ORDERS_DETAILS GROUP BY SRC_SYS"

  val query8: String = "select SUM(TOTAL_EXECUTED_QTY),SRC_SYS from ORDERS_DETAILS GROUP BY SRC_SYS"

  val query9: String = "select SUM(TOTAL_EXECUTED_QTY),MIN(TOTAL_EXECUTED_QTY)," +
      "MAX(TOTAL_EXECUTED_QTY),SRC_SYS " +
      "from ORDERS_DETAILS WHERE SRC_SYS='APFF' GROUP BY SRC_SYS"

  val query10: String = "select count(*) from ORDERS_DETAILS where Src_sys='OATC'"

  val query11: String = "select '5-CTFIX_ORDER' as SrcFl, a.* " +
      "from ORDERS_DETAILS a , ORDERS_DETAILS b " +
      "where a.glb_root_order_id = b.glb_root_order_id " +
      "and a.trd_date >='20160413' and b.trd_date >='20160413' " +
      "and b.src_sys ='CRIO' order by a.glb_root_order_id, a.trd_datE"

  val query12: String = "select '4-CTFIX_ORDER' as SrcFl, a.glb_root_order_id, a.src_sys, " +
      "count(*) from ORDERS_DETAILS a , ORDERS_DETAILS b " +
      "where a.glb_root_order_id = b.glb_root_order_id and a.trd_date ='20160413' " +
      "and b.trd_date ='20160413' and b.src_sys ='CRIO' " +
      "group by a.glb_root_order_id, a.src_sys order by a.glb_root_order_id, a.src_sys"

  val query13: String = "select '3-CTFIX_ORDER' as SrcFl, * from ORDERS_DETAILS " +
      "where trd_date='20160413' and src_sys='CRIO'"

  val query14: String = "select '3-CTFIX_ORDER' as SrcFl, * from ORDERS_DETAILS " +
      "where trd_date='20160413' and src_sys='CRIO' order by trd_date"

  val query15: String = "select '5-CTFIX_ORDER' as SrcFl, * from ORDERS_DETAILS " +
      "where trd_date>='20160413' and glb_root_order_id in " +
      "( select glb_root_order_id from ORDERS_DETAILS where trd_date>='20160413' " +
      "and src_sys='CRIO' ) " +
      "order by glb_root_order_id, trd_datE"

  val query16: String = "select '4-CTFIX_ORDER' as SrcFl, glb_root_order_id, src_sys, count(*) " +
      "from ORDERS_DETAILS " +
      "where trd_date='20160413' and glb_root_order_id in " +
      "( select glb_root_order_id from ORDERS_DETAILS where trd_date='20160413' " +
      "and src_sys='CRIO') " +
      "group by glb_root_order_id, src_sys order by glb_root_order_id, src_sys"

  val query17: String = "select Event_type_cd, count(1) from ORDERS_DETAILS " +
      "where TRD_DATE between '20160401' and '20160431' group by Event_type_cd limit 1000"

  val query18: String = "SELECT event_type_cd, src_sys FROM ORDERS_DETAILS " +
      "WHERE TRD_DATE = '20160416' AND sys_order_stat_cd is NULL limit 1000"

  val query19: String = "SELECT ESOD.EXEC_INSTR, count(*) FROM ORDERS_DETAILS ESOD " +
      "WHERE ESOD.TRD_DATE = '20160413' AND ESOD.EVENT_TYPE_CD = 'NEW_CONF' " +
      "AND ESOD.EXEC_INSTR like '%A%' GROUP BY ESOD.EXEC_INSTR"

  val query20: String = "select EVENT_RCV_TS, EVENT_TS, src_sys, glb_root_src_sys_id, " +
      "glb_root_order_id, ticker_symbol,SIDE,order_qty,EVENT_TYPE_CD,product_cat_cd,cntry_cd " +
      "from ORDERS_DETAILS " +
      "where trd_date > '20160212' and src_sys='CAIQS' " +
      "and event_ts not like '%.%' order by EVENT_RCV_TS limit 100 "


  val query21: String = "select event_type_cd,event_rcv_ts,event_ts,sent_ts " +
      "from ORDERS_DETAILS " +
      "where trd_date='20160413' and glb_root_order_id='15344x8c7' " +
      "and sys_order_id='20151210.92597'"

  val query22: String = "select count(*) from EXEC_DETAILS a " +
      "LEFT JOIN ORDERS_DETAILS b using (sys_root_order_id)"

  val query23: String = "(select TRD_DATE, ROOT_FLOW_CAT, sum(Notional) as notional, " +
      "count(*) as trades, sum(shares) as shares " +
      "from " +
      "(select execs.sys_order_id, execs.EXECUTED_QTY * execs.EXEC_PRICE as notional, " +
      "execs.EXECUTED_QTY as shares, execs.TRD_DATE, " +
      "case when coalesce(root_exec.flow_cat,root.flow_cat) is null then 'UNKNOWN' else " +
      "coalesce(root_exec.flow_cat,root.flow_cat) end as ROOT_FLOW_CAT " +
      "from EXEC_DETAILS as execs left join " +
      "( select distinct TRD_DATE,glb_root_order_id,flow_cat " +
      "from EXEC_DETAILS where TRD_DATE in ('20160325','20160413' ) " +
      "and (PRODUCT_CAT_CD is null or PRODUCT_CAT_CD not in ('OPT','FUT','MLEG')) " +
      "and (exec_price_curr_cd = 'USD' OR exec_price_curr_cd is null) " +
      "and sys_src_sys_id in ('93', '7', '70', '115' ,'6','150','189','31','157','185'," +
      "'7','153','163133','80','51','139','137') " +
      "and sys_order_id = glb_root_order_id " +
      "and sys_src_sys_id = glb_root_src_sys_id )root_exec on " +
      "execs.trd_date=root_exec.trd_date and execs.glb_root_order_id=root_exec.glb_root_order_id " +
      "left join " +
      "(select distinct TRD_DATE, glb_root_order_id,flow_cat " +
      "from ORDERS_DETAILS T " +
      "where T.sys_order_id = T.glb_root_order_id " +
      "and T.sys_src_sys_id = T.glb_root_src_sys_id " +
      "and T.sys_src_sys_id in ('93', '7', '70', '115' ,'6','150','189','31','157','185','7'," +
      "'153','163133','80','51','139','137') " +
      "and T.TRD_DATE in ('20160325','20160413' ) " +
      "and (T.CURR_CD = 'USD' or T.CURR_CD is null) " +
      "and (T.PRODUCT_CAT_CD is null or T.PRODUCT_CAT_CD not in ('OPT', 'FUT','MLEG')) ) root on " +
      "execs.trd_date=root.trd_date and execs.glb_root_order_id=root.glb_root_order_id " +
      "where execs.LEAF_EXEC_FG = 'Y' " +
      "and execs.event_type_cd = 'FILLED_CONF' " +
      "and execs.sys_src_sys_id in ('93', '7', '70', '115' ,'6','150','189','31','157','185','7'," +
      "'153','163133','80','51','139','137') " +
      "and execs.SYS_ORDER_STAT_CD in ('2','1') " +
      "and execs.TRD_DATE in ('20160325','20160413' ) " +
      "and (execs.PRODUCT_CAT_CD is null or execs.PRODUCT_CAT_CD not in ('OPT', 'FUT','MLEG')) " +
      "and (execs.exec_price_curr_cd = 'USD' or execs.exec_price_curr_cd = null) ) " +
      "Aggregated group by TRD_DATE, ROOT_FLOW_CAT order by TRD_DATE ) " +
      "union all " +
      "(select TRD_DATE, ROOT_FLOW_CAT, sum(Notional) as notional, " +
      "count(*) as trades, sum (shares) as shares " +
      "from " +
      "(select execs.sys_order_id, execs.EXECUTED_QTY * execs.EXEC_PRICE as notional, " +
      "execs.EXECUTED_QTY as shares, " +
      "execs.TRD_DATE, 'ALL' as ROOT_FLOW_CAT " +
      "from EXEC_DETAILS as execs " +
      "left join ( select distinct TRD_DATE,glb_root_order_id,flow_cat " +
      "from EXEC_DETAILS where TRD_DATE in ('20160325','20160413' ) " +
      "and (PRODUCT_CAT_CD is null or PRODUCT_CAT_CD not in ('OPT','FUT','MLEG')) " +
      "and (exec_price_curr_cd = 'USD' OR exec_price_curr_cd is null) " +
      "and sys_src_sys_id in ('93', '7', '70', '115' ,'6','150','189','31','157','185','7'," +
      "'153','163133','80','51','139','137') " +
      "and sys_order_id = glb_root_order_id and sys_src_sys_id = glb_root_src_sys_id) " +
      "root_exec on " +
      "execs.trd_date=root_exec.trd_date and " +
      "execs.glb_root_order_id=root_exec.glb_root_order_id left join " +
      "( select distinct TRD_DATE, glb_root_order_id,flow_cat " +
      "from ORDERS_DETAILS T " +
      "where T.sys_order_id = T.glb_root_order_id " +
      "and T.sys_src_sys_id = T.glb_root_src_sys_id " +
      "and T.sys_src_sys_id in ('93', '7', '70', '115' ,'6','150','189','31','157','185','7'," +
      "'153','163133','80','51','139','137') " +
      "and T.TRD_DATE in ('20160325','20160413' ) " +
      "and (T.CURR_CD = 'USD' or T.CURR_CD is null) " +
      "and (T.PRODUCT_CAT_CD is null or T.PRODUCT_CAT_CD not in ('OPT', 'FUT','MLEG')) ) root on " +
      "execs.trd_date=root.trd_date and execs.glb_root_order_id=root.glb_root_order_id " +
      "where execs.LEAF_EXEC_FG = 'Y' " +
      "and execs.event_type_cd = 'FILLED_CONF' " +
      "and execs.sys_src_sys_id in ('93', '7', '70', '115' ,'6','150','189','31','157','185','7'," +
      "'153','163133','80','51','139','137') " +
      "and execs.SYS_ORDER_STAT_CD in ('2','1') " +
      "and execs.TRD_DATE in ('20160325','20160413' ) " +
      "and (execs.PRODUCT_CAT_CD is null or execs.PRODUCT_CAT_CD not in ('OPT', 'FUT','MLEG')) " +
      "and (execs.exec_price_curr_cd = 'USD' or execs.exec_price_curr_cd = null) ) " +
      "Aggregated group by TRD_DATE, ROOT_FLOW_CAT order by TRD_DATE )"

  val query24: String = "select distinct FLOW_CLASS from ORDERS_DETAILS"

  val queries = List(
    "Q1" -> query1,
    "Q2" -> query2,
    "Q3" -> query3,
    "Q4" -> query4,
    "Q5" -> query5,
    "Q6" -> query6,
    "Q7" -> query7,
    "Q8" -> query8,
    "Q9" -> query9,
    "Q10" -> query10,
    "Q11" -> query11,
    "Q12" -> query12,
    "Q13" -> query13,
    "Q14" -> query14,
    "Q15" -> query15,
    "Q16" -> query16,
    "Q17" -> query17,
    "Q18" -> query18,
    "Q19" -> query19,
    "Q20" -> query20,
    "Q21" -> query21,
    "Q22" -> query22,
    "Q23" -> query23,
    "Q24" -> query24
  )

  def orders_details_df(sqlContext: SQLContext, isSpark: Boolean = false): DataFrame = {
      val df = sqlContext.read.format("com.databricks.spark.csv")
          .option("header", "true")
          .option("inferSchema", "false")
          .option("nullValue", "NULL")
          .option("maxCharsPerColumn", "4096")
      if (isSpark) {
          df.schema(structFieldsOrdersDetails)
      }
      df.load(s"${snc.getConf("dataFilesLocation")}/ORDERS_DETAILS.dat")
  }

    def exec_details_df(sqlContext: SQLContext, isSpark: Boolean = false): DataFrame = {
        val df = sqlContext.read.format("com.databricks.spark.csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .option("nullValue", "NULL")
            .option("maxCharsPerColumn", "4096")
        if (isSpark) {
            df.schema(structFieldsExecDetails)
        }
        df.load(s"${snc.getConf("dataFilesLocation")}/EXEC_DETAILS.dat")
    }

  val create_diskStore_ddl = "CREATE DISKSTORE OverflowDiskStore"

  val structFieldsOrdersDetails = StructType(Array(
    StructField("SINGLE_ORDER_DID", LongType, nullable = true) ,
        StructField("SYS_ORDER_ID", StringType, nullable = true) ,
        StructField("SYS_ORDER_VER", IntegerType, nullable = true) ,
        StructField("DATA_SNDG_SYS_NM", StringType, nullable = true) ,
        StructField("SRC_SYS", StringType, nullable = true) ,
        StructField("SYS_PARENT_ORDER_ID", StringType, nullable = true) ,
        StructField("SYS_PARENT_ORDER_VER", IntegerType, nullable = true) ,
        StructField("PARENT_ORDER_TRD_DATE", StringType, nullable = true) ,
        StructField("PARENT_ORDER_SYS_NM", StringType, nullable = true) ,
        StructField("SYS_ALT_ORDER_ID", StringType, nullable = true) ,
        StructField("TRD_DATE", StringType, nullable = true) ,
        StructField("GIVE_UP_BROKER", StringType, nullable = true) ,
        StructField("EVENT_RCV_TS", TimestampType, nullable = true) ,
        StructField("SYS_ROOT_ORDER_ID", StringType, nullable = true) ,
        StructField("GLB_ROOT_ORDER_ID", StringType, nullable = true) ,
        StructField("GLB_ROOT_ORDER_SYS_NM", StringType, nullable = true) ,
        StructField("GLB_ROOT_ORDER_RCV_TS", TimestampType, nullable = true) ,
        StructField("SYS_ORDER_STAT_CD", StringType, nullable = true) ,
        StructField("SYS_ORDER_STAT_DESC_TXT", StringType, nullable = true) ,
        StructField("DW_STAT_CD", StringType, nullable = true) ,
        StructField("EVENT_TS", TimestampType, nullable = true) ,
        StructField("ORDER_OWNER_FIRM_ID", StringType, nullable = true) ,
        StructField("RCVD_ORDER_ID", StringType, nullable = true) ,
        StructField("EVENT_INITIATOR_ID", StringType, nullable = true) ,
        StructField("TRDR_SYS_LOGON_ID", StringType, nullable = true) ,
        StructField("SOLICITED_FG", StringType, nullable = true) ,
        StructField("RCVD_FROM_FIRMID_CD", StringType, nullable = true) ,
        StructField("RCV_DESK", StringType, nullable = true) ,
        StructField("SYS_ACCT_ID_SRC", StringType, nullable = true) ,
        StructField("CUST_ACCT_MNEMONIC", StringType, nullable = true) ,
        StructField("CUST_SLANG", StringType, nullable = true) ,
        StructField("SYS_ACCT_TYPE", StringType, nullable = true) ,
        StructField("CUST_EXCH_ACCT_ID", StringType, nullable = true) ,
        StructField("SYS_SECURITY_ALT_ID", StringType, nullable = true) ,
        StructField("TICKER_SYMBOL", StringType, nullable = true) ,
        StructField("TICKER_SYMBOL_SUFFIX", StringType, nullable = true) ,
        StructField("PRODUCT_CAT_CD", StringType, nullable = true) ,
        StructField("SIDE", StringType, nullable = true) ,
        StructField("LIMIT_PRICE", DecimalType(28, 8), nullable = true) ,
        StructField("STOP_PRICE", DecimalType(28, 8), nullable = true) ,
        StructField("ORDER_QTY", DecimalType(28, 4), nullable = true) ,
        StructField("TOTAL_EXECUTED_QTY", DecimalType(28, 4), nullable = true) ,
        StructField("AVG_PRICE", DecimalType(28, 8), nullable = true) ,
        StructField("DAY_EXECUTED_QTY", DecimalType(28, 4), nullable = true) ,
        StructField("DAY_AVG_PRICE", DecimalType(28, 8), nullable = true) ,
        StructField("REMNG_QTY", DecimalType(28, 4), nullable = true) ,
        StructField("CNCL_QTY", DecimalType(28, 4), nullable = true) ,
        StructField("CNCL_BY_FG", StringType, nullable = true) ,
        StructField("EXPIRE_TS", TimestampType, nullable = true) ,
        StructField("EXEC_INSTR", StringType, nullable = true) ,
        StructField("TIME_IN_FORCE", StringType, nullable = true) ,
        StructField("RULE80AF", StringType, nullable = true) ,
        StructField("DEST_FIRMID_CD", StringType, nullable = true) ,
        StructField("SENT_TO_CONDUIT", StringType, nullable = true) ,
        StructField("SENT_TO_MPID", StringType, nullable = true) ,
        StructField("RCV_METHOD_CD", StringType, nullable = true) ,
        StructField("LIMIT_ORDER_DISP_IND", StringType, nullable = true) ,
        StructField("MERGED_ORDER_FG", StringType, nullable = true) ,
        StructField("MERGED_TO_ORDER_ID", StringType, nullable = true) ,
        StructField("RCV_DEPT_ID", StringType, nullable = true) ,
        StructField("ROUTE_METHOD_CD", StringType, nullable = true) ,
        StructField("LOCATE_ID", StringType, nullable = true) ,
        StructField("LOCATE_TS", TimestampType, nullable = true) ,
        StructField("LOCATE_OVERRIDE_REASON", StringType, nullable = true) ,
        StructField("LOCATE_BROKER", StringType, nullable = true) ,
        StructField("ORDER_BRCH_SEQ_TXT", StringType, nullable = true) ,
        StructField("IGNORE_CD", StringType, nullable = true) ,
        StructField("CLIENT_ORDER_REFID", StringType, nullable = true) ,
        StructField("CLIENT_ORDER_ORIG_REFID", StringType, nullable = true) ,
        StructField("ORDER_TYPE_CD", StringType, nullable = true) ,
        StructField("SENT_TO_ORDER_ID", StringType, nullable = true) ,
        StructField("ASK_PRICE", DecimalType(28, 8), nullable = true) ,
        StructField("ASK_QTY", DecimalType(28, 4), nullable = true) ,
        StructField("BID_PRICE", DecimalType(28, 10), nullable = true) ,
        StructField("BID_QTY", DecimalType(28, 4), nullable = true) ,
        StructField("REG_NMS_EXCEP_CD", StringType, nullable = true) ,
        StructField("REG_NMS_EXCEP_TXT", StringType, nullable = true) ,
        StructField("REG_NMS_LINK_ID", StringType, nullable = true) ,
        StructField("REG_NMS_PRINTS", StringType, nullable = true) ,
        StructField("REG_NMS_STOP_TIME", TimestampType, nullable = true) ,
        StructField("SENT_TS", TimestampType, nullable = true) ,
        StructField("RULE92", StringType, nullable = true) ,
        StructField("RULE92_OVERRIDE_TXT", StringType, nullable = true) ,
        StructField("RULE92_RATIO", DecimalType(28, 10), nullable = true) ,
        StructField("EXMPT_STGY_BEGIN_TIME", TimestampType, nullable = true) ,
        StructField("EXMPT_STGY_END_TIME", TimestampType, nullable = true) ,
        StructField("EXMPT_STGY_PRICE_INST", StringType, nullable = true) ,
        StructField("EXMPT_STGY_QTY", DecimalType(28, 4), nullable = true) ,
        StructField("CAPACITY", StringType, nullable = true) ,
        StructField("DISCRETION_QTY", DecimalType(28, 4), nullable = true) ,
        StructField("DISCRETION_PRICE", StringType, nullable = true) ,
        StructField("BRCHID_CD", StringType, nullable = true) ,
        StructField("BASKET_ORDER_ID", StringType, nullable = true) ,
        StructField("PT_STRTGY_CD", StringType, nullable = true) ,
        StructField("SETL_DATE", StringType, nullable = true) ,
        StructField("SETL_TYPE", StringType, nullable = true) ,
        StructField("SETL_CURR_CD", StringType, nullable = true) ,
        StructField("SETL_INSTRS", StringType, nullable = true) ,
        StructField("COMMENT_TXT", StringType, nullable = true) ,
        StructField("CHANNEL_NM", StringType, nullable = true) ,
        StructField("FLOW_CAT", StringType, nullable = true) ,
        StructField("FLOW_CLASS", StringType, nullable = true) ,
        StructField("FLOW_TGT", StringType, nullable = true) ,
        StructField("ORDER_FLOW_ENTRY", StringType, nullable = true) ,
        StructField("ORDER_FLOW_CHANNEL", StringType, nullable = true) ,
        StructField("ORDER_FLOW_DESK", StringType, nullable = true) ,
        StructField("FLOW_SUB_CAT", StringType, nullable = true) ,
        StructField("STRTGY_CD", StringType, nullable = true) ,
        StructField("RCVD_FROM_VENDOR", StringType, nullable = true) ,
        StructField("RCVD_FROM_CONDUIT", StringType, nullable = true) ,
        StructField("SLS_PERSON_ID", StringType, nullable = true) ,
        StructField("SYNTHETIC_FG", StringType, nullable = true) ,
        StructField("SYNTHETIC_TYPE", StringType, nullable = true) ,
        StructField("FXRT", DecimalType(25, 8), nullable = true) ,
        StructField("PARENT_CLREFID", StringType, nullable = true) ,
        StructField("REF_TIME_ID", IntegerType, nullable = true) ,
        StructField("OPT_CONTRACT_QTY", DecimalType(28, 4), nullable = true) ,
        StructField("OCEAN_PRODUCT_ID", LongType, nullable = true) ,
        StructField("CREATED_BY", StringType, nullable = true) ,
        StructField("CREATED_DATE", TimestampType, nullable = true) ,
        StructField("FIRM_ACCT_ID", LongType, nullable = true) ,
        StructField("DEST", StringType, nullable = true) ,
        StructField("CNTRY_CD", StringType, nullable = true) ,
        StructField("DW_SINGLE_ORDER_CAT", StringType, nullable = true) ,
        StructField("CLIENT_ACCT_ID", LongType, nullable = true) ,
        StructField("EXTERNAL_TRDR_ID", StringType, nullable = true) ,
        StructField("ANONYMOUS_ORDER_FG", StringType, nullable = true) ,
        StructField("SYS_SECURITY_ALT_SRC", StringType, nullable = true) ,
        StructField("CURR_CD", StringType, nullable = true) ,
        StructField("EVENT_TYPE_CD", StringType, nullable = true) ,
        StructField("SYS_CLIENT_ACCT_ID", StringType, nullable = true) ,
        StructField("SYS_FIRM_ACCT_ID", StringType, nullable = true) ,
        StructField("SYS_TRDR_ID", StringType, nullable = true) ,
        StructField("DEST_ID", IntegerType, nullable = true) ,
        StructField("OPT_PUT_OR_CALL", StringType, nullable = true) ,
        StructField("SRC_FEED_REF_CD", StringType, nullable = true) ,
        StructField("DIGEST_KEY", StringType, nullable = true) ,
        StructField("EFF_TS", TimestampType, nullable = true) ,
        StructField("ENTRY_TS", TimestampType, nullable = true) ,
        StructField("OPT_STRIKE_PRICE", DecimalType(28, 8), nullable = true) ,
        StructField("OPT_MATURITY_DATE", StringType, nullable = true) ,
        StructField("ORDER_RESTR", StringType, nullable = true) ,
        StructField("SHORT_SELL_EXEMPT_CD", StringType, nullable = true) ,
        StructField("QUOTE_TIME", TimestampType, nullable = true) ,
        StructField("SLS_CREDIT", StringType, nullable = true) ,
        StructField("SYS_SECURITY_ID", StringType, nullable = true) ,
        StructField("SYS_SECURITY_ID_SRC", StringType, nullable = true) ,
        StructField("SYS_SRC_SYS_ID", StringType, nullable = true) ,
        StructField("SYS_ORDER_ID_UNIQUE_SUFFIX", StringType, nullable = true) ,
        StructField("DEST_ID_SRC", StringType, nullable = true) ,
        StructField("GLB_ROOT_SRC_SYS_ID", StringType, nullable = true) ,
        StructField("GLB_ROOT_ORDER_ID_SUFFIX", StringType, nullable = true) ,
        StructField("SYS_ROOT_ORDER_ID_SUFFIX", StringType, nullable = true) ,
        StructField("SYS_PARENT_ORDER_ID_SUFFIX", StringType, nullable = true) ,
        StructField("CREDIT_BREACH_PERCENT", DecimalType(25, 10), nullable = true) ,
        StructField("CREDIT_BREACH_OVERRIDE", StringType, nullable = true) ,
        StructField("INFO_BARRIER_ID", StringType, nullable = true) ,
        StructField("EXCH_PARTICIPANT_ID", StringType, nullable = true) ,
        StructField("REJECT_REASON_CD", StringType, nullable = true) ,
        StructField("DIRECTED_DEST", StringType, nullable = true) ,
        StructField("REG_NMS_LINK_TYPE", StringType, nullable = true) ,
        StructField("CONVER_RATIO", DecimalType(28, 9), nullable = true) ,
        StructField("STOCK_REF_PRICE", DecimalType(28, 8), nullable = true) ,
        StructField("CB_SWAP_ORDER_FG", StringType, nullable = true) ,
        StructField("EV", DecimalType(28, 8), nullable = true) ,
        StructField("SYS_DATA_MODIFIED_TS", TimestampType, nullable = true) ,
        StructField("CMSN_TYPE", StringType, nullable = true) ,
        StructField("SYS_CREDIT_TRDR_ID", StringType, nullable = true) ,
        StructField("SYS_ENTRY_USER_ID", StringType, nullable = true) ,
        StructField("OPEN_CLOSE_CD", StringType, nullable = true) ,
        StructField("AS_OF_TRD_FG", StringType, nullable = true) ,
        StructField("HANDLING_INSTR", StringType, nullable = true) ,
        StructField("SECURITY_DESC", StringType, nullable = true) ,
        StructField("MINIMUM_QTY", DecimalType(28, 6), nullable = true) ,
        StructField("CUST_OR_FIRM", StringType, nullable = true) ,
        StructField("MAXIMUM_SHOW", DecimalType(28, 6), nullable = true) ,
        StructField("SECURITY_SUB_TYPE", StringType, nullable = true) ,
        StructField("MULTILEG_RPT_TYPE", StringType, nullable = true) ,
        StructField("ORDER_ACTION_TYPE", StringType, nullable = true) ,
        StructField("BARRIER_STYLE", StringType, nullable = true) ,
        StructField("AUTO_IOI_REF_TYPE", StringType, nullable = true) ,
        StructField("PEG_OFFSET_VAL", DecimalType(10, 2), nullable = true) ,
        StructField("AUTO_IOI_OFFSET", DecimalType(28, 10), nullable = true) ,
        StructField("IOI_PRICE", DecimalType(28, 10), nullable = true) ,
        StructField("TGT_PRICE", DecimalType(28, 10), nullable = true) ,
        StructField("IOI_QTY", StringType, nullable = true) ,
        StructField("IOI_ORDER_QTY", DecimalType(28, 4), nullable = true) ,
        StructField("CMSN", StringType, nullable = true) ,
        StructField("SYS_LEG_REF_ID", StringType, nullable = true) ,
        StructField("TRADING_TYPE", StringType, nullable = true) ,
        StructField("EXCH_ORDER_ID", StringType, nullable = true) ,
        StructField("DEAL_ID", StringType, nullable = true) ,
        StructField("ORDER_TRD_TYPE", StringType, nullable = true) ,
        StructField("CXL_REASON", StringType, nullable = true)))
  

  val structFieldsExecDetails = StructType( Array (
    StructField("EXEC_DID", LongType, nullable = true) ,
        StructField("SYS_EXEC_VER", IntegerType, nullable = true) ,
        StructField("SYS_EXEC_ID", StringType, nullable = true) ,
        StructField("TRD_DATE", StringType, nullable = true) ,
        StructField("ALT_EXEC_ID", StringType, nullable = true) ,
        StructField("SYS_EXEC_STAT", StringType, nullable = true) ,
        StructField("DW_EXEC_STAT", StringType, nullable = true) ,
        StructField("ORDER_OWNER_FIRM_ID", StringType, nullable = true) ,
        StructField("TRDR_SYS_LOGON_ID", StringType, nullable = true) ,
        StructField("CONTRA_BROKER_MNEMONIC", StringType, nullable = true) ,
        StructField("SIDE", StringType, nullable = true) ,
        StructField("TICKER_SYMBOL", StringType, nullable = true) ,
        StructField("SYS_SECURITY_ALT_ID", StringType, nullable = true) ,
        StructField("PRODUCT_CAT_CD", StringType, nullable = true) ,
        StructField("LAST_MKT", StringType, nullable = true) ,
        StructField("EXECUTED_QTY", DecimalType(18, 4), nullable = true) ,
        StructField("EXEC_PRICE", DecimalType(18, 8), nullable = true) ,
        StructField("EXEC_PRICE_CURR_CD", StringType, nullable = true) ,
        StructField("EXEC_CAPACITY", StringType, nullable = true) ,
        StructField("CLIENT_ACCT_ID", LongType, nullable = true) ,
        StructField("FIRM_ACCT_ID", LongType, nullable = true) ,
        StructField("AVG_PRICE_ACCT_ID", LongType, nullable = true) ,
        StructField("OCEAN_ACCT_ID", LongType, nullable = true) ,
        StructField("EXEC_CNTRY_CD", StringType, nullable = true) ,
        StructField("CMSN", StringType, nullable = true) ,
        StructField("COMMENT_TXT", StringType, nullable = true) ,
        StructField("ACT_BRCH_SEQ_TXT", StringType, nullable = true) ,
        StructField("IGNORE_CD", StringType, nullable = true) ,
        StructField("SRC_SYS", StringType, nullable = true) ,
        StructField("EXEC_TYPE_CD", StringType, nullable = true) ,
        StructField("LIQUIDITY_CD", StringType, nullable = true) ,
        StructField("ASK_PRICE", DecimalType(18, 8), nullable = true) ,
        StructField("ASK_QTY", DecimalType(18, 4), nullable = true) ,
        StructField("TRD_REPORT_ASOF_DATE", StringType, nullable = true) ,
        StructField("BID_PRICE", DecimalType(18, 8), nullable = true) ,
        StructField("BID_QTY", DecimalType(18, 4), nullable = true) ,
        StructField("CROSS_ID", StringType, nullable = true) ,
        StructField("NYSE_SUBREPORT_TYPE", StringType, nullable = true) ,
        StructField("QUOTE_COORDINATOR", StringType, nullable = true) ,
        StructField("QUOTE_TIME", TimestampType, nullable = true) ,
        StructField("REG_NMS_EXCEPT_CD", StringType, nullable = true) ,
        StructField("REG_NMS_EXCEPT_TXT", StringType, nullable = true) ,
        StructField("REG_NMS_LINK_ID", StringType, nullable = true) ,
        StructField("REG_NMS_MKT_CENTER_ID", StringType, nullable = true) ,
        StructField("REG_NMS_OVERRIDE", StringType, nullable = true) ,
        StructField("REG_NMS_PRINTS", StringType, nullable = true) ,
        StructField("EXECUTED_BY", StringType, nullable = true) ,
        StructField("TICKER_SYMBOL_SUFFIX", StringType, nullable = true) ,
        StructField("PREREGNMS_TRD_MOD1", StringType, nullable = true) ,
        StructField("PREREGNMS_TRD_MOD2", StringType, nullable = true) ,
        StructField("PREREGNMS_TRD_MOD3", StringType, nullable = true) ,
        StructField("PREREGNMS_TRD_MOD4", StringType, nullable = true) ,
        StructField("NMS_FG", StringType, nullable = true) ,
        StructField("GIVEUP_BROKER", StringType, nullable = true) ,
        StructField("CHANNEL_NM", StringType, nullable = true) ,
        StructField("ORDER_FLOW_ENTRY", StringType, nullable = true) ,
        StructField("FLOW_CAT", StringType, nullable = true) ,
        StructField("FLOW_CLASS", StringType, nullable = true) ,
        StructField("FLOW_TGT", StringType, nullable = true) ,
        StructField("ORDER_FLOW_CHANNEL", StringType, nullable = true) ,
        StructField("FLOW_SUBCAT", StringType, nullable = true) ,
        StructField("SYS_ACCT_ID_SRC", StringType, nullable = true) ,
        StructField("STRTGY_CD", StringType, nullable = true) ,
        StructField("EXECUTING_BROKER_CD", StringType, nullable = true) ,
        StructField("LEAF_EXEC_FG", StringType, nullable = true) ,
        StructField("RCVD_EXEC_ID", StringType, nullable = true) ,
        StructField("RCVD_EXEC_VER", IntegerType, nullable = true) ,
        StructField("ORDER_FLOW_DESK", StringType, nullable = true) ,
        StructField("SYS_ROOT_ORDER_ID", StringType, nullable = true) ,
        StructField("SYS_ROOT_ORDER_VER", IntegerType, nullable = true) ,
        StructField("GLB_ROOT_ORDER_ID", StringType, nullable = true) ,
        StructField("TOTAL_EXECUTED_QTY", DecimalType(18, 4), nullable = true) ,
        StructField("AVG_PRICE", DecimalType(18, 8), nullable = true) ,
        StructField("DEST_CD", StringType, nullable = true) ,
        StructField("CLIENT_ORDER_REFID", StringType, nullable = true) ,
        StructField("CLIENT_ORDER_ORIG_REFID", StringType, nullable = true) ,
        StructField("CROSS_EXEC_FG", StringType, nullable = true) ,
        StructField("OCEAN_PRODUCT_ID", LongType, nullable = true) ,
        StructField("TRDR_ID", LongType, nullable = true) ,
        StructField("REF_TIME_ID", IntegerType, nullable = true) ,
        StructField("CREATED_BY", StringType, nullable = true) ,
        StructField("CREATED_DATE", TimestampType, nullable = true) ,
        StructField("FIX_EXEC_ID", StringType, nullable = true) ,
        StructField("FIX_ORIGINAL_EXEC_ID", StringType, nullable = true) ,
        StructField("RELATED_MKT_CENTER", StringType, nullable = true) ,
        StructField("TRANS_TS", TimestampType, nullable = true) ,
        StructField("SYS_SECURITY_ALT_SRC", StringType, nullable = true) ,
        StructField("EVENT_TYPE_CD", StringType, nullable = true) ,
        StructField("SYS_CLIENT_ACCT_ID", StringType, nullable = true) ,
        StructField("SYS_FIRM_ACCT_ID", StringType, nullable = true) ,
        StructField("SYS_AVG_PRICE_ACCT_ID", StringType, nullable = true) ,
        StructField("SYS_TRDR_ID", StringType, nullable = true) ,
        StructField("ACT_BRCH_SEQ", StringType, nullable = true) ,
        StructField("SYS_ORDER_ID", StringType, nullable = true) ,
        StructField("SYS_ORDER_VER", IntegerType, nullable = true) ,
        StructField("SRC_FEED_REF_CD", StringType, nullable = true) ,
        StructField("DIGEST_KEY", StringType, nullable = true) ,
        StructField("TRUE_LAST_MKT", StringType, nullable = true) ,
        StructField("ENTRY_TS", TimestampType, nullable = true) ,
        StructField("OPT_STRIKE_PRICE", DecimalType(18, 8), nullable = true) ,
        StructField("OPT_MATURITY_DATE", StringType, nullable = true) ,
        StructField("EXPIRE_TS", TimestampType, nullable = true) ,
        StructField("OPT_PUT_OR_CALL", StringType, nullable = true) ,
        StructField("SYS_ORDER_STAT_CD", StringType, nullable = true) ,
        StructField("CONTRA_ACCT", StringType, nullable = true) ,
        StructField("CONTRA_ACCT_SRC", StringType, nullable = true) ,
        StructField("CONTRA_BROKER_SRC", StringType, nullable = true) ,
        StructField("SYS_SECURITY_ID", StringType, nullable = true) ,
        StructField("SYS_SECURITY_ID_SRC", StringType, nullable = true) ,
        StructField("SYS_SRC_SYS_ID", StringType, nullable = true) ,
        StructField("SYS_ORDER_ID_UNIQUE_SUFFIX", StringType, nullable = true) ,
        StructField("DEST", StringType, nullable = true) ,
        StructField("DEST_ID_SRC", StringType, nullable = true) ,
        StructField("CONVER_RATIO", DecimalType(18, 9), nullable = true) ,
        StructField("STOCK_REF_PRICE", DecimalType(18, 8), nullable = true) ,
        StructField("AS_OF_TRD_FG", StringType, nullable = true) ,
        StructField("MULTILEG_RPT_TYPE", StringType, nullable = true) ,
        StructField("REG_NMS_LINK_TYPE", StringType, nullable = true) ,
        StructField("EXEC_SUB_TYPE", StringType, nullable = true) ,
        StructField("CMSN_TYPE", StringType, nullable = true) ,
        StructField("QUOTE_CONDITION_IND", StringType, nullable = true) ,
        StructField("TRD_THROUGH_FG", StringType, nullable = true) ,
        StructField("REGNMS_ORDER_LINK_ID", StringType, nullable = true) ,
        StructField("REGNMS_ORDER_LINK_TYPE", StringType, nullable = true) ,
        StructField("DK_IND", StringType, nullable = true) ,
        StructField("NBBO_QUOTE_TIME", StringType, nullable = true) ,
        StructField("GLB_ROOT_SRC_SYS_ID", StringType, nullable = true) ,
        StructField("TRD_REPORT_TYPE", StringType, nullable = true) ,
        StructField("REPORT_TO_EXCH_FG", StringType, nullable = true) ,
        StructField("CMPLN_COMMENT", StringType, nullable = true) ,
        StructField("DEAL_TYPE", StringType, nullable = true) ,
        StructField("EXEC_COMMENTS", StringType, nullable = true) ,
        StructField("OPTAL_FIELDS", StringType, nullable = true) ,
        StructField("SPOT_REF_PRICE", StringType, nullable = true) ,
        StructField("DELTA_OVERRIDE", StringType, nullable = true) ,
        StructField("UNDERLYING_PRICE", StringType, nullable = true) ,
        StructField("PRICE_DELTA", StringType, nullable = true) ,
        StructField("NORMALIZED_LIQUIDITY_IND", StringType, nullable = true) ,
        StructField("USER_AVG_PRICE", StringType, nullable = true) ,
        StructField("LAST_EXEC_TS", TimestampType, nullable = true) ,
        StructField("LULD_LOWER_PRICE_BAND", StringType, nullable = true) ,
        StructField("LULD_UPPER_PRICE_BAND", StringType, nullable = true) ,
        StructField("LULD_PRICE_BAND_TS", TimestampType, nullable = true) ,
        StructField("REMNG_QTY", DecimalType(18, 4), nullable = true) ,
        StructField("ORDER_QTY", DecimalType(18, 4), nullable = true) ,
        StructField("AMD_TS", TimestampType, nullable = true) ,
        StructField("SETL_CODE", StringType, nullable = true) ,
        StructField("SETL_DATE", StringType, nullable = true) ,
        StructField("CUST_NM", StringType, nullable = true) ,
        StructField("EXEC_TYPE", StringType, nullable = true) ,
        StructField("TRDR_KEY", StringType, nullable = true) ,
        StructField("TRDR_NM", StringType, nullable = true) ,
        StructField("FX_RATE", StringType, nullable = true) ,
        StructField("CUST_FX_RATE", StringType, nullable = true) ,
        StructField("PARENT_ORDER_SYS_NM", StringType, nullable = true) ,
        StructField("CNC_TYPE", StringType, nullable = true) ,
        StructField("FEE_AMT", DecimalType(20, 2), nullable = true) ,
        StructField("FEE_CCY", StringType, nullable = true) ,
        StructField("BRKG_AMT", DecimalType(20, 2), nullable = true) ,
        StructField("BRKG_CCY", StringType, nullable = true) ,
        StructField("CLEAR", StringType, nullable = true) ,
        StructField("PMT_FIX_DATE", StringType, nullable = true) ,
        StructField("FOLLOW_ON_FG", StringType, nullable = true) ,
        StructField("FX_RATE_CCY_TO", StringType, nullable = true) ,
        StructField("FX_RATE_CCY_FROM", StringType, nullable = true) ,
        StructField("CUST_FX_RATE_CCY_TO", StringType, nullable = true) ,
        StructField("CUST_FX_RATE_CCY_FROM", StringType, nullable = true) ,
        StructField("SYS_GFCID", StringType, nullable = true) ,
        StructField("CONTRA_SIDE", StringType, nullable = true) ,
        StructField("OPT_CONTRACT_MULTIPLIER", DecimalType(10, 2), nullable = true) ,
        StructField("PRIOR_REF_PRICE_TS", TimestampType, nullable = true) ,
        StructField("SECURITY_SUB_TYPE", StringType, nullable = true) ,
        StructField("MSG_DIRECTION", StringType, nullable = true) ,
        StructField("LEAF_SYS_EXEC_ID", StringType, nullable = true) ,
        StructField("LEAF_SRC_SYS", StringType, nullable = true) ,
        StructField("FIX_LAST_MKT", StringType, nullable = true) ,
        StructField("FIX_CONTRA_BROKER_MNEMONIC", StringType, nullable = true) ,
        StructField("RIO_MSG_SRC", StringType, nullable = true) ,
        StructField("SNAPSHOT_TS", TimestampType, nullable = true) ,
        StructField("EXTERNAL_TRANS_TS", TimestampType, nullable = true) ,
        StructField("PRICE_CATEGORY", StringType, nullable = true) ,
        StructField("UNDERLYING_FX_RATE", DecimalType(18, 8), nullable = true) ,
        StructField("CONVERSION_RATE", DecimalType(18, 8), nullable = true) ,
        StructField("TRANS_COMMENT", StringType, nullable = true) ,
        StructField("AGGRESSOR_FLAG", StringType, nullable = true)
  ))


  val orders_details_create_ddl =
    "create table orders_details" +
        "(SINGLE_ORDER_DID BIGINT ,SYS_ORDER_ID VARCHAR(64) ," +
        "SYS_ORDER_VER INTEGER ,DATA_SNDG_SYS_NM VARCHAR(128) ,SRC_SYS VARCHAR(20) ,SYS_PARENT_ORDER_ID VARCHAR(64) ," +
        "SYS_PARENT_ORDER_VER SMALLINT ,PARENT_ORDER_TRD_DATE VARCHAR(20),PARENT_ORDER_SYS_NM VARCHAR(128) ," +
        "SYS_ALT_ORDER_ID VARCHAR(64) ,TRD_DATE VARCHAR(20),GIVE_UP_BROKER VARCHAR(20) ," +
        "EVENT_RCV_TS TIMESTAMP ,SYS_ROOT_ORDER_ID VARCHAR(64) ,GLB_ROOT_ORDER_ID VARCHAR(64) ," +
        "GLB_ROOT_ORDER_SYS_NM VARCHAR(128) ,GLB_ROOT_ORDER_RCV_TS TIMESTAMP ,SYS_ORDER_STAT_CD VARCHAR(20) ," +
        "SYS_ORDER_STAT_DESC_TXT VARCHAR(120) ,DW_STAT_CD VARCHAR(20) ,EVENT_TS TIMESTAMP,ORDER_OWNER_FIRM_ID VARCHAR(20)," +
        "RCVD_ORDER_ID VARCHAR(64) ,EVENT_INITIATOR_ID VARCHAR(64),TRDR_SYS_LOGON_ID VARCHAR(64),SOLICITED_FG  VARCHAR(1)," +
        "RCVD_FROM_FIRMID_CD VARCHAR(20),RCV_DESK VARCHAR(20),SYS_ACCT_ID_SRC VARCHAR(64) ,CUST_ACCT_MNEMONIC VARCHAR(128)," +
        "CUST_SLANG VARCHAR(20) ,SYS_ACCT_TYPE VARCHAR(20) ,CUST_EXCH_ACCT_ID VARCHAR(64) ,SYS_SECURITY_ALT_ID VARCHAR(64) ," +
        "TICKER_SYMBOL VARCHAR(32) ,TICKER_SYMBOL_SUFFIX VARCHAR(20) ,PRODUCT_CAT_CD VARCHAR(20) ,SIDE VARCHAR(20) ," +
        "LIMIT_PRICE DECIMAL(28, 8),STOP_PRICE DECIMAL(28, 8),ORDER_QTY DECIMAL(28, 4) ,TOTAL_EXECUTED_QTY DECIMAL(28, 4) ," +
        "AVG_PRICE DECIMAL(28, 8) ,DAY_EXECUTED_QTY DECIMAL(28, 4) ,DAY_AVG_PRICE DECIMAL(28, 8) ,REMNG_QTY DECIMAL(28, 4) ," +
        "CNCL_QTY DECIMAL(28, 4) ,CNCL_BY_FG  VARCHAR(1) ,EXPIRE_TS TIMESTAMP ,EXEC_INSTR VARCHAR(64) ,TIME_IN_FORCE VARCHAR(20) ," +
        "RULE80AF  VARCHAR(1) ,DEST_FIRMID_CD VARCHAR(20) ,SENT_TO_CONDUIT VARCHAR(20) ,SENT_TO_MPID VARCHAR(20) ," +
        "RCV_METHOD_CD VARCHAR(20) ,LIMIT_ORDER_DISP_IND  VARCHAR(1) ,MERGED_ORDER_FG  VARCHAR(1) ,MERGED_TO_ORDER_ID VARCHAR(64) ," +
        "RCV_DEPT_ID VARCHAR(20) ,ROUTE_METHOD_CD VARCHAR(20) ,LOCATE_ID VARCHAR(256) ,LOCATE_TS TIMESTAMP ,LOCATE_OVERRIDE_REASON VARCHAR(2000) ," +
        "LOCATE_BROKER VARCHAR(256) ,ORDER_BRCH_SEQ_TXT VARCHAR(20) ,IGNORE_CD VARCHAR(20) ,CLIENT_ORDER_REFID VARCHAR(64) ," +
        "CLIENT_ORDER_ORIG_REFID VARCHAR(64) ,ORDER_TYPE_CD VARCHAR(20) ,SENT_TO_ORDER_ID VARCHAR(64) ,ASK_PRICE DECIMAL(28, 8) ," +
        "ASK_QTY DECIMAL(28, 4) ,BID_PRICE DECIMAL(28, 10) ,BID_QTY DECIMAL(28, 4) ,REG_NMS_EXCEP_CD VARCHAR(20) ," +
        "REG_NMS_EXCEP_TXT VARCHAR(2000) ,REG_NMS_LINK_ID VARCHAR(64) ,REG_NMS_PRINTS  VARCHAR(1) ,REG_NMS_STOP_TIME TIMESTAMP ," +
        "SENT_TS TIMESTAMP ,RULE92  VARCHAR(1) ,RULE92_OVERRIDE_TXT VARCHAR(2000) ,RULE92_RATIO DECIMAL(25, 10) ," +
        "EXMPT_STGY_BEGIN_TIME TIMESTAMP ,EXMPT_STGY_END_TIME TIMESTAMP ,EXMPT_STGY_PRICE_INST VARCHAR(2000) ," +
        "EXMPT_STGY_QTY DECIMAL(28, 4) ,CAPACITY VARCHAR(20) ,DISCRETION_QTY DECIMAL(28, 4) ,DISCRETION_PRICE VARCHAR(64) ," +
        "BRCHID_CD VARCHAR(20) ,BASKET_ORDER_ID VARCHAR(64) ,PT_STRTGY_CD VARCHAR(20) ,SETL_DATE VARCHAR(20),SETL_TYPE VARCHAR(20) ," +
        "SETL_CURR_CD VARCHAR(20) ,SETL_INSTRS VARCHAR(2000) ,COMMENT_TXT VARCHAR(2000) ,CHANNEL_NM VARCHAR(128) ," +
        "FLOW_CAT VARCHAR(20) ,FLOW_CLASS VARCHAR(20) ,FLOW_TGT VARCHAR(20) ,ORDER_FLOW_ENTRY VARCHAR(20) ,ORDER_FLOW_CHANNEL VARCHAR(20) ," +
        "ORDER_FLOW_DESK VARCHAR(20) ,FLOW_SUB_CAT VARCHAR(20) ,STRTGY_CD VARCHAR(20) ,RCVD_FROM_VENDOR VARCHAR(20) ," +
        "RCVD_FROM_CONDUIT VARCHAR(20) ,SLS_PERSON_ID VARCHAR(64) ,SYNTHETIC_FG  VARCHAR(1) ,SYNTHETIC_TYPE VARCHAR(20) ," +
        "FXRT DECIMAL(25, 8) ,PARENT_CLREFID VARCHAR(64) ,REF_TIME_ID INTEGER ,OPT_CONTRACT_QTY DECIMAL(28, 4) ," +
        "OCEAN_PRODUCT_ID BIGINT ,CREATED_BY VARCHAR(64) ,CREATED_DATE TIMESTAMP ,FIRM_ACCT_ID BIGINT ,DEST VARCHAR(20) ," +
        "CNTRY_CD VARCHAR(20) ,DW_SINGLE_ORDER_CAT VARCHAR(20) ,CLIENT_ACCT_ID BIGINT ," +
        "EXTERNAL_TRDR_ID VARCHAR(64) ,ANONYMOUS_ORDER_FG  VARCHAR(1) ,SYS_SECURITY_ALT_SRC VARCHAR(20) ,CURR_CD VARCHAR(20) ," +
        "EVENT_TYPE_CD VARCHAR(20) ,SYS_CLIENT_ACCT_ID VARCHAR(64) ,SYS_FIRM_ACCT_ID VARCHAR(20) ,SYS_TRDR_ID VARCHAR(64) ," +
        "DEST_ID INTEGER ,OPT_PUT_OR_CALL VARCHAR(20) ,SRC_FEED_REF_CD VARCHAR(64) ,DIGEST_KEY VARCHAR(128) ,EFF_TS TIMESTAMP ," +
        "ENTRY_TS TIMESTAMP ,OPT_STRIKE_PRICE DECIMAL(28, 8) ,OPT_MATURITY_DATE VARCHAR(20) ,ORDER_RESTR VARCHAR(4) ," +
        "SHORT_SELL_EXEMPT_CD VARCHAR(4) ,QUOTE_TIME TIMESTAMP ,SLS_CREDIT VARCHAR(20) ,SYS_SECURITY_ID VARCHAR(64) ," +
        "SYS_SECURITY_ID_SRC VARCHAR(20) ,SYS_SRC_SYS_ID VARCHAR(20) ,SYS_ORDER_ID_UNIQUE_SUFFIX VARCHAR(20) ," +
        "DEST_ID_SRC VARCHAR(4) ,GLB_ROOT_SRC_SYS_ID VARCHAR(20) ,GLB_ROOT_ORDER_ID_SUFFIX VARCHAR(64) ,SYS_ROOT_ORDER_ID_SUFFIX VARCHAR(20) ," +
        "SYS_PARENT_ORDER_ID_SUFFIX VARCHAR(20) ,CREDIT_BREACH_PERCENT DECIMAL(25, 10) ,CREDIT_BREACH_OVERRIDE VARCHAR(256) ," +
        "INFO_BARRIER_ID VARCHAR(256) ,EXCH_PARTICIPANT_ID VARCHAR(64) ,REJECT_REASON_CD VARCHAR(4) ,DIRECTED_DEST VARCHAR(20) ," +
        "REG_NMS_LINK_TYPE VARCHAR(20) ,CONVER_RATIO DECIMAL(28, 9) ,STOCK_REF_PRICE DECIMAL(28, 8) ,CB_SWAP_ORDER_FG  VARCHAR(1) ," +
        "EV DECIMAL(28, 8) ,SYS_DATA_MODIFIED_TS TIMESTAMP ,CMSN_TYPE VARCHAR(20) ,SYS_CREDIT_TRDR_ID VARCHAR(20) ,SYS_ENTRY_USER_ID VARCHAR(20) ," +
        "OPEN_CLOSE_CD VARCHAR(20) ,AS_OF_TRD_FG  VARCHAR(1) ,HANDLING_INSTR VARCHAR(20) ,SECURITY_DESC VARCHAR(512) ," +
        "MINIMUM_QTY DECIMAL(21, 6) ,CUST_OR_FIRM VARCHAR(20) ,MAXIMUM_SHOW DECIMAL(21, 6) ,SECURITY_SUB_TYPE VARCHAR(20) ," +
        "MULTILEG_RPT_TYPE VARCHAR(4) ,ORDER_ACTION_TYPE VARCHAR(4) ,BARRIER_STYLE VARCHAR(4) ,AUTO_IOI_REF_TYPE VARCHAR(4) ," +
        "PEG_OFFSET_VAL DECIMAL(10, 2) ,AUTO_IOI_OFFSET DECIMAL(28, 10) ,IOI_PRICE DECIMAL(28, 10) ,TGT_PRICE DECIMAL(28, 10) ," +
        "IOI_QTY VARCHAR(64) ,IOI_ORDER_QTY DECIMAL(28, 4) ,CMSN VARCHAR(64) ,SYS_LEG_REF_ID VARCHAR(64) ,TRADING_TYPE VARCHAR(4) ," +
        "EXCH_ORDER_ID VARCHAR(64) ,DEAL_ID VARCHAR(64) ,ORDER_TRD_TYPE VARCHAR(4) ,CXL_REASON VARCHAR(64))"

  val exec_details_create_ddl =
    "create table exec_details " +
        "(EXEC_DID BIGINT,SYS_EXEC_VER INTEGER,SYS_EXEC_ID VARCHAR(64),TRD_DATE VARCHAR(20),ALT_EXEC_ID VARCHAR(64)," +
        "SYS_EXEC_STAT VARCHAR(20),DW_EXEC_STAT VARCHAR(20),ORDER_OWNER_FIRM_ID VARCHAR(20),TRDR_SYS_LOGON_ID VARCHAR(64)," +
        "CONTRA_BROKER_MNEMONIC VARCHAR(20),SIDE VARCHAR(20),TICKER_SYMBOL VARCHAR(32),SYS_SECURITY_ALT_ID VARCHAR(64)," +
        "PRODUCT_CAT_CD VARCHAR(20),LAST_MKT VARCHAR(20),EXECUTED_QTY DECIMAL(18, 4),EXEC_PRICE DECIMAL( 18, 8)," +
        "EXEC_PRICE_CURR_CD VARCHAR(20),EXEC_CAPACITY VARCHAR(20),CLIENT_ACCT_ID BIGINT,FIRM_ACCT_ID BIGINT," +
        "AVG_PRICE_ACCT_ID BIGINT,OCEAN_ACCT_ID BIGINT,EXEC_CNTRY_CD VARCHAR(20),CMSN VARCHAR(20),COMMENT_TXT VARCHAR(2000)," +
        "ACT_BRCH_SEQ_TXT VARCHAR(20),IGNORE_CD VARCHAR(20),SRC_SYS VARCHAR(20),EXEC_TYPE_CD VARCHAR(20)," +
        "LIQUIDITY_CD VARCHAR(20),ASK_PRICE DECIMAL( 18, 8),ASK_QTY DECIMAL(18, 4),TRD_REPORT_ASOF_DATE VARCHAR(20)," +
        "BID_PRICE DECIMAL( 18, 8),BID_QTY DECIMAL(18, 4),CROSS_ID VARCHAR(64),NYSE_SUBREPORT_TYPE VARCHAR(20)," +
        "QUOTE_COORDINATOR VARCHAR(20),QUOTE_TIME TIMESTAMP,REG_NMS_EXCEPT_CD VARCHAR(20),REG_NMS_EXCEPT_TXT VARCHAR(2000)," +
        "REG_NMS_LINK_ID VARCHAR(64),REG_NMS_MKT_CENTER_ID VARCHAR(64),REG_NMS_OVERRIDE VARCHAR(20),REG_NMS_PRINTS  VARCHAR(1)," +
        "EXECUTED_BY VARCHAR(20),TICKER_SYMBOL_SUFFIX VARCHAR(20),PREREGNMS_TRD_MOD1  VARCHAR(1),PREREGNMS_TRD_MOD2  VARCHAR(1)," +
        "PREREGNMS_TRD_MOD3  VARCHAR(1),PREREGNMS_TRD_MOD4  VARCHAR(1),NMS_FG  VARCHAR(1),GIVEUP_BROKER VARCHAR(20)," +
        "CHANNEL_NM VARCHAR(128),ORDER_FLOW_ENTRY VARCHAR(20),FLOW_CAT VARCHAR(20),FLOW_CLASS VARCHAR(20),FLOW_TGT VARCHAR(20)," +
        "ORDER_FLOW_CHANNEL VARCHAR(20),FLOW_SUBCAT VARCHAR(20),SYS_ACCT_ID_SRC VARCHAR(64),STRTGY_CD VARCHAR(20)," +
        "EXECUTING_BROKER_CD VARCHAR(20),LEAF_EXEC_FG  VARCHAR(1),RCVD_EXEC_ID VARCHAR(64),RCVD_EXEC_VER INTEGER," +
        "ORDER_FLOW_DESK VARCHAR(20),SYS_ROOT_ORDER_ID VARCHAR(64),SYS_ROOT_ORDER_VER INTEGER,GLB_ROOT_ORDER_ID VARCHAR(64)," +
        "TOTAL_EXECUTED_QTY DECIMAL(18, 4),AVG_PRICE DECIMAL( 18, 8),DEST_CD VARCHAR(20),CLIENT_ORDER_REFID VARCHAR(64)," +
        "CLIENT_ORDER_ORIG_REFID VARCHAR(64),CROSS_EXEC_FG  VARCHAR(1),OCEAN_PRODUCT_ID BIGINT,TRDR_ID BIGINT,REF_TIME_ID INTEGER," +
        "CREATED_BY VARCHAR(64),CREATED_DATE TIMESTAMP,FIX_EXEC_ID VARCHAR(64),FIX_ORIGINAL_EXEC_ID VARCHAR(64)," +
        "RELATED_MKT_CENTER VARCHAR(20),TRANS_TS TIMESTAMP,SYS_SECURITY_ALT_SRC VARCHAR(20),EVENT_TYPE_CD VARCHAR(20)," +
        "SYS_CLIENT_ACCT_ID VARCHAR(64),SYS_FIRM_ACCT_ID VARCHAR(20),SYS_AVG_PRICE_ACCT_ID VARCHAR(20),SYS_TRDR_ID VARCHAR(64)," +
        "ACT_BRCH_SEQ VARCHAR(20),SYS_ORDER_ID VARCHAR(64),SYS_ORDER_VER INTEGER,SRC_FEED_REF_CD VARCHAR(64)," +
        "DIGEST_KEY VARCHAR(128),TRUE_LAST_MKT VARCHAR(20),ENTRY_TS TIMESTAMP,OPT_STRIKE_PRICE DECIMAL( 18, 8)," +
        "OPT_MATURITY_DATE VARCHAR(20),EXPIRE_TS TIMESTAMP,OPT_PUT_OR_CALL VARCHAR(20),SYS_ORDER_STAT_CD VARCHAR(20)," +
        "CONTRA_ACCT VARCHAR(64),CONTRA_ACCT_SRC VARCHAR(20),CONTRA_BROKER_SRC VARCHAR(20),SYS_SECURITY_ID VARCHAR(64)," +
        "SYS_SECURITY_ID_SRC VARCHAR(20),SYS_SRC_SYS_ID VARCHAR(20),SYS_ORDER_ID_UNIQUE_SUFFIX VARCHAR(20),DEST VARCHAR(20)," +
        "DEST_ID_SRC VARCHAR(4),CONVER_RATIO DECIMAL(18, 9),STOCK_REF_PRICE DECIMAL( 18, 8),AS_OF_TRD_FG  VARCHAR(1)," +
        "MULTILEG_RPT_TYPE VARCHAR(4),REG_NMS_LINK_TYPE VARCHAR(20),EXEC_SUB_TYPE VARCHAR(4),CMSN_TYPE VARCHAR(20)," +
        "QUOTE_CONDITION_IND VARCHAR(20),TRD_THROUGH_FG  VARCHAR(1),REGNMS_ORDER_LINK_ID VARCHAR(64)," +
        "REGNMS_ORDER_LINK_TYPE VARCHAR(20),DK_IND VARCHAR(20),NBBO_QUOTE_TIME VARCHAR(20),GLB_ROOT_SRC_SYS_ID VARCHAR(20)," +
        "TRD_REPORT_TYPE VARCHAR(20),REPORT_TO_EXCH_FG VARCHAR(1),CMPLN_COMMENT VARCHAR(256),DEAL_TYPE VARCHAR(4)," +
        "EXEC_COMMENTS VARCHAR(256),OPTAL_FIELDS VARCHAR(120),SPOT_REF_PRICE VARCHAR(20),DELTA_OVERRIDE VARCHAR(20)," +
        "UNDERLYING_PRICE VARCHAR(20),PRICE_DELTA VARCHAR(20),NORMALIZED_LIQUIDITY_IND VARCHAR(4),USER_AVG_PRICE VARCHAR(20)," +
        "LAST_EXEC_TS TIMESTAMP,LULD_LOWER_PRICE_BAND VARCHAR(20),LULD_UPPER_PRICE_BAND VARCHAR(20)," +
        "LULD_PRICE_BAND_TS TIMESTAMP,REMNG_QTY DECIMAL(18, 4),ORDER_QTY DECIMAL(18, 4),AMD_TS TIMESTAMP,SETL_CODE VARCHAR(50)," +
        "SETL_DATE VARCHAR(20),CUST_NM VARCHAR(50),EXEC_TYPE VARCHAR(50),TRDR_KEY VARCHAR(50),TRDR_NM VARCHAR(50)," +
        "FX_RATE VARCHAR(50),CUST_FX_RATE VARCHAR(50),PARENT_ORDER_SYS_NM VARCHAR(10),CNC_TYPE VARCHAR(50)," +
        "FEE_AMT DECIMAL(20, 2),FEE_CCY VARCHAR(10),BRKG_AMT DECIMAL(20, 2),BRKG_CCY VARCHAR(10),CLEAR VARCHAR(50)," +
        "PMT_FIX_DATE VARCHAR(20),FOLLOW_ON_FG  VARCHAR(1),FX_RATE_CCY_TO VARCHAR(10),FX_RATE_CCY_FROM VARCHAR(10)," +
        "CUST_FX_RATE_CCY_TO VARCHAR(10),CUST_FX_RATE_CCY_FROM VARCHAR(10),SYS_GFCID VARCHAR(20),CONTRA_SIDE VARCHAR(20)," +
        "OPT_CONTRACT_MULTIPLIER DECIMAL(10, 2),PRIOR_REF_PRICE_TS TIMESTAMP,SECURITY_SUB_TYPE VARCHAR(20)," +
        "MSG_DIRECTION VARCHAR(20),LEAF_SYS_EXEC_ID VARCHAR(64),LEAF_SRC_SYS VARCHAR(20),FIX_LAST_MKT VARCHAR(20)," +
        "FIX_CONTRA_BROKER_MNEMONIC VARCHAR(20),RIO_MSG_SRC VARCHAR(64),SNAPSHOT_TS TIMESTAMP,EXTERNAL_TRANS_TS TIMESTAMP," +
        "PRICE_CATEGORY VARCHAR(32),UNDERLYING_FX_RATE DECIMAL(18, 8),CONVERSION_RATE DECIMAL(18, 8)," +
        "TRANS_COMMENT VARCHAR(256),AGGRESSOR_FLAG VARCHAR(1))"
  
}
