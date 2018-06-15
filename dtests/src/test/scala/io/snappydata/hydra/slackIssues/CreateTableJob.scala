/*
* Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

package io.snappydata.hydra.slackIssues

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config
import org.apache.spark.sql._

import scala.util.{Failure, Success, Try}

class CreateTableJob extends SnappySQLJob {

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    val outputFile = "CreateTableJob_" + jobConfig.getString("logFileName")
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    Try {
      snappySession.sql("create table if not exists mbl_test_scd(" +
          " ID BIGINT," +
          " DATEKEY INTEGER," +
          " CHECKIN_DATE INTEGER," +
          " CHECKOUT_DATE INTEGER," +
          " CRAWL_TIME INTEGER," +
          " BATCH SMALLINT," +
          " SOURCE SMALLINT," +
          " IS_HIGH_STAR SMALLINT," +
          " MT_POI_ID BIGINT," +
          " MT_ROOM_ID BIGINT," +
          " MT_BREAKFAST SMALLINT," +
          " MT_GOODS_ID BIGINT," +
          " MT_BD_ID INTEGER," +
          " MT_GOODS_VENDOR_ID BIGINT," +
          " MT_BUSINESS_TYPE SMALLINT," +
          " MT_ROOM_STATUS SMALLINT," +
          " MT_POI_UV INTEGER," +
          " MT_PRICE1 INTEGER," +
          " MT_PRICE2 INTEGER," +
          " MT_PRICE3 INTEGER," +
          " MT_PRICE4 INTEGER," +
          " MT_PRICE5 INTEGER," +
          " MT_PRICE6 INTEGER," +
          " MT_PRICE7 INTEGER," +
          " MT_PRICE8 INTEGER," +
          " MT_FLAG1 SMALLINT," +
          " MT_FLAG2 SMALLINT," +
          " MT_FLAG3 SMALLINT," +
          " COMP_SITE_ID INTEGER," +
          " COMP_POI_ID VARCHAR(200)," +
          " COMP_ROOM_ID BIGINT," +
          " COMP_BREAKFAST SMALLINT," +
          " COMP_GOODS_ID VARCHAR(200)," +
          " COMP_GOODS_VENDOR VARCHAR(200)," +
          " COMP_ROOM_STATUS SMALLINT," +
          " COMP_IS_PROMOTION SMALLINT," +
          " COMP_PAY_TYPE SMALLINT," +
          " COMP_GOODS_TYPE SMALLINT," +
          " COMP_PRICE1 INTEGER," +
          " COMP_PRICE2 INTEGER," +
          " COMP_PRICE3 INTEGER," +
          " COMP_PRICE4 INTEGER," +
          " COMP_PRICE5 INTEGER," +
          " COMP_PRICE6 INTEGER," +
          " COMP_PRICE7 INTEGER," +
          " COMP_PRICE8 INTEGER," +
          " COMP_FLAG1 SMALLINT," +
          " COMP_FLAG2 SMALLINT," +
          " COMP_FLAG3 SMALLINT," +
          " VALID_STATUS SMALLINT," +
          " GMT_TIME TIMESTAMP," +
          " VERSION TIMESTAMP) " +
          " using column options (" +
          " PARTITION_BY 'mt_poi_id'," +
          // " DISKSTORE 'mblStore1'," +
          " BUCKETS '383'," +
          // " COLOCATE_WITH 'OE_DIM_POI'," +
          " REDUNDANCY '0'," +
          " PERSISTENCE 'ASYNC', OVERFLOW 'true')")

      snappySession.sql("create table if not exists comp_5_min( " +
          " id bigint, " +
          " datekey int, " +
          " checkin_date int, " +
          " checkout_date int, " +
          " crawl_time bigint, " +
          " batch int, " +
          " source int, " +
          " is_high_star int, " +
          " mt_poi_id bigint, " +
          " mr_room_id bigint, " +
          " mt_breakfast int, " +
          " mt_goods_id bigint, " +
          " mt_bd_id int, " +
          " mt_goods_vendor_id bigint, " +
          " mr_business_type int, " +
          " mt_room_status int, " +
          " mt_poi_uv int, " +
          " mt_price1 int, " +
          " mt_price2 int, " +
          " mt_price3 int, " +
          " mt_price4 int, " +
          " mt_price5 int, " +
          " mt_price6 int, " +
          " mt_price7 int, " +
          " mt_price8 int, " +
          " mt_flag1 int, " +
          " mt_flag2 int, " +
          " mt_flag3 int, " +
          " comp_site_id int, " +
          " comp_poi_id varchar(200), " +
          " comp_room_id long, " +
          " comp_breakfast tinyint, " +
          " comp_goods_id varchar(200), " +
          " comp_goods_vendor varchar(200), " +
          " comp_room_status tinyint, " +
          " comp_is_promotion tinyint, " +
          " comp_pay_type tinyint, " +
          " comp_goods_type tinyint, " +
          " comp_price1 int, " +
          " comp_price2 int, " +
          " comp_price3 int, " +
          " comp_price4 int, " +
          " comp_price5 int, " +
          " comp_price6 int, " +
          " comp_price7 int, " +
          " comp_price8 int, " +
          " comp_flag1 tinyint, " +
          " comp_flag2 tinyint, " +
          " comp_flag3 tinyint, " +
          " valid_status tinyint, " +
          " gmt_time timestamp, " +
          " version timestamp, " +
          " interval_days int, " +
          " real_batch bigint, " +
          " start_time_long bigint, " +
          " end_time_long bigint, " +
          " start_time bigint, " +
          " end_time bigint, " +
          " start_real_batch bigint, " +
          " end_real_batch bigint, " +
          " flag int, " +
          " insert_time bigint " +
          " )USING column OPTIONS( " +
          " PARTITION_BY 'mt_poi_id', " +
          /* " DISKSTORE 'mblStore1', " + */
          /* " COLOCATE_WITH 'test_table', " + */
          " REDUNDANCY '0', " +
          " BUCKETS '383', " +
          " PERSISTENCE 'ASYNC', " +
          " OVERFLOW 'true')")

      snappySession.sql("create table if not exists oe_stream_comp_data_for_mbl( " +
          " id long, " +
          " datekey int, " +
          " checkin_date int, " +
          " checkout_date int, " +
          " crawl_time int, " +
          " batch tinyint, " +
          " source tinyint, " +
          " is_high_star tinyint, " +
          " mt_poi_id bigint, " +
          " mt_room_id bigint, " +
          " mt_breakfast tinyint, " +
          " mt_goods_id bigint, " +
          " mt_bd_id int, " +
          " mt_goods_vendor_id long, " +
          " mt_business_type tinyint, " +
          " mt_room_status tinyint, " +
          " mt_poi_uv int, " +
          " mt_price1 int, " +
          " mt_price2 int, " +
          " mt_price3 int, " +
          " mt_price4 int, " +
          " mt_price5 int, " +
          " mt_price6 int, " +
          " mt_price7 int, " +
          " mt_price8 int, " +
          " mt_flag1 tinyint, " +
          " mt_flag2 tinyint, " +
          " mt_flag3 tinyint, " +
          " comp_site_id int, " +
          " comp_poi_id varchar(200), " +
          " comp_room_id long, " +
          " comp_breakfast tinyint, " +
          " comp_goods_id varchar(200), " +
          " comp_goods_vendor varchar(200), " +
          " comp_room_status tinyint, " +
          " comp_is_promotion tinyint, " +
          " comp_pay_type tinyint, " +
          " comp_goods_type tinyint, " +
          " comp_price1 int, " +
          " comp_price2 int, " +
          " comp_price3 int, " +
          " comp_price4 int, " +
          " comp_price5 int, " +
          " comp_price6 int, " +
          " comp_price7 int, " +
          " comp_price8 int, " +
          " comp_flag1 tinyint, " +
          " comp_flag2 tinyint, " +
          " comp_flag3 tinyint, " +
          " valid_status tinyint, " +
          " gmt_time timestamp, " +
          " version timestamp, " +
          " interval_days int, " +
          " real_batch bigint, " +
          " start_time_long bigint, " +
          " end_time_long bigint, " +
          " start_time bigint, " +
          " end_time bigint, " +
          " start_real_batch bigint, " +
          " end_real_batch bigint, " +
          " flag int, " +
          " insert_time bigint " +
          " )USING column OPTIONS(" +
          " PARTITION_BY 'mt_poi_id', " +
          /* " DISKSTORE 'mblStore1', " +
          " COLOCATE_WITH 'OE_DIM_POI', " + */
          " REDUNDANCY '0', " +
          " BUCKETS '383', " +
          " PERSISTENCE 'ASYNC', " +
          " OVERFLOW 'true' " +
          " )")

      pw.close()

    } match {
      case Success(v) =>
        s"success"
      case Failure(e) =>
        throw e;
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = {
    SnappyJobValid()
  }

}
