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

import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}

import scala.util.{Failure, Random, Success, Try}

import com.typesafe.config.Config
import org.apache.commons.lang.RandomStringUtils

import org.apache.spark.sql._

case class Test_Table(ID: BigInt, DATEKEY: Integer, CHECKIN_DATE: Integer, CHECKOUT_DATE: Integer,
    CRAWL_TIME: Integer, BATCH: Integer, SOURCE: Integer, IS_HIGH_STAR: Integer,
    MT_POI_ID: BigInt, MT_ROOM_ID: BigInt, MT_BREAKFAST: Integer, MT_GOODS_ID: BigInt,
    MT_BD_ID: Integer, MT_GOODS_VENDOR_ID: BigInt, MT_BUSINESS_TYPE: Integer,
    MT_ROOM_STATUS: Integer, MT_POI_UV: Integer, MT_PRICE1: Integer, MT_PRICE2: Integer,
    MT_PRICE3: Integer, MT_PRICE4: Integer, MT_PRICE5: Integer, MT_PRICE6: Integer,
    MT_PRICE7: Integer, MT_PRICE8: Integer, MT_FLAG1: Integer, MT_FLAG2: Integer,
    MT_FLAG3: Integer, COMP_SITE_ID: Integer, COMP_POI_ID: String, COMP_ROOM_ID: BigInt,
    COMP_BREAKFAST: Integer, COMP_GOODS_ID: String, COMP_GOODS_VENDOR: String,
    COMP_ROOM_STATUS: Integer, COMP_IS_PROMOTION: Integer, COMP_PAY_TYPE: Integer,
    COMP_GOODS_TYPE: Integer, COMP_PRICE1: Integer, COMP_PRICE2: Integer, COMP_PRICE3: Integer,
    COMP_PRICE4: Integer, COMP_PRICE5: Integer, COMP_PRICE6: Integer, COMP_PRICE7: Integer,
    COMP_PRICE8: Integer, COMP_FLAG1: Integer, COMP_FLAG2: Integer, COMP_FLAG3: Integer,
    VALID_STATUS: Integer, GMT_TIME: Timestamp, VERSION: Timestamp)

class CreateAndLoadData extends SnappySQLJob {

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    Try {
      val numRows = jobConfig.getString("numRows").toLong
      snappySession.sql("drop table if exists APP.test_table")
      snappySession.sql("create table APP.test_table(" +
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

      import snappySession.implicits._

      val sc = snappySession.sparkContext
      val dataRDD = sc.range(0, numRows).mapPartitions { itr =>
        val rnd = new Random()
        // val dateTypes = ALL_DATETYPES.map(Int)
        // val dateTypesSize = dateTypes.length
        var day = 0
        val zoneId = ZoneId.systemDefault()
        var cal = ZonedDateTime.of(2016, 6, day + 6, 0, 0, 0, 0, zoneId)
        var millisTime = cal.toInstant.toEpochMilli
        itr.map { id =>
          val datetype = rnd.nextInt(30) + 20180301
          // dateTypes(math.abs(rnd.nextInt() % dateTypesSize))
          val gid = (id % 400).toInt
          // reset the timestamp every once in a while
          if (gid == 0) {
            // seconds < 59 so that millis+gid does not overflow into next hour
            cal = ZonedDateTime.of(2016, 6, day + 6, rnd.nextInt() & 0x07,
              math.abs(rnd.nextInt() % 60), math.abs(rnd.nextInt() % 59),
              math.abs(rnd.nextInt() % 1000000000), zoneId)
            millisTime = cal.toInstant.toEpochMilli
          }
          val time = new Timestamp(millisTime + gid)
          Test_Table((id + 1).toInt, rnd.nextInt(Integer.MAX_VALUE), rnd.nextInt(Integer.MAX_VALUE),
            rnd.nextInt(Integer.MAX_VALUE), rnd.nextInt(Integer.MAX_VALUE), rnd.nextInt(32767),
            rnd.nextInt(32767), rnd.nextInt(32767), rnd.nextInt(Integer.MAX_VALUE),
            rnd.nextInt(Integer.MAX_VALUE), rnd.nextInt(32767), rnd.nextInt(Integer.MAX_VALUE),
            rnd.nextInt(Integer.MAX_VALUE), rnd.nextInt(Integer.MAX_VALUE), rnd.nextInt(32767),
            rnd.nextInt(32767), rnd.nextInt(Integer.MAX_VALUE), rnd.nextInt(Integer.MAX_VALUE),
            rnd.nextInt(Integer.MAX_VALUE), rnd.nextInt(Integer.MAX_VALUE),
            rnd.nextInt(Integer.MAX_VALUE), rnd.nextInt(Integer.MAX_VALUE),
            rnd.nextInt(Integer.MAX_VALUE), rnd.nextInt(Integer.MAX_VALUE),
            rnd.nextInt(Integer.MAX_VALUE), rnd.nextInt(32767), rnd.nextInt(32767),
            rnd.nextInt(32767), rnd.nextInt(Integer.MAX_VALUE),
            RandomStringUtils.random(30, true, false), rnd.nextInt(Integer.MAX_VALUE),
            rnd.nextInt(32767), RandomStringUtils.random(30, true, false),
            RandomStringUtils.random(30, true, false),
            rnd.nextInt(32767), rnd.nextInt(32767), rnd.nextInt(32767),
            rnd.nextInt(32767), rnd.nextInt(Integer.MAX_VALUE), rnd.nextInt(Integer.MAX_VALUE),
            rnd.nextInt(Integer.MAX_VALUE), rnd.nextInt(Integer.MAX_VALUE),
            rnd.nextInt(Integer.MAX_VALUE), rnd.nextInt(Integer.MAX_VALUE),
            rnd.nextInt(Integer.MAX_VALUE), rnd.nextInt(Integer.MAX_VALUE),
            rnd.nextInt(32767), rnd.nextInt(32767), rnd.nextInt(32767),
            rnd.nextInt(32767), time, time)
        }
      }
      val qDF = snappySession.createDataset(dataRDD)
      qDF.write.insertInto("test_table")
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
