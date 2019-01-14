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
import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}

import scala.util.{Failure, Random, Success, Try}

import com.typesafe.config.Config
import io.snappydata.hydra.SnappyTestUtils
import org.apache.commons.lang.RandomStringUtils

import org.apache.spark.SparkContext
import org.apache.spark.sql._

case class mbl_test_scd(ID: BigInt, DATEKEY: Integer, CHECKIN_DATE: Integer, CHECKOUT_DATE: Integer,
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

case class oe_stream_comp_data_for_mbl(ID: BigInt, DATEKEY: Integer, CHECKIN_DATE: Integer, CHECKOUT_DATE: Integer,
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
    VALID_STATUS: Integer, GMT_TIME: Timestamp, VERSION: Timestamp, INTERVAL_DAYS: Integer,
    REAL_BATCH: BigInt, START_TIME_LONG: BigInt, END_TIME_LONG: BigInt, START_TIME: BigInt,
    END_TIME: BigInt, START_REAL_BATCH: BigInt, END_REAL_BATCH: BigInt, FLAG: Integer, INSERT_TIME: BigInt)

class LoadDataAndValidateQueriesJob extends SnappySQLJob {

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    val snc = snappySession.sqlContext

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    val outputFile = "LoadDataAndValidateQueriesJob_" + jobConfig.getString("logFileName")
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    val sc = SparkContext.getOrCreate()
    val sqlContext = SQLContext.getOrCreate(sc)
    Try {
      val numRows = jobConfig.getString("numRows").toLong
      val ingestData = jobConfig.getString("ingestData").toBoolean

      import snappySession.implicits._

      val sc = snappySession.sparkContext
      val idNum: Long = 0
      val rows = numRows
      val dataRDD = sc.range(idNum, rows).mapPartitions { itr =>
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
          mbl_test_scd((id + 1).toInt, rnd.nextInt(Integer.MAX_VALUE),
            rnd.nextInt(Integer.MAX_VALUE),
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

      val dataRDD1 = sc.range(idNum, rows).mapPartitions { itr =>
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
          oe_stream_comp_data_for_mbl((id + 1).toInt, rnd.nextInt(Integer.MAX_VALUE),
            rnd.nextInt(Integer.MAX_VALUE),
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
            rnd.nextInt(32767), time, time, rnd.nextInt(32767), rnd.nextInt(Integer.MAX_VALUE),
            rnd.nextInt(Integer.MAX_VALUE), rnd.nextInt(Integer.MAX_VALUE),
            rnd.nextInt(Integer.MAX_VALUE), rnd.nextInt(Integer.MAX_VALUE),
            rnd.nextInt(Integer.MAX_VALUE), rnd.nextInt(Integer.MAX_VALUE),
            rnd.nextInt(Integer.MAX_VALUE), rnd.nextInt(Integer.MAX_VALUE))
        }
      }

      val qDF = snappySession.createDataset(dataRDD)
      val cacheDF = qDF.cache();
      cacheDF.write.insertInto("mbl_test_scd")
      val qDF3 = snappySession.createDataset(dataRDD1)
      val cacheDF3 = qDF3.cache()
      cacheDF3.write.insertInto("oe_stream_comp_data_for_mbl")

      // val tempDir: File = new File("/export/shared/QA_DATA/slackIssues")
      /* val tempDir: File = new File("/data/snappyHydraLogs/slackIssues_table2")
      if (tempDir.exists()) FileUtils.deleteDirectory(tempDir)
      cacheDF3.repartition(1).write.csv("/export/shared/QA_DATA/slackIssues_table2") */

      // cacheDF.write.parquet("/data/snappyHydraLogs/slackIssues")
      // cacheDF.repartition(1).write.csv("/data/snappyHydraLogs/slackIssues/slackIssues")
      // val qDFSpark = sqlContext.read.load("/data/snappyHydraLogs/slackIssues")
      /* val qDFSpark = sqlContext.read.load("/export/shared/QA_DATA/slackIssues")
       qDFSpark.registerTempTable("mbl_test_scd")
       sqlContext.cacheTable("mbl_test_scd")
       validateQueries(snc, sqlContext, pw) */

      if (ingestData) {
        for (_ <- 1 to 70) {
          val qDF1 = snappySession.createDataset(dataRDD)
          val cacheDF1 = qDF1.cache()
          cacheDF1.write.insertInto("mbl_test_scd")
        }
        for (_ <- 1 to 10) {
          val qDF2 = snappySession.createDataset(dataRDD1)
          val cacheDF2 = qDF2.cache()
          cacheDF2.write.insertInto("comp_5_min")
        }
        for (_ <- 1 to 150) {
          val qDF3 = snappySession.createDataset(dataRDD1)
          val cacheDF3 = qDF3.cache()
          cacheDF3.write.insertInto("oe_stream_comp_data_for_mbl")
        }

        validateQueries(snc, sqlContext, pw)
      }
      pw.close()

    } match {
      case Success(v) =>
        s"success"
      case Failure(e) =>
        throw e;
    }
  }

  def validateQueries(snc: SnappyContext, sqlContext: SQLContext, pw: PrintWriter): Unit = {
    var start = System.currentTimeMillis
    sqlContext.table("mbl_test_scd").count()
    var end = System.currentTimeMillis
    // scalastyle:off println
    pw.println(s"\nTime to load into table mbl_test_scd in spark= " +
        (end - start) + " ms")
    pw.println(s"ValidateQueriesFullResultSet job started at" +
        s" :  " + System.currentTimeMillis)
    var query = "select datekey, count(1) from mbl_test_scd group by datekey order by datekey asc";
    start = System.currentTimeMillis
    SnappyTestUtils.assertQueryFullResultSet(snc, query, "Q1", "column", pw, sqlContext)
    end = System.currentTimeMillis
    pw.println(s"\nExecution Time for $query: " +
        (end - start) + " ms")
    query = "select count(*) from mbl_test_scd"
    start = System.currentTimeMillis
    SnappyTestUtils.assertQueryFullResultSet(snc, query, "Q2", "column", pw, sqlContext)
    end = System.currentTimeMillis
    pw.println(s"\nExecution Time for $query: " +
        (end - start) + " ms")
    query = "select ID from mbl_test_scd group by ID order by ID asc"
    start = System.currentTimeMillis
    SnappyTestUtils.assertQueryFullResultSet(snc, query, "Q3", "column", pw, sqlContext)
    end = System.currentTimeMillis
    pw.println(s"\nExecution Time for $query: " +
        (end - start) + " ms")
    query = "select COMP_PRICE1, VALID_STATUS from mbl_test_scd" +
        " WHERE ID >= 25000 AND ID <= 50000000"
    start = System.currentTimeMillis
    SnappyTestUtils.assertQueryFullResultSet(snc, query, "Q4", "column", pw, sqlContext)
    end = System.currentTimeMillis
    pw.println(s"\nExecution Time for $query: " +
        (end - start) + " ms")
    query = "select CHECKIN_DATE, CHECKOUT_DATE from mbl_test_scd WHERE ID IN (500, 25000000)"
    start = System.currentTimeMillis
    SnappyTestUtils.assertQueryFullResultSet(snc, query, "Q5", "column", pw, sqlContext)
    end = System.currentTimeMillis
    pw.println(s"\nExecution Time for $query: " +
        (end - start) + " ms")
    pw.println(s"ValidateQueriesFullResultSet job completed  " +
        s"successfully at : " + System.currentTimeMillis)
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = {
    SnappyJobValid()
  }

}
