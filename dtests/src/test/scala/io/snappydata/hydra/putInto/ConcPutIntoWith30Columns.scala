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
package io.snappydata.hydra.putInto

import java.io.{File, FileOutputStream, PrintWriter}
import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.config.Config
import io.snappydata.hydra.northwind.NWTestJob
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.sql.snappydata._
import scala.util.Random

object ConcPutIntoWith30Columns extends SnappySQLJob {
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    snSession.sql("set snappydata.cache.putIntoInnerJoinResultSize=10GB")
    // val numThreads = jobConfig.getString("numThreadsForConcExecution").toInt
    val blockSize = jobConfig.getString("blockSize").toLong
    val stepSize = jobConfig.getString("stepSize").toLong
    val tableName = jobConfig.getString("tableName")
    val maxResultWaitSec = jobConfig.getString("maxResultWaitSec").toLong
    // val fileCnt = jobConfig.getString("fileCnt").toInt
    /*  val fromVal = jobConfig.getString("fromVal").toInt
      val untilVal = jobConfig.getString("untilVal").toInt
      val filePath = jobConfig.getString("jsonFile")
  */ val pw = new PrintWriter(new FileOutputStream(new File("ConcPutIntoWith30ColumnsJob.out"), true))
    Try {
      runTasksWithChangingRangeForPutInto(snSession, blockSize, stepSize, tableName, maxResultWaitSec)
      // doInsert(snSession, stepSize, blockSize, tableName, fileCnt)
      //   loadDataFromJsonFile(snSession, filePath, tableName, fromVal, untilVal)
      // insertFromJsonFile(snSession, filePath, tableName, fromVal, untilVal, fileCnt)
      /* runTasksWithChangingRangeForPutInto(snSession, 100000L, 10000L)
      runTasksWithChangingRangeForPutInto(snSession, 150000L, 20000L)
      runTasksWithChangingRangeForPutInto(snSession, 200000L, 30000L)
      runTasksWithChangingRangeForPutInto(snSession, 500000L, 40000L)
      runTasksWithChangingRangeForPutInto(snSession, 1010000L, 10000L)
      runTasksWithChangingRangeForPutInto(snSession, 100000L, 50000L) */
      pw.close()
    } match {
      case Success(v) => pw.close()
        s"See ${NWTestJob.getCurrentDirectory}/ConcurrentPutIntoJob.out"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  def insertFromJsonFile(snSession: SnappySession, file: String, tableName: String,
                         fromVal: Int, untilVal: Int, fileCnt: Int): Unit = {
    // scalastyle:off println
    var rnd = Random.nextInt(fileCnt)
    if (rnd == 0) {
      rnd = rnd + 1
    }
    val file1 = file + "_" + rnd
    println("File picked to do insert is " + file1)
    for (i <- fromVal to untilVal) {
      val jsonDF = snSession.read.json(file1)
      jsonDF.write.putInto(tableName + i)
    }
  }

  def loadDataFromJsonFile(snSession: SnappySession, file: String, tableName: String,
                           fromVal: Int, untilVal: Int): Unit = {
    for (i <- fromVal to untilVal) {
      val jsonDF = snSession.read.json(file)
      jsonDF.write.putInto(tableName + i)
    }
  }

  /* def doInsert(snSession: SnappySession, startRange:
   Long, endRange: Long, tableName: String, fileCnt: Int): Unit = {
     var newStarRange = startRange
     var newEndRange = endRange
     for (j <- 1 to fileCnt) {
       snSession.sql(s"DROP TABLE IF EXISTS ${tableName} ")
       snSession.sql(s"create table ${tableName} (id string NOT NULL, data string," +
           s" data2 decimal, APPLICATION_ID string NOT NULL, ORDERGROUPID string," +
           s" PAYMENTADDRESS1 string, PAYMENTADDRESS2 string, PAYMENTCOUNTRY string," +
           s" PAYMENTSTATUS string, PAYMENTRESULT string, PAYMENTZIP string," +
           s" PAYMENTSETUP string, PROVIDER_RESPONSE_DETAILS string, PAYMENTAMOUNT string," +
           s" PAYMENTCHANNEL string, PAYMENTCITY string, PAYMENTSTATECODE string," +
           s"PAYMENTSETDOWN string,  PAYMENTREFNUMBER string, PAYMENTST string," +
           s"PAYMENTAUTHCODE string,  PAYMENTID string, PAYMENTMERCHID string," +
           s" PAYMENTHOSTRESPONSECODE string,PAYMENTNAME string," +
           s" PAYMENTOUTLETID string, PAYMENTTRANSTYPE string,  PAYMENTDATE string," +
           s" CLIENT_ID string, CUSTOMERID string) using column options" +
           s" (key_columns 'id,APPLICATION_ID', BUCKETS '128', REDUNDANCY '1')")
       for (i <- newStarRange until newEndRange) {
         snSession.sql(s"insert into ${tableName} select ${i}, " +
             s"'biggerDataForInsertsIntoTheTable1_' || ${i}, ${i} * 10.2 ," +
             s" 'APPLICATION_ID_' || ${i}, " +
             "'ORDERGROUPID_' || 'id', 'PAYMENTADDRESS1' ||'id', 'PAYMENTADDRESS2' ||'id'," +
             " 'PAYMENTCOUNTRY' ||'id'," +
             " 'PAYMENTSTATUS' ||'id', 'PAYMENTRESULT' ||'id', 'PAYMENTZIP' ||'id'," +
             " 'PAYMENTSETUP' ||'id'," +
             " 'PROVIDER_RESPONSE_DETAILS' ||'id', 'PAYMENTAMOUNT' ||'id'," +
             " 'PAYMENTCHANNEL' ||'id', " +
             " 'PAYMENTCITY' ||'id', 'PAYMENTSTATECODE' ||'id', 'PAYMENTSETDOWN' ||'id',  " +
             " 'PAYMENTREFNUMBER' ||'id', 'PAYMENTST' ||'id', " +
             "  '', '', '', '', '', '', '', '', '', '' ")
       }
       newStarRange = startRange + newEndRange
       newEndRange = newStarRange + endRange
       // scalastyle:off println
       println("FinalStart range = " + newStarRange + " FinalEnd Range = " +
           newEndRange + " filecnt = " + j)
       val df = snSession.table(tableName)
       df.repartition(1).write.json("/export/shared/QA_DATA/jsonFile/insertFile"
           + endRange + "_" + j)

     }
   } */

  def runTasksWithChangingRangeForPutInto(snSession: SnappySession, blockSize:
  Long, stepSize: Long, tableName: String, maxResultWaitSec: Long): Unit = {
    val globalId = new AtomicInteger()
    val doPut = () => Future {
      val myId = globalId.getAndIncrement()
      for (i <- (myId * 1) until 1) {
        snSession.sql(s"put into ${tableName} select id, " +
            "'biggerDataForInsertsIntoTheTable1_' || id, id * 10.2 , 'APPLICATION_ID_' || id, " +
            "'ORDERGROUPID_' || 'id', 'PAYMENTADDRESS1' ||'id', 'PAYMENTADDRESS2' ||'id'," +
            " 'PAYMENTCOUNTRY' ||'id'," +
            " 'PAYMENTSTATUS' ||'id', 'PAYMENTRESULT' ||'id', 'PAYMENTZIP' ||'id'," +
            " 'PAYMENTSETUP' ||'id'," +
            " 'PROVIDER_RESPONSE_DETAILS' ||'id', 'PAYMENTAMOUNT' ||'id'," +
            " 'PAYMENTCHANNEL' ||'id', " +
            " 'PAYMENTCITY' ||'id', 'PAYMENTSTATECODE' ||'id', 'PAYMENTSETDOWN' ||'id',  " +
            " 'PAYMENTREFNUMBER' ||'id', 'PAYMENTST' ||'id', " +
            "  '', '', '', '', '', '', '', '', '', '' " +
            s" from range(${stepSize}, ${blockSize})")
        val df = snSession.table(tableName)
        df.repartition(1).write.json("/export/shared/QA_DATA/jsonFile/" + stepSize)
      }
    }
    val doQuery = () => Future {
      val myId = globalId.getAndIncrement()
      val randomInt = new Random().nextInt(32767)
      val startTime = System.currentTimeMillis
      val endTime = startTime + maxResultWaitSec * 1000
      do {
        snSession.sql(s"select avg(id), max(data), last(data2) from ${tableName} " +
            s"where id <> ${myId + randomInt}")
      } while (endTime > System.currentTimeMillis);

    }
    val putTasks = Array.fill(1)(doPut())
    val queryTasks = Array.fill(1)(doQuery())

    putTasks.foreach(Await.result(_, Duration.Inf))
    queryTasks.foreach(Await.result(_, Duration.Inf))
  }

  def saveDataAsJson(snSession: SnappySession): Unit = {
    val tableDf = snSession.table("testL")
    tableDf.write.json("");
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}


