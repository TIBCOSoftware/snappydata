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
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.snappydata._
import scala.util.Random

object InsertFromJsonFile extends SnappySQLJob {
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    // snSession.sql("set snappydata.cache.putIntoInnerJoinResultSize=10GB")
    val tableName = jobConfig.getString("tableName")
    val fileCnt = jobConfig.getString("fileCnt").toInt
    val fromVal = jobConfig.getString("fromVal").toInt
    val untilVal = jobConfig.getString("untilVal").toInt
    val filePath = jobConfig.getString("jsonFile")
    val maxResultWaitSec = jobConfig.getString("maxResultWaitSec").toLong
    val pw = new PrintWriter(new FileOutputStream(new File("InsertFromJsonFileJob.out"), true))
    Try {
      insertFromJsonFile(snSession, filePath, tableName, fromVal, untilVal, fileCnt)
      doSelectQueries(snSession, tableName, fromVal, untilVal, maxResultWaitSec)
      pw.close()
    } match {
      case Success(v) => pw.close()
        s"See ${NWTestJob.getCurrentDirectory}/InsertFromJsonFileJob.out"
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

  def doSelectQueries(snSession: SnappySession, tableName: String,
                      fromVal: Int, untilVal: Int, maxResultWaitSec: Long): Unit = {
    val globalId = new AtomicInteger()
    val randomInt = new Random().nextInt(32767)
    val startTime = System.currentTimeMillis
    val endTime = startTime + maxResultWaitSec * 1000
    do {
      val myId = globalId.getAndIncrement()
      val df = snSession.sql(s"select avg(id), max(data), last(data2) from ${tableName} " +
          s"where id <> ${myId + randomInt}")
      df.collect()
    } while (endTime > System.currentTimeMillis);
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}


