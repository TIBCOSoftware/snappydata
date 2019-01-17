/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
package io.snappydata.hydra.concurrency

import java.io.{File, FileOutputStream, PrintWriter}

import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

import com.typesafe.config.Config

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

class SnappyConcurrencyTestJob extends SnappySQLJob {
  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    val snc = snappySession.sqlContext

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    val runAnalyticalQueries: Boolean = jobConfig.getString("runAnalyticalQueries").toBoolean
    val warmUpTimeSec = jobConfig.getString("warmUpTimeSec").toLong
    val totalTaskTime = jobConfig.getString("totalTaskTime").toLong
    val outputFile = "RunAnalyticalQueries_" + Thread.currentThread().getId + "_" +
        jobConfig.getString("logFileName")
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true))
    // val dataFilesLocation = jobConfig.getString("dataFilesLocation")
    val sc = SparkContext.getOrCreate()
    val sqlContext = SQLContext.getOrCreate(sc)
    Try {
      if (runAnalyticalQueries) {
        // scalastyle:off println
        pw.println(s"RunAnalyticalQueriesJob_ ${Thread.currentThread().getId} started  " +
            s"successfully at : " + System.currentTimeMillis)
        ConcTestUtils.runAnalyticalQueries(snc, pw, warmUpTimeSec, totalTaskTime)
      }
      pw.println(s"RunAnalyticalQueriesJob_ ${Thread.currentThread().getId} completed  " +
          s"successfully at : " + System.currentTimeMillis)
      pw.close()
    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/${outputFile}"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}
