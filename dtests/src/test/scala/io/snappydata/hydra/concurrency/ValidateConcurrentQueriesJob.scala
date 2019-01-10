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

import com.typesafe.config.Config
import io.snappydata.hydra.northwind
import io.snappydata.hydra.northwind.{NWQueries, NWTestUtil}
import org.apache.spark.SparkContext
import org.apache.spark.sql._

import scala.util.{Failure, Success, Try}

class ValidateConcurrentQueriesJob extends SnappySQLJob {
  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    val snc = snappySession.sqlContext

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    val tableType = jobConfig.getString("tableType")
    val runPLQueries: Boolean = jobConfig.getString("runPLQueries").toBoolean
    val runAnalyticalQueries: Boolean = jobConfig.getString("runAnalyticalQueries").toBoolean
    val runMixedQueries: Boolean = jobConfig.getString("runMixedQueries").toBoolean
    val outputFile = "ValidateConcNWQueries_" + tableType + "_" + jobConfig.getString("logFileName")
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    val dataFilesLocation = jobConfig.getString("dataFilesLocation")
    snc.setConf("dataFilesLocation", dataFilesLocation)
    northwind.NWQueries.snc = snc
    NWQueries.dataFilesLocation = dataFilesLocation
    val sc = SparkContext.getOrCreate()
    val sqlContext = SQLContext.getOrCreate(sc)
    Try {
      snc.sql("set spark.sql.shuffle.partitions=23")
      // scalastyle:off println
      pw.println(s"createAndLoadSparkTables Test started at : " + System.currentTimeMillis)
      NWTestUtil.createAndLoadSparkTables(sqlContext)
      println(s"createAndLoadSparkTables Test completed successfully at : " + System
          .currentTimeMillis)
      pw.println(s"createAndLoadSparkTables Test completed successfully at : " + System
          .currentTimeMillis)
      pw.println(s"ValidateConcQueriesFullResultSet for ${tableType} tables Queries Test started " +
          s"at:  " + System.currentTimeMillis)
      if (runMixedQueries) {
      ConcTestUtils.validatePointLookUPQueriesFullResultSet(snc, tableType, pw, sqlContext)
      ConcTestUtils.validateAnalyticalQueriesFullResultSet(snc, tableType, pw, sqlContext)
      }
      else if (runPLQueries){
        ConcTestUtils.validatePointLookUPQueriesFullResultSet(snc, tableType, pw, sqlContext)
      }
      else if (runAnalyticalQueries){
        ConcTestUtils.validateAnalyticalQueriesFullResultSet(snc, tableType, pw, sqlContext)
      }
      pw.println(s"validateConcQueriesFullResultSet ${tableType} tables Queries Test completed  " +
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
