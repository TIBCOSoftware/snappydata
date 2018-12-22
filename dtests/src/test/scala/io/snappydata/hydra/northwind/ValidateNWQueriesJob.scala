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
package io.snappydata.hydra.northwind

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config
import io.snappydata.hydra.northwind
import org.apache.spark.SparkContext
import org.apache.spark.sql._

import scala.util.{Failure, Success, Try}

class ValidateNWQueriesJob extends SnappySQLJob {
  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    val snc = snappySession.sqlContext

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    val tableType = jobConfig.getString("tableType")
    val outputFile = "ValidateNWQueries_" + tableType + "_" + jobConfig.getString("logFileName")
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    val isSmokeRun: Boolean = jobConfig.getString("isSmokeRun").toBoolean
    val fullResultSetValidation: Boolean = jobConfig.getString("fullResultSetValidation").toBoolean
    val numRowsValidation: Boolean = jobConfig.getString("numRowsValidation").toBoolean
    val sc = SparkContext.getOrCreate()
    val sqlContext = SQLContext.getOrCreate(sc)
    Try {
      snc.sql("set spark.sql.shuffle.partitions=23")
      val dataFilesLocation = jobConfig.getString("dataFilesLocation")
      snc.setConf("dataFilesLocation", dataFilesLocation)
      northwind.NWQueries.snc = snc
      NWQueries.dataFilesLocation = dataFilesLocation
      if (numRowsValidation) {
        // scalastyle:off println
        pw.println(s"Validate ${tableType} tables Queries Test started at : " + System
            .currentTimeMillis)
        NWTestUtil.validateQueries(snc, tableType, pw)
        pw.println(s"Validate ${tableType} tables Queries Test completed successfully at : " +
            System.currentTimeMillis)
      }
      if (fullResultSetValidation) {
        pw.println(s"createAndLoadSparkTables Test started at : " + System.currentTimeMillis)
        NWTestUtil.createAndLoadSparkTables(sqlContext)
        println(s"createAndLoadSparkTables Test completed successfully at : " + System
            .currentTimeMillis)
        pw.println(s"createAndLoadSparkTables Test completed successfully at : " + System
            .currentTimeMillis)
        pw.println(s"ValidateQueriesFullResultSet for ${tableType} tables Queries Test started at" +
            s" :  " + System.currentTimeMillis)
        if (isSmokeRun) {
          NWTestUtil.validateSelectiveQueriesFullResultSet(snc, tableType, pw, sqlContext)
        }
        else {
          NWTestUtil.validateQueriesFullResultSet(snc, tableType, pw, sqlContext)
        }
        pw.println(s"validateQueriesFullResultSet ${tableType} tables Queries Test completed  " +
            s"successfully at : " + System.currentTimeMillis)
      }
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
