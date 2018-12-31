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

import scala.util.{Failure, Success, Try}

import com.typesafe.config.Config
import io.snappydata.hydra.{SnappyTestUtils, northwind}

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import _root_.util.TestException

class ValidateNWQueriesWithChangingConstantsJob extends SnappySQLJob {
  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    val snc = snappySession.sqlContext

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    val tableType = jobConfig.getString("tableType")
    val outputFile = "ValidateNWQueries_" + tableType + "_" + jobConfig.getString("logFileName")
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    val sc = SparkContext.getOrCreate()
    val sqlContext = SQLContext.getOrCreate(sc)
    SnappyTestUtils.validateFullResultSet = true
    SnappyTestUtils.numRowsValidation = true
    SnappyTestUtils.tableType = tableType
    Try {
      snc.sql("set spark.sql.shuffle.partitions=23")
      val dataFilesLocation = jobConfig.getString("dataFilesLocation")
      snc.setConf("dataFilesLocation", dataFilesLocation)
      northwind.NWQueries.snc = snc
      NWQueries.dataFilesLocation = dataFilesLocation
      // scalastyle:off println
      var startTime = System.currentTimeMillis()
      pw.println(s"${SnappyTestUtils.logTime} createAndLoadSparkTables started.")
      NWTestUtil.createAndLoadSparkTables(sqlContext)
      var finishTime = System.currentTimeMillis()
      var totalTime = (finishTime - startTime) / 1000
      pw.println(s"${SnappyTestUtils.logTime} createAndLoadSparkTables completed successfully in " +
          s"$totalTime secs.")
      pw.flush()
      pw.println(s"${SnappyTestUtils.logTime} Validation for ${tableType} tables queries started..")
      startTime = System.currentTimeMillis()
      val failedQueries: String = NWTestUtil.executeAndValidateQueriesByChangingConstants(snc,
        tableType, pw, sqlContext)
      finishTime = System.currentTimeMillis()
      totalTime = (finishTime - startTime) / 1000
      if (!failedQueries.isEmpty) {
        println(s"Validation failed for ${tableType} tables for queries ${failedQueries}. " +
            s"See ${getCurrentDirectory}/${outputFile}")
        pw.println(s"${SnappyTestUtils.logTime} Total execution took ${totalTime} seconds.")
        pw.println(s"${SnappyTestUtils.logTime} Validation failed for ${tableType} tables for " +
            s"queries ${failedQueries}. ")
        pw.close()
        throw new TestException(s"Validation failed for ${tableType}. " +
            s"See ${getCurrentDirectory}/${outputFile}")
      }
      pw.println(s"${SnappyTestUtils.logTime} ValidateQueries for $tableType tables completed " +
          s"successfully in $totalTime secs.")
      pw.close()
    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/${outputFile}"
      case Failure(e) => pw.close();
        throw new TestException(s"Validation failed. See ${getCurrentDirectory}/${outputFile}");
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}
