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
package io.snappydata.hydra.northwind

import java.io.{File, FileOutputStream, PrintWriter}

import io.snappydata.hydra.SnappyTestUtils

import org.apache.spark.sql.{SQLContext, SnappyContext}
import org.apache.spark.{SparkConf, SparkContext}

object ValidateNWQueriesWithChangingConstantsApp {

  def main(args: Array[String]) {
    val conf = new SparkConf().
        setAppName("ValidateNWQueries Application_" + System.currentTimeMillis())
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    val snc = SnappyContext(sc)
    // snc.sql("set spark.sql.shuffle.partitions=6")
    val dataFilesLocation: String = args(0)
    snc.setConf("dataFilesLocation", dataFilesLocation)
    NWQueries.snc = snc
    NWQueries.dataFilesLocation = dataFilesLocation
    val tableType = args(1)
    val threadID = Thread.currentThread().getId
    SnappyTestUtils.validateFullResultSet = true
    SnappyTestUtils.numRowsValidation = true
    SnappyTestUtils.tableType = tableType

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    val outputFile = "ValidateNWQueriesApp_thread_" + threadID + "_" + System.currentTimeMillis +
        ".out"
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    // scalastyle:off println
    var startTime = System.currentTimeMillis()
    pw.println(s"${SnappyTestUtils.logTime} createAndLoadSparkTables started.. ")
    NWTestUtil.createAndLoadSparkTables(sqlContext)
    var finishTime = System.currentTimeMillis()
    var totalTime = (finishTime - startTime) / 1000
    pw.println(s"${SnappyTestUtils.logTime} createAndLoadSparkTables completed successfully in "
        + s"$totalTime secs.")
    pw.flush()
    pw.println(s"${SnappyTestUtils.logTime} Validation for ${tableType} tables queries started..")
    startTime = System.currentTimeMillis()
    val failedQueries: String = NWTestUtil.executeAndValidateQueriesByChangingConstants(snc,
      tableType, pw, sqlContext)
    finishTime = System.currentTimeMillis()
    totalTime = (finishTime - startTime) / 1000
    if (!failedQueries.isEmpty) {
      println(s"Validation failed for ${tableType} tables for queries ${failedQueries}. See " +
          s"${getCurrentDirectory}/${outputFile}")
      pw.println(s"${SnappyTestUtils.logTime} Total execution took ${totalTime} seconds.")
      pw.println(s"${SnappyTestUtils.logTime} Validation failed for ${tableType} tables for " +
          s"queries ${failedQueries}. ")
      pw.close()
      throw new Exception(s"Validation failed for ${tableType} tables. See " +
          s"${getCurrentDirectory}/${outputFile}")
    }
    pw.println(s"ValidateQueries for $tableType tables completed successfully in $totalTime secs.")
    pw.close()
  }
}

