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

object ValidateNWQueriesApp {

  def main(args: Array[String]) {
    val threadID = Thread.currentThread().getId
    val conf = new SparkConf().
        setAppName("ValidateNWQueriesApplication_" + threadID + "_" + System.currentTimeMillis())
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    val snc = SnappyContext(sc)
    // snc.sql("set spark.sql.shuffle.partitions=6")
    val dataFilesLocation: String = args(0)
    snc.setConf("dataFilesLocation", dataFilesLocation)
    NWQueries.snc = snc
    NWQueries.dataFilesLocation = dataFilesLocation
    val tableType = args(1)
    val fullResultSetValidation: Boolean = args(2).toBoolean
    val numRowsValidation: Boolean = args(4).toBoolean
    SnappyTestUtils.validateFullResultSet = fullResultSetValidation
    SnappyTestUtils.numRowsValidation = numRowsValidation
    SnappyTestUtils.tableType = tableType
    val isSmokeRun: Boolean = args(3).toBoolean

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    val outputFile = "ValidateNWQueriesApp_thread_" + threadID + "_" + System.currentTimeMillis +
        ".out"
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    var failedQueries = ""
    // scalastyle:off println
    val startTime = System.currentTimeMillis()
    pw.println(s"${SnappyTestUtils.logTime} ValidateQueries for ${tableType} tables " +
        s"started ..")
    if (isSmokeRun) {
      NWTestUtil.validateSelectiveQueriesFullResultSet(snc, tableType, pw, sqlContext)
    }
    else {
      failedQueries = NWTestUtil.validateQueries(snc, tableType, pw, sqlContext)
    }
    val finishTime = System.currentTimeMillis()
    val totalTime = (finishTime -startTime)/1000
    if (!failedQueries.isEmpty) {
      println(s"Validation failed for ${tableType} tables for queries ${failedQueries}. See " +
          s"${getCurrentDirectory}/${outputFile}")
      pw.println(s"${SnappyTestUtils.logTime} Total execution took ${totalTime} " +
          s"seconds.")
      pw.println(s"${SnappyTestUtils.logTime} Validation failed for ${tableType} " +
          s"tables for queries ${failedQueries}. ")
      pw.close()
      throw new Exception(s"Validation task failed for ${tableType} tables. See " +
          s"${getCurrentDirectory}/${outputFile}")
    }
    pw.println(s"${SnappyTestUtils.logTime} ValidateQueries for ${tableType} tables " +
        s"completed successfully in ${totalTime} seconds ")
    pw.close()
  }
}

