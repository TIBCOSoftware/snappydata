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

package io.snappydata.hydra.ct

import java.io.{File, FileOutputStream, PrintWriter}

import io.snappydata.hydra.SnappyTestUtils

import org.apache.spark.sql.{SQLContext, SnappyContext}
import org.apache.spark.{SparkConf, SparkContext}

object ValidateCTQueriesApp {

  def main(args: Array[String]) {

    val threadID = Thread.currentThread().getId
    val conf = new SparkConf().
        setAppName("ValidateCTQueriesApp_" + threadID + "_" + System.currentTimeMillis())
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    val snc = SnappyContext(sc)

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    val outputFile = "ValidateCTQueriesApp_thread_" + threadID + "_" + System.currentTimeMillis +
        ".out"
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true))
    val dataFilesLocation = args(0)
    snc.setConf("dataFilesLocation", dataFilesLocation)
    // scalastyle:off println
    CTQueries.snc = snc
    val tableType = args(1)
    val fullResultSetValidation: Boolean = args(2).toBoolean
    val numRowsValidation: Boolean = args(3).toBoolean
    SnappyTestUtils.numRowsValidation = numRowsValidation
    SnappyTestUtils.validateFullResultSet = fullResultSetValidation
    pw.println(s"${SnappyTestUtils.logTime} Validation for queries with ${tableType} " +
        s"tables started")

    val startTime = System.currentTimeMillis
    val failedQueries = CTTestUtil.executeQueries(snc, tableType, pw, sqlContext)
    val endTime = System.currentTimeMillis
    val totalTime = (endTime - startTime) / 1000
    pw.println(s"${SnappyTestUtils.logTime} Total time for execution is :" +
        s" ${totalTime} seconds.")
    if (!failedQueries.isEmpty) {
      println(s"Validation failed for ${tableType} tables for queries ${failedQueries}. See " +
          s"${getCurrentDirectory}/${outputFile}")
      pw.println(s"${SnappyTestUtils.logTime} Validation failed for ${tableType} " +
          s"tables for queries ${failedQueries}. ")
      pw.close()
      throw new Exception(s"Validation task failed for ${tableType}. See " +
          s"${getCurrentDirectory}/${outputFile}")
    }
    println(s"Validation for queries with ${tableType} tables completed successfully. See " +
        s"${getCurrentDirectory}/${outputFile}")
    pw.println(s"${SnappyTestUtils.logTime} Validation for queries with ${tableType} " +
        s"tables completed successfully.")
    pw.close()
  }
}
