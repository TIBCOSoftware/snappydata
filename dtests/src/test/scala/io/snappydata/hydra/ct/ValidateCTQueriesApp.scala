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

package io.snappydata.hydra.ct

import java.io.{File, FileOutputStream, PrintWriter}

import org.apache.spark.sql.{SQLContext, SnappyContext}
import org.apache.spark.{SparkContext, SparkConf}

object ValidateCTQueriesApp {

  def main(args: Array[String]) {
    val connectionURL = args(args.length - 1)
    val conf = new SparkConf().
        setAppName("ValidateCTQueriesApp Application_" + System.currentTimeMillis()).
        set("snappydata.connection", connectionURL)
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    val snc = SnappyContext(sc)

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    val threadID = Thread.currentThread().getId
    val outputFile = "ValidateCTQueriesApp_thread_" + threadID + "_" + System.currentTimeMillis +
        ".out"
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true))
    val dataFilesLocation = args(0)
    snc.setConf("dataFilesLocation", dataFilesLocation)
    // scalastyle:off println
    pw.println(s"dataFilesLocation : ${dataFilesLocation}")
    CTQueries.snc = snc
    val tableType = args(1)
    val fullResultSetValidation: Boolean = args(2).toBoolean
    pw.println(s"Validation for queries with ${tableType} tables started")
    if (fullResultSetValidation) {
      pw.println(s"Test will perform fullResultSetValidation")
    }
    else {
      pw.println(s"Test will not perform fullResultSetValidation")
    }
    val startTime = System.currentTimeMillis
    val failedQueries = CTTestUtil.executeQueries(snc, tableType, pw, fullResultSetValidation,
      sqlContext, false, false)
    val endTime = System.currentTimeMillis
    val totalTime = (endTime - startTime) / 1000
    pw.println(s"Total time for execution is :: ${totalTime} seconds.")
    if (!failedQueries.isEmpty) {
      println(s"Validation failed for ${tableType} for queries ${failedQueries}.. See " +
          s"${getCurrentDirectory}/${outputFile}")
      pw.println(s"Validation failed for ${tableType} for queries ${failedQueries}. ")
      pw.close()
      throw new Exception(s"Validation task failed for ${tableType}. See " +
          s"${getCurrentDirectory}/${outputFile}")
    }
    println(s"Validation for queries with ${tableType} tables completed successfully. See " +
        s"${getCurrentDirectory}/${outputFile}")
    pw.println(s"Validation for queries with ${tableType} tables completed successfully.")
    pw.close()
  }
}
