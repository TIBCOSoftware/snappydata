/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

import io.snappydata.test.util.TestException

import org.apache.spark.sql.{SQLContext, SnappyContext}
import org.apache.spark.{SparkContext, SparkConf}

object ValidateCTQueriesApp {
  val conf = new SparkConf().setAppName("ValidateCTQueriesApp Application")
  val sc = new SparkContext(conf)
  val sqlContext = SQLContext.getOrCreate(sc)
  val snc = SnappyContext(sc)

  def main(args: Array[String]){
    val threadID = Thread.currentThread().getId
    val outputFile = "ValidateCTQueriesApp_thread_" + threadID + "_" + System.currentTimeMillis + ".out"
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true))
    val dataFilesLocation = args(0)
    snc.setConf("dataFilesLocation",dataFilesLocation)
    pw.println(s"dataFilesLocation : ${dataFilesLocation}")
    CTQueries.snc = snc
    CTQueries.dataFilesLocation = dataFilesLocation
    val tableType = args(1)
    val fullResultSetValidation: Boolean = args(2).toBoolean

    pw.println(s"Validation for queries with ${tableType} tables started")
    val hasValidationFailed = CTTestUtil.executeQueries(snc, tableType, pw, fullResultSetValidation,sqlContext)
    if(hasValidationFailed) {
      pw.println(s"Validation failed for ${tableType}")
      throw new TestException(s"Validation task failed for ${tableType}. Please check logs.")
    }
    pw.println(s"Validation for queries with ${tableType} tables completed successfully")
    pw.close()
  }
}
