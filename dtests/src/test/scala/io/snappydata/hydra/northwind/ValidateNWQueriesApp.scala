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
package io.snappydata.hydra.northwind

import java.io.{File, FileOutputStream, PrintWriter}

import org.apache.spark.sql.{SQLContext, SnappyContext}
import org.apache.spark.{SparkConf, SparkContext}

object ValidateNWQueriesApp {
  val conf = new SparkConf().
    setAppName("ValidateNWQueriesApp Application")
  val sc = new SparkContext(conf)
  val sqlContext = SQLContext.getOrCreate(sc)
  val snc = SnappyContext(sc)

  def main(args: Array[String]) {
    //snc.sql("set spark.sql.shuffle.partitions=6")
    val dataFilesLocation: String = args(0)
    snc.setConf("dataFilesLocation", dataFilesLocation)
    NWQueries.snc = snc
    NWQueries.dataFilesLocation = dataFilesLocation
    val tableType = args(1)
    val threadID = Thread.currentThread().getId
    val outputFile = "ValidateNWQueriesApp_thread_" + threadID + "_" + System.currentTimeMillis + ".out"
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    pw.println(s"Validate ${tableType} tables Queries Test started")
    pw.println(s"dataFilesLocation : ${dataFilesLocation}")
    NWTestUtil.validateQueries(snc, tableType, pw)
    pw.println(s"Validate ${tableType} tables Queries Test completed successfully")
    println(s"createAndLoadSparkTables Test started")
    pw.println(s"createAndLoadSparkTables Test started")
    NWTestUtil.createAndLoadSparkTables(sqlContext)
    println(s"createAndLoadSparkTables Test completed successfully")
    pw.println(s"createAndLoadSparkTables Test completed successfully")
    pw.println(s"ValidateQueriesFullResultSet for ${tableType} tables Queries Test started")
    NWTestUtil.validateQueriesFullResultSet(snc, tableType, pw, sqlContext)
    pw.println(s"validateQueriesFullResultSet ${tableType} tables Queries Test completed successfully")
    pw.close()
  }
}

