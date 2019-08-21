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

import org.apache.spark.sql.{SQLContext, SnappyContext}
import org.apache.spark.{SparkConf, SparkContext}

object ValidateNWQueriesWithChangingConstantsApp {

  def main(args: Array[String]) {
    val connectionURL = args(args.length - 1)
    val conf = new SparkConf().
        setAppName("ValidateNWQueries Application_" + System.currentTimeMillis()).
        set("snappydata.connection", connectionURL)
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
    val outputFile = "ValidateNWQueriesApp_thread_" + threadID + "_" + System.currentTimeMillis +
        ".out"
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    // scalastyle:off println
      pw.println(s"createAndLoadSparkTables Test started at : " + System.currentTimeMillis)
      NWTestUtil.createAndLoadSparkTables(sqlContext)
      println(s"createAndLoadSparkTables Test completed successfully at : " + System
          .currentTimeMillis)
      pw.println(s"createAndLoadSparkTables Test completed successfully at : " + System
          .currentTimeMillis)
      pw.println(s"ValidateQueries for ${tableType} tables Queries Test started at :" +
          s" " + System.currentTimeMillis)
        NWTestUtil.executeAndValidateQueriesByChangingConstants(snc, tableType, pw, sqlContext)
      pw.println(s"validateQueries for ${tableType} tables Queries Test completed  " +
          s"successfully at : " + System.currentTimeMillis)
    pw.close()
  }
}

