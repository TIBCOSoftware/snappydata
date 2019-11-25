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

import org.apache.spark.sql.SnappyContext
import org.apache.spark.{SparkConf, SparkContext}

object EvictionTestNWQueriesApp {

  def main(args: Array[String]) {
    val conf = new SparkConf().
        setAppName("EvictionTestNWQueriesApp Application_" + System.currentTimeMillis())
    val sc = SparkContext.getOrCreate(conf)
    val snc = SnappyContext(sc)
    val tableType = args(0)
    val totalSecs = args(1).toLong
    val threadID = Thread.currentThread().getId
    val outputFile = s"EvictionTestNWQueriesApp_{$tableType}_thread_" + threadID + "_" + System
        .currentTimeMillis + ".out"
    val start = System.currentTimeMillis
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true))
    // scalastyle:off println
    pw.println(s"Run ${tableType} tables Queries Test started at : $start")

    while ((System.currentTimeMillis() - start) / 1000 < totalSecs) {
      val s1 = System.currentTimeMillis()
      val Q34: String = "SELECT p.ProductName, SUM(od.Quantity) AS TotalUnits" +
          " FROM Order_Details od JOIN Products p ON" +
          " (p.ProductID = od.ProductID)" +
          " GROUP BY p.ProductName"
      val rs = snc.sql(Q34).collect()
      val s2 = System.currentTimeMillis()
      pw.println(s"Q34 executed in ${s2 - s1} msecs and " +
          s"returned ${rs.size} rows")
      val rs1 = snc.sql(NWQueries.Q37).collect()
      val s3 = System.currentTimeMillis()
      pw.println(s"Q37 executed in ${s3 - s2} msecs and " +
          s"returned ${rs1.size} rows")
      pw.flush()
      // assert(rs.size > 0)
      // assert(rs1.size > 0)
    }

    pw.close()

  }
}