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
import org.apache.spark.SparkContext
import org.apache.spark.sql._

import scala.util.{Failure, Success, Try}

class EvictionTestNWQueriesJob extends SnappySQLJob {
  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    val snc = snappySession.sqlContext

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    val tableType = jobConfig.getString("tableType")
    val totalSecs = jobConfig.getString("totalSecs").toLong
    val outputFile = "EvictionTestNWQueriesJob_" + tableType + "_" +
        jobConfig.getString("logFileName")
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    val sc = SparkContext.getOrCreate()
    val start = System.currentTimeMillis()
    Try {
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

    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/${outputFile}"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}
