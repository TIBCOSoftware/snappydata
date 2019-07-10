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

package io.snappydata.hydra.consistency

import java.io.{File, FileOutputStream, PrintWriter}

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.{SQLContext, SnappyContext}
import org.apache.spark.{SparkConf, SparkContext}

object ValidateConsistencyWithDMLOpsApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("VerifyConsistency_App_" + System.currentTimeMillis())
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    val snc = SnappyContext(sc)
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath
    val tid = args(0).toInt
    val outputFile = "VerifyConsistency_thr_" + tid + "_" + System.currentTimeMillis + ".out"
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true))
    Try {
      val operation = args(1)
      val batchSize = args(2).toInt
      val tableName = args(3)
      val selectStmt = args(4)
      val dmlStmt = args(5)
      // scalastyle:off println
      val startTime = System.currentTimeMillis
      val consistencyTest = new ConsistencyTest()
      consistencyTest.performOpsAndVerifyConsistency(snc, pw, tid, operation, batchSize, selectStmt,
        dmlStmt, tableName)
      val endTime = System.currentTimeMillis
      val totalTime = (endTime - startTime) / 1000
      pw.println(s"Total time for execution is :: ${totalTime} seconds.")
      pw.close()
    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/${outputFile}"
      case Failure(e) => pw.close();
        throw e;
    }
  }
}
