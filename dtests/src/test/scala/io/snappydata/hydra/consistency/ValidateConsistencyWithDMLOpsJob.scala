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

import com.typesafe.config.Config

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

object ValidateConsistencyWithDMLOpsJob extends SnappySQLJob {

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath
    val tid = jobConfig.getInt("tid")
    val outputFile = "VerifyConsistency_thr_" + tid + "_" + System.currentTimeMillis + ".out"
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true))
    val operation = jobConfig.getString("operation")
    val batchSize = jobConfig.getInt("batchsize")
    val selectStmt = jobConfig.getString("selectStmt")
    val dmlStmt = jobConfig.getString("dmlStmt")
    val tableName = jobConfig.getString("tableName")
    // scalastyle:off println
    Try {
      val snc = snSession.sqlContext
      snc.sql("set spark.sql.shuffle.partitions=23")
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      val startTime = System.currentTimeMillis
      val consistencyTest = new ConsistencyTest()
      pw.println("Starting execution for dml operation " +  operation)
      pw.flush()
      consistencyTest.performOpsAndVerifyConsistency(snc, pw, tid, operation, batchSize, dmlStmt,
        selectStmt, tableName)
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

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}
