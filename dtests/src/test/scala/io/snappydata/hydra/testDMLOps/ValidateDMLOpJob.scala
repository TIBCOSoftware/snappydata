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

package io.snappydata.hydra.testDMLOps

import java.io.{File, FileOutputStream, PrintWriter}

import scala.util.{Failure, Success, Try}

import com.typesafe.config.Config

import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

class ValidateDMLOpJob extends SnappySQLJob {

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    val threadID = jobConfig.getString("tid")
    val outputFile = "DMLOpJob_thr_" + threadID + "_" + System.currentTimeMillis +
        ".out"
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    val tableName = jobConfig.getString("tableName")

    Try {
      val snc = snSession.sqlContext
      snc.sql("set spark.sql.shuffle.partitions=23")
      val stmt = jobConfig.getString("stmt")
      snc.setConf("stmt", stmt)
      // scalastyle:off println
      pw.println(s"Executing ${stmt} on snappy")
      val startTime = System.currentTimeMillis
      val df = snc.sql(stmt)
      val endTime = System.currentTimeMillis
      val totalTime = (endTime - startTime) / 1000
      pw.println(df.show);
      pw.println(s"Total time for execution is :: ${totalTime} seconds.")
      println(s"Operation completed successfully. See ${getCurrentDirectory}/${outputFile}")
      pw.println(s"Operation completed successfully.")
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
