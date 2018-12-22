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
package io.snappydata.hydra.distIndex

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config
import org.apache.spark.sql._

import scala.util.{Failure, Success, Try}

class DistIndexJob extends SnappySQLJob {
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val snc = snSession.sqlContext

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    //
    val fileOutputStream = new FileOutputStream(new File(jobConfig.getString("logFileName")))
    val pw: PrintWriter = new PrintWriter(fileOutputStream, true)
    val resultValidation: Boolean = jobConfig.getString("resultValidation").toBoolean
    Try {
      snc.sql("set spark.sql.crossJoin.enabled = true")
      // scalastyle:off println
      pw.println(s"****** DistIndexJob started at time : " + System.currentTimeMillis + " ******")
      if (resultValidation) {
        pw.println("****** executeQueriesWithResultValidation task started ******")
        DistIndexTestUtils.executeQueriesWithResultValidation(snc, pw)
        pw.println("****** executeQueriesWithResultValidation task finished ******")
      } else {
        pw.println("****** executeQueriesForBenchmarkResults task started ******")
        DistIndexTestUtils.executeQueriesForBenchmarkResults(snc, pw, fileOutputStream)
        pw.println("****** executeQueriesForBenchmarkResults task finished ******")
      }
      pw.println(s"****** DistIndexJob finished : " + System.currentTimeMillis + " ******")
      return String.format("See %s/" + jobConfig.getString("logFileName"), getCurrentDirectory)
    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/${jobConfig.getString("logFileName")}"
      case Failure(e) =>
        pw.println("Exception occurred while executing the job " + "\nError Message:" + e
            .getMessage)
        pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

}