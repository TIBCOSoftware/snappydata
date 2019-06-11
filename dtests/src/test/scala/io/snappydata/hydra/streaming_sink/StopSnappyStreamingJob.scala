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

package io.snappydata.hydra.streaming_sink

import java.io.{File, FileOutputStream, PrintWriter}

import scala.util.{Failure, Success, Try}

import com.typesafe.config.Config

import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

class StopSnappyStreamingJob extends SnappySQLJob {
  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath
    val outputFile = "StopSnappyStreaming.log"
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    Try {
      // scalastyle:off println
      pw.println("Stopping Snappy Streaming Context...")
      // SnappyStreamingContext.getActive.get.stop(false, true)
      snappySession.sql("streaming stop");
      pw.println("Snappy Streaming Context stopped successfully.")
      pw.close()
    }match {
      case Success(v) => pw.close()
        s"Streaming Job stopped successfully. See ${getCurrentDirectory}/${outputFile}"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}
