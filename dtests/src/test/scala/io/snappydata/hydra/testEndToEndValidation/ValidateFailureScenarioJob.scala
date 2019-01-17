
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
package io.snappydata.hydra.testEndToEndValidation

import java.io.{File, FileOutputStream, PrintWriter}

import scala.util.{Failure, Success, Try}

import com.typesafe.config.Config
import util.TestException

import org.apache.spark.sql._

class ValidateFailureScenarioJob extends SnappySQLJob {
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val snc = snSession.sqlContext
    val pw: PrintWriter
    = new PrintWriter(new FileOutputStream(new File("testJobFailure.out")), true)
    Try {
      // scalastyle:off println
      pw.println("Throwing test exception...")
      pw.flush()
      throw new TestException("Throwing test exception...")
    } match {
      case Success(v) => pw.close()
        s"See logs"
      case Failure(e) =>
        pw.println("Exception occurred while executing the job "
            + "\nError Message:" + e.getMessage)
        pw.close();
        throw new TestException(s"Job failed. Check logs.");
    }
    pw.close();
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}
