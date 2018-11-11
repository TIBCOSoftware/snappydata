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

package io.snappydata.hydra.snapshotIsolation

import java.io.{File, FileOutputStream, PrintWriter}

import scala.util.{Try, Failure, Success}

import com.typesafe.config.Config
import io.snappydata.hydra.ct.CTTestUtil

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SnappySQLJob, SnappyJobValid, SnappyJobValidation, SnappySession}

class SnappyDummyJob extends SnappySQLJob {

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val snc = snSession.sqlContext

    Try {
      snSession.sparkContext.parallelize(1 to 1000, 200).foreachPartition(x=> {
        Thread.sleep(5000)
      })

    } match {
      case Success(v) =>
        s"success"
      case Failure(e) =>
        throw e;
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}


