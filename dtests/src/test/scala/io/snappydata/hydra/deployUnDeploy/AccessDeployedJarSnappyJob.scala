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
package io.snappydata.hydra.deployUnDeploy

import com.typesafe.config.Config

import org.apache.spark.sql.{SaveMode, SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}


object AccessDeployedJarSnappyJob extends SnappySQLJob {
  // scalastyle:off println

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val isException: Boolean = jobConfig.getBoolean("isException")
    val ExpectedCnt: Integer = jobConfig.getInteger("ExpectedCnt")
    try {
      val df = snSession.read.format("org.apache.spark.sql.cassandra").
          options(Map("table" -> "customer", "keyspace" -> "test")).load
      df.write.format("column").mode(SaveMode.Overwrite).saveAsTable("CUSTOMERSNAPPY_JOB")
      val showDF = snSession.sql("select * from CUSTOMERSNAPPY_JOB")
      println("Number of rows = " + showDF.count())
      val actualCnt = showDF.count()
      if (actualCnt == ExpectedCnt) {
        println(s"The counts match actualCnt = $actualCnt and expectedCnt = $ExpectedCnt")
      }
      else {
        println("The counts donot match")
        println(s"The counts match actualCnt = $actualCnt and expectedCnt = $ExpectedCnt")
      }
    }
    catch {
      case ex: Exception => {
        if (isException) {
          println("Got Expected Exception " + ex.printStackTrace())
        }
        else {
          println("Got UnExpected exception " + ex.printStackTrace())
        }
      }
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}

