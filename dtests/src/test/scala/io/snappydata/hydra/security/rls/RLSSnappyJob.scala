/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
package io.snappydata.hydra.security.rls

import java.io.PrintWriter

import scala.util.{Failure, Success, Try}

import com.typesafe.config.Config
import io.snappydata.hydra.northwind.NWQueries
import io.snappydata.hydra.security.SecurityTestUtil

import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

object RLSSnappyJob extends SnappySQLJob {
  // scalastyle:off println
  println(" Inside RLSSnappyJob")
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val snc = snSession.sqlContext
   // val dataFilesLocation = jobConfig.getString("dataFilesLocation")
    val schema1: String = jobConfig.getString("schema1")
    val schema2: String = jobConfig.getString("schema2")
    val user3: String = jobConfig.getString("user3")
    val user4: String = jobConfig.getString("user4")
    val userSchema = new Array[String](2)
    userSchema(0) = schema1;
    userSchema(1) = schema2;
    val userNames = new Array[String](2)
    userNames(0) = user3;
    userNames(1) = user4;

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    // scalastyle:off println
    val pw = new PrintWriter("RLSSnappyJob.out")
    Try {
    //  snc.setConf("dataFilesLocation", dataFilesLocation)
      NWQueries.snc = snc
    //  NWQueries.dataFilesLocation = dataFilesLocation
      // SecurityTestUtil.createColRowTables(snc)
      SecurityTestUtil.createPolicy(snc, userSchema, userNames)
      SecurityTestUtil.enableRLS(snc, userSchema)
    /*  SecurityTestUtil.runQueries(snc, queryArray, expectedCntWithGrant,
        unExpectedCntWithGrant, isGrant, userSchema, isSelect, pw)
   */   runQuery()
    }
    match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/RLSSnappyJob.out"
      case Failure(e) => pw.close();
        throw e;
    }
    def runQuery() : Unit = {
      println("Inside runQuery")

    }
  }
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}
