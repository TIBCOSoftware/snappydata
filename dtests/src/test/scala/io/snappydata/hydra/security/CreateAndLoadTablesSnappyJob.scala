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
package io.snappydata.hydra.security

import java.io.PrintWriter

import scala.util.{Failure, Success, Try}

import com.typesafe.config.Config
import io.snappydata.hydra.northwind.NWQueries

import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession, _}

object CreateAndLoadTablesSnappyJob extends SnappySQLJob {
  // scalastyle:off println
  println(" Inside CreateAndLoadSnappyJob")
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val snc = snSession.sqlContext
    val dataFilesLocation = jobConfig.getString("dataFilesLocation")
    val queryFile: String = jobConfig.getString("queryFile")
    val queryArray = scala.io.Source.fromFile(queryFile).getLines().mkString.split(";")
    val schema1: String = jobConfig.getString("schema1")
    val schema2: String = jobConfig.getString("schema2")
  //  val passfile: String = jobConfig.getString("passfile1")
    val isGrant: Boolean = jobConfig.getBoolean("isGrant")
    val expectedCntWithGrant: Int = jobConfig.getInt("expectedCntWithGrant")
    val unExpectedCntWithGrant: Int = jobConfig.getInt("unExpectedCntWithGrant")

    val userSchema = new Array[String](2)
    userSchema(0) = schema1;
    userSchema(1) = schema2;

    val isSelect = true;
    val props = Map[String, String]()
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    // scalastyle:off println
    val pw = new PrintWriter("SnappyJobOutPut.out")
    Try {
      snc.setConf("dataFilesLocation", dataFilesLocation)
      NWQueries.snc = snc
      NWQueries.dataFilesLocation = dataFilesLocation
      SecurityTestUtil.createColRowTables(snc)
      SecurityTestUtil.runQueries(snc, queryArray, expectedCntWithGrant,
      unExpectedCntWithGrant, isGrant, userSchema, isSelect, pw)
     runQuery()
   /*   snc.sql("select * from user2.airline limit 10").show()
      println("SELECT successful")
      snc.sql("UPDATE user2.airline SET DEST = 'INDIA' WHERE UNIQUECARRIER = 'WN' ")
      println("UPDATE successful")
      snc.sql("select uniquecarrier,dest from user2.airline where dest = 'INDIA '").show()
*/
     /* snc.sql("DELETE FROM user2.airline WHERE DEST = 'INDIA' AND UNIQUECARRIER = 'WN' ").show()
      snc.sql("select uniquecarrier,dest from user2.airline where dest = 'INDIA '").show() */
    }
    match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/SnappyJobOutPut.out"
      case Failure(e) => pw.close();
        throw e;
    //  println("Got Exception " + ex.getMessage())
      /* case ex : Exception => {
      /*  if (isAuth) {
          unExpectedExpCnt = unExpectedExpCnt + 1
          println("Got unExpected Exception " + ex.printStackTrace())
          println("unExpectedExpCnt = " + unExpectedExpCnt)
        }
        else {
          expectedExpCnt = expectedExpCnt + 1
          println("Got Expected exception " + ex.printStackTrace())
          println("expectedCnt = " + expectedExpCnt)
        } */
        println("Got Exception " + ex.getMessage())
      } */
    }

    def runQuery() : Unit = {
      println("Inside runQuery")

    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}
