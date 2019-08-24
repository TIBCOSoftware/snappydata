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
package io.snappydata.hydra.hiveThriftServer

import java.io.{File, FileOutputStream, PrintWriter}
import java.sql.{Connection, DriverManager, SQLException}
import java.util

import com.typesafe.config.Config
import io.snappydata.hydra.SnappyTestUtils
import org.apache.spark.sql._

import scala.util.Random



//  Not currently in use
class HiveThriftServerConcurrentOps extends SnappySQLJob {

  var connection : Connection = null;

  override def isValidJob(snappySession: SnappySession, config: Config):
  SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    // scalastyle:off println

    val snc : SnappyContext = snappySession.sqlContext
//    snc.sql("set snappydata.hiveServer.enabled=true")
    val spark : SparkSession = SparkSession.builder().getOrCreate()
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath()
    val tid = jobConfig.getInt("tid")
    val outputFile = "ValidateHiveThriftServerConcurrency" + "_" +
                   tid + "_" + System.currentTimeMillis() + ".out"
   val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), false))
   pw.println("concurret dml job started.....")
    val sqlContext : SQLContext = spark.sqlContext

    val queryFile: String = jobConfig.getString("queryFile");
    val queryArray = scala.io.Source.fromFile(queryFile).getLines().mkString.split(";")

    for (j <- 0 to queryArray.length - 1) {
      // val index = new Random().nextInt(queryArray.length )
      var query: String = queryArray(j)

      if(query.contains(";")) {
        query = query.replace(";", "")
      }

      if (query.toUpperCase.contains("WHERE")) {
        query = query + " AND tid = " + tid
      }
      else {
        query = query + " WHERE tid = " + tid
      }
      pw.println("Executing Query : " + query)
      snc.sql(query)
      pw.println(snc.sql(query).show(10))
      pw.println("concurrent dml job finished....")
      connection.close()
      pw.flush()
      pw.close()
    }
   }
 }

