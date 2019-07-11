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
package io.snappydata.hydra.hiveThriftServer

import java.io.{File, FileOutputStream, PrintWriter}
import java.sql.{Connection, DriverManager}
import java.util.Random

import com.typesafe.config.Config
import org.apache.spark.sql._

class ThriftServerInsertFromSnappy extends SnappySQLJob {

  override def isValidJob(snappySession: SnappySession, config: Config):
  SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    // scalastyle:off println


    var index = 0
    val snc : SnappyContext = snappySession.sqlContext
    val spark : SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
//    def getCurrentDirectory = new java.io.File(".").getCanonicalPath()
//    val tids = jobConfig.getString("tids")
//    val threadID = Thread.currentThread().getId
//    println("threadID : " + threadID)
//    val outputFile = "ValidateHiveThriftServerConcurrency" + "_" + threadID + "_" +
//      System.currentTimeMillis() + jobConfig.getString("logFileName")
//    val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), false))
    val sqlContext : SQLContext = spark.sqlContext
//    val random = new Random()
//    val tidList = tids.split(",")
//    tidList.foreach{println}
    snc.sql("insert into default.Student select id, concat('TIBCO_',id), " +
      "default.subject(id%10), rand() * 1000, id%10 from range(10000);")
    println(snc.sql("select * from default.Student order by id DESC").show(1000))
    println(snc.sql("select count(*) from default.Student").show())
  }
}
