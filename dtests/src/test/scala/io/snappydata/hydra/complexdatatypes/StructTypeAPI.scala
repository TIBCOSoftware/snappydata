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

package io.snappydata.hydra.complexdatatypes

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config
import io.snappydata.hydra.SnappyTestUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class StructTypeAPI extends SnappySQLJob {
  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
     //  scalastyle:off println
     println("Query StructType Via API, Job Started....")
    val snc : SnappyContext = snappySession.sqlContext
    val spark : SparkSession = SparkSession.builder().getOrCreate()
    val sqlContext : SQLContext = SQLContext.getOrCreate(spark.sparkContext)
    val dataLocation : String = jobConfig.getString("dataFilesLocation")
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath()
    val outputFile = "ValidateStructType_Via_API_" +  System.currentTimeMillis()
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), true))
    val printDFContent : Boolean = false

    val sncReadDF : DataFrame = snc.read.json(dataLocation)
    val sparkReadDF : DataFrame = spark.read.json(dataLocation)

//    println("Start the StructType Query-1")
//    val sncStructQuery1DF : DataFrame = sncReadDF.select(sncReadDF("name"),
//      sncReadDF("TestRecord").getField("Runs").as("Runs"),
//      sncReadDF("TestRecord").getItem("Avg").as("Avg"))
//      .orderBy(desc("Runs"))
//    val sparkStructQuery1DF : DataFrame = sparkReadDF.select(sparkReadDF("name"),
//      sparkReadDF("TestRecord").getField("Runs").as("Runs"),
//      sparkReadDF("TestRecord").getItem("Avg").as("Avg"))
//      .orderBy(desc("Runs"))

//    println("Start the StructType Query-2")
//    NOTE : as() or alias() produce the syntax error.
//    val sncStructQuery2DF : DataFrame = sncReadDF.agg(
//    sum(sncReadDF("TestRecord").getItem("Runs").as("TotalRuns")))
//    val sparkStructQuery2DF : DataFrame = sparkReadDF.agg(
//      sum(sparkReadDF("TestRecord").getField("Runs").as("TotalRuns")))
//    println("snc show : " + sncStructQuery2DF.show())
//    println("spark show : " + sparkStructQuery2DF.show())

//    val sncStructQuery2DF : DataFrame = sncReadDF.agg(
//      sum(sncReadDF("TestRecord").getItem("Runs")))
//    val sparkStructQuery2DF : DataFrame = sparkReadDF.agg(
//      sum(sparkReadDF("TestRecord").getField("Runs")))
//
//    println("Start the StructType Query-3")
//    val sncStructQuery3DF : DataFrame = sncReadDF.select(sncReadDF("name"))
//      .filter(sncReadDF("TestRecord").getItem("batStyle") === "LeftHand")
//    val sparkStructQuery3DF : DataFrame = sparkReadDF.select(sparkReadDF("name"))
//        .filter(sparkReadDF("TestRecord").getField("batStyle") === "LeftHand")

    println("Start the StructType Query-4")
    val sncStructQuery4DF : DataFrame = sncReadDF.select(sncReadDF("name"),
      sncReadDF("TestRecord").getField("batStyle"),
      sncReadDF("TestRecord").getField("Matches").as("Matches"),
      sncReadDF("TestRecord").getField("Runs"),
      sncReadDF("TestRecord").getField("Avg"))
        .orderBy(desc("Matches"))
    val sparkStructQuery4DF : DataFrame = sparkReadDF.select(sparkReadDF("name"),
      sparkReadDF("TestRecord").getField("batStyle"),
      sparkReadDF("TestRecord").getField("Matches").as("Matches"),
      sparkReadDF("TestRecord").getField("Runs"),
      sparkReadDF("TestRecord").getField("Avg"))
      .orderBy(desc("Matches"))
    println("snc count : " + sncStructQuery4DF.count())
    println("spark count : " + sparkStructQuery4DF.count())


//    SnappyTestUtils.assertQueryFullResultSet(snc, sncStructQuery1DF, sparkStructQuery1DF,
//    "StructTypeQuery1", "column", pw, sqlContext)
//    println("Finished the StructType Query-1")
//    SnappyTestUtils.assertQueryFullResultSet(snc, sncStructQuery2DF, sparkStructQuery2DF,
//      "StructTypeQuery2", "column", pw, sqlContext)
//    println("Finished the StructType Query-2")
//    SnappyTestUtils.assertQueryFullResultSet(snc, sncStructQuery3DF, sparkStructQuery3DF,
//      "StructTypeQuery3", "column", pw, sqlContext)
//    println("Finished the StructType Query-3")
    SnappyTestUtils.assertQueryFullResultSet(snc, sncStructQuery4DF, sparkStructQuery4DF,
            "StructTypeQuery4", "column", pw, sqlContext)
          println("Finished the StructType Query-4")


    println("Query StructType Via API, Job completed....")



  }
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}
