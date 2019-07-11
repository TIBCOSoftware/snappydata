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
import io.snappydata.hydra.SnappyTestUtils
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SnappyContext, SparkSession}

object SmartConnectorStructTypeAPI {
  def main(args: Array[String]): Unit = {
    // scalastyle:off println
    println("Smart Connector Struct Type API Job started...")
    val connectionURL = args(args.length - 1)
    println("Connection URL is : " + connectionURL)
    val conf = new SparkConf()
      .setAppName("Spark_ComplexType_StructTypeAPI_Validation")
      .set("snappydata.connection", connectionURL)
    val sc : SparkContext = SparkContext.getOrCreate(conf)
    val snc : SnappyContext = SnappyContext(sc)
    val spark : SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sqlContext : SQLContext = SQLContext.getOrCreate(sc)

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath()
    val dataLocation : String = args(0)
    println("DataLocation : " + dataLocation)
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(
      new File("ValidateSmartConnectorStructTypeAPI" + "_" + "column" + System.currentTimeMillis())
      , false))
    val printContent : Boolean = false

    val sncReadDF : DataFrame = snc.read.json(dataLocation)
    val sparkReadDF : DataFrame = spark.read.json(dataLocation)

        println("Start the StructType Query-1")
        val sncStructQuery1DF : DataFrame = sncReadDF.select(sncReadDF("name"),
          sncReadDF("TestRecord").getField("Runs").as("Runs"),
          sncReadDF("TestRecord").getItem("Avg").as("Avg"))
          .orderBy(desc("Runs"))
        val sparkStructQuery1DF : DataFrame = sparkReadDF.select(sparkReadDF("name"),
          sparkReadDF("TestRecord").getField("Runs").as("Runs"),
          sparkReadDF("TestRecord").getItem("Avg").as("Avg"))
          .orderBy(desc("Runs"))

        println("Start the StructType Query-2")
        val sncStructQuery2DF : DataFrame = sncReadDF.agg(
          sum(sncReadDF("TestRecord").getItem("Runs")))
        val sparkStructQuery2DF : DataFrame = sparkReadDF.agg(
          sum(sparkReadDF("TestRecord").getField("Runs")))

        println("Start the StructType Query-3")
        val sncStructQuery3DF : DataFrame = sncReadDF.select(sncReadDF("name"))
          .filter(sncReadDF("TestRecord").getItem("batStyle") === "LeftHand")
        val sparkStructQuery3DF : DataFrame = sparkReadDF.select(sparkReadDF("name"))
            .filter(sparkReadDF("TestRecord").getField("batStyle") === "LeftHand")

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

     SnappyTestUtils.assertQueryFullResultSet(snc, sncStructQuery1DF, sparkStructQuery1DF,
        "StructTypeQuery1", "column", pw, sqlContext)
     println("Finished the StructType Query-1")
     SnappyTestUtils.assertQueryFullResultSet(snc, sncStructQuery2DF, sparkStructQuery2DF,
          "StructTypeQuery2", "column", pw, sqlContext)
     println("Finished the StructType Query-2")
     SnappyTestUtils.assertQueryFullResultSet(snc, sncStructQuery3DF, sparkStructQuery3DF,
          "StructTypeQuery3", "column", pw, sqlContext)
     println("Finished the StructType Query-3")
    SnappyTestUtils.assertQueryFullResultSet(snc, sncStructQuery4DF, sparkStructQuery4DF,
      "StructTypeQuery4", "column", pw, sqlContext)
    println("Finished the StructType Query-4")


    println("Query StructType Via API, Job completed....")
  }
}
