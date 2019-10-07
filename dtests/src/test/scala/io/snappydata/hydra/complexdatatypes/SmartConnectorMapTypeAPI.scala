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
package io.snappydata.hydra.complexdatatypes

import java.io.{File, FileOutputStream, PrintWriter}

import io.snappydata.hydra.SnappyTestUtils
import org.apache.spark.sql.functions.{desc, sum, when}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SnappyContext, SparkSession}

object SmartConnectorMapTypeAPI {
  def main(args: Array[String]): Unit = {
    // scalastyle:off println
    println("Smart Connector Map Type API Job started...")
    val connectionURL = args(args.length - 1)
    println("Connection URL is : " + connectionURL)
    val conf = new SparkConf()
      .setAppName("Spark_ComplexType_MapTypeAPI_Validation")
      .set("snappydata.connection", connectionURL)
    val sc : SparkContext = SparkContext.getOrCreate(conf)
    val snc : SnappyContext = SnappyContext(sc)
    val spark : SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sqlContext : SQLContext = SQLContext.getOrCreate(sc)

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath()
    val dataLocation : String = args(0)
    println("DataLocation : " + dataLocation)
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(
      new File("ValidateSmartConnectorMapTypeAPI" + "_" + "column" + System.currentTimeMillis())
      , false))
    val printContent : Boolean = false

    val sncReadDF : DataFrame = snc.read.json(dataLocation)
    val sparkReadDF : DataFrame = spark.read.json(dataLocation)

    println("Start the Map Type Query1")
    val sncSelectDF : DataFrame = sncReadDF.select("*").orderBy("id")
    val sparkSelectDF : DataFrame = sparkReadDF.select("*").orderBy("id")
    if(printContent) {
      println("sncSelectDF count : " + sncSelectDF.count())
      println("sparkSelectDF count : " + sparkSelectDF.count())
    }

    println("Start the Map Type Query2")
    val sncMapQuery2DF : DataFrame = sncReadDF
      .select(sncReadDF("id"), sncReadDF("name"), sncReadDF("Maths").getItem("maths"),
        sncReadDF("Science").getField("science"), sncReadDF("English").getItem("english"),
        sncReadDF("Computer").getItem("computer"), sncReadDF("Music").getField("music"),
        sncReadDF("History").getItem("history")).filter(sncReadDF("name")==="JxVJBxYlNT")

    val sparkMapQuery2DF : DataFrame = sparkReadDF
      .select(sparkReadDF("id"), sparkReadDF("name"), sparkReadDF("Maths").getItem("maths"),
        sparkReadDF("Science").getField("science"), sparkReadDF("English").getItem("english"),
        sparkReadDF("Computer").getItem("computer"), sparkReadDF("Music").getField("music"),
        sparkReadDF("History").getItem("history")).where(sparkReadDF("name")==="JxVJBxYlNT")
    if(printContent) {
      println("sncMapQuery2DF count : " + sncMapQuery2DF.count())
      println("sncMapQuery2DF count : " + sncMapQuery2DF.count())
    }

    println("Start the Map Type Query3 and Query4")
    val snc3DF2 : DataFrame = sncReadDF.select(("id"), ("name"))
    val snc3DF3 : DataFrame = sncReadDF.select("id",
      "Maths.maths", "Science.science", "English.english", "History.history",
      "Music.music", "Computer.computer")
    val snc3DF4 : DataFrame = snc3DF2.join(snc3DF3, snc3DF2("id") === snc3DF3("id"))
    val snc3DF5 : DataFrame = snc3DF4.select("name",
      "maths", "science", "english", "history", "music", "computer")
    val sncMapQuery3DF : DataFrame = snc3DF5.select("*").groupBy("name")
      .agg(sum(snc3DF5("maths") + snc3DF5("science") + snc3DF5("english")
        + snc3DF5("music") + snc3DF5("history") + snc3DF5("computer")).as("Total"))
      .orderBy(desc("Total"))
    val sncMapQuery4DF : DataFrame = sncMapQuery3DF.select(sncMapQuery3DF("name"),
      when(sncMapQuery3DF("Total").geq(500), "A")
        .when(sncMapQuery3DF("Total").geq(400), "B")
        .when(sncMapQuery3DF("Total").geq(300), "C")
        .otherwise("Fail").as("Grade"))
      .orderBy(desc("Total"))


    val spark3DF2 : DataFrame = sparkReadDF.select(("id"), ("name"))
    val spark3DF3 : DataFrame = sparkReadDF.select("id",
      "Maths.maths", "Science.science", "English.english", "History.history",
      "Music.music", "Computer.computer")
    val spark3DF4 : DataFrame = spark3DF2.join(spark3DF3, spark3DF2("id") === spark3DF3("id"))
    val spark3DF5 : DataFrame = spark3DF4.select("name",
      "maths", "science", "english", "history", "music", "computer")
    val sparkMapQuery3DF : DataFrame = spark3DF5.select("*").groupBy("name")
      .agg(sum(spark3DF5("maths") + spark3DF5("science") + spark3DF5("english")  +
        spark3DF5("history") + spark3DF5("music") + spark3DF5("computer")).as("Total"))
      .orderBy(desc("Total"))
    val sparkMapQuery4DF : DataFrame = sparkMapQuery3DF.select(sparkMapQuery3DF("name"),
      when(sparkMapQuery3DF("Total").geq(500), "A")
        .when(sparkMapQuery3DF("Total").geq(400), "B")
        .when(sparkMapQuery3DF("Total").geq(300), "C")
        .otherwise("Fail").as("Grade"))
      .orderBy(desc("Total"))


    println("Start the Map Type Query5")
    val snc5DF2 : DataFrame = sncReadDF.select(("id"), ("name"))
    val snc5DF3 : DataFrame = sncReadDF.select("id",
      "Maths.maths", "Science.science", "English.english", "History.history",
      "Music.music", "Computer.computer")
    val snc5DF4 : DataFrame = snc5DF2.join(snc5DF3, snc5DF2("id") === snc5DF3("id"))
    val snc5DF5 : DataFrame = snc5DF4.select("name",
      "maths", "science", "english", "history", "music", "computer")
    val sncMapQuery5DF : DataFrame = snc5DF5.select("*").groupBy("name")
      .agg((sum(snc5DF5("maths") + snc5DF5("science") + snc5DF5("english") +
        snc5DF5("music") +  snc5DF5("history") + snc5DF5("computer"))
        * 100.0/600.0).as("Percentage"))
      .orderBy(desc("Percentage"))

    val spark5DF2 : DataFrame = sparkReadDF.select(("id"), ("name"))
    val spark5DF3 : DataFrame = sparkReadDF.select("id",
      "Maths.maths", "Science.science", "English.english", "History.history",
      "Music.music", "Computer.computer")
    val spark5DF4 : DataFrame = spark5DF2.join(spark5DF3, spark5DF2("id") === spark5DF3("id"))
    val spark5DF5 : DataFrame = spark5DF4.select("name",
      "maths", "science", "english", "history", "music", "computer")
    val sparkMapQuery5DF : DataFrame = spark5DF5.select("*").groupBy("name")
      .agg((sum(spark5DF5("maths") + spark5DF5("science") + spark5DF5("english")  +
        spark5DF5("history") + spark5DF5("music") + spark5DF5("computer"))
        * 100.0/600.0).as("Percentage"))
      .orderBy(desc("Percentage"))

    SnappyTestUtils.assertQueryFullResultSet(snc, sncSelectDF, sparkSelectDF,
      "MapTypeQuery1", "column", pw, sqlContext )
    println("Finish the Map Type Query1")
    SnappyTestUtils.assertQueryFullResultSet(snc, sncMapQuery2DF, sparkMapQuery2DF,
      "MapTypeQuery2", "column", pw, sqlContext)
    println("Finish the Map Type Query2")
    SnappyTestUtils.assertQueryFullResultSet(snc, sncMapQuery3DF, sparkMapQuery3DF,
      "MapTypeQuery3", "column", pw, sqlContext)
    println("Finish the Map Type Query3")

    SnappyTestUtils.assertQueryFullResultSet(snc, sncMapQuery4DF, sparkMapQuery4DF,
      "MapTypeQuery4", "column", pw, sqlContext)
    println("Finish the Map Type Query4")
    SnappyTestUtils.assertQueryFullResultSet(snc, sncMapQuery5DF, sparkMapQuery5DF,
      "MapTypeQuery5", "column", pw, sqlContext)
    println("Finish the Map Type Query6")

    println("Query MapType Via API, Job Completed....")

  }
}
