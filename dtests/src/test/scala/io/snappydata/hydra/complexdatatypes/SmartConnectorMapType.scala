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
import org.apache.spark.sql.{SQLContext, SnappyContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SmartConnectorMapType {
  def main(args: Array[String]): Unit = {
    // scalastyle:off println
    println("Smart Connector MapType Job started...")
    val connectionURL = args(args.length - 1)
    println("Connection URL is : " + connectionURL)
    val conf = new SparkConf()
      .setAppName("Spark_ComplexType_MapType_Validation")
      .set("snappydata.connection", connectionURL)
    val sc : SparkContext = SparkContext.getOrCreate(conf)
    val snc : SnappyContext = SnappyContext(sc)
    val spark : SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sqlContext = SQLContext.getOrCreate(sc)

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath()
    val dataLocation = args(0)
    println("DataLocation : " + dataLocation)
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(
      new File("ValidateSmartConnectorMapType" + "_" + "column" + System.currentTimeMillis())
      , false))
    val printContent : Boolean = false

    snc.sql("DROP TABLE IF EXISTS TempStRecord")
    snc.sql("DROP TABLE IF EXISTS StudentMarksRecord")

    /* --- Snappy Job --- */

    snc.sql("CREATE EXTERNAL TABLE IF NOT EXISTS TempStRecord  USING JSON " +
      "OPTIONS(path '" + dataLocation + "')")
    snc.sql("CREATE TABLE IF NOT EXISTS StudentMarksRecord USING COLUMN " +
      "AS (SELECT * FROM TempStRecord)")

    snc.sql(ComplexTypeUtils.Map_Q1)
    snc.sql(ComplexTypeUtils.Map_Q2)
    snc.sql(ComplexTypeUtils.Map_Q3)
    snc.sql(ComplexTypeUtils.Map_Q4)
    snc.sql(ComplexTypeUtils.Map_Q5)
    snc.sql(ComplexTypeUtils.Map_Q6)

    if(printContent) {
      println("snc Map_Q1:" + snc.sql(ComplexTypeUtils.Map_Q1).show)
      println("snc Map_Q2:" + snc.sql(ComplexTypeUtils.Map_Q2).show)
      println("snc Map_Q3:" + snc.sql(ComplexTypeUtils.Map_Q3).show)
      println("snc Map_Q4:" + snc.sql(ComplexTypeUtils.Map_Q4).show)
      println("snc Map_Q5:" + snc.sql(ComplexTypeUtils.Map_Q5).show)
      println("snc Map_Q6: " + snc.sql(ComplexTypeUtils.Map_Q6).show)
    }

    /* --- Create the Spark Tables / Views for Verification  --- */
    val mapType = spark.read.json(dataLocation)
    mapType.createTempView("StudentMarksRecord")

    /* --- Verification --- */

    // TODO Due to SNAP-2782 Below line is commented, Hydra Framework required changes.
    // SnappyTestUtils.assertQueryFullResultSet(snc, Map_Q1, "Map_Q1", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.Map_Q2,
      "Map_Q2", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.Map_Q3,
      "Map_Q3", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.Map_Q4,
      "Map_Q4", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.Map_Q5,
      "Map_Q5", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.Map_Q6,
      "Map_Q6", "column", pw, sqlContext)

    /* --- Clean Up --- */
    snc.sql("DROP TABLE IF EXISTS TempStRecord")
    snc.sql("DROP TABLE IF EXISTS StudentMarksRecord")
    println("Smart Connector Map Type job finished....")
  }
}
