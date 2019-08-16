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
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SnappyContext, SparkSession}

object SmartConnectorArrayType {
  def main(args: Array[String]): Unit = {
    // scalastyle:off println
    println("Smart Connector ArraysType Job started...")
    val connectionURL = args(args.length - 1)
    println("Connection URL is : " + connectionURL)
    val conf = new SparkConf()
      .setAppName("Spark_ComplexType_ArrayType_Validation")
      .set("snappydata.connection", connectionURL)
    val sc : SparkContext = SparkContext.getOrCreate(conf)
    val snc : SnappyContext = SnappyContext(sc)
    val spark : SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sqlContext = SQLContext.getOrCreate(sc)

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath()
    val dataLocation = args(0)
    println("DataLocation : " + dataLocation)
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(
      new File("ValidateSmartConnectorArrayType" + "_" + "column" + System.currentTimeMillis())
      , false))
    val printContent : Boolean = false

    /* --- Snappy Job --- */
    snc.sql("DROP TABLE IF EXISTS Student")
    snc.sql("DROP VIEW IF EXISTS StudentMark")
    snc.sql("DROP TABLE IF EXISTS TempArray")

    snc.sql("CREATE EXTERNAL TABLE IF NOT EXISTS TempArray USING JSON " +
      "OPTIONS(path  '" + dataLocation + "')")
    snc.sql("CREATE TABLE Student USING COLUMN AS (SELECT * FROM TempArray)")

    snc.sql(ComplexTypeUtils.Array_Q1)
    snc.sql(ComplexTypeUtils.Array_Q2)
    snc.sql(ComplexTypeUtils.Array_Q3)
    snc.sql(ComplexTypeUtils.Array_View)
    snc.sql(ComplexTypeUtils.Array_Q4)
    snc.sql(ComplexTypeUtils.Array_Q5)

    if(printContent) {
      println(snc.sql(ComplexTypeUtils.Array_Q1).show())
      println(snc.sql(ComplexTypeUtils.Array_Q2).show())
      println(snc.sql(ComplexTypeUtils.Array_Q3).show())
      println(snc.sql(ComplexTypeUtils.Array_Q4).show())
      println(snc.sql(ComplexTypeUtils.Array_Q5).show())
    }

    /* --- Create the Spark Tables / Views for Verification  --- */
      val arrType = spark.read.json( dataLocation)
      arrType.createTempView("Student")
      spark.sql("CREATE OR REPLACE TEMPORARY VIEW StudentMark " +
      "AS  SELECT id,name,explode(marks) AS Marks FROM Student")

    /* --- Verification --- */

    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.Array_Q1,
      "Array_Q1", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.Array_Q2,
      "Array_Q2", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.Array_Q3,
      "Array_Q3", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.Array_Q4,
      "Array_Q4", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.Array_Q5,
      "Array_Q5", "column", pw, sqlContext)

    /* --- Clean up --- */
    spark.sql("DROP VIEW IF EXISTS Student")
    spark.sql("DROP VIEW IF EXISTS StudentMark")
    snc.sql("DROP VIEW IF EXISTS StudentMark")
    snc.sql("DROP TABLE IF EXISTS Student")
    snc.sql("DROP TABLE IF EXISTS TempArray")

    println("Smart Connector ArrayType SnappyJob completed...")
  }
}
