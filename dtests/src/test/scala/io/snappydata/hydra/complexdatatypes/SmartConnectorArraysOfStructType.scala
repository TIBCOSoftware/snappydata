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
import org.apache.spark.sql.{DataFrame, SQLContext, SnappyContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SmartConnectorArraysOfStructType {
  def main(args: Array[String]): Unit = {
    // scalastyle:off println
    println("Smart Connector ArraysOfStruct Type Job started...")
    val connectionURL = args(args.length - 1)
    println("Connection URL is : " + connectionURL)
    val conf = new SparkConf()
      .setAppName("Spark_ComplexType_ArraysOfStructType_Validation")
      .set("snappydata.connection", connectionURL)
    val sc : SparkContext = SparkContext.getOrCreate(conf)
    val snc : SnappyContext = SnappyContext(sc)
    val spark : SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sqlContext : SQLContext = SQLContext.getOrCreate(sc)

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath()
    val dataLocation : String = args(0)
    println("DataLocation : " + dataLocation)
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(
      new File("ValidateSmartConnectorArraysOfStructType" +
        "_" + "column" + System.currentTimeMillis())
      , false))
    val printContent : Boolean = true

    /* --- Snappy Job --- */
    snc.sql("DROP TABLE IF EXISTS TwoWheeler")
    snc.sql("DROP TABLE IF EXISTS TempBike")

    snc.sql("CREATE EXTERNAL TABLE IF NOT EXISTS TempBike USING JSON " +
      "OPTIONS(path '" + dataLocation + "')" )
    snc.sql("CREATE TABLE IF NOT EXISTS TwoWheeler USING COLUMN " +
      "AS (SELECT * FROM TempBike)")

    snc.sql(ComplexTypeUtils.ArraysOfStruct_Q1)
    snc.sql(ComplexTypeUtils.ArraysOfStruct_Q2)
    snc.sql(ComplexTypeUtils.ArraysOfStruct_Q3)
    snc.sql(ComplexTypeUtils.ArraysOfStruct_Q4)
    snc.sql(ComplexTypeUtils.ArraysOfStruct_Q5)

    if(printContent) {
      println("snc : ArraysOfStruct_Q1  " + (snc.sql(ComplexTypeUtils.ArraysOfStruct_Q1).show()))
      println("snc : ArraysOfStruct_Q2  " + (snc.sql(ComplexTypeUtils.ArraysOfStruct_Q2).show()))
      println("snc : ArraysOfStruct_Q3  " + (snc.sql(ComplexTypeUtils.ArraysOfStruct_Q3).show()))
      println("snc : ArraysOfStruct_Q4  " + (snc.sql(ComplexTypeUtils.ArraysOfStruct_Q4).show()))
      println("snc : ArraysOfStruct_Q5  " + (snc.sql(ComplexTypeUtils.ArraysOfStruct_Q5).show()))
    }

    /* --- Create the Spark Tables / Views for Verification  --- */
    val arrOfStruct : DataFrame = spark.read.json(dataLocation)
    arrOfStruct.createOrReplaceTempView("TwoWheeler")

    /* --- Verification --- */

    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.ArraysOfStruct_Q1,
      "ArraysOfStruct_Q1", "column", pw, sqlContext)
    // TODO Due to SNAP-2782 Below line is commented, Hydra Framework required changes.
    //    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.ArraysOfStruct_Q2,
    //      "ArraysOfStruct_Q2", "column", pw, sqlContext)
    //    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.ArraysOfStruct_Q3,
    //      "ArraysOfStruct_Q3","column", pw, sqlContext)
    //    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.ArraysOfStruct_Q4,
    //      "ArraysOfStruct_Q4","column", pw, sqlContext)
    //    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.ArraysOfStruct_Q5,
    //      "ArraysOfStruct_Q5","column", pw, sqlContext)

    /* --- Clean up --- */
    snc.sql("DROP TABLE IF EXISTS TwoWheeler")
    snc.sql("DROP TABLE IF EXISTS TempBike")

    println("Smart Connector ArraysOfStruct Type Job completed...")
  }
}
