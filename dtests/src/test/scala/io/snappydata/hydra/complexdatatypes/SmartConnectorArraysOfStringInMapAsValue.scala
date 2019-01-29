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
import org.apache.spark.sql.{DataFrame, SQLContext, SnappyContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SmartConnectorArraysOfStringInMapAsValue {
  def main(args: Array[String]): Unit = {
    // scalastyle:off println
    println("Smart Connector ArraysOfStringInMapAsValue Type Job started...")
    val connectionURL = args(args.length - 1)
    println("Connection URL is : " + connectionURL)
    val conf = new SparkConf()
      .setAppName("Spark_ComplexType_ArraysOfStringInMapAsValueType_Validation")
      .set("snappydata.connection", connectionURL)
    val sc : SparkContext = SparkContext.getOrCreate(conf)
    val snc : SnappyContext = SnappyContext(sc)
    val spark : SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sqlContext : SQLContext = SQLContext.getOrCreate(sc)

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath()
    val dataLocation : String = args(0)
    println("DataLocation : " + dataLocation)
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(
      new File("ValidateSmartConnectorMapType" + "_" + "column" + System.currentTimeMillis())
      , false))
    val printContent : Boolean = false

    /* --- Snappy Job --- */
    snc.sql("DROP TABLE IF EXISTS TempFamousPeople")
    snc.sql("DROP TABLE IF EXISTS FamousPeople")
    snc.sql("DROP VIEW IF EXISTS FamousPeopleView")

    /* Need to do it using JSON as well */
    snc.sql("CREATE EXTERNAL TABLE IF NOT EXISTS TempFamousPeople " +
      "USING PARQUET OPTIONS(path '" + dataLocation + "')")
    val fp = snc.sql(  "CREATE TABLE IF NOT EXISTS FamousPeople " +
      "USING COLUMN AS (SELECT * FROM TempFamousPeople)")

    snc.sql(ComplexTypeUtils.Array_Map_TempView)
    snc.sql(ComplexTypeUtils.Array_Map_Q1)
    snc.sql(ComplexTypeUtils.Array_Map_Q2)
    snc.sql(ComplexTypeUtils.Array_Map_Q3)

    if(printContent) {
      println("snc : Array_Map_Q1 " + snc.sql(ComplexTypeUtils.Array_Map_Q1).show)
      println("snc : Array_Map_Q2 " + snc.sql(ComplexTypeUtils.Array_Map_Q2).show)
      println("snc : Array_Map_Q3 " + snc.sql(ComplexTypeUtils.Array_Map_Q3).show)
    }

    /* --- Create the Spark Tables / Views for Verification  --- */
    val arrInMap : DataFrame = spark.read.parquet(dataLocation)
    arrInMap.createOrReplaceTempView("FamousPeople")
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW FamousPeopleView " +
      "AS SELECT country, explode(celebrities) FROM FamousPeople")

    /* --- Verification --- */

    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.Array_Map_Q1, "Array_Map_Q1",
      "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.Array_Map_Q2, "Array_Map_Q2",
      "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.Array_Map_Q3, "Array_Map_Q3",
      "column", pw, sqlContext)

    /* --- Clean up --- */
    snc.sql("DROP TABLE IF EXISTS TempFamousPeople")
    snc.sql("DROP TABLE IF EXISTS FamousPeople")
    snc.sql("DROP VIEW IF EXISTS FamousPeopleView")
    println("Smart Connector ArraysOfStringInMapAsValue Type Job completed...")
  }
}
