/*
 * Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
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

object SmartConnectorAllMixedType {
  def main(args: Array[String]): Unit = {
    // scalastyle:off println
    println("Smart Connector AllMixedType Job started...")
    val connectionURL = args(args.length - 1)
    println("Connection URL is : " + connectionURL)
    val conf = new SparkConf()
      .setAppName("Spark_ComplexType_AllMixedType_Validation")
      .set("snappydata.connection", connectionURL)
    val sc : SparkContext = SparkContext.getOrCreate(conf)
    val snc : SnappyContext = SnappyContext(sc)
    val spark : SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sqlContext : SQLContext = SQLContext.getOrCreate(sc)

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath()
    val dataLocation : String = args(0)
    println("DataLocation : " + dataLocation)
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(
      new File("ValidateSmartConnectorAllMixedType" + "_" + "column" + System.currentTimeMillis())
      , false))
    val printContent : Boolean = false

    /* --- Snappy Job --- */
    snc.sql("DROP TABLE IF EXISTS TwentyTwenty")
    snc.sql("DROP TABLE IF EXISTS TempTwenty")

    snc.sql("CREATE EXTERNAL TABLE IF NOT EXISTS TempTwenty " +
      "USING JSON OPTIONS(path '" + dataLocation + "')")
    snc.sql("CREATE TABLE IF NOT EXISTS TwentyTwenty USING COLUMN " +
      "AS (SELECT * FROM TempTwenty)")

    snc.sql(ComplexTypeUtils.Mixed_Q1)
    snc.sql(ComplexTypeUtils.Mixed_Q2)
    snc.sql(ComplexTypeUtils.Mixed_Q3)
    snc.sql(ComplexTypeUtils.Mixed_Q4)
    snc.sql(ComplexTypeUtils.Mixed_Q5)

    if(printContent) {
      println("snc : Mixed_Q1 " + (snc.sql(ComplexTypeUtils.Mixed_Q1).show))
      println("snc : Mixed_Q2 " + (snc.sql(ComplexTypeUtils.Mixed_Q2).show))
      println("snc : Mixed_Q3 " + (snc.sql(ComplexTypeUtils.Mixed_Q3).show))
      println("snc : Mixed_Q4 " + (snc.sql(ComplexTypeUtils.Mixed_Q4).show))
      println("snc : Mixed_Q5 " + (snc.sql(ComplexTypeUtils.Mixed_Q5).show))
    }

    /* --- Create the Spark Tables / Views for Verification  --- */
    val allMixed : DataFrame = spark.read.json(dataLocation)
    allMixed.createOrReplaceTempView("TwentyTwenty")

    /* --- Verification --- */

    // TODO Due to SNAP-2782 Below line is commented, Hydra Framework required changes.
    // SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.Mixed_Q1,
    // "Mixed_Q1", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.Mixed_Q2,
      "Mixed_Q2", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.Mixed_Q3,
      "Mixed_Q3", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.Mixed_Q4,
      "Mixed_Q4", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, ComplexTypeUtils.Mixed_Q5,
      "Mixed_Q5", "column", pw, sqlContext)

    /* --- Clean up --- */
    snc.sql("DROP TABLE IF EXISTS TwentyTwenty")
    snc.sql("DROP TABLE IF EXISTS TempTwenty")

    println("Smart Connector AllMixedType Job completed...")
  }
}
