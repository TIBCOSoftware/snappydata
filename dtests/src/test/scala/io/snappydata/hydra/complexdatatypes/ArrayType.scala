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
import org.apache.spark.SparkContext
import org.apache.spark.sql._

class ArrayType extends SnappySQLJob {
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {

    // scalastyle:off println
    println("ArraysType Job started...")

    val snc : SnappyContext = snappySession.sqlContext
    val spark : SparkSession = SparkSession.builder().getOrCreate()

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath()
    val outputFile = "ValidateArrayType" + "_" + "column" +
      System.currentTimeMillis() + jobConfig.getString("logFileName")
    val dataLocation = jobConfig.getString("dataFilesLocation")
    println("DataLocation : " + dataLocation)
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), false))
    val sc = SparkContext.getOrCreate()
    val sqlContext = SQLContext.getOrCreate(sc)
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
    val arrType = spark.read.json(dataLocation)
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

    snc.sql("DROP TABLE IF EXISTS Student")
    snc.sql("DROP VIEW IF EXISTS StudentMark")
    snc.sql(("DROP TABLE IF EXISTS TempArray"))

    println("ArrayType SnappyJob completed...")
  }
}
