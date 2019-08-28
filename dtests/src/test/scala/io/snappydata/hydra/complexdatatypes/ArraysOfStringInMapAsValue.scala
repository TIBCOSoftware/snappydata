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

import com.typesafe.config.Config
import io.snappydata.hydra.SnappyTestUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql._

class ArraysOfStringInMapAsValue extends SnappySQLJob{
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {

    // scalastyle:off println
    println("ArraysofStringInMapAsValue Type Job started...")

    val snc : SnappyContext = snappySession.sqlContext
    val spark : SparkSession = SparkSession.builder().getOrCreate()
    val sc : SparkContext = SparkContext.getOrCreate()
    val sqlContext : SQLContext = SQLContext.getOrCreate(sc)

    def getCurrentDirectory : String = new File(".").getCanonicalPath()
    val outputFile = "ValidateArraysOfStringInMaptype" + "_" + "column" +
      System.currentTimeMillis() + jobConfig.getString("logFileName")
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), false))
    val dataLocation = jobConfig.getString("dataFilesLocation")
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
    SnappyTestUtils.tableType = "column"
    SnappyTestUtils.assertQuery(snc, ComplexTypeUtils.Array_Map_Q1, "Array_Map_Q1", pw, sqlContext)
    SnappyTestUtils.assertQuery(snc, ComplexTypeUtils.Array_Map_Q2, "Array_Map_Q2", pw, sqlContext)
    SnappyTestUtils.assertQuery(snc, ComplexTypeUtils.Array_Map_Q3, "Array_Map_Q3", pw, sqlContext)

    /* --- Clean up --- */
    snc.sql("DROP TABLE IF EXISTS TempFamousPeople")
    snc.sql("DROP TABLE IF EXISTS FamousPeople")
    snc.sql("DROP VIEW IF EXISTS FamousPeopleView")

    println("ArraysOfStringInMapAsValue SQL, job completed...")
  }
}
