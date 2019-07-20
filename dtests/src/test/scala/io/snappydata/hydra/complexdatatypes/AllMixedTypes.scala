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

class AllMixedTypes extends SnappySQLJob{
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {

    // scalastyle:off println
    println("AllMixedType Job started...")

    val snc : SnappyContext = snappySession.sqlContext
    val spark : SparkSession = SparkSession.builder().getOrCreate()
    val sc : SparkContext = SparkContext.getOrCreate()
    val sqlContext : SQLContext = SQLContext.getOrCreate(sc)
    val printContent : Boolean = false

    def getCurrentDirectory : String = new File(".").getCanonicalPath
    val outputFile : String = "ValidateAllMixedTypes" + "_" + "column" +
      System.currentTimeMillis() + jobConfig.getString("logFileName")
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), false))
    val dataLocation = jobConfig.getString("dataFilesLocation")

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

    println("AllMixedTypes SQL, job completed...")
  }
}
