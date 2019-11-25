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

class StructType extends SnappySQLJob{
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {

    // scalastyle:off println
    println("Struct Type Job started...")

    val snc : SnappyContext = snappySession.sqlContext
    val spark : SparkSession = SparkSession.builder().getOrCreate()
    val sc : SparkContext = SparkContext.getOrCreate()
    def getCurrentDirectory : String = new File(".").getCanonicalPath()
    val outputFile = "ValidateStructType" + "_" + "column" +
      System.currentTimeMillis() + jobConfig.getString("logFileName")
    val dataLocation = jobConfig.getString("dataFilesLocation")
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), false))
    val sqlContext = SQLContext.getOrCreate(sc)
    val printContent : Boolean = false

    /* --- Snappy Job --- */
    snc.sql("DROP TABLE IF EXISTS CricketRecord")
    snc.sql("DROP TABLE IF EXISTS TempCRRecord")

    snc.sql("CREATE EXTERNAL TABLE IF NOT EXISTS TempCRRecord  USING JSON " +
      "OPTIONS(path '" + dataLocation + "')")
    snc.sql("CREATE TABLE IF NOT EXISTS CricketRecord USING COLUMN " +
      "AS (SELECT * FROM TempCRRecord)")

    if(printContent) {
      println("Struct_Q1 : " + snc.sql(ComplexTypeUtils.Struct_Q1).show())
      println("Struct_Q2 : " + snc.sql(ComplexTypeUtils.Struct_Q2).show())
      println("Struct_Q3 : " + snc.sql(ComplexTypeUtils.Struct_Q3).show())
      println("Struct_Q4 : " + snc.sql(ComplexTypeUtils.Struct_Q4).show())
    }

    /* --- Create the Spark Tables / Views for Verification  --- */
    val structType = spark.read.json(dataLocation)
    structType.createTempView("CricketRecord")

    SnappyTestUtils.tableType = "column"
    /* --- Verification --- */
    SnappyTestUtils.assertQuery(snc, ComplexTypeUtils.Struct_Q1, "Struct_Q1", pw, sqlContext)
    SnappyTestUtils.assertQuery(snc, ComplexTypeUtils.Struct_Q2, "Struct_Q2", pw, sqlContext)
    SnappyTestUtils.assertQuery(snc, ComplexTypeUtils.Struct_Q3, "Struct_Q3", pw, sqlContext)
    SnappyTestUtils.assertQuery(snc, ComplexTypeUtils.Struct_Q4, "Struct_Q4", pw, sqlContext)

    /* --- Clean up --- */
    snc.sql("DROP TABLE IF EXISTS CricketRecord")
    snc.sql("DROP TABLE IF EXISTS TempCRRecord")

    println("StructType SQL, Job completed....")
  }
}
