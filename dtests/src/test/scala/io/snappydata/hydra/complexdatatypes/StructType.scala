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

class StructType extends SnappySQLJob{
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {

    // scalastyle:off println
    println("Struct Type Job started...")

    val Q1 = "SELECT name, TestRecord.Runs, TestRecord.Avg FROM CR.CricketRecord " +
      "ORDER BY TestRecord.Runs DESC"
    val Q2 = "SELECT SUM(TestRecord.Runs) AS TotalRuns FROM CR.CricketRecord"
    val Q3 = "SELECT name FROM CR.CricketRecord WHERE TestRecord.batStyle = 'Left Hand'"
    val Q4 = "SELECT name, TestRecord.batStyle,TestRecord.Matches,TestRecord.Runs,TestRecord.Avg " +
      "FROM CR.CricketRecord " +
      "ORDER BY TestRecord.Matches DESC"


    val snc : SnappyContext = snappySession.sqlContext
    val spark : SparkSession = SparkSession.builder().getOrCreate()
    val sc : SparkContext = SparkContext.getOrCreate()
    def getCurrentDirectory : String = new File(".").getCanonicalPath()
    val outputFile = "ValidateStructType" + "_" + "column" +
      System.currentTimeMillis() + jobConfig.getString("logFileName")
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), false))
    val sqlContext = SQLContext.getOrCreate(sc)

    /* --- Snappy Job --- */
    snc.sql("CREATE SCHEMA CR")

    snc.sql("CREATE TABLE IF NOT EXISTS CR.CricketRecord(name String, " +
      "TestRecord STRUCT<batStyle:String,Matches:Long,Runs:Int,Avg:Double>) USING column")

    snc.sql("INSERT INTO CR.CricketRecord " +
      "SELECT 'Sachin Tendulkar',STRUCT('Right Hand',200,15921,53.79)")
    snc.sql("INSERT INTO CR.CricketRecord " +
      "SELECT 'Saurav Ganguly',STRUCT('Left Hand',113,7212,51.26)")
    snc.sql("INSERT INTO CR.CricketRecord " +
      "SELECT 'Rahul Drvaid',STRUCT('Right Hand',164,13288,52.31)")
    snc.sql("INSERT INTO CR.CricketRecord " +
      "SELECT 'Yuvraj Singh',STRUCT('Left Hand',40,1900,33.93)")
    snc.sql("INSERT INTO CR.CricketRecord " +
      "SELECT 'MahendraSingh Dhoni',STRUCT('Right Hand',90,4876,38.09)")
    snc.sql("INSERT INTO CR.CricketRecord " +
      "SELECT 'Kapil Dev',STRUCT('Right Hand',131,5248,31.05)")
    snc.sql("INSERT INTO CR.CricketRecord " +
      "SELECT 'Zahir Khan',STRUCT('Right Hand',92,1230,11.94)")
    snc.sql("INSERT INTO CR.CricketRecord " +
      "SELECT 'Gautam Gambhir',STRUCT('Left Hand',58,4154,41.96)")
    snc.sql("INSERT INTO CR.CricketRecord " +
      "SELECT 'VVS Laxman',STRUCT('Right Hand',134,8781,45.5)")
    snc.sql("INSERT INTO CR.CricketRecord " +
      "SELECT 'Virendra Sehwag',STRUCT('Right Hand',104,8586,49.34)")
    snc.sql("INSERT INTO CR.CricketRecord " +
      "SELECT 'Sunil Gavaskar',STRUCT('Right Hand',125,10122,51.12)")
    snc.sql("INSERT INTO CR.CricketRecord " +
      "SELECT 'Anil Kumble',STRUCT('Right Hand',132,2506,17.65)")

    snc.sql(Q1)
    snc.sql(Q2)
    snc.sql(Q3)
    snc.sql(Q4)

    /* --- Spark Job --- */
    spark.sql("CREATE SCHEMA CR")

    spark.sql("CREATE TABLE IF NOT EXISTS CR.CricketRecord(name String, " +
      "TestRecord STRUCT<batStyle:String,Matches:Long,Runs:Int,Avg:Double>) USING PARQUET")

    spark.sql("INSERT INTO CR.CricketRecord " +
      "SELECT 'Sachin Tendulkar',STRUCT('Right Hand',200,15921,53.79)")
    spark.sql("INSERT INTO CR.CricketRecord " +
      "SELECT 'Saurav Ganguly',STRUCT('Left Hand',113,7212,51.26)")
    spark.sql("INSERT INTO CR.CricketRecord " +
      "SELECT 'Rahul Drvaid',STRUCT('Right Hand',164,13288,52.31)")
    spark.sql("INSERT INTO CR.CricketRecord " +
      "SELECT 'Yuvraj Singh',STRUCT('Left Hand',40,1900,33.93)")
    spark.sql("INSERT INTO CR.CricketRecord " +
      "SELECT 'MahendraSingh Dhoni',STRUCT('Right Hand',90,4876,38.09)")
    spark.sql("INSERT INTO CR.CricketRecord " +
      "SELECT 'Kapil Dev',STRUCT('Right Hand',131,5248,31.05)")
    spark.sql("INSERT INTO CR.CricketRecord " +
      "SELECT 'Zahir Khan',STRUCT('Right Hand',92,1230,11.94)")
    spark.sql("INSERT INTO CR.CricketRecord " +
      "SELECT 'Gautam Gambhir',STRUCT('Left Hand',58,4154,41.96)")
    spark.sql("INSERT INTO CR.CricketRecord " +
      "SELECT 'VVS Laxman',STRUCT('Right Hand',134,8781,45.5)")
    spark.sql("INSERT INTO CR.CricketRecord " +
      "SELECT 'Virendra Sehwag',STRUCT('Right Hand',104,8586,49.34)")
    spark.sql("INSERT INTO CR.CricketRecord " +
      "SELECT 'Sunil Gavaskar',STRUCT('Right Hand',125,10122,51.12)")
    spark.sql("INSERT INTO CR.CricketRecord " +
      "SELECT 'Anil Kumble',STRUCT('Right Hand',132,2506,17.65)")

    spark.sql(Q1)
    spark.sql(Q2)
    spark.sql(Q3)
    spark.sql(Q4)

    /* --- Verification --- */

    SnappyTestUtils.assertQueryFullResultSet(snc, Q1, "Q1", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, Q2, "Q2", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, Q3, "Q3", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, Q4, "Q4", "column", pw, sqlContext)


    /* --- Clean up --- */

    snc.sql("DROP TABLE IF EXISTS CR.CricketRecord")
    spark.sql("DROP TABLE IF EXISTS CR.CricketRecord")
    snc.sql("DROP SCHEMA CR")
    spark.sql("DROP SCHEMA CR")
  }
}
