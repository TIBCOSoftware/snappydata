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

    val Array_Q1 : String = "SELECT * FROM ST.Student ORDER BY rollno"
    val Array_Q2 : String = "SELECT rollno, marks[0] AS Maths, marks[1] AS Science," +
                      "marks[2] AS English,marks[3] AS Computer, marks[4] AS Music, marks[5] " +
                      "FROM ST.Student WHERE name = 'Salman Khan'"
    val Array_Q3 : String = "SELECT rollno, name, explode(marks) as Marks FROM ST.Student"
    val Array_Q4 : String = "SELECT name,SUM(Marks) AS Total FROM StudentMark " +
                       "GROUP BY name ORDER BY Total DESC"
    val Array_Q5 : String = "SELECT name,MAX(Marks),MIN(Marks) FROM StudentMark GROUP BY name"

    val snc : SnappyContext = snappySession.sqlContext
    val spark : SparkSession = SparkSession.builder().getOrCreate()

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath()
    val outputFile = "ValidateArrayType" + "_" + "column" +
      System.currentTimeMillis() + jobConfig.getString("logFileName")
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), false))
    val sc = SparkContext.getOrCreate()
    val sqlContext = SQLContext.getOrCreate(sc)

    /* --- Snappy Job --- */
    snc.sql("CREATE SCHEMA ST")

    snc.sql("CREATE TABLE IF NOT EXISTS ST.Student(rollno Int, name String, marks ARRAY<Double>)" +
      " USING column")
    snc.sql("INSERT INTO ST.Student " +
      "SELECT 1,'Mohit Shukla', Array(97.8,85.2,63.9,45.2,75.2,96.5)")
    snc.sql("INSERT INTO ST.Student " +
      "SELECT 2,'Nalini Gupta',Array(89.3,56.3,89.1,78.4,84.1,99.2)")
    snc.sql("INSERT INTO ST.Student " +
      "SELECT 3,'Kareena Kapoor',Array(99.9,25.3,45.8,65.8,77.9,23.1)")
    snc.sql("INSERT INTO ST.Student " +
      "SELECT 4,'Salman Khan',Array(99.9,89.2,85.3,90.2,83.9,96.1)")
    snc.sql("INSERT INTO ST.Student " +
      "SELECT 5,'Aranav Goswami',Array(90.1,80.1,70.1,60.1,50.1,40.1)")
    snc.sql("INSERT INTO ST.Student " +
      "SELECT 6,'Sudhir Chudhari',Array(81.1,81.2,81.3,81.4,81.5,81.6)")
    snc.sql("INSERT INTO ST.Student " +
      "SELECT 7,'Anjana Kashyap',Array(71.2,65.0,52.3,89.4,95.1,90.9)")
    snc.sql("INSERT INTO ST.Student " +
      "SELECT 8,'Navika Kumar',Array(95.5,75.5,55.5,29.3,27.4,50.9)")
    snc.sql("INSERT INTO ST.Student " +
      "SELECT 9,'Atul Singh',Array(40.1,42.3,46.9,47.8,44.4,42.0)")
    snc.sql("INSERT INTO ST.Student " +
      "SELECT 10,'Dheeraj Sen',Array(62.1,50.7,52.3,67.9,69.9,66.8)")

    snc.sql(Array_Q1)
    snc.sql(Array_Q2)
    snc.sql(Array_Q3)
    snc.sql("CREATE TEMPORARY VIEW StudentMark AS " +
      "SELECT rollno,name,explode(marks) AS Marks FROM ST.Student")
    snc.sql(Array_Q4)
    snc.sql(Array_Q5)

    /* --- Spark Job --- */
    spark.sql("CREATE SCHEMA ST")

    spark.sql("CREATE TABLE IF NOT EXISTS ST.Student(rollno Int, name String, " +
      "marks ARRAY<Double>) USING PARQUET")

    spark.sql("INSERT INTO ST.Student " +
      "SELECT 1,'Mohit Shukla', Array(97.8,85.2,63.9,45.2,75.2,96.5)")
    spark.sql("INSERT INTO ST.Student " +
      "SELECT 2,'Nalini Gupta',Array(89.3,56.3,89.1,78.4,84.1,99.2)")
    spark.sql("INSERT INTO ST.Student " +
      "SELECT 3,'Kareena Kapoor',Array(99.9,25.3,45.8,65.8,77.9,23.1)")
    spark.sql("INSERT INTO ST.Student " +
      "SELECT 4,'Salman Khan',Array(99.9,89.2,85.3,90.2,83.9,96.1)")
    spark.sql("INSERT INTO ST.Student " +
      "SELECT 5,'Aranv Goswami',Array(90.1,80.1,70.1,60.1,50.1,40.1)")
    spark.sql("INSERT INTO ST.Student " +
      "SELECT 6,'Sudhir Chudhari',Array(81.1,81.2,81.3,81.4,81.5,81.6)")
    spark.sql("INSERT INTO ST.Student " +
      "SELECT 7,'Anjana Kashyap',Array(71.2,65.0,52.3,89.4,95.1,90.9)")
    spark.sql("INSERT INTO ST.Student " +
      "SELECT 8,'Navika Kumar',Array(95.5,75.5,55.5,29.3,27.4,50.9)")
    spark.sql("INSERT INTO ST.Student " +
      "SELECT 9,'Atul Singh',Array(40.1,42.3,46.9,47.8,44.4,42.0)")
    spark.sql("INSERT INTO ST.Student " +
      "SELECT 10,'Dheeraj Sen',Array(62.1,50.7,52.3,67.9,69.9,66.8)")

    spark.sql(Array_Q1)
    spark.sql(Array_Q2)
    spark.sql(Array_Q3)
    spark.sql("CREATE TEMPORARY VIEW StudentMark AS " +
      "SELECT rollno,name,explode(marks) AS Marks FROM ST.Student")
    spark.sql(Array_Q4)
    spark.sql(Array_Q5)

    /* --- Verification --- */

    SnappyTestUtils.assertQueryFullResultSet(snc, Array_Q1, "Array_Q1", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, Array_Q2, "Array_Q2", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, Array_Q3, "Array_Q3", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, Array_Q4, "Array_Q4", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, Array_Q5, "Array_Q5", "column", pw, sqlContext)

    /* --- Clean up --- */

    snc.sql("DROP TABLE IF EXISTS ST.Student")
    snc.sql("DROP VIEW IF EXISTS StudentMark")
    spark.sql("DROP TABLE IF EXISTS ST.Student")
    spark.sql("DROP VIEW IF EXISTS StudentMark")
    snc.sql("DROP SCHEMA IF EXISTS ST")
    spark.sql("DROP SCHEMA IF EXISTS ST")
  }
}
