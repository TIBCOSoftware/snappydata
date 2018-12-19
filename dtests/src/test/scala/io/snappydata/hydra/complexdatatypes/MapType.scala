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

class MapType extends SnappySQLJob{
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snappySession : SnappySession, jobConfig: Config): Any = {

    // scalastyle:off println
    println("Map Type Job started...")

    val snc : SnappyContext = snappySession.sqlContext
    val spark : SparkSession = SparkSession.builder().getOrCreate()
    val sc : SparkContext = SparkContext.getOrCreate()
    val sqlContext : SQLContext = SQLContext.getOrCreate(sc)

    def getCurrentDirectory : String = new File(".").getCanonicalPath
    val outputFile = "ValidateMapType" + "_" + "column" +
      System.currentTimeMillis() + jobConfig.getString("logFileName")
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), false))

    val Q1 = "SELECT * FROM ST.StudentMarksRecord ORDER BY rollno"

    val Q2 = "SELECT rollno, name, " +
             "Maths['maths'],Science['science'] AS SCI ,English['english'] AS ENG," +
             "Computer['computer'],Music['music'],History['history'] " +
             "FROM ST.StudentMarksRecord " +
             "WHERE name = 'Salman Khan'"

    val Q3 = "SELECT name, SUM(Maths['maths'] + Science['science'] + English['english'] + " +
             "Computer['computer'] + Music['music'] + History['history']) AS Total " +
             "FROM ST.StudentMarksRecord " +
             "GROUP BY name ORDER BY Total DESC"

    val Q4 = "SELECT name, " +
             "SUM(Maths['maths'] + Science['science'] + English['english'] + " +
             "Computer['computer'] + Music['music'] + History['history']) AS Total, " +
             "CASE " +
             "WHEN " +
                "SUM(Maths['maths'] + Science['science'] + English['english'] + " +
                 "Computer['computer'] + Music['music'] + History['history']) >= 500 THEN 'A' " +
             "WHEN " +
                "SUM(Maths['maths'] + Science['science'] + English['english'] + " +
                "Computer['computer'] + Music['music'] + History['history']) >=400 THEN 'B' " +
             "WHEN " +
                "SUM(Maths['maths'] + Science['science'] + English['english'] + " +
                "Computer['computer'] + Music['music'] + History['history']) >= 300 THEN 'C' " +
                  "ELSE 'FAIL' " +
             "END AS Grade " +
             "FROM ST.StudentMarksRecord " +
             "GROUP BY name ORDER BY Total DESC"

    val Q5 = "SELECT name, " +
             "(SUM(Maths['maths'] + Science['science'] + English['english'] + " +
                   "Computer['computer'] + Music['music'] + " +
                   "History['history'])*100.0/600.0) AS Percentage " +
             "FROM ST.StudentMarksRecord " +
             "GROUP BY name ORDER BY Percentage DESC"

    val Q6 = "SELECT name, MAX(marks) AS Max, MIN(marks) AS Min FROM " +
             "(SELECT name, Maths['maths'] AS marks FROM ST.StudentMarksRecord " +
             "UNION ALL " +
             "SELECT name, Science['science'] AS marks FROM ST.StudentMarksRecord " +
             "UNION ALL " +
             "SELECT name, English['english'] AS marks FROM ST.StudentMarksRecord " +
             "UNION ALL " +
             "SELECT name, Computer['computer'] AS marks FROM ST.StudentMarksRecord " +
             "UNION ALL " +
             "SELECT name,Music['music'] AS marks FROM ST.StudentMarksRecord " +
             "UNION ALL " +
             "SELECT name,History['history'] AS marks FROM ST.StudentMarksRecord) " +
             "GROUP BY name"

    /* --- Snappy Job --- */
    snc.sql("CREATE SCHEMA ST")

    snc.sql("CREATE TABLE IF NOT EXISTS ST.StudentMarksRecord (rollno Integer, name String, " +
      "Maths MAP<STRING,DOUBLE>, Science MAP<STRING,DOUBLE>, English MAP<STRING,DOUBLE>, " +
      "Computer MAP<STRING,DOUBLE>, Music MAP<STRING,Double>, History MAP<STRING,DOUBLE>) " +
      "USING column")

    snc.sql("INSERT INTO ST.StudentMarksRecord " +
      "SELECT 1,'Mohit Shukla',MAP('maths',97.8),MAP('science',85.2)," +
      "MAP('english',63.9),MAP('computer',45.2),MAP('music',75.2),MAP('history',96.5)")
    snc.sql("INSERT INTO ST.StudentMarksRecord " +
      "SELECT 2,'Nalini Gupta',MAP('maths',89.3),MAP('science',56.3)," +
      "MAP('english',89.1),MAP('computer',78.4),MAP('music',84.1),MAP('history',99.2)")
    snc.sql("INSERT INTO ST.StudentMarksRecord " +
      "SELECT 3,'Kareena Kapoor',MAP('maths',99.9),MAP('science',25.3)," +
      "MAP('english',45.8),MAP('computer',65.8),MAP('music',77.9),MAP('history',23.1)")
    snc.sql("INSERT INTO ST.StudentMarksRecord " +
      "SELECT 4,'Salman Khan',MAP('maths',99.9),MAP('science',89.2)," +
      "MAP('english',85.3),MAP('computer',90.2),MAP('music',83.9),MAP('history',96.1)")
    snc.sql("INSERT INTO ST.StudentMarksRecord " +
      "SELECT 5,'Aranav Goswami',MAP('maths',90.1),MAP('science',80.1)," +
      "MAP('english',70.1),MAP('computer',60.1),MAP('music',50.1),MAP('history',40.1)")
    snc.sql("INSERT INTO ST.StudentMarksRecord " +
      "SELECT 6,'Sudhir Chudhari',MAP('maths',81.1),MAP('science',81.2)," +
      "MAP('english',81.3),MAP('computer',81.4),MAP('music',81.5),MAP('history',81.6)")
    snc.sql("INSERT INTO ST.StudentMarksRecord " +
      "SELECT 7,'Anjana Kashyap',MAP('maths',71.2),MAP('science',65.0)," +
      "MAP('english',52.3),MAP('computer',89.4),MAP('music',95.1),MAP('history',90.9)")
    snc.sql("INSERT INTO ST.StudentMarksRecord " +
      "SELECT 8,'Navika Kumar',MAP('maths',95.5),MAP('science',75.5)," +
      "MAP('english',55.5),MAP('computer',29.3),MAP('music',27.4),MAP('history',50.9)")
    snc.sql("INSERT INTO ST.StudentMarksRecord " +
      "SELECT 9,'Atul Singh',MAP('maths',40.1),MAP('science',42.3)," +
      "MAP('english',46.9),MAP('computer',47.8),MAP('music',44.4),MAP('history',42.0)")
    snc.sql("INSERT INTO ST.StudentMarksRecord " +
      "SELECT 10,'Dheeraj Sen',MAP('maths',62.1),MAP('science',50.7)," +
      "MAP('english',52.3),MAP('computer',67.9),MAP('music',69.9),MAP('history',66.8)")

    snc.sql(Q1)
    println("snc Q1:" + snc.sql(Q1).show)
    snc.sql(Q2)
    snc.sql(Q3)
    snc.sql(Q4)
    snc.sql(Q5)
    println("snc Q5: " + snc.sql(Q5).show)
    snc.sql(Q6)

    /* --- Spark Job --- */
    spark.sql("CREATE SCHEMA ST")

    spark.sql("CREATE TABLE IF NOT EXISTS ST.StudentMarksRecord (rollno Integer, name String, " +
      "Maths MAP<STRING,DOUBLE>, Science MAP<STRING,DOUBLE>, English MAP<STRING,DOUBLE>, " +
      "Computer MAP<STRING,DOUBLE>, Music MAP<STRING,Double>, History MAP<STRING,DOUBLE>) " +
      "USING PARQUET")

    spark.sql("INSERT INTO ST.StudentMarksRecord " +
      "SELECT 1,'Mohit Shukla',MAP('maths',97.8),MAP('science',85.2)," +
      "MAP('english',63.9),MAP('computer',45.2),MAP('music',75.2),MAP('history',96.5)")
    spark.sql("INSERT INTO ST.StudentMarksRecord " +
      "SELECT 2,'Nalini Gupta',MAP('maths',89.3),MAP('science',56.3)," +
      "MAP('english',89.1),MAP('computer',78.4),MAP('music',84.1),MAP('history',99.2)")
    spark.sql("INSERT INTO ST.StudentMarksRecord " +
      "SELECT 3,'Kareena Kapoor',MAP('maths',99.9),MAP('science',25.3)," +
      "MAP('english',45.8),MAP('computer',65.8),MAP('music',77.9),MAP('history',23.1)")
    spark.sql("INSERT INTO ST.StudentMarksRecord " +
      "SELECT 4,'Salman Khan',MAP('maths',99.9),MAP('science',89.2)," +
      "MAP('english',85.3),MAP('computer',90.2),MAP('music',83.9),MAP('history',96.1)")
    spark.sql("INSERT INTO ST.StudentMarksRecord " +
      "SELECT 5,'Aranav Goswami',MAP('maths',90.1),MAP('science',80.1)," +
      "MAP('english',70.1),MAP('computer',60.1),MAP('music',50.1),MAP('history',40.1)")
    spark.sql("INSERT INTO ST.StudentMarksRecord " +
      "SELECT 6,'Sudhir Chudhari',MAP('maths',81.1),MAP('science',81.2)," +
      "MAP('english',81.3),MAP('computer',81.4),MAP('music',81.5),MAP('history',81.6)")
    spark.sql("INSERT INTO ST.StudentMarksRecord " +
      "SELECT 7,'Anjana Kashyap',MAP('maths',71.2),MAP('science',65.0)," +
      "MAP('english',52.3),MAP('computer',89.4),MAP('music',95.1),MAP('history',90.9)")
    spark.sql("INSERT INTO ST.StudentMarksRecord " +
      "SELECT 8,'Navika Kumar',MAP('maths',95.5),MAP('science',75.5)," +
      "MAP('english',55.5),MAP('computer',29.3),MAP('music',27.4),MAP('history',50.9)")
    spark.sql("INSERT INTO ST.StudentMarksRecord " +
      "SELECT 9,'Atul Singh',MAP('maths',40.1),MAP('science',42.3)," +
      "MAP('english',46.9),MAP('computer',47.8),MAP('music',44.4),MAP('history',42.0)")
    spark.sql("INSERT INTO ST.StudentMarksRecord " +
      "SELECT 10,'Dheeraj Sen',MAP('maths',62.1),MAP('science',50.7)," +
      "MAP('english',52.3),MAP('computer',67.9),MAP('music',69.9),MAP('history',66.8)")

    spark.sql(Q1)
    println("spark : Q1" + spark.sql(Q1).show)
    spark.sql(Q2)
    spark.sql(Q3)
    spark.sql(Q4)
    spark.sql(Q5)
    println("spark : Q5" + spark.sql(Q5).show)
    spark.sql(Q6)

    /* --- Verification --- */

    // TODO Due to SNAP-2782 Below line is commented, Hydra Framework required changes.
    // SnappyTestUtils.assertQueryFullResultSet(snc, Q1, "Q1", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, Q2, "Q2", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, Q3, "Q3", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, Q4, "Q4", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, Q5, "Q5", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, Q6, "Q6", "column", pw, sqlContext)

    /* --- Clean Up --- */

    snc.sql("DROP TABLE ST.StudentMarksRecord")
    spark.sql("DROP TABLE ST.StudentMarksRecord")
    snc.sql("DROP SCHEMA ST")
    spark.sql("DROP SCHEMA ST")
  }
}
