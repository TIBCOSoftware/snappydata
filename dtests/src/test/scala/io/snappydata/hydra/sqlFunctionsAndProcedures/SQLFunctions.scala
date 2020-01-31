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
package io.snappydata.hydra.sqlFunctionsAndProcedures

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config
import io.snappydata.hydra.SnappyTestUtils
import org.apache.spark.sql._

class SQLFunctions extends SnappySQLJob {
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    // scalastyle:off println
    println("Snappy Embedded Job -  SQL Functions")
    val snc : SnappyContext = snappySession.sqlContext
    val spark : SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    val outputFile = "ValidateSQLFunctions" + "_" + "column" +
      System.currentTimeMillis() + jobConfig.getString("logFileName")
    val pw : PrintWriter = new PrintWriter(new FileOutputStream(new File(outputFile), false))
    val sqlContext : SQLContext = spark.sqlContext
    pw.println("Snappy Embedded Job -  SQL Functions")

    /**
      *  Below queries/code test the functions :
      *  1. date,
      *  2. date_add,
      *  3. date_sub,
      *  4. date_diff,
      *  5. date_format
      */
    //  CREATE TABLE IN SPARK / SNAPPY.
    spark.sql(SQLFunctionsUtils.createColTypeTblInSpark)
    spark.sql(SQLFunctionsUtils.createRowTypeTblInSpark)
    snc.sql(SQLFunctionsUtils.createColumnTbl_DateFunctions_1)
    snc.sql(SQLFunctionsUtils.createRowTbl_DateFunctions_1)
    //  INSERT RECORDS IN SPARK / SNAPPY.
    spark.sql(SQLFunctionsUtils.insertInto +
      SQLFunctionsUtils.columnTbl + SQLFunctionsUtils.values + SQLFunctionsUtils.dateSet1)
    spark.sql(SQLFunctionsUtils.insertInto +
      SQLFunctionsUtils.columnTbl + SQLFunctionsUtils.values + SQLFunctionsUtils.dateSet2)
    spark.sql(SQLFunctionsUtils.insertInto +
      SQLFunctionsUtils.columnTbl + SQLFunctionsUtils.values + SQLFunctionsUtils.dateSet3)
    spark.sql(SQLFunctionsUtils.insertInto +
      SQLFunctionsUtils.rowTbl + SQLFunctionsUtils.values + SQLFunctionsUtils.dateSet1)
    spark.sql(SQLFunctionsUtils.insertInto +
      SQLFunctionsUtils.rowTbl + SQLFunctionsUtils.values + SQLFunctionsUtils.dateSet2)
    spark.sql(SQLFunctionsUtils.insertInto +
      SQLFunctionsUtils.rowTbl + SQLFunctionsUtils.values + SQLFunctionsUtils.dateSet3)
    snc.sql(SQLFunctionsUtils.insertInto +
      SQLFunctionsUtils.columnTbl + SQLFunctionsUtils.values + SQLFunctionsUtils.dateSet1)
    snc.sql(SQLFunctionsUtils.insertInto +
      SQLFunctionsUtils.columnTbl + SQLFunctionsUtils.values + SQLFunctionsUtils.dateSet2)
    snc.sql(SQLFunctionsUtils.insertInto +
      SQLFunctionsUtils.columnTbl + SQLFunctionsUtils.values + SQLFunctionsUtils.dateSet3)
    snc.sql(SQLFunctionsUtils.insertInto +
      SQLFunctionsUtils.rowTbl + SQLFunctionsUtils.values + SQLFunctionsUtils.dateSet1)
    snc.sql(SQLFunctionsUtils.insertInto +
      SQLFunctionsUtils.rowTbl + SQLFunctionsUtils.values + SQLFunctionsUtils.dateSet2)
    snc.sql(SQLFunctionsUtils.insertInto +
      SQLFunctionsUtils.rowTbl + SQLFunctionsUtils.values + SQLFunctionsUtils.dateSet3)
    //  SELECT QUERY / VALIDATION ROUTINE.
    SnappyTestUtils.assertQueryFullResultSet(snc,
      SQLFunctionsUtils.selectQueryOnColTbl_DateFunctions_1,
      "Q1_ColumnTbl_DateFunc", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc,
      SQLFunctionsUtils.selectQueryOnRowTbl_DateFunctions_1,
      "Q2_RowTbl_DateFunc", "row", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc,
      SQLFunctionsUtils.selectMonth_In_DateFormatFunc,
      "Q3_SelectMonth_DateFormat", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc,
      SQLFunctionsUtils.selectDay_DateFormatFunc,
      "Q4_SelectDay_DateFormat", "column", pw, sqlContext)
    // DROP SPARK / SNAPPY TABLES.
    snc.sql(SQLFunctionsUtils.dropColumnTbl_DateFunctions_1)
    snc.sql(SQLFunctionsUtils.dropRowTbl_DateFunctions_1)
    spark.sql(SQLFunctionsUtils.dropColumnTbl_DateFunctions_1)
    spark.sql(SQLFunctionsUtils.dropRowTbl_DateFunctions_1)

    /**
      *  Below queries test the functions :
      *  6. repeat
      *  7. reverse
      *  (NOTE : reverse logic for arrays is available since  Spark 2.4.0,
      *  test to be added after spark 2.4.0 merge
      */
    //  CREATE TABLE IN SPARK / SNAPPY.
    spark.sql(SQLFunctionsUtils.createColTypeTbl_RseRpt_Spark)
    spark.sql(SQLFunctionsUtils.createRowTypeTbl_rserpt_Spark)
    snc.sql(SQLFunctionsUtils.createColumnTbl_RseRpt)
    snc.sql(SQLFunctionsUtils.createRowTbl_RseRpt)
    //  INSERT RECORDS IN SPARK / SNAPPY.
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.rseRptSet1)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.rseRptSet2)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.rseRptSet1)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.rseRptSet2)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.rseRptSet1)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.rseRptSet2)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.rseRptSet1)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.rseRptSet2)
    //  SELECT QUERY / VALIDATION ROUTINE.
    SnappyTestUtils.assertQueryFullResultSet(snc, SQLFunctionsUtils.select_ColTbl_RseRpt,
      "Q5_Select_RevRpt", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, SQLFunctionsUtils.select_RowTbl_RseRpt,
      "Q6_Select_RevRpt", "row", pw, sqlContext)
    // DROP SPARK / SNAPPY TABLES.
    snc.sql(SQLFunctionsUtils.dropColTbl_RseRpt)
    snc.sql(SQLFunctionsUtils.dropRowTbl_RseRpt)
    spark.sql(SQLFunctionsUtils.dropColTbl_RseRpt)
    spark.sql(SQLFunctionsUtils.dropRowTbl_RseRpt)

    /**
      * Below queries test the functions :
      * 8. !(Logical Not)
      * (Logical NOT is not working in Snappy Cluster)
      * Add the boolean column and data once issue resolved.
      * 9. & (Bitwise AND)
      * 10. ^ (Bitwise exclusiveOR/ExOR)
      * val NOT_AND_ExOR_Set1 : String="(1,7,3,true)"
      * val createColTypeTbl_NOT_AND_ExOR_Spark : String= createTbl + columnTbl +
      * "(id int,n1 int,n2 int,b boolean)"
      */
    //  CREATE TABLE IN SPARK / SNAPPY.
    spark.sql(SQLFunctionsUtils.createColTypeTbl_NOT_AND_ExOR_Spark)
    spark.sql(SQLFunctionsUtils.createRowTypeTbl_NOT_AND_ExOR_Spark)
    snc.sql(SQLFunctionsUtils.createColumnTbl_NOT_AND_ExOR)
    snc.sql(SQLFunctionsUtils.createRowTbl_NOT_AND_ExOR)
    //  INSERT RECORDS IN SPARK / SNAPPY.
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.NOT_AND_ExOR_Set1)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.NOT_AND_ExOR_Set2)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.NOT_AND_ExOR_Set3)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.NOT_AND_ExOR_Set4)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.NOT_AND_ExOR_Set1)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.NOT_AND_ExOR_Set2)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.NOT_AND_ExOR_Set3)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.NOT_AND_ExOR_Set4)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.NOT_AND_ExOR_Set1)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.NOT_AND_ExOR_Set2)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.NOT_AND_ExOR_Set3)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.NOT_AND_ExOR_Set4)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.NOT_AND_ExOR_Set1)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.NOT_AND_ExOR_Set2)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.NOT_AND_ExOR_Set3)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.NOT_AND_ExOR_Set4)
    //  SELECT QUERY / VALIDATION ROUTINE.
    SnappyTestUtils.assertQueryFullResultSet(snc, SQLFunctionsUtils.select_ColTbl_NOT_AND_ExOR,
      "Q7_NOT_AND_ExOR", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, SQLFunctionsUtils.select_RowTbl_NOT_AND_ExOR,
      "Q8_NOT_AND_ExOR", "row", pw, sqlContext)
    // DROP SPARK / SNAPPY TABLES.
    snc.sql(SQLFunctionsUtils.dropColTbl_NOT_AND_ExOR)
    snc.sql(SQLFunctionsUtils.dropRowTbl_NOT_AND_ExOR)
    spark.sql(SQLFunctionsUtils.dropColTbl_NOT_AND_ExOR)
    spark.sql(SQLFunctionsUtils.dropRowTbl_NOT_AND_ExOR)

    pw.println("Snappy Embedded Job - SQL Functions passed successfully.")
    pw.close()
  }
}
