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
    spark.sql(SQLFunctionsUtils.createColTypeTblInSpark)
    spark.sql(SQLFunctionsUtils.createRowTypeTblInSpark)

    snc.sql(SQLFunctionsUtils.createColumnTbl_DateFunctions_1)
    snc.sql(SQLFunctionsUtils.createRowTbl_DateFunctions_1)

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
    pw.println("Table creation and data ingestion is ok for date functions")
    pw.flush()

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
    snc.sql(SQLFunctionsUtils.dropColumnTbl_DateFunctions_1)
    snc.sql(SQLFunctionsUtils.dropRowTbl_DateFunctions_1)

    pw.println("Snappy Embedded Job - SQL Functions passed successfully.")
    pw.close()
  }
}
