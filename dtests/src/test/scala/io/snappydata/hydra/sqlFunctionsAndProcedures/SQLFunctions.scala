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
    pw.println()
    pw.flush()
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
    pw.println()
    pw.flush()
    /**
      * Below queries test the functions :
      * 8. !(Logical Not)
      * (Logical NOT is not working in Snappy Cluster)
      * Add the boolean column and data once issue resolved.
      * 9. & (Bitwise AND
      * 10. ^ (Bitwise exclusiveOR/ExOR)
      * val NOT_AND_ExOR_Set1 : String="(1,7,3,true)"
      * val createColTypeTbl_NOT_AND_ExOR_Spark : String= createTbl + columnTbl +
      * "(id int,n1 int,n2 int,b boolean)"
      *  11. | (Bitwise OR)
      *  12. ~ (Bitwise NOT)
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
    pw.println()
    pw.flush()
    /**
      *  Below queries test the functions :
      *  13. day ,14. dayofmonth, 15. dayofyear, 16. last_day,
      *  17. month, 18. next_day, 19. weekofyear
      *   20 year
      */
    //  CREATE TABLE IN SPARK / SNAPPY.
    spark.sql(SQLFunctionsUtils.createColTypeTbl_Day_Month_Year_Spark)
    spark.sql(SQLFunctionsUtils.createRowTypeTbl_Day_Month_Year_Spark)
    snc.sql(SQLFunctionsUtils.createColumnTbl_Day_Month_Year)
    snc.sql(SQLFunctionsUtils.createRowTbl_Day_Month_Year)
    //  INSERT RECORDS IN SPARK / SNAPPY.
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.day_Month_Year_Set1)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.day_Month_Year_Set2)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.day_Month_Year_Set1)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.day_Month_Year_Set2)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.day_Month_Year_Set1)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.day_Month_Year_Set2)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.day_Month_Year_Set1)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.day_Month_Year_Set2)
    //  SELECT QUERY / VALIDATION ROUTINE.
    val snappyDF1 : DataFrame = snc.sql(SQLFunctionsUtils.select_ColTbl_Day_Month_Year)
    val sparkDF1 : DataFrame = spark.sql(SQLFunctionsUtils.select_ColTbl_Day_Month_Year)
    SnappyTestUtils.assertQueryFullResultSet(snc, snappyDF1, sparkDF1,
      "Q9_Day_Mnth_Yr", "column", pw, sqlContext, true)
    val snappyDF2 : DataFrame = snc.sql(SQLFunctionsUtils.select_RowTbl_Day_Month_Year)
    val sparkDF2 : DataFrame = spark.sql(SQLFunctionsUtils.select_RowTbl_Day_Month_Year)
    SnappyTestUtils.assertQueryFullResultSet(snc, snappyDF2, sparkDF2,
      "Q10_Day_Mnth_Yr", "row", pw, sqlContext, true)
    // DROP SPARK / SNAPPY TABLES.
    snc.sql(SQLFunctionsUtils.dropColTbl_Day_Month_Year)
    snc.sql(SQLFunctionsUtils.dropRowTbl_Day_Month_Year)
    spark.sql(SQLFunctionsUtils.dropColTbl_Day_Month_Year)
    spark.sql(SQLFunctionsUtils.dropRowTbl_Day_Month_Year)
//    pw.println()
    pw.flush()
    /**
      *  Below queries test the functions :
      *  21. map , 22. map_keys, 23. map_values
      */
    //  CREATE TABLE IN SPARK / SNAPPY.
    spark.sql(SQLFunctionsUtils.createColTypeTbl_map_Keys_Values_Spark)
    snc.sql(SQLFunctionsUtils.createColumnTbl_map_Keys_Values)
    //  INSERT RECORDS IN SPARK / SNAPPY.
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + "(" + SQLFunctionsUtils.map_Keys_Values_Set1 + ")")
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + "("  + SQLFunctionsUtils.map_Keys_Values_Set2 + ")")
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      " SELECT " + SQLFunctionsUtils.map_Keys_Values_Set1)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      " SELECT " + SQLFunctionsUtils.map_Keys_Values_Set2)
    //  SELECT QUERY / VALIDATION ROUTINE.
    val snappyDF_map_Keys_Values : DataFrame = snc.sql(SQLFunctionsUtils.select_ColTbl_map_Keys_Values)
    val sparkDF_map_Keys_Values : DataFrame =
      spark.sql(SQLFunctionsUtils.select_ColTbl_map_Keys_Values)
    SnappyTestUtils.assertQueryFullResultSet(snc, snappyDF_map_Keys_Values, sparkDF_map_Keys_Values,
      "Q11_map_Keys_Values", "column", pw, sqlContext, true)
    // DROP SPARK / SNAPPY TABLES.
    snc.sql(SQLFunctionsUtils.dropColTbl_map_Keys_Values)
    spark.sql(SQLFunctionsUtils.dropColTbl_map_Keys_Values)
//    pw.println()
    pw.flush()
    /**
      *  Below queries test the functions :
      *  24. array , 25. array_contains
      */
    //  CREATE TABLE IN SPARK / SNAPPY.
    spark.sql(SQLFunctionsUtils.createColTypeTbl_array_Contains_Spark)
    snc.sql(SQLFunctionsUtils.createColumnTbl_array_Contains_Values)
    //  INSERT RECORDS IN SPARK / SNAPPY.
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      " SELECT " + SQLFunctionsUtils.array_Contains_Set1)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      " SELECT " + SQLFunctionsUtils.array_Contains_Set2)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      " SELECT " + SQLFunctionsUtils.array_Contains_Set1)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      " SELECT " + SQLFunctionsUtils.array_Contains_Set2)
    //  SELECT QUERY / VALIDATION ROUTINE.
    SnappyTestUtils.assertQueryFullResultSet(snc, SQLFunctionsUtils.select_ColTbl_array_Contains,
      "Q12_array_Contains", "column", pw, sqlContext, true)
    // DROP SPARK / SNAPPY TABLES.
    snc.sql(SQLFunctionsUtils.dropColTbl_array_Contains)
    spark.sql(SQLFunctionsUtils.dropColTbl_array_Contains)
    pw.println()
    pw.flush()
    /**
      *  Below queries test the functions :
      *  26. and, 27. or , 28. not
      */
    //  CREATE TABLE IN SPARK / SNAPPY.
    spark.sql(SQLFunctionsUtils.createColTypeTbl_And_Or_Not_Spark)
    spark.sql(SQLFunctionsUtils.createRowTypeTbl_And_Or_Not_Spark)
    snc.sql(SQLFunctionsUtils.createColumnTbl_And_Or_Not)
    snc.sql(SQLFunctionsUtils.createRowTbl_And_Or_Not)
    //  INSERT RECORDS IN SPARK / SNAPPY.
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.And_Or_Not_Set1)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.And_Or_Not_Set2)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.And_Or_Not_Set3)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.And_Or_Not_Set4)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.And_Or_Not_Set1)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.And_Or_Not_Set2)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.And_Or_Not_Set3)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.And_Or_Not_Set4)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.And_Or_Not_Set1)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.And_Or_Not_Set2)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.And_Or_Not_Set3)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.And_Or_Not_Set4)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.And_Or_Not_Set1)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.And_Or_Not_Set2)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.And_Or_Not_Set3)
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.And_Or_Not_Set4)
    //  SELECT QUERY / VALIDATION ROUTINE.
    SnappyTestUtils.assertQueryFullResultSet(snc, SQLFunctionsUtils.select_ColTbl_And_Or_Not,
      "Q13_And_Or_Not", "column", pw, sqlContext)
    SnappyTestUtils.assertQueryFullResultSet(snc, SQLFunctionsUtils.select_RowTbl_And_Or_Not,
      "Q14_And_Or_Not", "row", pw, sqlContext)
    // DROP SPARK / SNAPPY TABLES.
    snc.sql(SQLFunctionsUtils.dropColTbl_And_Or_Not)
    snc.sql(SQLFunctionsUtils.dropRowTbl_And_Or_Not)
    spark.sql(SQLFunctionsUtils.dropColTbl_And_Or_Not)
    spark.sql(SQLFunctionsUtils.dropRowTbl_And_Or_Not)
    pw.println()
    pw.flush()
    /**
    Below queries test the function:
     29. current_database().
    */
    pw.println("Q15 : current_database(), For Snappy -> APP, For Spark -> Default")
    val snappyDBDF : DataFrame = snc.sql("SELECT current_database() as DB")
    val sparkDBDF : DataFrame = spark.sql("SELECT current_database() as DB")
    pw.println("Snappy SELECT current_databse() -> " + snappyDBDF.take(1).mkString)
    pw.println("Spark SELECT current_databse() -> " + sparkDBDF.take(1).mkString)
    pw.println()
    pw.flush()





























    //spark.sql(SQLFunctionsUtils.xmlPathQuery).show()
    //snc.sql(SQLFunctionsUtils.xmlPathQuery).show()

//    val xpathSparkDF : DataFrame = spark.sql("select xpath('<bookstore>" +
//      "<book category=\"cooking\"><title lang=\"en\">Everyday Italian</title>" +
//      "<author>Giada De Laurentiis</author><year>2005</year><price>30.00</price>" +
//      "</book><book category=\"children\"><title lang=\"en\">Harry Potter</title>" +
//      "<author>J K. Rowling</author><year>2005</year><price>29.99</price>" +
//      "</book><book category=\"web\"><title lang=\"en\">XQuery Kick Start</title>" +
//      "<author>James McGovern</author><author>Per Bothner</author><author>Kurt Cagle</author>" +
//      "<author>James Linn</author><author>Vaidyanathan Nagarajan</author><year>2003</year>" +
//      "<price>49.99</price></book><book category=\"web\"><title lang=\"en\">Learning XML</title>" +
//      "<author>Erik T. Ray</author><year>2003</year><price>39.95</price></book></bookstore>'," +
//      "'/bookstore/book/author/text()')")
//
//    val xpathSnappyDF : DataFrame = snc.sql("select xpath('<bookstore>" +
//      "<book category=\"cooking\"><title lang=\"en\">Everyday Italian</title>" +
//      "<author>Giada De Laurentiis</author><year>2005</year><price>30.00</price>" +
//      "</book><book category=\"children\"><title lang=\"en\">Harry Potter</title>" +
//      "<author>J K. Rowling</author><year>2005</year><price>29.99</price>" +
//      "</book><book category=\"web\"><title lang=\"en\">XQuery Kick Start</title>" +
//      "<author>James McGovern</author><author>Per Bothner</author><author>Kurt Cagle</author>" +
//      "<author>James Linn</author><author>Vaidyanathan Nagarajan</author><year>2003</year>" +
//      "<price>49.99</price></book><book category=\"web\"><title lang=\"en\">Learning XML</title>" +
//      "<author>Erik T. Ray</author><year>2003</year><price>39.95</price></book></bookstore>'," +
//      "'/bookstore/book/author/text()')")
//
//    println("xpath Snappy: ")
//    xpathSnappyDF.count()
//    xpathSnappyDF.show()
//    println("xpath Spark:")
//    xpathSparkDF.count()
//    xpathSparkDF.show()

    pw.println("Snappy Embedded Job - SQL Functions passed successfully.")
    pw.close()
  }
}
