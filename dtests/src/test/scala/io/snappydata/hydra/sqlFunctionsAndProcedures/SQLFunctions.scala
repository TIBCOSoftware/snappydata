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

    //  CREATE TABLE IN SPARK / SNAPPY.
    def createTables(sparkColumnTypeTbl : String, sparkRowTypeTbl: String,
                                     sncColumnTbl : String, sncRowTbl : String) : Unit = {
          spark.sql(sparkColumnTypeTbl)
          snc.sql(sncColumnTbl)
          if(sparkRowTypeTbl != "" &&
          sncRowTbl != "") {
            spark.sql(sparkRowTypeTbl)
            snc.sql(sncRowTbl)
          }
    }

    // DROP SPARK / SNAPPY TABLES.
    def dropTablesAndPrint(sparkColumnTypeTbl : String, sparkRowTypeTbl: String,
                           sncColumnTbl : String, sncRowTbl : String) : Unit = {
      spark.sql(sparkColumnTypeTbl)
      snc.sql(sncColumnTbl)
      if(sparkRowTypeTbl != "" &&
        sncRowTbl != "") {
        spark.sql(sparkRowTypeTbl)
        snc.sql(sncRowTbl)
      }
      pw.println()
      pw.flush()
    }

    //  INSERT RECORDS IN SPARK / SNAPPY.
    def insertRecordsToTable(length : Int, records : Array[String],
                             isColumnTbl : Boolean = true) : Unit = {
      if (isColumnTbl) {
        for (index <- 0 to length) {
          spark.sql(SQLFunctionsUtils.insertInto +
            SQLFunctionsUtils.columnTbl + SQLFunctionsUtils.values + records(index))
        }
        for (index <- 0 to length) {
          snc.sql(SQLFunctionsUtils.insertInto +
            SQLFunctionsUtils.columnTbl + SQLFunctionsUtils.values + records(index))
        }
        for (index <- 0 to length) {
          spark.sql(SQLFunctionsUtils.insertInto +
            SQLFunctionsUtils.rowTbl + SQLFunctionsUtils.values + records(index))
        }
        for (index <- 0 to length) {
          snc.sql(SQLFunctionsUtils.insertInto +
            SQLFunctionsUtils.rowTbl + SQLFunctionsUtils.values + records(index))
        }
      }
      else if (isColumnTbl == false) {
          for (index <- 0 to length) {
            spark.sql(SQLFunctionsUtils.insertInto +
              SQLFunctionsUtils.columnTbl + " SELECT " + records(index))
          }
          for (index <- 0 to length) {
            snc.sql(SQLFunctionsUtils.insertInto +
              SQLFunctionsUtils.columnTbl + " SELECT " + records(index))
          }
        }
      }

    //  SELECT QUERY / VALIDATION ROUTINE.
    def validateResult(selectColumnTbl : String, selectRowTbl : String,
                       columnStr : String, rowStr : String) : Unit = {
      SnappyTestUtils.assertQueryFullResultSet(snc, selectColumnTbl,
        columnStr, "column", pw, sqlContext)
      if(selectRowTbl != "" ) {
      SnappyTestUtils.assertQueryFullResultSet(snc, selectRowTbl,
          rowStr, "row", pw, sqlContext)
      }
    }

    //  SELECT QUERY / VALIDATION ROUTINE.
    def validateResultThroughDataFrames(selectSnappyDF : DataFrame, selectSparkDF : DataFrame,
                                        queryIndentifier : String, tableType : String) : Unit = {
      SnappyTestUtils.assertQueryFullResultSet(snc, selectSnappyDF, selectSparkDF,
        queryIndentifier, tableType, pw, sqlContext, true)
    }

    /**
      *  Below queries/code test the functions :
      *  1. date, 2. date_add, 3. date_sub, 4. date_diff, 5. date_format
      */
    createTables(SQLFunctionsUtils.createColTypeTblInSpark,
      SQLFunctionsUtils.createRowTypeTblInSpark, SQLFunctionsUtils.createColumnTbl_DateFunctions_1,
      SQLFunctionsUtils.createRowTbl_DateFunctions_1)
    insertRecordsToTable(2, SQLFunctionsUtils.dateSet)
    validateResult(SQLFunctionsUtils.selectQueryOnColTbl_DateFunctions_1,
      SQLFunctionsUtils.selectQueryOnRowTbl_DateFunctions_1,
      "Q1_ColumnTbl_DateFunc", "Q2_RowTbl_DateFunc"  )
    validateResult(SQLFunctionsUtils.selectMonth_In_DateFormatFunc, "",
      "Q3_SelectMonth_DateFormat", "")
    validateResult(SQLFunctionsUtils.selectDay_DateFormatFunc, "",
      "Q4_SelectDay_DateFormat", "")
    dropTablesAndPrint(SQLFunctionsUtils.dropColumnTbl_DateFunctions_1,
      SQLFunctionsUtils.dropRowTbl_DateFunctions_1, SQLFunctionsUtils.dropColumnTbl_DateFunctions_1,
      SQLFunctionsUtils.dropRowTbl_DateFunctions_1)
    /**
      *  Below queries test the functions :
      *  6. repeat, 7. reverse
      *  (NOTE : reverse logic for arrays is available since  Spark 2.4.0,
      *  test to be added after spark 2.4.0 merge
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_RseRpt_Spark,
      SQLFunctionsUtils.createRowTypeTbl_rserpt_Spark, SQLFunctionsUtils.createColumnTbl_RseRpt,
      SQLFunctionsUtils.createRowTbl_RseRpt)
    insertRecordsToTable(1, SQLFunctionsUtils.rseRptSet)
    validateResult(SQLFunctionsUtils.select_ColTbl_RseRpt,
      SQLFunctionsUtils.select_RowTbl_RseRpt, "Q5_Select_RevRpt", "Q6_Select_RevRpt")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_RseRpt,
      SQLFunctionsUtils.dropRowTbl_RseRpt, SQLFunctionsUtils.dropColTbl_RseRpt,
      SQLFunctionsUtils.dropRowTbl_RseRpt)
    /**
      * Below queries test the functions :
      * 8. !(Logical Not)
      * (Logical NOT is not working in Snappy Cluster)
      * Add the boolean column and data once issue resolved
      *  9. & (Bitwise AND), 10. ^ (Bitwise exclusiveOR/ExOR),
      *  11. | (Bitwise OR), 12. ~ (Bitwise NOT)
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_NOT_AND_ExOR_Spark,
      SQLFunctionsUtils.createRowTypeTbl_NOT_AND_ExOR_Spark,
      SQLFunctionsUtils.createColumnTbl_NOT_AND_ExOR, SQLFunctionsUtils.createRowTbl_NOT_AND_ExOR)
    insertRecordsToTable(3, SQLFunctionsUtils.NOT_AND_ExOR_Set)
    validateResult(SQLFunctionsUtils.select_ColTbl_NOT_AND_ExOR,
      SQLFunctionsUtils.select_RowTbl_NOT_AND_ExOR, "Q7_NOT_AND_ExOR", "Q8_NOT_AND_ExOR")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_NOT_AND_ExOR,
      SQLFunctionsUtils.dropRowTbl_NOT_AND_ExOR, SQLFunctionsUtils.dropColTbl_NOT_AND_ExOR,
      SQLFunctionsUtils.dropRowTbl_NOT_AND_ExOR)
    /**
      *  Below queries test the functions :
      *  13. day ,14. dayofmonth, 15. dayofyear, 16. last_day,
      *  17. month, 18. next_day, 19. weekofyear, 20 year
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_Day_Month_Year_Spark,
      SQLFunctionsUtils.
        createRowTypeTbl_Day_Month_Year_Spark,
      SQLFunctionsUtils.createColumnTbl_Day_Month_Year,
      SQLFunctionsUtils.createRowTbl_Day_Month_Year)
    insertRecordsToTable(1, SQLFunctionsUtils.day_Month_Year_Set)
    val snappyDF1 : DataFrame = snc.sql(SQLFunctionsUtils.select_ColTbl_Day_Month_Year)
    val sparkDF1 : DataFrame = spark.sql(SQLFunctionsUtils.select_ColTbl_Day_Month_Year)
    validateResultThroughDataFrames(snappyDF1, sparkDF1,
      "Q9_Day_Mnth_Yr", "column")
    val snappyDF2 : DataFrame = snc.sql(SQLFunctionsUtils.select_RowTbl_Day_Month_Year)
    val sparkDF2 : DataFrame = spark.sql(SQLFunctionsUtils.select_RowTbl_Day_Month_Year)
    validateResultThroughDataFrames(snappyDF2, sparkDF2,
      "Q10_Day_Mnth_Yr", "row")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_Day_Month_Year,
      SQLFunctionsUtils.dropRowTbl_Day_Month_Year, SQLFunctionsUtils.dropColTbl_Day_Month_Year,
      SQLFunctionsUtils.dropRowTbl_Day_Month_Year)
    /**
      *  Below queries test the functions :
      *  21. map , 22. map_keys, 23. map_values
      */
     createTables(SQLFunctionsUtils.createColTypeTbl_map_Keys_Values_Spark, "",
      SQLFunctionsUtils.createColumnTbl_map_Keys_Values, "")
    insertRecordsToTable(1, SQLFunctionsUtils.map_Keys_Values_Set, false)
    val snappyDF_map_Keys_Values : DataFrame =
      snc.sql(SQLFunctionsUtils.select_ColTbl_map_Keys_Values)
    val sparkDF_map_Keys_Values : DataFrame =
      spark.sql(SQLFunctionsUtils.select_ColTbl_map_Keys_Values)
    validateResultThroughDataFrames(snappyDF_map_Keys_Values, sparkDF_map_Keys_Values,
      "Q11_map_Keys_Values", "column")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_map_Keys_Values, "",
      SQLFunctionsUtils.dropColTbl_map_Keys_Values, "")
    /**
      *  Below queries test the functions :
      *  24. array , 25. array_contains
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_array_Contains_Spark, "",
      SQLFunctionsUtils.createColumnTbl_array_Contains_Values, "")
    insertRecordsToTable(1, SQLFunctionsUtils.array_Contains_Set, false)
    validateResult(SQLFunctionsUtils.select_ColTbl_array_Contains, "",
      "Q12_array_Contains", "")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_array_Contains, "",
      SQLFunctionsUtils.dropColTbl_array_Contains, "")
    /**
      *  Below queries test the functions :
      *  26. and, 27. or , 28. not
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_And_Or_Not_Spark,
      SQLFunctionsUtils.createRowTypeTbl_And_Or_Not_Spark,
      SQLFunctionsUtils.createColumnTbl_And_Or_Not, SQLFunctionsUtils.createRowTbl_And_Or_Not)
    insertRecordsToTable(3, SQLFunctionsUtils.And_Or_Not_Set)
    validateResult(SQLFunctionsUtils.select_ColTbl_And_Or_Not,
      SQLFunctionsUtils.select_RowTbl_And_Or_Not, "Q13_And_Or_Not", "Q14_And_Or_Not")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_And_Or_Not,
      SQLFunctionsUtils.dropRowTbl_And_Or_Not, SQLFunctionsUtils.dropColTbl_And_Or_Not,
      SQLFunctionsUtils.dropRowTbl_And_Or_Not)
    /**
    Below queries test the function:
     29. current_database(), 92. current_schema().
    */
    pw.println("Q15 : current_database(), For Snappy -> APP, For Spark -> Default")
    val snappyDBDF : DataFrame = snc.sql("SELECT current_database() as DB")
    val sparkDBDF : DataFrame = spark.sql("SELECT current_database() as DB")
    val snappySchemaDF : DataFrame = snc.sql("SELECT current_schema() as DB")
    pw.println("Snappy SELECT current_databse() -> " + snappyDBDF.take(1).mkString)
    pw.println("Spark SELECT current_databse() -> " + sparkDBDF.take(1).mkString)
    pw.println("Snappy SELECT current_schema() -> " + snappySchemaDF.take(1).mkString)
    pw.println()
    pw.flush()
    /**
      *  Below queries test the functions :
      *  30. size.
      *  NOTE : Following test case is Pending.
      *  SET spark.sql.legacy.sizeOfNull is set to false, the function returns null for null input
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_Size_Spark, "",
      SQLFunctionsUtils.createColumnTbl_Size, "")
    insertRecordsToTable(3, SQLFunctionsUtils.size_Set, false)
   validateResult(SQLFunctionsUtils.select_ColTbl_Size, "", "Q16_Size", "")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_Size, "",
      SQLFunctionsUtils.dropColTbl_Size, "")
    /**
      *  Below queries test the functions :
      *  31. rpad, 32. in
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_rpad_in_Spark,
      SQLFunctionsUtils.createRowTypeTbl_rpad_in_Spark, SQLFunctionsUtils.createColumnTbl_rpad_in,
      SQLFunctionsUtils.createRowTbl_rpad_in)
    insertRecordsToTable(2, SQLFunctionsUtils.rpad_in_Set)
    validateResult(SQLFunctionsUtils.select_ColTbl_rpad_in, SQLFunctionsUtils.select_RowTbl_rpad_in,
      "Q17_rpad_in", "Q18_rpad_in")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_rpad_in, SQLFunctionsUtils.dropRowTbl_rpad_in,
      SQLFunctionsUtils.dropColTbl_rpad_in, SQLFunctionsUtils.dropRowTbl_rpad_in)
    /**
      *  Below queries test the functions :
      *  33. hour, 34. minute, 35. second
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_hr_min_sec_Spark,
      SQLFunctionsUtils.
        createRowTypeTbl_hr_min_sec_Spark, SQLFunctionsUtils.createColumnTbl_hr_min_sec,
      SQLFunctionsUtils.createRowTbl_hr_min_sec)
    insertRecordsToTable(1, SQLFunctionsUtils.hr_min_sec_Set)
    validateResult(SQLFunctionsUtils.select_ColTbl_hr_min_sec,
      SQLFunctionsUtils.select_RowTbl_hr_min_sec, "Q19_hr_min_sec", "Q20_hr_min_sec")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_hr_min_sec,
      SQLFunctionsUtils.dropRowTbl_hr_min_sec,
      SQLFunctionsUtils.dropColTbl_hr_min_sec, SQLFunctionsUtils.dropRowTbl_hr_min_sec)
    /**
      *  Below queries test the functions :
      *  36. ascii, 37. months_between, 35. current_timestamp
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_ascii_mnthbet_ts_Spark,
      SQLFunctionsUtils.createRowTypeTbl_ascii_mnthbet_ts_Spark,
      SQLFunctionsUtils.
        createColumnTbl_ascii_mnthbet_ts, SQLFunctionsUtils.createRowTbl_ascii_mnthbet_ts)
    insertRecordsToTable(2, SQLFunctionsUtils.ascii_mnthbet_ts_Set)
    validateResult(SQLFunctionsUtils.select_ColTbl_ascii_mnthbet_ts,
      SQLFunctionsUtils.select_RowTbl_ascii_mnthbet_ts,
      "Q21_ascii_mnthbet_ts", "Q22_ascii_mnthbet_ts")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_ascii_mnthbet_ts,
      SQLFunctionsUtils.dropRowTbl_ascii_mnthbet_ts, SQLFunctionsUtils.dropColTbl_ascii_mnthbet_ts,
      SQLFunctionsUtils.dropRowTbl_ascii_mnthbet_ts)
    /**
      *  Below queries test the functions :
      *  39. string, 40. substr, 41. substring
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_str_substr_Spark,
      SQLFunctionsUtils.createRowTypeTbl_str_substr_Spark,
      SQLFunctionsUtils.createColumnTbl_str_substr, SQLFunctionsUtils.createRowTbl_str_substr)
    insertRecordsToTable(1, SQLFunctionsUtils.str_subStr)
    val str_subStr_Col_SparkDF : DataFrame = spark.sql(SQLFunctionsUtils.select_ColTbl_str_substr)
    val str_subStr_Col_SnappyDF : DataFrame = snc.sql(SQLFunctionsUtils.select_ColTbl_str_substr)
    val str_subStr_Row_SparkDF : DataFrame = spark.sql(SQLFunctionsUtils.select_RowTbl_str_substr)
    val str_subStr_Row_SnappyDF : DataFrame = snc.sql(SQLFunctionsUtils.select_RowTbl_str_substr)
    validateResultThroughDataFrames(str_subStr_Col_SnappyDF, str_subStr_Col_SparkDF,
      "Q23_str_substr", "column")
    validateResultThroughDataFrames(str_subStr_Row_SnappyDF, str_subStr_Row_SparkDF,
      "Q24_str_substr", "row")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_str_substr,
      SQLFunctionsUtils.dropRowTbl_str_substr, SQLFunctionsUtils.dropColTbl_str_substr,
      SQLFunctionsUtils.dropRowTbl_str_substr)
    /**
      *  Below queries test the functions :
      *  42. >, 43. >=, 44. <, 45. <=, 46 hypot
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_hypot_gt_lt_Spark,
      SQLFunctionsUtils.createRowTypeTbl_hypot_gt_lt_Spark,
      SQLFunctionsUtils.createColumnTbl_hypot_gt_lt, SQLFunctionsUtils.createRowTbl_hypot_gt_lt)
    insertRecordsToTable(7, SQLFunctionsUtils.hypot_gt_lt)
    validateResult(SQLFunctionsUtils.select_ColTbl_hypot_gt_lt,
      SQLFunctionsUtils.select_RowTbl_hypot_gt_lt, "Q25_hypot_gt_lt", "Q26_hypot_gt_lt")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_hypot_gt_lt,
      SQLFunctionsUtils.dropRowTbl_hypot_gt_lt, SQLFunctionsUtils.dropColTbl_hypot_gt_lt,
      SQLFunctionsUtils.dropRowTbl_hypot_gt_lt)
    /**
      *  Below queries test the functions :
      *  46. space, 47. soundex
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_spc_soundex_Spark,
      SQLFunctionsUtils.createRowTypeTbl_spc_soundex_Spark,
      SQLFunctionsUtils.createColumnTbl_spc_soundex, SQLFunctionsUtils.createRowTbl_spc_soundex)
    insertRecordsToTable(3, SQLFunctionsUtils.spc_soundex)
    validateResult(SQLFunctionsUtils.select_ColTbl_spc_soundex,
      SQLFunctionsUtils.select_RowTbl_spc_soundex, "Q27_spc_soundex", "Q28_spc_soundex")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_spc_soundex,
      SQLFunctionsUtils.dropRowTbl_spc_soundex,
      SQLFunctionsUtils.dropColTbl_spc_soundex, SQLFunctionsUtils.dropRowTbl_spc_soundex)
    /**
      *  Below queries test the functions :
      *  48. xpath, 49. xpath_boolean, 50. xpath_double, 51. xpath_float,
      *  52. xpath_int, 53. xpath_long, 54. xpath_number, 55. xpath_short,
      *  56. xpath_string.
      */
    val xPath_SnappyDF : DataFrame = snc.sql(SQLFunctionsUtils.xPath)
    val xPath_SparkDF : DataFrame = spark.sql(SQLFunctionsUtils.xPath)
    SnappyTestUtils.assertQueryFullResultSet(snc, xPath_SnappyDF, xPath_SparkDF,
      "Q29_xpath", "column", pw, sqlContext, true)
    val xPath_bool_true_sncDF : DataFrame = snc.sql(SQLFunctionsUtils.xPath_Boolean_true)
    val xPath_bool_true_sparkDF : DataFrame = spark.sql(SQLFunctionsUtils.xPath_Boolean_true)
    SnappyTestUtils.assertQueryFullResultSet(snc, xPath_bool_true_sncDF, xPath_bool_true_sparkDF,
    "Q30_xpath_boolean_true", "column", pw, sqlContext, true)
    val xPath_bool_false_sncDF : DataFrame = snc.sql(SQLFunctionsUtils.xPath_Boolean_false)
    val xPath_bool_false_sparkDF : DataFrame = spark.sql(SQLFunctionsUtils.xPath_Boolean_false)
    SnappyTestUtils.assertQueryFullResultSet(snc, xPath_bool_false_sncDF, xPath_bool_false_sparkDF,
      "Q31_xpath_boolean_false", "column", pw, sqlContext, true)
    val xPath_double_sncDF : DataFrame = snc.sql(SQLFunctionsUtils.xPath_double)
    val xPath_double_sparkDF : DataFrame = spark.sql(SQLFunctionsUtils.xPath_double)
    SnappyTestUtils.assertQueryFullResultSet(snc, xPath_double_sncDF, xPath_double_sparkDF,
      "Q32_xpath_double", "column", pw, sqlContext, true)
    val xPath_float_sncDF : DataFrame = snc.sql(SQLFunctionsUtils.xPath_float)
    val xPath_float_sparkDF : DataFrame = spark.sql(SQLFunctionsUtils.xPath_float)
    SnappyTestUtils.assertQueryFullResultSet(snc, xPath_float_sncDF, xPath_float_sparkDF,
      "Q33_xpath_float", "column", pw, sqlContext, true)
    val xPath_int_sncDF : DataFrame = snc.sql(SQLFunctionsUtils.xPath_int)
    val xPath_int_sparkDF : DataFrame = spark.sql(SQLFunctionsUtils.xPath_int)
    SnappyTestUtils.assertQueryFullResultSet(snc, xPath_int_sncDF, xPath_int_sparkDF,
      "Q34_xpath_int", "column", pw, sqlContext, true)
    val xPath_long_sncDF : DataFrame = snc.sql(SQLFunctionsUtils.xPath_long)
    val xPath_long_sparkDF : DataFrame = spark.sql(SQLFunctionsUtils.xPath_long)
    SnappyTestUtils.assertQueryFullResultSet(snc, xPath_long_sncDF, xPath_long_sparkDF,
      "Q35_xpath_long", "column", pw, sqlContext, true)
    val xPath_number_sncDF : DataFrame = snc.sql(SQLFunctionsUtils.xPath_number)
    val xPath_number_sparkDF : DataFrame = spark.sql(SQLFunctionsUtils.xPath_number)
    SnappyTestUtils.assertQueryFullResultSet(snc, xPath_number_sncDF, xPath_number_sparkDF,
      "Q36_xpath_number", "column", pw, sqlContext, true)
    val xPath_short_sncDF : DataFrame = snc.sql(SQLFunctionsUtils.xPath_short)
    val xPath_short_sparkDF : DataFrame = spark.sql(SQLFunctionsUtils.xPath_short)
    SnappyTestUtils.assertQueryFullResultSet(snc, xPath_short_sncDF, xPath_short_sparkDF,
      "Q37_xpath_short", "column", pw, sqlContext, true)
    val xPath_string_sncDF : DataFrame = snc.sql(SQLFunctionsUtils.xPath_string)
    val xPath_string_sparkDF : DataFrame = spark.sql(SQLFunctionsUtils.xPath_string)
    SnappyTestUtils.assertQueryFullResultSet(snc, xPath_string_sncDF, xPath_string_sparkDF,
      "Q38_xpath_string", "column", pw, sqlContext, true)
    /**
      *  Below queries test the functions :
      *  57. trim, 58. ltrim, 59. rtrim, 60. isnotnull
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_trim_isnotnull_Spark,
      SQLFunctionsUtils.createRowTypeTbl_trim_isnotnull_Spark, SQLFunctionsUtils.
        createColumnTbl_trim_isnotnull, SQLFunctionsUtils.createRowTbl_trim_isnotnull)
    insertRecordsToTable(1, SQLFunctionsUtils.trim_isnotnull)
    validateResult(SQLFunctionsUtils.select_ColTbl_trim_isnotnull, SQLFunctionsUtils.
      select_RowTbl_trim_isnotnull, "Q39_trim_isnotnull", "Q40_trim_isnotnull")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_trim_isnotnull,
      SQLFunctionsUtils.dropRowTbl_trim_isnotnull,
      SQLFunctionsUtils.dropColTbl_trim_isnotnull, SQLFunctionsUtils.dropRowTbl_trim_isnotnull)
    /**
      *  Below queries test the functions :
      *  61. =, 62. ==, 63. <=>
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_operators_Spark,
      SQLFunctionsUtils.
        createRowTypeTbl_operators_Spark, SQLFunctionsUtils.createColumnTbl_operators,
      SQLFunctionsUtils.createRowTbl_operators)
    insertRecordsToTable(4, SQLFunctionsUtils.operators)
    val operators_sncDF_Col : DataFrame = snc.sql(SQLFunctionsUtils.select_ColTbl_operators)
    val operators_sparkDF_Col : DataFrame = spark.sql(SQLFunctionsUtils.select_ColTbl_operators)
    validateResultThroughDataFrames(operators_sncDF_Col, operators_sparkDF_Col,
      "Q41_operators", "column")
    val operators_sncDF_Row : DataFrame = snc.sql(SQLFunctionsUtils.select_RowTbl_operators)
    val operators_sparkDF_Row : DataFrame = spark.sql(SQLFunctionsUtils.select_RowTbl_operators)
    validateResultThroughDataFrames(operators_sncDF_Row, operators_sparkDF_Row,
      "Q42_operators", "row")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_operators,
      SQLFunctionsUtils.dropRowTbl_operators, SQLFunctionsUtils.dropColTbl_operators,
      SQLFunctionsUtils.dropRowTbl_operators)
    /**
      *  Below queries test the functions :
      *  64. row_number(), 65. rank(), 66. dense_rank()
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_rownumber_rank_Spark,
      SQLFunctionsUtils.createRowTypeTbl_rownumber_rank_Spark,
      SQLFunctionsUtils.createColumnTbl_rownumber_rank,
      SQLFunctionsUtils.createRowTbl_rownumber_rank)
    insertRecordsToTable(12, SQLFunctionsUtils.rownumber_rank)
    validateResult(SQLFunctionsUtils.select_ColTbl_rownumber_rank,
      SQLFunctionsUtils.select_RowTbl_rownumber_rank,
      "Q43_rowno_rank", "Q44_rowno_rank")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_rownumber_rank,
      SQLFunctionsUtils.dropRowTbl_rownumber_rank, SQLFunctionsUtils.
        dropColTbl_rownumber_rank, SQLFunctionsUtils.dropRowTbl_rownumber_rank)
    /**
      *  Below queries test the functions :
      *  67. encode, 68. decode
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_encode_decode_Spark,
      SQLFunctionsUtils.createRowTypeTbl_encode_decode_Spark,
      SQLFunctionsUtils.createColumnTbl_encode_decode,
      SQLFunctionsUtils.createRowTbl_encode_decode)
    insertRecordsToTable(3, SQLFunctionsUtils.encode_decode)
    validateResult(SQLFunctionsUtils.select_ColTbl_encode_decode,
      SQLFunctionsUtils.select_RowTbl_encode_decode,
      "Q45_encode_decode", "Q46_encode_decode")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_encode_decode,
      SQLFunctionsUtils.dropRowTbl_encode_decode,
      SQLFunctionsUtils.dropColTbl_encode_decode,
      SQLFunctionsUtils.dropRowTbl_encode_decode)
    /**
      *  Below queries test the functions :
      *  69. bigint, 70. binary, 71. boolean, 72. decimal,
      *  73. double, 74. float, 75. int, 76. smallint,
      *  77. tinyint
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_dataTypes_Spark,
      SQLFunctionsUtils.createRowTypeTbl_dataTypes_Spark,
      SQLFunctionsUtils.createColumnTbl_dataTypes,
      SQLFunctionsUtils.createRowTbl_dataTypes)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
        SQLFunctionsUtils.values + SQLFunctionsUtils.dataTypes(0))
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
        SQLFunctionsUtils.values + SQLFunctionsUtils.dataTypes(0))
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
        SQLFunctionsUtils.values + SQLFunctionsUtils.dataTypes(0))
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
        SQLFunctionsUtils.values + SQLFunctionsUtils.dataTypes(0))
    val sncDataType_column : DataFrame = snc.sql(SQLFunctionsUtils.select_ColTbl_dataTypes)
    val sparkDataType_column : DataFrame = spark.sql(SQLFunctionsUtils.select_RowTbl_dataTypes)
    validateResultThroughDataFrames(sncDataType_column, sparkDataType_column,
      "Q47_datatypes", "column")
    val sncDataType_Row : DataFrame = snc.sql(SQLFunctionsUtils.select_RowTbl_dataTypes)
    val sparkDataType_Row : DataFrame = spark.sql(SQLFunctionsUtils.select_RowTbl_dataTypes)
    validateResultThroughDataFrames(sncDataType_Row, sparkDataType_Row,
      "Q48_datatypes", "row")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_dataTypes,
      SQLFunctionsUtils.dropRowTbl_dataTypes, SQLFunctionsUtils.dropColTbl_dataTypes,
      SQLFunctionsUtils.dropRowTbl_dataTypes)
    /**
      *  Below queries test the functions :
      *  78. hash, 79. sha, 80. sha1, 81. sha2.
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_hash_sha_Spark,
      SQLFunctionsUtils.createRowTypeTbl_hash_sha_Spark,
      SQLFunctionsUtils.createColumnTbl_hash_sha,
      SQLFunctionsUtils.createRowTbl_hash_sha)
    insertRecordsToTable(2, SQLFunctionsUtils.hash_sha)
    val sncDF_hash_sha_column : DataFrame = snc.sql(SQLFunctionsUtils.select_ColTbl_hash_sha)
    val sparkDF_hash_sha_column : DataFrame = spark.sql(SQLFunctionsUtils.select_ColTbl_hash_sha)
    validateResultThroughDataFrames(sncDF_hash_sha_column, sparkDF_hash_sha_column,
      "Q49_hash_sha", "column")
    val sncDF_hash_sha_row : DataFrame = snc.sql(SQLFunctionsUtils.select_RowTbl_hash_sha)
    val sparkDF_hash_sha_row : DataFrame = spark.sql(SQLFunctionsUtils.select_RowTbl_hash_sha)
//    SnappyTestUtils.assertQueryFullResultSet(snc, sncDF_hash_sha_row, sparkDF_hash_sha_row,
//      "Q50_hash_sha", "row", pw, sqlContext, true)
    validateResultThroughDataFrames(sncDF_hash_sha_row, sparkDF_hash_sha_row,
      "Q50_hash_sha", "row")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_hash_sha,
      SQLFunctionsUtils.dropRowTbl_hash_sha,
      SQLFunctionsUtils.dropColTbl_hash_sha, SQLFunctionsUtils.dropRowTbl_hash_sha)
    /**
      *  Below queries test the functions :
      *  82. translate, 83. substring_index,
      *  84. split, 85. sentences.
      *   In this row table query has result mismatch, need to look.
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_translate_split_Spark,
      SQLFunctionsUtils.createRowTypeTbl_translate_split_Spark,
      SQLFunctionsUtils.createColumnTbl_translate_split,
      SQLFunctionsUtils.createRowTbl_translate_split)
    insertRecordsToTable(2, SQLFunctionsUtils.translate_split)
    validateResult(SQLFunctionsUtils.select_ColTbl_translate_split,
      SQLFunctionsUtils.select_RowTbl_translate_split, "Q51_translate_split", "Q52_translate_split")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_translate_split,
      SQLFunctionsUtils.dropRowTbl_translate_split,
      SQLFunctionsUtils.dropColTbl_translate_split, SQLFunctionsUtils.dropRowTbl_translate_split)
    /**
      *  Below queries test the functions :
      *  86. monotonically_increasing_id
      */
    snc.sql(SQLFunctionsUtils.createColumnTbl_monotonically_increasing_id)
    snc.sql(SQLFunctionsUtils.createRowTbl_monotonically_increasing_id)
    for(i <- 0 to 36) {
      snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
        SQLFunctionsUtils.values + SQLFunctionsUtils.monotonically_increasing_id(i))
    }
    for(i <- 0 to 36) {
      snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
        SQLFunctionsUtils.values + SQLFunctionsUtils.monotonically_increasing_id(i))
    }
    val sncDF_mono_Col : DataFrame =
      snc.sql(SQLFunctionsUtils.select_ColTbl_monotonically_increasing_id)
    val sncDF_mono_Row : DataFrame =
      snc.sql(SQLFunctionsUtils.select_RowTbl_monotonically_increasing_id)
    pw.println("Column table -> monotonically_increasing_id : ")
    val sncColResult = sncDF_mono_Col.collect()
    for(i <- 0 to 36) {
      pw.println(sncColResult(i).mkString(","))
    }
    pw.println()
    pw.println("Row table -> monotonically_increasing_id : ")
    val sncRowResult = sncDF_mono_Row.collect()
    for(i <- 0 to 36) {
      pw.println(sncRowResult(i).mkString(","))
    }
    snc.sql(SQLFunctionsUtils.dropColTbl_monotonically_increasing_id)
    snc.sql(SQLFunctionsUtils.dropRowTbl_monotonically_increasing_id)
    pw.println()
    pw.flush()
    /**
      *  Below queries test the functions :
      *  87. to_unix_timestamp, 88. to_utc_timestamp, 89. to_date,
      *  90. from_unixtime, 91. from_utc_timestamp.
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_date_time_Spark,
      SQLFunctionsUtils.createRowTypeTbl_date_time_Spark,
      SQLFunctionsUtils.createColumnTbl_date_time,
      SQLFunctionsUtils.createRowTbl_date_time)
    insertRecordsToTable(2, SQLFunctionsUtils.date_time)
    validateResult(SQLFunctionsUtils.select_ColTbl_date_time,
      SQLFunctionsUtils.select_RowTbl_date_time,
      "Q53_date_time", "Q54_date_time")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_date_time,
      SQLFunctionsUtils.dropRowTbl_date_time, SQLFunctionsUtils.dropColTbl_date_time,
      SQLFunctionsUtils.dropRowTbl_date_time)
    /**
      *  Below queries test the functions :
      *  93. lag, 94. lead, 95. ntile,
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_lead_lag_ntile_Spark,
      SQLFunctionsUtils.createRowTypeTbl_lead_lag_ntile_Spark, SQLFunctionsUtils.
        createColumnTbl_lead_lag_ntile, SQLFunctionsUtils.createRowTbl_lead_lag_ntile)
    insertRecordsToTable(12, SQLFunctionsUtils.rownumber_rank)
    validateResult(SQLFunctionsUtils.select_ColTbl_lead_lag_ntile,
      SQLFunctionsUtils.select_RowTbl_lead_lag_ntile, "Q55_lead_lag_ntile",
      "Q56_lead_lag_ntile")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_rownumber_rank,
      SQLFunctionsUtils.dropRowTbl_rownumber_rank, SQLFunctionsUtils.
        dropColTbl_rownumber_rank, SQLFunctionsUtils.dropRowTbl_rownumber_rank)
    /**
      *  96th function is timestamp and it is already
      *  tested in above queries.
      *  Below queries test the functions :
      *  97. base64, 98. unbase64, 99. unix_timestamp, 100. unhex
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_base_unbase_Spark,
      SQLFunctionsUtils.createRowTypeTbl_base_unbase_Spark, SQLFunctionsUtils.
        createColumnTbl_base_unbase, SQLFunctionsUtils.createRowTbl_base_unbase)
    insertRecordsToTable(2, SQLFunctionsUtils.base_unbase)
    validateResult(SQLFunctionsUtils.select_ColTbl_base_unbase,
      SQLFunctionsUtils.select_RowTbl_base_unbase, "Q57_base_unbase",
      "Q58_base_unbase")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_base_unbase,
      SQLFunctionsUtils.dropRowTbl_base_unbase, SQLFunctionsUtils.
        dropColTbl_base_unbase, SQLFunctionsUtils.dropRowTbl_base_unbase)
    /**
      *  Below queries test the functions :
      *  101. trunc, 102. quarter, 103. parse_url, 104. java_method
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_parseurl_Spark,
      SQLFunctionsUtils.createRowTypeTbl_parseurl_Spark, SQLFunctionsUtils.
        createColumnTbl_parseurl, SQLFunctionsUtils.createRowTbl_parseurl)
    insertRecordsToTable(1, SQLFunctionsUtils.parseurl)
    val sncurlDF : DataFrame = snc.sql(SQLFunctionsUtils.select_ColTbl_parseurl)
    val sparkurlDF : DataFrame = spark.sql(SQLFunctionsUtils.select_ColTbl_parseurl)
    validateResultThroughDataFrames(sncurlDF, sparkurlDF, "Q59_parseurl", "column")
    val sncurlDF_Row : DataFrame = snc.sql(SQLFunctionsUtils.select_RowTbl_parseurl)
    val sparkurlDF_Row : DataFrame = spark.sql(SQLFunctionsUtils.select_RowTbl_parseurl)
    validateResultThroughDataFrames(sncurlDF_Row, sparkurlDF_Row, "Q60_parseurl", "row")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_parseurl, SQLFunctionsUtils.
      dropRowTbl_parseurl, SQLFunctionsUtils.dropColTbl_parseurl,
      SQLFunctionsUtils.dropRowTbl_parseurl)
    /**
      *  Below queries test the functions :
      *  105. spark_partition_id.
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_sparkpartitionid_Spark,
      SQLFunctionsUtils.createRowTypeTbl_sparkpartitionid_Spark, SQLFunctionsUtils.
        createColumnTbl_sparkpartitionid, SQLFunctionsUtils.createRowTbl_sparkpartitionid)
    spark.sql("INSERT INTO " + SQLFunctionsUtils.columnTbl +
      " SELECT id,concat('TIBCO_',id) from range(1000000)")
    spark.sql("INSERT INTO " + SQLFunctionsUtils.rowTbl +
      " SELECT id,concat('TIBCO_',id) from range(1000000)")
    snc.sql("INSERT INTO " + SQLFunctionsUtils.columnTbl +
      " SELECT id,concat('TIBCO_',id) from range(1000000)")
    snc.sql("INSERT INTO " + SQLFunctionsUtils.rowTbl +
      " SELECT id,concat('TIBCO_',id) from range(1000000)")
    val snc_spid_col : DataFrame = snc.sql(SQLFunctionsUtils.select_ColTbl_sparkpartitionid)
    val spark_spid_col : DataFrame = spark.sql(SQLFunctionsUtils.select_ColTbl_sparkpartitionid)
    pw.println("Snappy Column table count for spark_partition_id() -> "
      + snc_spid_col.take(1).mkString)
    pw.println("Spark Column type table count for spark_partition_id() -> " +
      spark_spid_col.take(1).mkString)
    val snc_spid_Row : DataFrame = snc.sql(SQLFunctionsUtils.select_RowTbl_cdbsparkpartitionid)
    val spark_spid_Row : DataFrame = spark.sql(SQLFunctionsUtils.select_RowTbl_sparkpartitionid)
    pw.println("Snappy Row Table Count for spark_partition_id() -> "
      + snc_spid_Row.take(1).mkString)
    pw.println("Spark Row type Table Count for spark_partition_id() -> " +
      spark_spid_Row.take(1).mkString)
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_sparkpartitionid,
      SQLFunctionsUtils.dropRowTbl_sparkpartitionid, SQLFunctionsUtils.
        dropColTbl_sparkpartitionid, SQLFunctionsUtils.dropRowTbl_sparkpartitionid)
    /**
      *  Below queries test the functions :
      *  106. rollup, 107. cube.
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_rollup_cube_Spark,
      SQLFunctionsUtils.createRowTypeTbl_rollup_cube_Spark, SQLFunctionsUtils.
        createColumnTbl_rollup_cube, SQLFunctionsUtils.createRowTbl_rollup_cube)
    insertRecordsToTable(9, SQLFunctionsUtils.rollup_cube)
    validateResult(SQLFunctionsUtils.select_ColTbl_rollup, SQLFunctionsUtils.
      select_RowTbl_rollup, "Q61_rollup_column", "Q62_rollup_row")
    validateResult(SQLFunctionsUtils.select_ColTbl_cube, SQLFunctionsUtils.
      select_RowTbl_cube, "Q63_cube_column", "Q64_cube_row")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_rollup_cube,
      SQLFunctionsUtils.dropRowTbl_rollup_cube, SQLFunctionsUtils.
        dropColTbl_rollup_cube, SQLFunctionsUtils.dropRowTbl_rollup_cube)
    /**
      *  Below queries test the functions :
      *  108. grouping, 109. grouping_id
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_grouping_grouping_id_Spark,
      SQLFunctionsUtils.createRowTypeTbl_grouping_grouping_id_Spark,
      SQLFunctionsUtils.createColumnTbl_grouping_grouping_id,
      SQLFunctionsUtils.createRowTbl_grouping_grouping_id)
    insertRecordsToTable(7, SQLFunctionsUtils.grouping_grouping_id)
    validateResult(SQLFunctionsUtils.select_ColTbl_grouping_grouping_id,
      SQLFunctionsUtils.select_RowTbl_grouping_grouping_id, "Q65_grouping_grouping_id",
      "Q66_grouping_grouping_id")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_grouping_grouping_id,
      SQLFunctionsUtils.dropRowTbl_grouping_grouping_id, SQLFunctionsUtils.
        dropColTbl_grouping_grouping_id, SQLFunctionsUtils.dropRowTbl_grouping_grouping_id)
    /**
      *  Below queries test the functions :
      *  110. approx_count_dist, 111. mean,
      *   116. cume_dist, 117. percent_rank
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_approxcntdist_mean_Spark,
      SQLFunctionsUtils.createRowTypeTbl_approxcntdist_mean_Spark,
      SQLFunctionsUtils.createColumnTbl_approxcntdist_mean,
      SQLFunctionsUtils.createRowTbl_approxcntdist_mean)
    insertRecordsToTable(9, SQLFunctionsUtils.approxcntdist_mean)
    validateResult(SQLFunctionsUtils.select_ColTbl_approxcntdist_mean,
      SQLFunctionsUtils.select_RowTbl_approxcntdist_mean, "Q67_approxcntdist_mean",
      "Q68_approxcntdist_mean")
    /**
      *  116. cume_dist, 117. percent_rank
      */
    validateResult(SQLFunctionsUtils.select_ColTbl_cumedist_prank,
      SQLFunctionsUtils.select_RowTbl_cumedist_prank, "Q74_cumedist_percentrank",
    "Q75_cumedist_percentrank")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_approxcntdist_mean, SQLFunctionsUtils.
      dropRowTbl_approxcntdist_mean, SQLFunctionsUtils.dropColTbl_approxcntdist_mean,
      SQLFunctionsUtils.dropRowTbl_approxcntdist_mean)
    /**
      *  Below queries test the functions :
      *  112. printf, 113. md5
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_printf_md5_Spark,
      SQLFunctionsUtils.createRowTypeTbl_printf_md5_Spark,
      SQLFunctionsUtils.createColumnTbl_printf_md5, SQLFunctionsUtils.createRowTbl_printf_md5)
    insertRecordsToTable(2, SQLFunctionsUtils.printf_md5)
    validateResult(SQLFunctionsUtils.select_ColTbl_printf_md5, SQLFunctionsUtils.
      select_RowTbl_printf_md5, "Q69_printf_md5", "Q70_printf_md5")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_printf_md5, SQLFunctionsUtils.
      dropRowTbl_printf_md5, SQLFunctionsUtils.dropColTbl_printf_md5,
      SQLFunctionsUtils.dropRowTbl_printf_md5)
    /**
      *  Below queries test the functions :
      *  114.  assert_true
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_assert_true_Spark,
      SQLFunctionsUtils.createRowTypeTbl_assert_true_Spark, SQLFunctionsUtils.
        createColumnTbl_assert_true, SQLFunctionsUtils.createRowTbl_assert_true)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.assert_true(0))
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
        SQLFunctionsUtils.values + SQLFunctionsUtils.assert_true(0))
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
        SQLFunctionsUtils.values + SQLFunctionsUtils.assert_true(0))
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
        SQLFunctionsUtils.values + SQLFunctionsUtils.assert_true(0))
    validateResult(SQLFunctionsUtils.select_ColTbl_assert_true, SQLFunctionsUtils.
      select_RowTbl_assert_true, "Q71_asserttrue_column",
      "Q71_asserttrue_row")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_printf_md5, SQLFunctionsUtils.
      dropRowTbl_printf_md5, SQLFunctionsUtils.dropColTbl_printf_md5,
      SQLFunctionsUtils.dropRowTbl_printf_md5)
    createTables(SQLFunctionsUtils.createColTypeTbl_assert_true_Spark,
      SQLFunctionsUtils.createRowTypeTbl_assert_true_Spark, SQLFunctionsUtils.
        createColumnTbl_assert_true, SQLFunctionsUtils.createRowTbl_assert_true)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.assert_true(1))
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.assert_true(1))
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.assert_true(1))
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.assert_true(1))
    try {
      validateResult(SQLFunctionsUtils.select_ColTbl_assert_true, SQLFunctionsUtils.
        select_RowTbl_assert_true, "Q72_asserttrue_column", "Q72_asserttrue_row")
    } catch {
      case e : Exception => {
        pw.println("assert_true equal condition:")
        pw.println(e.getCause)
        pw.flush()
      }
    }
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_printf_md5, SQLFunctionsUtils.
      dropRowTbl_printf_md5, SQLFunctionsUtils.dropColTbl_printf_md5,
      SQLFunctionsUtils.dropRowTbl_printf_md5)
    createTables(SQLFunctionsUtils.createColTypeTbl_assert_true_Spark,
      SQLFunctionsUtils.createRowTypeTbl_assert_true_Spark, SQLFunctionsUtils.
        createColumnTbl_assert_true, SQLFunctionsUtils.createRowTbl_assert_true)
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.assert_true(2))
    spark.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.assert_true(2))
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.columnTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.assert_true(2))
    snc.sql(SQLFunctionsUtils.insertInto + SQLFunctionsUtils.rowTbl +
      SQLFunctionsUtils.values + SQLFunctionsUtils.assert_true(2))
    try {
      validateResult(SQLFunctionsUtils.select_ColTbl_assert_true, SQLFunctionsUtils.
        select_RowTbl_assert_true, "Q73_asserttrue_column",
        "Q73_asserttrue_row")
    } catch {
      case e : Exception => {
        pw.println("assert_true > condition:")
        pw.println(e.getCause)
        pw.flush()
      }
    }
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_printf_md5, SQLFunctionsUtils.
      dropRowTbl_printf_md5, SQLFunctionsUtils.dropColTbl_printf_md5,
      SQLFunctionsUtils.dropRowTbl_printf_md5)
    /**
      *  Below  segment test the function: 115. input_file_name
      */
    snc.sql(SQLFunctionsUtils.externalTbl)
    snc.sql(SQLFunctionsUtils.manageTblCol)
    snc.sql(SQLFunctionsUtils.manageTblRow)
    val input_file_name_colTbl = snc.sql(SQLFunctionsUtils.input_file_name_colTbl).collect()
    val input_file_name_rowTbl = snc.sql(SQLFunctionsUtils.input_file_name_RowTbl).collect()
    val input_file_name_externalTbl =
      snc.sql(SQLFunctionsUtils.input_file_name_externaTbl).collect()
    pw.println(input_file_name_colTbl.mkString(","))
    pw.println(input_file_name_rowTbl.mkString(","))
    pw.println(input_file_name_externalTbl.mkString(","))
    snc.sql("DROP TABLE IF EXISTS snappy_regions_col")
    snc.sql("DROP TABLE IF EXISTS snappy_regions_row")
    snc.sql("DROP TABLE IF EXISTS staging_regions")
    pw.println()
    /**
      *  Below queries test the functions :
      *  118.  regexp_extract, 119. regexp_replace
      */
    createTables(SQLFunctionsUtils.createColTypeTbl_regexp_extract_replace_Spark,
      SQLFunctionsUtils.createRowTypeTbl_regexp_extract_replace_Spark,
      SQLFunctionsUtils.createColumnTbl_regexp_extract_replace,
      SQLFunctionsUtils.createRowTbl_regexp_extract_replace)
    insertRecordsToTable(2, SQLFunctionsUtils.regexp_extract_replace)
    validateResult(SQLFunctionsUtils.select_ColTbl_regexp_extract_replace, SQLFunctionsUtils.
      select_RowTbl_regexp_extract_replace, "Q76_regexp_extract_replace",
      "Q77_regexp_extract_replace")
    dropTablesAndPrint(SQLFunctionsUtils.dropColTbl_regexp_extract_replace, SQLFunctionsUtils.
      dropRowTbl_regexp_extract_replace, SQLFunctionsUtils.dropColTbl_regexp_extract_replace,
      SQLFunctionsUtils.dropRowTbl_regexp_extract_replace)
    /**
      *  Below queries test the functions :
      *  120.  now()
      */
    snc.sql("CREATE TABLE columnTbl(id int, ts timestamp) using column")
    snc.sql("CREATE TABLE rowTbl(id int, ts timestamp) using row")
    snc.sql("INSERT INTO columnTbl SELECT 1,now()")
    snc.sql("INSERT INTO rowTbl SELECT 1,now()")
    val columnDF = snc.sql("SELECT * FROM columnTbl")
    val rowDF = snc.sql("SELECT * FROM rowTbl")
    pw.println("now() function in Column table :")
    pw.println(columnDF.collect().mkString)
    pw.println("now() function in Row table :")
    pw.println(rowDF.collect().mkString)

    pw.println("Snappy Embedded Job - SQL Functions passed successfully.")
    pw.close()
  }
}
