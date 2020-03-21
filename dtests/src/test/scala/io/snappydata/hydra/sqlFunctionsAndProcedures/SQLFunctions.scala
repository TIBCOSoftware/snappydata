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
    def createTables(schema : String, tbls : String) : Unit = {
      if (tbls == "R") {
        spark.sql(SQLFunctionsUtils.createTbl + SQLFunctionsUtils.rowTbl + schema)
        snc.sql(SQLFunctionsUtils.createTbl + SQLFunctionsUtils.rowTbl +
          schema + SQLFunctionsUtils.usingRow)
      }
      else if (tbls == "C") {
        spark.sql(SQLFunctionsUtils.createTbl + SQLFunctionsUtils.columnTbl + schema)
        snc.sql(SQLFunctionsUtils.createTbl + SQLFunctionsUtils.columnTbl +
          schema + SQLFunctionsUtils.usingCol)
      }
      else if (tbls == "RC") {
        spark.sql(SQLFunctionsUtils.createTbl + SQLFunctionsUtils.rowTbl + schema)
        spark.sql(SQLFunctionsUtils.createTbl + SQLFunctionsUtils.columnTbl + schema)
        snc.sql(SQLFunctionsUtils.createTbl + SQLFunctionsUtils.rowTbl +
          schema + SQLFunctionsUtils.usingRow)
        snc.sql(SQLFunctionsUtils.createTbl + SQLFunctionsUtils.columnTbl +
          schema + SQLFunctionsUtils.usingCol)
      }
    }
    // DROP SPARK / SNAPPY TABLES.
    def dropTablesAndPrint(tbls : String) : Unit = {
      if (tbls == "R") {
        spark.sql(SQLFunctionsUtils.dropTbl + SQLFunctionsUtils.rowTbl )
        snc.sql(SQLFunctionsUtils.dropTbl + SQLFunctionsUtils.rowTbl )
      }
      else if (tbls == "C") {
        spark.sql(SQLFunctionsUtils.dropTbl + SQLFunctionsUtils.columnTbl )
        snc.sql(SQLFunctionsUtils.dropTbl + SQLFunctionsUtils.columnTbl)
      }
      else if (tbls == "RC") {
        spark.sql(SQLFunctionsUtils.dropTbl + SQLFunctionsUtils.rowTbl)
        spark.sql(SQLFunctionsUtils.dropTbl + SQLFunctionsUtils.columnTbl)
        snc.sql(SQLFunctionsUtils.dropTbl + SQLFunctionsUtils.rowTbl)
        snc.sql(SQLFunctionsUtils.dropTbl + SQLFunctionsUtils.columnTbl)
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
    createTables("(id int, name String, date1 date, date2 date, n1 int, n2 int)", "RC")
    insertRecordsToTable(2, SQLFunctionsUtils.dateSet)
    validateResult(SQLFunctionsUtils.selectQueryOnColTbl_DateFunctions_1,
      SQLFunctionsUtils.selectQueryOnRowTbl_DateFunctions_1,
      "Q1_ColumnTbl_DateFunc", "Q2_RowTbl_DateFunc"  )
    validateResult(SQLFunctionsUtils.selectMonth_In_DateFormatFunc, "",
      "Q3_SelectMonth_DateFormat", "")
    validateResult(SQLFunctionsUtils.selectDay_DateFormatFunc, "",
      "Q4_SelectDay_DateFormat", "")
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  6. repeat, 7. reverse
      *  (NOTE : reverse logic for arrays is available since  Spark 2.4.0,
      *  test to be added after spark 2.4.0 merge
      */
    createTables("(id int,reversename string,repeatname string)", "RC")
    insertRecordsToTable(1, SQLFunctionsUtils.rseRptSet)
    validateResult(SQLFunctionsUtils.select_ColTbl_RseRpt,
      SQLFunctionsUtils.select_RowTbl_RseRpt, "Q5_Select_RevRpt", "Q6_Select_RevRpt")
    dropTablesAndPrint("RC")
    /**
      * Below queries test the functions :
      * 8. !(Logical Not)
      * (Logical NOT is not working in Snappy Cluster)
      * Add the boolean column and data once issue resolved
      *  9. & (Bitwise AND), 10. ^ (Bitwise exclusiveOR/ExOR),
      *  11. | (Bitwise OR), 12. ~ (Bitwise NOT)
      */
    createTables("(id int,n1 int,n2 int,n3 int)", "RC")
    insertRecordsToTable(3, SQLFunctionsUtils.NOT_AND_ExOR_Set)
    validateResult(SQLFunctionsUtils.select_ColTbl_NOT_AND_ExOR,
      SQLFunctionsUtils.select_RowTbl_NOT_AND_ExOR, "Q7_NOT_AND_ExOR", "Q8_NOT_AND_ExOR")
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  13. day ,14. dayofmonth, 15. dayofyear, 16. last_day,
      *  17. month, 18. next_day, 19. weekofyear, 20 year
      */
    createTables("(id int,dt date)", "RC")
    insertRecordsToTable(1, SQLFunctionsUtils.day_Month_Year_Set)
    val snappyDF1 : DataFrame = snc.sql(SQLFunctionsUtils.select_ColTbl_Day_Month_Year)
    val sparkDF1 : DataFrame = spark.sql(SQLFunctionsUtils.select_ColTbl_Day_Month_Year)
    validateResultThroughDataFrames(snappyDF1, sparkDF1,
      "Q9_Day_Mnth_Yr", "column")
    val snappyDF2 : DataFrame = snc.sql(SQLFunctionsUtils.select_RowTbl_Day_Month_Year)
    val sparkDF2 : DataFrame = spark.sql(SQLFunctionsUtils.select_RowTbl_Day_Month_Year)
    validateResultThroughDataFrames(snappyDF2, sparkDF2,
      "Q10_Day_Mnth_Yr", "row")
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  21. map , 22. map_keys, 23. map_values
      */
     createTables("(id int,marks map<string,int>)", "C")
    insertRecordsToTable(1, SQLFunctionsUtils.map_Keys_Values_Set, false)
    val snappyDF_map_Keys_Values : DataFrame =
      snc.sql(SQLFunctionsUtils.select_ColTbl_map_Keys_Values)
    val sparkDF_map_Keys_Values : DataFrame =
      spark.sql(SQLFunctionsUtils.select_ColTbl_map_Keys_Values)
    validateResultThroughDataFrames(snappyDF_map_Keys_Values, sparkDF_map_Keys_Values,
      "Q11_map_Keys_Values", "column")
    dropTablesAndPrint("C")
    /**
      *  Below queries test the functions :
      *  24. array , 25. array_contains
      */
    createTables("(id int,arr Array<Int>)", "C")
    insertRecordsToTable(1, SQLFunctionsUtils.array_Contains_Set, false)
    validateResult(SQLFunctionsUtils.select_ColTbl_array_Contains, "",
      "Q12_array_Contains", "")
    dropTablesAndPrint("C")
    /**
      *  Below queries test the functions :
      *  26. and, 27. or , 28. not
      */
    createTables("(id int,b1 boolean,b2 boolean,b boolean)", "RC")
    insertRecordsToTable(3, SQLFunctionsUtils.And_Or_Not_Set)
    validateResult(SQLFunctionsUtils.select_ColTbl_And_Or_Not,
      SQLFunctionsUtils.select_RowTbl_And_Or_Not, "Q13_And_Or_Not", "Q14_And_Or_Not")
    dropTablesAndPrint("RC")
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
    createTables("(id int,testArr Array<Int>,testMap Map<String,Double>)", "C")
    insertRecordsToTable(3, SQLFunctionsUtils.size_Set, false)
   validateResult(SQLFunctionsUtils.select_ColTbl_Size, "", "Q16_Size", "")
    dropTablesAndPrint("C")
    /**
      *  Below queries test the functions :
      *  31. rpad, 32. in
      */
    createTables("(id int,testStr string)", "RC")
    insertRecordsToTable(2, SQLFunctionsUtils.rpad_in_Set)
    validateResult(SQLFunctionsUtils.select_ColTbl_rpad_in, SQLFunctionsUtils.select_RowTbl_rpad_in,
      "Q17_rpad_in", "Q18_rpad_in")
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  33. hour, 34. minute, 35. second
      */
    createTables("(id int,ts timestamp)", "RC")
    insertRecordsToTable(1, SQLFunctionsUtils.hr_min_sec_Set)
    validateResult(SQLFunctionsUtils.select_ColTbl_hr_min_sec,
      SQLFunctionsUtils.select_RowTbl_hr_min_sec, "Q19_hr_min_sec", "Q20_hr_min_sec")
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  36. ascii, 37. months_between, 35. current_timestamp
      */
    createTables("(id int,s1 string,s2 string,dt1 date,dt2 timestamp)", "RC")
    insertRecordsToTable(2, SQLFunctionsUtils.ascii_mnthbet_ts_Set)
    validateResult(SQLFunctionsUtils.select_ColTbl_ascii_mnthbet_ts,
      SQLFunctionsUtils.select_RowTbl_ascii_mnthbet_ts,
      "Q21_ascii_mnthbet_ts", "Q22_ascii_mnthbet_ts")
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  39. string, 40. substr, 41. substring
      */
    createTables("(id int,testNumber int,testDouble Double,testStr string)", "RC")
    insertRecordsToTable(1, SQLFunctionsUtils.str_subStr)
    val str_subStr_Col_SparkDF : DataFrame = spark.sql(SQLFunctionsUtils.select_ColTbl_str_substr)
    val str_subStr_Col_SnappyDF : DataFrame = snc.sql(SQLFunctionsUtils.select_ColTbl_str_substr)
    val str_subStr_Row_SparkDF : DataFrame = spark.sql(SQLFunctionsUtils.select_RowTbl_str_substr)
    val str_subStr_Row_SnappyDF : DataFrame = snc.sql(SQLFunctionsUtils.select_RowTbl_str_substr)
    validateResultThroughDataFrames(str_subStr_Col_SnappyDF, str_subStr_Col_SparkDF,
      "Q23_str_substr", "column")
    validateResultThroughDataFrames(str_subStr_Row_SnappyDF, str_subStr_Row_SparkDF,
      "Q24_str_substr", "row")
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  42. >, 43. >=, 44. <, 45. <=, 46 hypot
      */
    createTables("(id int,n1 int,n2 int)", "RC")
    insertRecordsToTable(7, SQLFunctionsUtils.hypot_gt_lt)
    validateResult(SQLFunctionsUtils.select_ColTbl_hypot_gt_lt,
      SQLFunctionsUtils.select_RowTbl_hypot_gt_lt, "Q25_hypot_gt_lt", "Q26_hypot_gt_lt")
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  46. space, 47. soundex
      */
    createTables("(id int,str1 string,str2 string)", "RC")
    insertRecordsToTable(3, SQLFunctionsUtils.spc_soundex)
    validateResult(SQLFunctionsUtils.select_ColTbl_spc_soundex,
      SQLFunctionsUtils.select_RowTbl_spc_soundex, "Q27_spc_soundex", "Q28_spc_soundex")
    dropTablesAndPrint("RC")
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
    createTables("(id int,s1 string,s2 string,s3 string,s4 string)", "RC")
    insertRecordsToTable(1, SQLFunctionsUtils.trim_isnotnull)
    validateResult(SQLFunctionsUtils.select_ColTbl_trim_isnotnull, SQLFunctionsUtils.
      select_RowTbl_trim_isnotnull, "Q39_trim_isnotnull", "Q40_trim_isnotnull")
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  61. =, 62. ==, 63. <=>
      */
    createTables("(id int,n1 int,n2 int,s1 string,s2 string)", "RC")
    insertRecordsToTable(4, SQLFunctionsUtils.operators)
    val operators_sncDF_Col : DataFrame = snc.sql(SQLFunctionsUtils.select_ColTbl_operators)
    val operators_sparkDF_Col : DataFrame = spark.sql(SQLFunctionsUtils.select_ColTbl_operators)
    validateResultThroughDataFrames(operators_sncDF_Col, operators_sparkDF_Col,
      "Q41_operators", "column")
    val operators_sncDF_Row : DataFrame = snc.sql(SQLFunctionsUtils.select_RowTbl_operators)
    val operators_sparkDF_Row : DataFrame = spark.sql(SQLFunctionsUtils.select_RowTbl_operators)
    validateResultThroughDataFrames(operators_sncDF_Row, operators_sparkDF_Row,
      "Q42_operators", "row")
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  64. row_number(), 65. rank(), 66. dense_rank()
      */
    createTables("(name string,class string,total double)", "RC")
    insertRecordsToTable(12, SQLFunctionsUtils.rownumber_rank)
    validateResult(SQLFunctionsUtils.select_ColTbl_rownumber_rank,
      SQLFunctionsUtils.select_RowTbl_rownumber_rank,
      "Q43_rowno_rank", "Q44_rowno_rank")
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  67. encode, 68. decode
      */
    createTables("(id int,testStr String)", "RC")
    insertRecordsToTable(3, SQLFunctionsUtils.encode_decode)
    validateResult(SQLFunctionsUtils.select_ColTbl_encode_decode,
      SQLFunctionsUtils.select_RowTbl_encode_decode,
      "Q45_encode_decode", "Q46_encode_decode")
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  69. bigint, 70. binary, 71. boolean, 72. decimal,
      *  73. double, 74. float, 75. int, 76. smallint,
      *  77. tinyint
      */
    createTables("(id int,testStr string,testDouble double,testInt int)", "RC")
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
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  78. hash, 79. sha, 80. sha1, 81. sha2.
      */
    createTables("(id int,testStr string)", "RC")
    insertRecordsToTable(2, SQLFunctionsUtils.hash_sha)
    val sncDF_hash_sha_column : DataFrame = snc.sql(SQLFunctionsUtils.select_ColTbl_hash_sha)
    val sparkDF_hash_sha_column : DataFrame = spark.sql(SQLFunctionsUtils.select_ColTbl_hash_sha)
    validateResultThroughDataFrames(sncDF_hash_sha_column, sparkDF_hash_sha_column,
      "Q49_hash_sha", "column")
    val sncDF_hash_sha_row : DataFrame = snc.sql(SQLFunctionsUtils.select_RowTbl_hash_sha)
    val sparkDF_hash_sha_row : DataFrame = spark.sql(SQLFunctionsUtils.select_RowTbl_hash_sha)
    validateResultThroughDataFrames(sncDF_hash_sha_row, sparkDF_hash_sha_row,
      "Q50_hash_sha", "row")
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  82. translate, 83. substring_index,
      *  84. split, 85. sentences.
      *   In this row table query has result mismatch, need to look.
      */
    createTables("(id int,str1 string,str2 string,str3 string,str4 string)", "RC")
    insertRecordsToTable(2, SQLFunctionsUtils.translate_split)
    validateResult(SQLFunctionsUtils.select_ColTbl_translate_split,
      SQLFunctionsUtils.select_RowTbl_translate_split, "Q51_translate_split", "Q52_translate_split")
    dropTablesAndPrint("RC")
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
    createTables("(id int,date Date,ts timestamp,number int)", "RC")
    insertRecordsToTable(2, SQLFunctionsUtils.date_time)
    validateResult(SQLFunctionsUtils.select_ColTbl_date_time,
      SQLFunctionsUtils.select_RowTbl_date_time,
      "Q53_date_time", "Q54_date_time")
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  93. lag, 94. lead, 95. ntile,
      */
    createTables("(name string,class string,total double)", "RC")
    insertRecordsToTable(12, SQLFunctionsUtils.rownumber_rank)
    validateResult(SQLFunctionsUtils.select_ColTbl_lead_lag_ntile,
      SQLFunctionsUtils.select_RowTbl_lead_lag_ntile, "Q55_lead_lag_ntile",
      "Q56_lead_lag_ntile")
    dropTablesAndPrint("RC")
    /**
      *  96th function is timestamp and it is already
      *  tested in above queries.
      *  Below queries test the functions :
      *  97. base64, 98. unbase64, 99. unix_timestamp, 100. unhex
      */
    createTables("(id int,testStr1 string,testStr2 string,testStr3 string,ts timestamp)", "RC")
    insertRecordsToTable(2, SQLFunctionsUtils.base_unbase)
    validateResult(SQLFunctionsUtils.select_ColTbl_base_unbase,
      SQLFunctionsUtils.select_RowTbl_base_unbase, "Q57_base_unbase",
      "Q58_base_unbase")
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  101. trunc, 102. quarter, 103. parse_url, 104. java_method
      */
    createTables("(id int,dt date,testStr1 string)", "RC")
    insertRecordsToTable(1, SQLFunctionsUtils.parseurl)
    val sncurlDF : DataFrame = snc.sql(SQLFunctionsUtils.select_ColTbl_parseurl)
    val sparkurlDF : DataFrame = spark.sql(SQLFunctionsUtils.select_ColTbl_parseurl)
    validateResultThroughDataFrames(sncurlDF, sparkurlDF, "Q59_parseurl", "column")
    val sncurlDF_Row : DataFrame = snc.sql(SQLFunctionsUtils.select_RowTbl_parseurl)
    val sparkurlDF_Row : DataFrame = spark.sql(SQLFunctionsUtils.select_RowTbl_parseurl)
    validateResultThroughDataFrames(sncurlDF_Row, sparkurlDF_Row, "Q60_parseurl", "row")
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  105. spark_partition_id.
      */
    spark.sql(SQLFunctionsUtils.createColTypeTbl_sparkpartitionid_Spark)
    spark.sql(SQLFunctionsUtils.createRowTypeTbl_sparkpartitionid_Spark)
    snc.sql(SQLFunctionsUtils.createColumnTbl_sparkpartitionid)
    snc.sql(SQLFunctionsUtils.createRowTbl_sparkpartitionid)
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
    spark.sql(SQLFunctionsUtils.dropColTbl_sparkpartitionid)
    spark.sql(SQLFunctionsUtils.dropRowTbl_sparkpartitionid)
    snc.sql(SQLFunctionsUtils.dropColTbl_sparkpartitionid)
    snc.sql(SQLFunctionsUtils.dropRowTbl_sparkpartitionid)
    pw.println()
    /**
      *  Below queries test the functions :
      *  106. rollup, 107. cube.
      */
    createTables("(id int,name string,gender string,salary int,country string)", "RC")
    insertRecordsToTable(9, SQLFunctionsUtils.rollup_cube)
    validateResult(SQLFunctionsUtils.select_ColTbl_rollup, SQLFunctionsUtils.
      select_RowTbl_rollup, "Q61_rollup_column", "Q62_rollup_row")
    validateResult(SQLFunctionsUtils.select_ColTbl_cube, SQLFunctionsUtils.
      select_RowTbl_cube, "Q63_cube_column", "Q64_cube_row")
    /**
      * window function validation.
      */
    validateResult(SQLFunctionsUtils.select_ColTbl_window, SQLFunctionsUtils.
      select_RowTbl_window, "Q84_window_col", "Q85_window_row")
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  108. grouping, 109. grouping_id
      */
    createTables("(continent string,country string,city string,salesamount int)", "RC")
    insertRecordsToTable(7, SQLFunctionsUtils.grouping_grouping_id)
    validateResult(SQLFunctionsUtils.select_ColTbl_grouping_grouping_id,
      SQLFunctionsUtils.select_RowTbl_grouping_grouping_id, "Q65_grouping_grouping_id",
      "Q66_grouping_grouping_id")
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  110. approx_count_dist, 111. mean,
      *   116. cume_dist, 117. percent_rank
      */
    createTables("(empname string,department string,salary int)", "RC")
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
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  112. printf, 113. md5
      */
    createTables("(id int,str1 string,str2 string)", "RC")
    insertRecordsToTable(2, SQLFunctionsUtils.printf_md5)
    validateResult(SQLFunctionsUtils.select_ColTbl_printf_md5, SQLFunctionsUtils.
      select_RowTbl_printf_md5, "Q69_printf_md5", "Q70_printf_md5")
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  114.  assert_true
      */
    createTables("(i1 int,i2 int)", "RC")
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
    dropTablesAndPrint("RC")
    createTables("(i1 int,i2 int)", "RC")
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
    dropTablesAndPrint("RC")
    createTables("(i1 int,i2 int)", "RC")
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
    dropTablesAndPrint("RC")
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
    createTables("(empid int,empname string,email string)", "RC")
    insertRecordsToTable(2, SQLFunctionsUtils.regexp_extract_replace)
    validateResult(SQLFunctionsUtils.select_ColTbl_regexp_extract_replace, SQLFunctionsUtils.
      select_RowTbl_regexp_extract_replace, "Q76_regexp_extract_replace",
      "Q77_regexp_extract_replace")
    dropTablesAndPrint("RC")
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
    pw.println()
    /**
      *  Below queries test the functions :
      *  121.  json_tuple, 122.  crc32
      */
    createTables("(id int,checksum string,jsonstr string)", "RC")
    insertRecordsToTable(2, SQLFunctionsUtils.json_tuple_crc32)
    validateResult(SQLFunctionsUtils.select_ColTbl_json_tuple_crc32, SQLFunctionsUtils.
      select_RowTbl_json_tuple_crc32, "Q78_json_tuple_crc32",
      "Q79_json_tuple_crc32")
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  123.  like, 124.  rlike
      */
    createTables("(name string,state string,party string)", "RC")
    insertRecordsToTable(14, SQLFunctionsUtils.like_rlike)
    validateResult(SQLFunctionsUtils.select_ColTbl_like, SQLFunctionsUtils.select_RowTbl_like,
      "Q80_like", "Q81_like")
    validateResult(SQLFunctionsUtils.select_ColTbl_rlike, SQLFunctionsUtils.select_RowTbl_rlike,
      "Q82_rlike", "Q83_rlike")
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  126. variance, 127. var_samp, 128. var_pop,
      *  129. stddev, 130. stddev_samp, 131. stddev_pop, 132. std,
      *  135. skewness, 136. kurtosis
      */
    createTables("(id int,state string,indian_nationals_corona int,foreign_national_corana int)",
      "RC")
    insertRecordsToTable(19, SQLFunctionsUtils.variance)
    val varianceSncColDF = snc.sql(SQLFunctionsUtils.select_ColTbl_variance)
    val varianceSparkColDF = spark.sql(SQLFunctionsUtils.select_ColTbl_variance)
    validateResultThroughDataFrames(varianceSncColDF, varianceSparkColDF,
    "Q86_column_variance", "column")
    val varianceSncRowDF = snc.sql(SQLFunctionsUtils.select_RowTbl_variance)
    val varianceSparkRowDF = spark.sql(SQLFunctionsUtils.select_RowTbl_variance)
    validateResultThroughDataFrames(varianceSncRowDF, varianceSparkRowDF,
    "Q87_row_variance", "row")
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  133. named_struct
      */
    createTables("(id int,v1 string,v2 double,v3 boolean)", "RC")
    insertRecordsToTable(2, SQLFunctionsUtils.named_struct)
    val namedStructSncColDF = snc.sql(SQLFunctionsUtils.select_ColTbl_named_struct)
    val namedStructSparkColDF = spark.sql(SQLFunctionsUtils.select_ColTbl_named_struct)
    validateResultThroughDataFrames(namedStructSncColDF, namedStructSparkColDF,
      "Q88_column_namedstruct", "column")
    val namedStructSncRowDF = snc.sql(SQLFunctionsUtils.select_RowTbl_named_struct)
    val namedStructSparkRowDF = spark.sql(SQLFunctionsUtils.select_RowTbl_named_struct)
    validateResultThroughDataFrames(namedStructSncRowDF, namedStructSparkRowDF,
      "Q89_row_namedstruct", "row")
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  134. dsid() - Returns the unique distributed member ID of executor fetching current row.
      */
    snc.sql(SQLFunctionsUtils.createColumnTbl_dsid)
    snc.sql(SQLFunctionsUtils.createRowTbl_dsid)
    for(index <- 0 to 3) {
      snc.sql("INSERT INTO " + SQLFunctionsUtils.columnTbl +
        " VALUES" + SQLFunctionsUtils.dsid(index))
    }
    for(index <- 0 to 3) {
      snc.sql("INSERT INTO " + SQLFunctionsUtils.rowTbl +
        " VALUES" + SQLFunctionsUtils.dsid(index))
    }
    val dsidColDF = snc.sql(SQLFunctionsUtils.select_ColTbl_dsid)
    val dsidRowDF = snc.sql(SQLFunctionsUtils.select_RowTbl_dsid)
    pw.println("dsid COLUMN Table :")
    pw.println(dsidColDF.collect().mkString)
    pw.println("dsid ROW Table:")
    pw.println(dsidRowDF.collect().mkString)
    snc.sql(SQLFunctionsUtils.dropColTbl_dsid)
    snc.sql(SQLFunctionsUtils.dropRowTbl_dsid)
    pw.println()
    /**
      *  Below queries test the functions :
      *  137.  corr,138. covar_pop,139. covar_samp,
      */
    createTables("(temperature double,icecreamsales int)", "RC")
    insertRecordsToTable(11, SQLFunctionsUtils.correlation)
    validateResult(SQLFunctionsUtils.select_ColTbl_correlation, SQLFunctionsUtils.
      select_RowTbl_correlation, "Q90_correlation", "Q91_correlation")
    dropTablesAndPrint("RC")
    /**
      *  Below queries test the functions :
      *  140. approx_percentile,141. percentile,142. percentile_approx
      */
    createTables("(id int,d double)", "RC")
    insertRecordsToTable(4, SQLFunctionsUtils.percentile)
    val percentileSncColDF = snc.sql(SQLFunctionsUtils.select_ColTbl_percentile)
    val percentileSparkColDF = spark.sql(SQLFunctionsUtils.select_ColTbl_percentile)
    validateResultThroughDataFrames(percentileSncColDF, percentileSparkColDF,
      "Q92_percentile_Column", "Column")
    val percentileSncRowDF = snc.sql(SQLFunctionsUtils.select_RowTbl_percentile)
    val percentileSparkRowDF = spark.sql(SQLFunctionsUtils.select_RowTbl_percentile)
    validateResultThroughDataFrames(percentileSncRowDF, percentileSparkRowDF,
      "Q93_percentile_Row", "Row")
    dropTablesAndPrint("RC")

    pw.println("Snappy Embedded Job - SQL Functions passed successfully.")
    pw.close()
  }
}
