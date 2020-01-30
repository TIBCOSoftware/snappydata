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

object SQLFunctionsUtils {
  // scalastyle:off println

  val rowTbl : String = "row_table"
  val columnTbl : String = "column_table"
  val usingCol : String = " USING COLUMN"
  val usingRow : String = " USING ROW"
  val createTbl : String = "CREATE TABLE IF NOT EXISTS "
  val insertInto : String = "INSERT INTO "
  val values : String = " VALUES "
  val dropTbl : String = "DROP TABLE IF EXISTS "


  /**
    *  Below queries test the functions :
    *  1. date,
    *  2. date_add,
    *  3. date_sub,
    *  4. datediff,
    *  5. date_format
    */
  val dateSet1 : String = "(1, 'AAA', current_date, '2020-01-19', 5, 3)"
  val dateSet2 : String = "(2, 'BBB', current_date, current_date, 7, 6)"
  val dateSet3 : String = "(3, 'CCC', '2019-12-31', '2020-12-31', 10, 12)"
  val createColTypeTblInSpark : String = createTbl + columnTbl +
    "(id int, name String, date1 date, date2 date, n1 int, n2 int)"
  val createRowTypeTblInSpark : String = createTbl + rowTbl +
    "(id int, name String, date1 date, date2 date, n1 int, n2 int)"
  val createColumnTbl_DateFunctions_1 : String = createTbl + columnTbl +
    "(id int, name String, date1 date, date2 date, n1 int, n2 int)" + usingCol
  val createRowTbl_DateFunctions_1 : String = createTbl + rowTbl +
    "(id int, name String, date1 date, date2 date, n1 int, n2 int)" + usingRow
  val selectQueryOnColTbl_DateFunctions_1 : String =
    "SELECT id, name, date1, date_add(date1, n1), " +
    "date_sub(date2, n2), datediff(date1, date2), date_format(current_date, 'y') " +
    "FROM " + columnTbl + " ORDER BY id"
  val selectQueryOnRowTbl_DateFunctions_1 : String =
    "SELECT id, name, date1, date_add(date1, n1), " +
    "date_sub(date2, n2), datediff(date1, date2), date_format(current_date, 'y') " +
    "FROM " + rowTbl + " ORDER BY id"
  val selectMonth_In_DateFormatFunc : String = "SELECT date_format(current_date, 'M')"
  val selectDay_DateFormatFunc : String = "SELECT date_format(current_date, 'd')"
  val dropColumnTbl_DateFunctions_1 : String = dropTbl + columnTbl
  val dropRowTbl_DateFunctions_1 : String = dropTbl + rowTbl

  /**
    *  Below queries test the fuctions :
    *  6. repeat
    *  7. reverse
    *  (NOTE : reverse logic for arrays is available since  Spark 2.4.0,
    *  test to be added after spark 2.4.0 merge
    */
  val rseRptSet1 : String = "(1,'TIBCO ComputeDB','TIBCO ComputeDB-')"
  val rseRptSet2 : String = "(2,'SQL Functions','SQL Functions ')"
  val createColTypeTbl_RseRpt_Spark : String = createTbl + columnTbl +
  "(id int,reversename string,repeatname string)"
  val createRowTypeTbl_rserpt_Spark : String = createTbl + rowTbl +
    "(id int,reversename string,repeatname string)"
  val createColumnTbl_RseRpt : String = createTbl + columnTbl +
  "(id int,reversename string,repeatname string) " + usingCol
  val createRowTbl_RseRpt : String = createTbl + rowTbl +
    "(id int,reversename string,repeatname string) " + usingRow
  val select_ColTbl_RseRpt : String = "SELECT id, reverse(reversename), " +
    "repeat(repeatname, 3) FROM " + columnTbl +  " ORDER BY id"
  val select_RowTbl_RseRpt : String = "SELECT id, reverse(reversename), " +
    "repeat(repeatname, 3) FROM " + rowTbl +  " ORDER BY id"
  val dropColTbl_RseRpt : String = dropTbl  + columnTbl
  val dropRowTbl_RseRpt : String = dropTbl + rowTbl
}
