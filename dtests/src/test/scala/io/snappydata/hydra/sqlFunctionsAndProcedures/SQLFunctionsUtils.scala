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
  val xmlPathQuery : String = "SELECT xpath('<bookstore>" +
    "<book category=\"cooking\"><title lang=\"en\">Everyday Italian</title>" +
    "<author>Giada De Laurentiis</author><year>2005</year><price>30.00</price>" +
    "</book><book category=\"children\"><title lang=\"en\">Harry Potter</title>" +
    "<author>J K. Rowling</author><year>2005</year><price>29.99</price>" +
    "</book><book category=\"web\"><title lang=\"en\">XQuery Kick Start</title>" +
    "<author>James McGovern</author><author>Per Bothner</author><author>Kurt Cagle</author>" +
    "<author>James Linn</author><author>Vaidyanathan Nagarajan</author><year>2003</year>" +
    "<price>49.99</price></book><book category=\"web\"><title lang=\"en\">Learning XML</title>" +
    "<author>Erik T. Ray</author><year>2003</year><price>39.95</price></book></bookstore>'," +
    "'/bookstore/book/author/text()')"

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
    *  Below queries test the functions :
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
    *  11. | (Bitwise OR)
    *  12. ~ (Bitwise NOT)
    */
  val NOT_AND_ExOR_Set1 : String = "(1,7,3,333)"
  val NOT_AND_ExOR_Set2 : String = "(2,3,5,7)"
  val NOT_AND_ExOR_Set3 : String = "(3,5,6,1098)"
  val NOT_AND_ExOR_Set4 : String = "(4,1,8,1234567)"
  val createColTypeTbl_NOT_AND_ExOR_Spark : String = createTbl + columnTbl +
  "(id int,n1 int,n2 int,n3 int)"
  val createRowTypeTbl_NOT_AND_ExOR_Spark : String = createTbl + rowTbl +
    "(id int,n1 int,n2 int,n3 int)"
  val createColumnTbl_NOT_AND_ExOR : String = createTbl + columnTbl +
    "(id int,n1 int,n2 int,n3 int) " + usingCol
  val createRowTbl_NOT_AND_ExOR : String = createTbl + rowTbl +
    "(id int,n1 int,n2 int,n3 int) " + usingRow
  val select_ColTbl_NOT_AND_ExOR : String = "SELECT id, (n1 & n2) as BitwiseAND, " +
    "(n1 ^ n2) as BitwiseExOR, (n1 | n2) as BitwiseOR,~(n3) as BitwiseNOT " +
    "FROM " + columnTbl + " ORDER BY id"
  val select_RowTbl_NOT_AND_ExOR : String = "SELECT id, (n1 & n2) as BitwiseAND, " +
    "(n1 ^ n2) as BitwiseExOR, (n1 | n2) as BitwiseOR,~n3 as BitwiseNOT " +
    "FROM " + rowTbl + " ORDER BY id"
  val dropColTbl_NOT_AND_ExOR : String = dropTbl + columnTbl
  val dropRowTbl_NOT_AND_ExOR : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  13. day ,14. dayofmonth, 15. dayofyear, 16. last_day,
    *  17. month, 18. next_day, 19. weekofyear
    *   20 year
    */
  val day_Month_Year_Set1 : String = "(1,current_date)"
  val day_Month_Year_Set2 : String = "(2,'2014-04-05')"
  val createColTypeTbl_Day_Month_Year_Spark : String = createTbl + columnTbl + " (id int,dt date)"
  val createRowTypeTbl_Day_Month_Year_Spark : String = createTbl + rowTbl + " (id int,dt date)"
  val createColumnTbl_Day_Month_Year : String = createTbl + columnTbl +
    "(id int,dt date) " + usingCol
  val createRowTbl_Day_Month_Year : String = createTbl + rowTbl +
    "(id int,dt date) " + usingRow
  val select_ColTbl_Day_Month_Year : String = "SELECT id,day(dt),dayofmonth(dt)," +
    "dayofyear(dt),last_day(dt),month(dt),next_day(dt,'FR'),weekofyear(dt)," +
    "year(dt) FROM " + columnTbl + " ORDER BY id"
  val select_RowTbl_Day_Month_Year : String = "SELECT id,day(dt),dayofmonth(dt)," +
    "dayofyear(dt),last_day(dt),month(dt),next_day(dt,'FR'),weekofyear(dt)," +
    "year(dt) FROM " + rowTbl + " ORDER BY id"
  val dropColTbl_Day_Month_Year : String = dropTbl  + columnTbl
  val dropRowTbl_Day_Month_Year : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  21. map , 22. map_keys, 23. map_values
    */
  val map_Keys_Values_Set1 : String = "1,MAP('Maths',14,'Science',18,'Hindi',15)"
  val map_Keys_Values_Set2 : String = "2,MAP('Maths',19,'Science',19,'Hindi',19)"
  val createColTypeTbl_map_Keys_Values_Spark : String = createTbl + columnTbl +
    " (id int,marks map<string,int>)"
  val createColumnTbl_map_Keys_Values : String = createTbl + columnTbl +
    "(id int,marks map<string,int>) " + usingCol
  val select_ColTbl_map_Keys_Values : String = "SELECT id,map_keys(marks)," +
    "map_values(marks),marks FROM " + columnTbl + " ORDER BY ID"
  val dropColTbl_map_Keys_Values : String = dropTbl  + columnTbl
  /**
    *  Below queries test the functions :
    *  24. array , 25. array_contains
    */
  val array_Contains_Set1 : String = "1,Array(3,5,6,8,1)"
  val array_Contains_Set2 : String = "2,Array(13,45,66,98,101)"
  val createColTypeTbl_array_Contains_Spark : String = createTbl + columnTbl +
    " (id int,arr Array<Int>)"
  val createColumnTbl_array_Contains_Values : String = createTbl + columnTbl +
    "(id int,arr Array<Int>) " + usingCol
  val select_ColTbl_array_Contains : String = "SELECT id,arr as Array,array_contains(arr,8) " +
    "FROM " + columnTbl + " ORDER BY ID"
  val dropColTbl_array_Contains : String = dropTbl  + columnTbl
  /**
    *  Below queries test the functions :
    *  26. and, 27. or , 28. not
    */
  val And_Or_Not_Set1 : String = "(1,false,false,false)"
  val And_Or_Not_Set2 : String = "(2,false,true,false)"
  val And_Or_Not_Set3 : String = "(3,true,false,true)"
  val And_Or_Not_Set4 : String = "(4,true,true,true)"
  val createColTypeTbl_And_Or_Not_Spark : String = createTbl + columnTbl +
    "(id int,b1 boolean,b2 boolean,b boolean)"
  val createRowTypeTbl_And_Or_Not_Spark : String = createTbl + rowTbl +
    "(id int,b1 boolean,b2 boolean,b boolean)"
  val createColumnTbl_And_Or_Not : String = createTbl + columnTbl +
    "(id int,b1 boolean,b2 boolean,b boolean) " + usingCol
  val createRowTbl_And_Or_Not : String = createTbl + rowTbl +
    "(id int,b1 boolean,b2 boolean,b boolean) " + usingRow
  val select_ColTbl_And_Or_Not : String = "SELECT id,(b1 AND b2) as LogicalAND," +
    "(b1 OR b2) as LogicalOR, NOT(b) as LogicalNOT FROM " + columnTbl + " ORDER BY id"
  val select_RowTbl_And_Or_Not : String = "SELECT id,(b1 AND b2) as LogicalAND," +
    "(b1 OR b2) as LogicalOR, NOT(b) as LogicalNOT FROM " + rowTbl + " ORDER BY id"
  val dropColTbl_And_Or_Not : String = dropTbl  + columnTbl
  val dropRowTbl_And_Or_Not : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  30. size.
    *  NOTE : Following test case is Pending.
    *  SET spark.sql.legacy.sizeOfNull is set to false, the function returns null for null input
    */
  val size_Set1 : String = "1,Array(10,20,30,40,50,60),Map('Rahul',40.45,'Virat',54.32,'Dhoni',56.78)"
  val size_Set2 : String = "2,Array(80,99,65,78),null"
  val size_Set3 : String = "3,null,Map('AAA',0.0,'BBB',6.67)"
  val size_Set4 : String = "4,null,null"
  val createColTypeTbl_Size_Spark : String = createTbl + columnTbl +
    " (id int,testArr Array<Int>,testMap Map<String,Double>)"
  val createColumnTbl_Size : String = createTbl + columnTbl +
    "(id int,testArr Array<Int>,testMap Map<String,Double>) " + usingCol
  val select_ColTbl_Size : String = "SELECT id,size(testArr),size(testMap) FROM " +
    columnTbl + " ORDER BY ID"
  val dropColTbl_Size : String = dropTbl  + columnTbl
  /**
    *  Below queries test the functions :
    *  31. rpad, 32. in
    */
  val rpad_in_Set1 : String = "(1,'TIBCO ComputeDB')"
  val rpad_in_Set2 : String = "(2,'Spot fire')"
  val rpad_in_Set3 : String = "(3,'DBVisualizer')"
  val createColTypeTbl_rpad_in_Spark : String = createTbl + columnTbl +
    "(id int,testStr string)"
  val createRowTypeTbl_rpad_in_Spark : String = createTbl + rowTbl +
    "(id int,testStr string)"
  val createColumnTbl_rpad_in : String = createTbl + columnTbl +
    "(id int,testStr string) " + usingCol
  val createRowTbl_rpad_in : String = createTbl + rowTbl +
    "(id int,testStr string) " + usingRow
  val select_ColTbl_rpad_in : String = "SELECT id,rpad(testStr,50,'-The TIBCO Product')," +
    "'Spot fire' in(testStr) FROM " + columnTbl + " ORDER BY id"
  val select_RowTbl_rpad_in : String = "SELECT id,rpad(testStr,50,'-The TIBCO Product')," +
    "'Spot fire' in(testStr) FROM " + rowTbl + " ORDER BY id"
  val dropColTbl_rpad_in : String = dropTbl  + columnTbl
  val dropRowTbl_rpad_in : String = dropTbl + rowTbl
}
