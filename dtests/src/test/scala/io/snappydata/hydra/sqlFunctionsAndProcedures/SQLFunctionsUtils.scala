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
  val dateSet = new Array[String](3)
  dateSet(0) = "(1, 'AAA', current_date, '2020-01-19', 5, 3)"
  dateSet(1) = "(2, 'BBB', current_date, current_date, 7, 6)"
  dateSet(2) = "(3, 'CCC', '2019-12-31', '2020-12-31', 10, 12)"
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
  val rseRptSet = new Array[String](2)
  rseRptSet(0) = "(1,'TIBCO ComputeDB','TIBCO ComputeDB-')"
  rseRptSet(1) = "(2,'SQL Functions','SQL Functions ')"
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
    * hence test case needs to be add.
    * Add the boolean column and data once issue resolved.
    * 9. & (Bitwise AND)
    * 10. ^ (Bitwise exclusiveOR/ExOR)
    * val NOT_AND_ExOR_Set1 : String="(1,7,3,true)"
    * val createColTypeTbl_NOT_AND_ExOR_Spark : String= createTbl + columnTbl +
    * "(id int,n1 int,n2 int,b boolean)"
    *  11. | (Bitwise OR)
    *  12. ~ (Bitwise NOT)
    */
  val NOT_AND_ExOR_Set = new Array[String](4)
  NOT_AND_ExOR_Set(0) = "(1,7,3,333)"
  NOT_AND_ExOR_Set(1) = "(2,3,5,7)"
  NOT_AND_ExOR_Set(2) = "(3,5,6,1098)"
  NOT_AND_ExOR_Set(3) = "(4,1,8,1234567)"
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
  val day_Month_Year_Set = new Array[String](2)
  day_Month_Year_Set(0) = "(1,current_date)"
  day_Month_Year_Set(1) = "(2,'2014-04-05')"
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
  val map_Keys_Values_Set = new Array[String](2)
  map_Keys_Values_Set(0) = "1,MAP('Maths',14,'Science',18,'Hindi',15)"
  map_Keys_Values_Set(1) = "2,MAP('Maths',19,'Science',19,'Hindi',19)"
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
  val array_Contains_Set = new Array[String](2)
  array_Contains_Set(0) = "1,Array(3,5,6,8,1)"
  array_Contains_Set(1) = "2,Array(13,45,66,98,101)"
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
  val And_Or_Not_Set = new Array[String](4)
  And_Or_Not_Set(0) = "(1,false,false,false)"
  And_Or_Not_Set(1) = "(2,false,true,false)"
  And_Or_Not_Set(2) = "(3,true,false,true)"
  And_Or_Not_Set(3) = "(4,true,true,true)"
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
  val size_Set = new Array[String](4)
  size_Set(0) = "1,Array(10,20,30,40,50,60),Map('Rahul',40.45,'Virat',54.32,'Dhoni',56.78)"
  size_Set(1) = "2,Array(80,99,65,78),null"
  size_Set(2) = "3,null,Map('AAA',0.0,'BBB',6.67)"
  size_Set(3) = "4,null,null"
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
  val rpad_in_Set = new Array[String](3)
  rpad_in_Set(0) = "(1,'TIBCO ComputeDB')"
  rpad_in_Set(1) = "(2,'Spot fire')"
  rpad_in_Set(2) = "(3,'DBVisualizer')"
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
  /**
    *  Below queries test the functions :
    *  33. hour, 34. minute, 35. second
    */
  val hr_min_sec_Set = new Array[String](2)
  hr_min_sec_Set(0) = "(1,'2019-03-29 12:58:59')"
  hr_min_sec_Set(1) = "(2,'2020-11-14 23:45:12')"
  val createColTypeTbl_hr_min_sec_Spark : String = createTbl + columnTbl + "(id int,ts timestamp)"
  val createRowTypeTbl_hr_min_sec_Spark : String = createTbl + rowTbl + "(id int,ts timestamp)"
  val createColumnTbl_hr_min_sec : String = createTbl + columnTbl +
    "(id int,ts timestamp) " + usingCol
  val createRowTbl_hr_min_sec : String = createTbl + rowTbl +
    "(id int,ts timestamp) " + usingRow
  val select_ColTbl_hr_min_sec : String = "SELECT id,hour(ts),minute(ts),second(ts) FROM " +
    columnTbl + " ORDER BY id"
  val select_RowTbl_hr_min_sec : String = "SELECT id,hour(ts),minute(ts),second(ts) FROM " +
    rowTbl + " ORDER BY id"
  val dropColTbl_hr_min_sec : String = dropTbl  + columnTbl
  val dropRowTbl_hr_min_sec : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  36. ascii, 37. months_between, 38. current_timestamp
    */
  val ascii_mnthbet_ts_Set = new Array[String](3)
  ascii_mnthbet_ts_Set(0) = "(1,'Spark','S','2019-09-07',current_timestamp)"
  ascii_mnthbet_ts_Set(1) = "(2,'abcd','a','2018-11-15','2017-12-25 16:55:43')"
  ascii_mnthbet_ts_Set(2) = "(3,'0123','0','2019-12-31','2019-12-31 11:12:13')"
  val createColTypeTbl_ascii_mnthbet_ts_Spark : String = createTbl + columnTbl +
    "(id int,s1 string,s2 string,dt1 date,dt2 timestamp)"
  val createRowTypeTbl_ascii_mnthbet_ts_Spark : String = createTbl + rowTbl +
    "(id int,s1 string,s2 string,dt1 date,dt2 timestamp)"
  val createColumnTbl_ascii_mnthbet_ts : String = createTbl + columnTbl +
    "(id int,s1 string,s2 string,dt1 date,dt2 timestamp) " + usingCol
  val createRowTbl_ascii_mnthbet_ts : String = createTbl + rowTbl +
    "(id int,s1 string,s2 string,dt1 date,dt2 timestamp) " + usingRow
  val select_ColTbl_ascii_mnthbet_ts : String = "SELECT id,ascii(s1),ascii(s2)," +
    "months_between(dt1,dt2) FROM " + columnTbl + " ORDER BY id"
  val select_RowTbl_ascii_mnthbet_ts : String = "SELECT id,ascii(s1),ascii(s2)," +
    "months_between(dt1,dt2) FROM " + rowTbl + " ORDER BY id"
  val dropColTbl_ascii_mnthbet_ts : String = dropTbl  + columnTbl
  val dropRowTbl_ascii_mnthbet_ts : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  39. string, 40. substr, 41. substring
    */
  val str_subStr = new Array[String](2)
  str_subStr(0) = "(1,123456,43.45,'TIBCO ComputeDB')"
  str_subStr(1) = "(2,9999,1004.56,'JDBC Driver')"
  val createColTypeTbl_str_substr_Spark : String = createTbl + columnTbl +
    "(id int,testNumber int,testDouble Double,testStr string)"
  val createRowTypeTbl_str_substr_Spark : String = createTbl + rowTbl +
    "(id int,testNumber int,testDouble Double,testStr string)"
  val createColumnTbl_str_substr : String = createTbl + columnTbl +
    "(id int,testNumber int,testDouble Double,testStr string) " + usingCol
  val createRowTbl_str_substr : String = createTbl + rowTbl +
    "(id int,testNumber int,testDouble Double,testStr string) " + usingRow
  val select_ColTbl_str_substr : String = "SELECT id,string(testNumber),string(testDouble)," +
    "substr(teststr,0,5),substring(teststr,0,5),substr(teststr,3),substring(teststr,3)," +
    "substr(teststr,-5),substring(teststr,-5) FROM " + columnTbl + " ORDER BY id"
  val select_RowTbl_str_substr : String = "SELECT id,string(testNumber),string(testDouble)," +
    "substr(teststr,0,5),substring(teststr,0,5),substr(teststr,3),substring(teststr,3)," +
    "substr(teststr,-5),substring(teststr,-5) FROM " + rowTbl + " ORDER BY id"
  val dropColTbl_str_substr : String = dropTbl  + columnTbl
  val dropRowTbl_str_substr : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  42. >, 43. >=, 44. <, 45. <=, 46 hypot (hypot already done)
    *  So 46 is duplicate number and need not to be removed.
    *  Need to add the test case for complex data types for 42 to 45.
    */
  val hypot_gt_lt = new Array[String](9)
  hypot_gt_lt(0) = "(1,15,12)"
  hypot_gt_lt(1) = "(2,4,5)"
  hypot_gt_lt(2) = "(3,100,20)"
  hypot_gt_lt(3) = "(4,10,68)"
  hypot_gt_lt(4) = "(5,NULL,NULL)"
  hypot_gt_lt(5) = "(6,8,NULL)"
  hypot_gt_lt(6) = "(7,NULL,225)"
  hypot_gt_lt(7) = "(8,4,4)"
  val createColTypeTbl_hypot_gt_lt_Spark : String = createTbl + columnTbl +
    "(id int,n1 int,n2 int)"
  val createRowTypeTbl_hypot_gt_lt_Spark : String = createTbl + rowTbl +
    "(id int,n1 int,n2 int)"
  val createColumnTbl_hypot_gt_lt : String = createTbl + columnTbl +
    "(id int,n1 int,n2 int) " + usingCol
  val createRowTbl_hypot_gt_lt : String = createTbl + rowTbl +
    "(id int,n1 int,n2 int) " + usingRow
  val select_ColTbl_hypot_gt_lt : String = "SELECT id,(n1>n2) as GT,(n1>=n2) as GTEQ," +
    "(n1<n2) as LT,(n1<=n2) as LTEQ,hypot(n1,n2) as HYPOT FROM " + columnTbl + " ORDER BY id"
  val select_RowTbl_hypot_gt_lt : String = "SELECT id,(n1>n2) as GT,(n1>=n2) as GTEQ," +
    "(n1<n2) as LT,(n1<=n2) as LTEQ,hypot(n1,n2) as HYPOT FROM " + rowTbl + " ORDER BY id"
  val dropColTbl_hypot_gt_lt : String = dropTbl  + columnTbl
  val dropRowTbl_hypot_gt_lt : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  46. space, 47. soundex
    */
  val spc_soundex = new Array[String](4)
  spc_soundex(0) = "(1,'TIBCO','TIBCO')"
  spc_soundex(1) = "(2,'Computation','Computation')"
  spc_soundex(2) = "(3,'Spark','Spark')"
  spc_soundex(3) = "(4,NULL,NULL)"
  val createColTypeTbl_spc_soundex_Spark : String = createTbl + columnTbl +
    "(id int,str1 string,str2 string)"
  val createRowTypeTbl_spc_soundex_Spark : String = createTbl + rowTbl +
    "(id int,str1 string,str2 string)"
  val createColumnTbl_spc_soundex : String = createTbl + columnTbl +
    "(id int,str1 string,str2 string) " + usingCol
  val createRowTbl_spc_soundex : String = createTbl + rowTbl +
    "(id int,str1 string,str2 string) " + usingRow
  val select_ColTbl_spc_soundex : String = "SELECT id,concat(space(20),str1)," +
    "soundex(str2) as soundex FROM " + columnTbl + " ORDER BY id"
  val select_RowTbl_spc_soundex : String = "SELECT id,concat(space(20),str1)," +
    "soundex(str2) as soundex FROM " + rowTbl + " ORDER BY id"
  val dropColTbl_spc_soundex : String = dropTbl  + columnTbl
  val dropRowTbl_spc_soundex : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  48. xpath, 49. xpath_boolean, 50. xpath_double, 51. xpath_float,
    *  52. xpath_int, 53. xpath_long, 54. xpath_number, 55. xpath_short,
    *  56. xpath_string.
    */
  val xml : String = "<bookstore>" +
    "<book category=\"cooking\"><title lang=\"en\">Everyday Italian</title>" +
      "<author>Giada De Laurentiis</author><year>2005</year><price>30.00</price>" +
      "</book><book category=\"children\"><title lang=\"en\">Harry Potter</title>" +
      "<author>J K. Rowling</author><year>2005</year><price>29.99</price>" +
      "</book><book category=\"web\"><title lang=\"en\">XQuery Kick Start</title>" +
      "<author>James McGovern</author><author>Per Bothner</author><author>Kurt Cagle</author>" +
      "<author>James Linn</author><author>Vaidyanathan Nagarajan</author><year>2003</year>" +
      "<price>49.99</price></book><book category=\"web\"><title lang=\"en\">Learning XML</title>" +
      "<author>Erik T. Ray</author><year>2003</year><price>39.95</price></book></bookstore>"
  val xPath : String = "SELECT xpath(" + "'" + xml + "'," + "'/bookstore/book/author/text()')"
  val xPath_Boolean_true : String = "SELECT xpath_boolean(" + "'" + xml + "'," +
    "'/bookstore/book/price')"
  val xPath_Boolean_false : String = "SELECT xpath_boolean(" + "'" + xml + "'," +
    "'/bookstore/book/publisher')"
  val xPath_double : String = "SELECT xpath_double(" + "'" + xml + "'," +
    "'sum(/bookstore/book/price)')"
  val xPath_float : String = "SELECT xpath_double(" + "'" + xml + "'," +
    "'sum(/bookstore/book/oldprice)')"
  val xPath_int : String = "SELECT xpath_int(" + "'" + xml + "'," +
    "'sum(/bookstore/book/year)')"
  val xPath_long : String = "SELECT xpath_long(" + "'" + xml + "'," +
    "'sum(/bookstore/book/year)')"
  val xPath_number : String = "SELECT xpath_number(" + "'" + xml + "'," +
    "'sum(/bookstore/book/price)')"
  val xPath_short : String = "SELECT xpath_short(" + "'" + xml + "'," +
    "'sum(/bookstore/book/newprice)')"
  val xPath_string : String = "SELECT xpath_string(" + "'" + xml + "'," +
    "'/bookstore/book/title')"
  /**
    *  Below queries test the functions :
    *  57. trim, 58. ltrim, 59. rtrim, 60. isnotnull
    */
  val trim_isnotnull = new Array[String](2)
  trim_isnotnull(0) = "(1,' TIBCO-ComputeDB ','       TIBCO-ComputeDB'," +
    "'TIBCO-ComputeDB       ','JDBC Client')"
  trim_isnotnull(1) = "(2,' DB Visualizer    ','     DB Visualizer','DB Visualizer         ',null)"
  val createColTypeTbl_trim_isnotnull_Spark : String = createTbl + columnTbl +
    "(id int,s1 string,s2 string,s3 string,s4 string)"
  val createRowTypeTbl_trim_isnotnull_Spark : String = createTbl + rowTbl +
    "(id int,s1 string,s2 string,s3 string,s4 string)"
  val createColumnTbl_trim_isnotnull : String = createTbl + columnTbl +
    "(id int,s1 string,s2 string,s3 string,s4 string) " + usingCol
  val createRowTbl_trim_isnotnull : String = createTbl + rowTbl +
    "(id int,s1 string,s2 string,s3 string,s4 string) " + usingRow
  val select_ColTbl_trim_isnotnull : String = "SELECT id,trim(s1),ltrim(s2),rtrim(s3)," +
    "isnotnull(s4) FROM " + columnTbl + " ORDER BY id"
  val select_RowTbl_trim_isnotnull : String = "SELECT id,trim(s1),ltrim(s2),rtrim(s3)," +
    "isnotnull(s4) FROM " + rowTbl + " ORDER BY id"
  val dropColTbl_trim_isnotnull : String = dropTbl  + columnTbl
  val dropRowTbl_trim_isnotnull : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  61. =, 62. ==, 63. <=>
    */
  val operators = new Array[String](6)
  operators(0) = "(1,100,250,'100','abc')"
  operators(1) = "(2,10,23,'10','xyz')"
  operators(2) = "(3,5,9,'5','9')"
  operators(3) = "(4,44,56,null,null)"
  operators(4) = "(5,34,78,'76','98')"
  val createColTypeTbl_operators_Spark : String = createTbl + columnTbl +
    "(id int,n1 int,n2 int,s1 string,s2 string)"
  val createRowTypeTbl_operators_Spark : String = createTbl + rowTbl +
    "(id int,n1 int,n2 int,s1 string,s2 string)"
  val createColumnTbl_operators : String = createTbl + columnTbl +
    "(id int,n1 int,n2 int,s1 string,s2 string) " + usingCol
  val createRowTbl_operators : String = createTbl + rowTbl +
    "(id int,n1 int,n2 int,s1 string,s2 string) " + usingRow
  val select_ColTbl_operators : String = "SELECT id,n1=s1,n2=s2,n1==s1,n2==s2," +
    "n1<=>s1,n2<=>s2 FROM " + columnTbl + " ORDER BY id"
  val select_RowTbl_operators : String = "SELECT id,n1=s1,n2=s2,n1==s1,n2==s2," +
    "n1<=>s1,n2<=>s2 FROM " + rowTbl + " ORDER BY id"
  val dropColTbl_operators : String = dropTbl  + columnTbl
  val dropRowTbl_operators : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  64. row_number(), 65. rank(), 66. dense_rank()
    */
  val rownumber_rank = new Array[String](13)
  rownumber_rank(0) = "('Suresh','First',98.6)"
  rownumber_rank(1) = "('Suresh','First',98.4)"
  rownumber_rank(2) = "('Mahesh','First',99.4)"
  rownumber_rank(3) = "('Giri','Second',96.7)"
  rownumber_rank(4) = "('Neel','Third',80.8)"
  rownumber_rank(5) = "('Suresh','First',98.6)"
  rownumber_rank(6) = "('Viru','Second',91.3)"
  rownumber_rank(7) = "('Mahi','Second',99.9)"
  rownumber_rank(8) = "('Sachin','Third',97.4)"
  rownumber_rank(9) = "('Anu','Second',76.7)"
  rownumber_rank(10) = "('Harish','Third',67.4)"
  rownumber_rank(11) = "('Kapil','Third',88.9)"
  rownumber_rank(12) = "('Rahul','Fifth',60.3)"
  val createColTypeTbl_rownumber_rank_Spark : String = createTbl + columnTbl +
    "(name string,class string,total double)"
  val createRowTypeTbl_rownumber_rank_Spark : String = createTbl + rowTbl +
    "(name string,class string,total double)"
  val createColumnTbl_rownumber_rank : String = createTbl + columnTbl +
    "(name string,class string,total double) " + usingCol
  val createRowTbl_rownumber_rank : String = createTbl + rowTbl +
    "(name string,class string,total double) " + usingRow
  val select_ColTbl_rownumber_rank : String = "SELECT *," +
    "row_number() over (partition by class order by total) row_number," +
    "rank() over (partition by class order by total) rank," +
    " dense_rank() over (partition by class order by total) dense_rank " +
    "FROM " + columnTbl
  val select_RowTbl_rownumber_rank : String = "SELECT *," +
    "row_number() over (partition by class order by total) row_number," +
    "rank() over (partition by class order by total) rank," +
    " dense_rank() over (partition by class order by total) dense_rank " +
    "FROM " + rowTbl
  val dropColTbl_rownumber_rank : String = dropTbl  + columnTbl
  val dropRowTbl_rownumber_rank : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  67. encode, 68. decode
    */
  val encode_decode = new Array[String](4)
  encode_decode(0) = "(1,'TIBCO')"
  encode_decode(1) = "(2,'ComputeDB')"
  encode_decode(2) = "(3,'Spark')"
  encode_decode(3) = "(4,'Docker')"
  val createColTypeTbl_encode_decode_Spark : String = createTbl + columnTbl +
    "(id int,testStr String)"
  val createRowTypeTbl_encode_decode_Spark : String = createTbl + rowTbl +
    "(id int,testStr String)"
  val createColumnTbl_encode_decode : String = createTbl + columnTbl +
    "(id int,testStr String) " + usingCol
  val createRowTbl_encode_decode : String = createTbl + rowTbl +
    "(id int,testStr String) " + usingRow
  val select_ColTbl_encode_decode : String = "SELECT id,decode(encode(testStr,'utf-16'),'utf-8')," +
    "decode(encode(testStr,'utf-8'),'us-ascii')," +
    "decode(encode(testStr,'us-ascii'),'utf-16') FROM " + columnTbl
  val select_RowTbl_encode_decode : String = "SELECT id,decode(encode(testStr,'utf-16'),'utf-8')," +
    "decode(encode(testStr,'utf-8'),'us-ascii')," +
    "decode(encode(testStr,'us-ascii'),'utf-16') FROM " + rowTbl
  val dropColTbl_encode_decode : String = dropTbl  + columnTbl
  val dropRowTbl_encode_decode : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  69. bigint, 70. binary, 71. boolean, 72. decimal,
    *  73. double, 74. float, 75. int, 76. smallint,
    *  77. tinyint
    */
  val dataTypes = new Array[String](1)
  dataTypes(0) = "(1,'45',56.7,121)"
  val createColTypeTbl_dataTypes_Spark : String = createTbl + columnTbl +
    "(id int,testStr string,testDouble double,testInt int)"
  val createRowTypeTbl_dataTypes_Spark : String = createTbl + rowTbl +
    "(id int,testStr string,testDouble double,testInt int)"
  val createColumnTbl_dataTypes : String = createTbl + columnTbl +
    "(id int,testStr string,testDouble double,testInt int) " + usingCol
  val createRowTbl_dataTypes : String = createTbl + rowTbl +
    "(id int,testStr string,testDouble double,testInt int) " + usingRow
  val select_ColTbl_dataTypes : String = "SELECT id,bigint(testStr) as bigint," +
    "binary(testStr) as binary,boolean(testStr) as boolean,decimal(testStr) as decimal," +
    "double(testStr) as double,float(testStr) as float,int(testStr) as int," +
    "smallint(testStr) as smallint,tinyint(testStr) as tinyint,decimal(testDouble) as decimal," +
    "float(testDouble) as float,boolean(0),boolean(1) FROM " + columnTbl
  val select_RowTbl_dataTypes : String = "SELECT id,bigint(testStr) as bigint," +
    "binary(testStr) as binary,boolean(testStr) as boolean,decimal(testStr) as decimal," +
    "double(testStr) as double,float(testStr) as float,int(testStr) as int," +
    "smallint(testStr) as smallint,tinyint(testStr) as tinyint,decimal(testDouble) as decimal," +
    "float(testDouble) as float,boolean(0),boolean(1) FROM " + rowTbl
  val dropColTbl_dataTypes : String = dropTbl  + columnTbl
  val dropRowTbl_dataTypes : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  78. hash, 79. sha, 80. sha1, 81. sha2.
    *   In this row table query has result mismatch, need to look.
    */
  val hash_sha = new Array[String](3)
  hash_sha(0) = "(1,'ComputeDB')"
  hash_sha(1) = "(2,'SnappyData')"
  hash_sha(2) = "(3,'TDV')"
  val createColTypeTbl_hash_sha_Spark : String = createTbl + columnTbl +
    "(id int,testStr string)"
  val createRowTypeTbl_hash_sha_Spark : String = createTbl + rowTbl +
    "(id int,testStr string)"
  val createColumnTbl_hash_sha : String = createTbl + columnTbl +
    "(id int,testStr string) " + usingCol
  val createRowTbl_hash_sha : String = createTbl + rowTbl +
    "(id int,testStr string) " + usingRow
  val select_ColTbl_hash_sha : String = "SELECT id,hash(1,testStr,Map('ABC','XYZ'))," +
    "sha(testStr),sha1(testStr),sha2(testStr,224),sha2(testStr,256)," +
    "sha2(testStr,384),sha2(testStr,512) FROM " + columnTbl
  val select_RowTbl_hash_sha : String = "SELECT id,hash(1,testStr)," +
    "sha(testStr),sha1(testStr),sha2(testStr,224),sha2(testStr,256)," +
    "sha2(testStr,384),sha2(testStr,512) FROM " + rowTbl
  val dropColTbl_hash_sha : String = dropTbl  + columnTbl
  val dropRowTbl_hash_sha : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  82. translate, 83. substring_index,
    *  84. split, 85. sentences.
    */
  val translate_split = new Array[String](3)
  translate_split(0) = "(1,'CDB','TIBCO-ComputeDB-The InmemoryDatabase'," +
    "'Spark SQL','TIBCO ComputeDB is a in-memory database and efficient compute engine')"
  translate_split(1) = "(2,null,null,null,null)"
  translate_split(2) = "(3,'','','','')"
  val createColTypeTbl_translate_split_Spark : String = createTbl + columnTbl +
    "(id int,str1 string,str2 string,str3 string,str4 string)"
  val createRowTypeTbl_translate_split_Spark : String = createTbl + rowTbl +
    "(id int,str1 string,str2 string,str3 string,str4 string)"
  val createColumnTbl_translate_split : String = createTbl + columnTbl +
    "(id int,str1 string,str2 string,str3 string,str4 string) " + usingCol
  val createRowTbl_translate_split : String = createTbl + rowTbl +
    "(id int,str1 string,str2 string,str3 string,str4 string) " + usingRow
  val select_ColTbl_translate_split : String = "SELECT id,translate(str1,'CB','TV')," +
    "substring_index(str2,'-',2),substring_index(str2,'-',1)," +
    "substring_index(str2,'-',-1),substring_index(str2,'-',-2)," +
    "substring_index(str2,'-',-3),split(str3,'S+'),sentences(str4) FROM " + columnTbl
  val select_RowTbl_translate_split : String = "SELECT id,translate(str1,'CB','TV')," +
    "substring_index(str2,'-',2),substring_index(str2,'-',1),substring_index(str2,'-',-1)," +
    "substring_index(str2,'-',-2),substring_index(str2,'-',-3)," +
    "split(str3,'S+'),sentences(str4) FROM " + rowTbl
  val dropColTbl_translate_split : String = dropTbl  + columnTbl
  val dropRowTbl_translate_split : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  86. monotonically_increasing_id
    */
  val monotonically_increasing_id = new Array[String](38)
  monotonically_increasing_id(0) = "('Andhra Pradesh','Hyderabad')"
  monotonically_increasing_id(1) = "('Arunachal Pradesh','Itanagar')"
  monotonically_increasing_id(2) = "('Assam','Dispur')"
  monotonically_increasing_id(3) = "('Bihar','Patna')"
  monotonically_increasing_id(4) = "('Chhattisgarh','Raipur')"
  monotonically_increasing_id(5) = "('Goa','Panaji')"
  monotonically_increasing_id(6) = "('Gujarat','Gandhinagar')"
  monotonically_increasing_id(7) = "('Haryana','Chandigarh')"
  monotonically_increasing_id(8) = "('Himachal Pradesh','Shimla')"
  monotonically_increasing_id(9) = "('Jammu and Kashmir','Jammu/Srinagar')"
  monotonically_increasing_id(10) = "('Jharkhand','Ranchi')"
  monotonically_increasing_id(11) = "('Karnataka','Bengaluru')"
  monotonically_increasing_id(12) = "('Kerala','Thiruvananthapuram')"
  monotonically_increasing_id(13) = "('Madhya Pradesh','Bhopal')"
  monotonically_increasing_id(14) = "('Maharashtra','Mumbai')"
  monotonically_increasing_id(15) = "('Manipur','Imphal')"
  monotonically_increasing_id(16) = "('Meghalaya','Shillong')"
  monotonically_increasing_id(17) = "('Mizoram','Aizawl')"
  monotonically_increasing_id(18) = "('Nagaland','Kohima')"
  monotonically_increasing_id(19) = "('Odisha','Bhubaneswar')"
  monotonically_increasing_id(20) = "('Punjab','Chandigarh')"
  monotonically_increasing_id(21) = "('Rajasthan','Jaipur')"
  monotonically_increasing_id(22) = "('Sikkim','Gangtok')"
  monotonically_increasing_id(23) = "('Tamil Nadu','Chennai')"
  monotonically_increasing_id(24) = "('Telangana','Hyderabad')"
  monotonically_increasing_id(25) = "('Tripura','Agartala')"
  monotonically_increasing_id(26) = "('Uttar Pradesh','Lucknow')"
  monotonically_increasing_id(27) = "('Uttarakhand','Dehradun')"
  monotonically_increasing_id(28) = "('West Bengal','Kolkata')"
  monotonically_increasing_id(29) = "('Andaman and Nicobar Islands','Port Blair')"
  monotonically_increasing_id(30) = "('Chandigarh','Chandigarh')"
  monotonically_increasing_id(31) = "('Dadar and Nagar Haveli','Silvassa')"
  monotonically_increasing_id(32) = "('Daman and Diu','Daman')"
  monotonically_increasing_id(33) = "('Delhi','Delhi')"
  monotonically_increasing_id(34) = "('Lakshadweep','Kavaratti')"
  monotonically_increasing_id(35) = "('Puducherry','Pondicherry')"
  monotonically_increasing_id(36) = "('Ladakh','Leh/Kargil')"
  val createColumnTbl_monotonically_increasing_id : String = createTbl + columnTbl +
    "(state string,capital string) " + usingCol
  val createRowTbl_monotonically_increasing_id : String = createTbl + rowTbl +
    "(state string,capital string) " + usingRow
  val select_ColTbl_monotonically_increasing_id : String = "SELECT " +
    "monotonically_increasing_id() + 1 as id,state,capital FROM " + columnTbl + " ORDER BY state"
  val select_RowTbl_monotonically_increasing_id : String = "SELECT " +
    "monotonically_increasing_id() + 1 as id,state,capital FROM " + rowTbl + " ORDER BY id"
  val dropColTbl_monotonically_increasing_id : String = dropTbl  + columnTbl
  val dropRowTbl_monotonically_increasing_id : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  87. to_unix_timestamp, 88. to_utc_timestamp, 89. to_date,
    *  90. from_unixtime, 91. from_utc_timestamp.
    */
  val date_time = new Array[String](3)
  date_time(0) = "(1,current_date,current_timestamp,12345678)"
  date_time(1) = "(2,'2020-01-31',current_timestamp,234598765)"
  date_time(2) = "(3,'2000-02-02',null,0)"
  val createColTypeTbl_date_time_Spark : String = createTbl + columnTbl +
    "(id int,date Date,ts timestamp,number int)"
  val createRowTypeTbl_date_time_Spark : String = createTbl + rowTbl +
    "(id int,date Date,ts timestamp,number int)"
  val createColumnTbl_date_time : String = createTbl + columnTbl +
    "(id int,date Date,ts timestamp,number int) " + usingCol
  val createRowTbl_date_time : String = createTbl + rowTbl +
    "(id int,date Date,ts timestamp,number int) " + usingRow
  val select_ColTbl_date_time : String = "SELECT id,to_date(ts)," +
    "to_unix_timestamp(date,'yyyy-MM-dd'),to_utc_timestamp(date,'UTC-11')," +
    "from_utc_timestamp(date,'UTC-8'),from_unixtime(number,'yyyy-MM-dd') FROM " + columnTbl +
    " ORDER BY id"
  val select_RowTbl_date_time : String = "SELECT id,to_date(ts)," +
    "to_unix_timestamp(date,'yyyy-MM-dd'),to_utc_timestamp(date,'UTC-11')," +
    "from_utc_timestamp(date,'UTC-8'),from_unixtime(number,'yyyy-MM-dd') FROM " + rowTbl +
    " ORDER BY id"
  val dropColTbl_date_time : String = dropTbl  + columnTbl
  val dropRowTbl_date_time : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  93. lag, 94. lead, 95. ntile,
    */
  val createColTypeTbl_lead_lag_ntile_Spark : String = createTbl + columnTbl +
    "(name string,class string,total double)"
  val createRowTypeTbl_lead_lag_ntile_Spark : String = createTbl + rowTbl +
    "(name string,class string,total double)"
  val createColumnTbl_lead_lag_ntile : String = createTbl + columnTbl +
    "(name string,class string,total double) " + usingCol
  val createRowTbl_lead_lag_ntile : String = createTbl + rowTbl +
    "(name string,class string,total double) " + usingRow
  val select_ColTbl_lead_lag_ntile : String = "SELECT name,class,total," +
    "lag(total) over (partition by class order by total) as lag," +
    "lead(total) over (partition by class order by total) as lead," +
    "ntile(3) over (partition by class order by total) as ntile FROM " + columnTbl
  val select_RowTbl_lead_lag_ntile : String = "SELECT name,class,total," +
    "lag(total) over (partition by class order by total) as lag," +
    "lead(total) over (partition by class order by total) as lead," +
    "ntile(3) over (partition by class order by total) as ntile FROM " + rowTbl
  val dropColTbl_lead_lag_ntile : String = dropTbl  + columnTbl
  val dropRowTbl_lead_lag_ntile : String = dropTbl + rowTbl
  /**
    *  96th function is timestamp and it is already
    *  tested in above queries.
    *  Below queries test the functions :
    *  97. base64, 98. unbase64, 99. unix_timestamp, 100. unhex
    */
  val base_unbase = new Array[String](3)
  base_unbase(0) = "(1,'TIBCO ComputeDB','VElCQ08gQ29tcHV0ZURC'," +
    "'544942434f20436f6d707574654442','2019-12-26 11:22:34')"
  base_unbase(1) = "(2,'Hive MetaStore','SGl2ZSBNZXRhU3RvcmU='," +
    "'48697665204d65746153746f7265','2011-09-30 16:30:22')"
  base_unbase(2) = "(3,'AWS','QVdT','415753','2017-10-16 8:30:57')"
  val createColTypeTbl_base_unbase_Spark : String = createTbl + columnTbl +
    "(id int,testStr1 string,testStr2 string,testStr3 string,ts timestamp)"
  val createRowTypeTbl_base_unbase_Spark : String = createTbl + rowTbl +
    "(id int,testStr1 string,testStr2 string,testStr3 string,ts timestamp)"
  val createColumnTbl_base_unbase : String = createTbl + columnTbl +
    "(id int,testStr1 string,testStr2 string,testStr3 string,ts timestamp) " + usingCol
  val createRowTbl_base_unbase : String = createTbl + rowTbl +
    "(id int,testStr1 string,testStr2 string,testStr3 string,ts timestamp) " + usingRow
  val select_ColTbl_base_unbase : String = "SELECT id,base64(testStr1)," +
    "decode(unbase64(testStr2),'UTF-8'),decode(unhex(testStr3),'UTF-8'),unix_timestamp(ts) " +
    "FROM " + columnTbl
  val select_RowTbl_base_unbase : String = "SELECT id,base64(testStr1)," +
    "decode(unbase64(testStr2),'UTF-8')," +
    "decode(unhex(testStr3),'UTF-8'),unix_timestamp(ts) FROM " + rowTbl
  val dropColTbl_base_unbase : String = dropTbl  + columnTbl
  val dropRowTbl_base_unbase : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  101. trunc, 102. quarter, 103. parse_url, 104. java_method
    */
  val parseurl = new Array[String](2)
  parseurl(0) = "(1,current_date," +
    "'https://spark.apache.org/docs/latest/api/sql/index.html#parse_url')"
  parseurl(1) = "(2,null,null)"
  val createColTypeTbl_parseurl_Spark : String = createTbl + columnTbl +
    "(id int,dt date,testStr1 string)"
  val createRowTypeTbl_parseurl_Spark : String = createTbl + rowTbl +
    "(id int,dt date,testStr1 string)"
  val createColumnTbl_parseurl : String = createTbl + columnTbl +
    "(id int,dt date,testStr1 string) " + usingCol
  val createRowTbl_parseurl : String = createTbl + rowTbl +
    "(id int,dt date,testStr1 string) " + usingRow
  val select_ColTbl_parseurl : String = "SELECT id,trunc(dt,'YEAR') as year," +
    "trunc(dt,'MM') as month,trunc(dt,'DAY') as day,quarter(dt) as q1," +
    "quarter('2019-11-26') as q2,parse_url(testStr1,'PROTOCOL') as protocol," +
    "parse_url(testStr1,'HOST') as host,parse_url(testStr1,'PATH') as path," +
    "parse_url(testStr1,'QUERY') as query," +
    "java_method('java.util.UUID','randomUUID') FROM " + columnTbl + " ORDER BY id"
  val select_RowTbl_parseurl : String = "SELECT id,trunc(dt,'YEAR') as year," +
    "trunc(dt,'MM') as month,trunc(dt,'DAY') as day,quarter(dt) as q1," +
    "quarter('2019-11-26') as q2,parse_url(testStr1,'PROTOCOL') as protocol," +
    "parse_url(testStr1,'HOST') as host,parse_url(testStr1,'PATH') as path," +
    "parse_url(testStr1,'QUERY') as query,java_method('java.util.UUID','randomUUID') FROM " +
    rowTbl + " ORDER BY id"
  val dropColTbl_parseurl : String = dropTbl  + columnTbl
  val dropRowTbl_parseurl : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  105. spark_partition_id.
    */
  val createColTypeTbl_sparkpartitionid_Spark : String = createTbl + columnTbl +
    "(id int,name string)"
  val createRowTypeTbl_sparkpartitionid_Spark : String = createTbl + rowTbl +
    "(id int,name string)"
  val createColumnTbl_sparkpartitionid : String = createTbl + columnTbl +
    "(id int,name string) " + usingCol + " OPTIONS(PARTITION_BY 'id');"
  val createRowTbl_sparkpartitionid : String = createTbl + rowTbl +
    "(id int,name string) " + usingRow
  val select_ColTbl_sparkpartitionid : String = "SELECT SUM(total) FROM" +
    "(SELECT COUNT(*) AS total FROM " + columnTbl + " GROUP BY spark_partition_id())"
  val select_RowTbl_sparkpartitionid : String = "SELECT SUM(total) FROM" +
    "(SELECT COUNT(*) AS total FROM " +  rowTbl + " GROUP BY spark_partition_id())"
  val select_RowTbl_cdbsparkpartitionid : String = "SELECT COUNT(*) FROM " + rowTbl +
    " GROUP BY spark_partition_id()"
  val dropColTbl_sparkpartitionid : String = dropTbl  + columnTbl
  val dropRowTbl_sparkpartitionid : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  106. rollup, 107. cube.
    *  125. window
    */
  val rollup_cube = new Array[String](10)
  rollup_cube(0) = "(1,'Mark','Male',5000,'USA')"
  rollup_cube(1) = "(2,'John','Male',4500,'India')"
  rollup_cube(2) = "(3,'Pam','Female',5500,'USA')"
  rollup_cube(3) = "(4,'Sara','Female',4000,'India')"
  rollup_cube(4) = "(5,'Todd','Male',3500,'India')"
  rollup_cube(5) = "(6,'Mary','Female',5000,'UK')"
  rollup_cube(6) = "(7,'Ben','Male',6500,'UK')"
  rollup_cube(7) = "(8,'Elizabeth','Female',7000,'USA')"
  rollup_cube(8) = "(9,'Tom','Male',5500,'UK')"
  rollup_cube(9) = "(10,'Rom','Male',5000,'USA')"
  val createColTypeTbl_rollup_cube_Spark : String = createTbl + columnTbl +
    "(id int,name string,gender string,salary int,country string)"
  val createRowTypeTbl_rollup_cube_Spark : String = createTbl + rowTbl +
    "(id int,name string,gender string,salary int,country string)"
  val createColumnTbl_rollup_cube : String = createTbl + columnTbl +
    "(id int,name string,gender string,salary int,country string) " + usingCol
  val createRowTbl_rollup_cube : String = createTbl + rowTbl +
    "(id int,name string,gender string,salary int,country string) " + usingRow
  val select_ColTbl_rollup : String = "SELECT country,gender,sum(salary) AS total FROM " +
    columnTbl + " GROUP BY ROLLUP(country,gender)"
  val select_RowTbl_rollup: String = "SELECT country,gender,sum(salary) AS total FROM " +
    rowTbl + " GROUP BY ROLLUP(country,gender)"
  val select_ColTbl_cube : String = "SELECT country,gender,sum(salary) AS total FROM " +
    columnTbl + " GROUP BY CUBE(country,gender)"
  val select_RowTbl_cube: String = "SELECT country,gender,sum(salary) AS total FROM " +
    rowTbl + " GROUP BY CUBE(country,gender)"
  /**
    *  window function.
    */
  val select_ColTbl_window : String = "SELECT name,gender,salary," +
    "AVG(salary) OVER (PARTITION BY gender ORDER BY salary " +
    "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS Average," +
    "COUNT(salary) OVER (PARTITION BY gender ORDER BY salary " +
    "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS TotalCount," +
    "SUM(salary) OVER (PARTITION BY gender ORDER BY salary " +
    "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS SalarySum" +
    " FROM " + columnTbl
  val select_RowTbl_window: String = "SELECT name,gender,salary," +
    "AVG(salary) OVER (PARTITION BY gender ORDER BY salary " +
    "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS Average," +
    "COUNT(salary) OVER (PARTITION BY gender ORDER BY salary " +
    "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS TotalCount," +
    "SUM(salary) OVER (PARTITION BY gender ORDER BY salary " +
    "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS SalarySum" +
    " FROM " + rowTbl
  val dropColTbl_rollup_cube : String = dropTbl  + columnTbl
  val dropRowTbl_rollup_cube : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  108. grouping, 109. grouping_id
    */
  val grouping_grouping_id = new Array[String](8)
  grouping_grouping_id(0) = "('Asia','India','Banglore',1000)"
  grouping_grouping_id(1) = "('Asia','India','Chennai',2000)"
  grouping_grouping_id(2) = "('Asia','Japan','Tokyo',4000)"
  grouping_grouping_id(3) = "('Asia','Japan','Hiroshima',5000)"
  grouping_grouping_id(4) = "('Europe','United Kingdom','London',1000)"
  grouping_grouping_id(5) = "('Europe','United Kingdom','Manchester',2000)"
  grouping_grouping_id(6) = "('Europe','France','Paris',4000)"
  grouping_grouping_id(7) = "('Europe','France','Cannes',5000)"
  val createColTypeTbl_grouping_grouping_id_Spark : String = createTbl + columnTbl +
    "(continent string,country string,city string,salesamount int)"
  val createRowTypeTbl_grouping_grouping_id_Spark : String = createTbl + rowTbl +
    "(continent string,country string,city string,salesamount int)"
  val createColumnTbl_grouping_grouping_id : String = createTbl + columnTbl +
    "(continent string,country string,city string,salesamount int) " + usingCol
  val createRowTbl_grouping_grouping_id : String = createTbl + rowTbl +
    "(continent string,country string,city string,salesamount int) " + usingRow
  val select_ColTbl_grouping_grouping_id : String = "SELECT continent,country,city," +
    "SUM(salesamount) as totalsales,GROUPING(continent) as GP_Continent," +
    "GROUPING(country) as GP_Country,GROUPING(city) as GP_City," +
    "GROUPING_ID(continent,country,city) as GID FROM " +  columnTbl +
    " GROUP BY ROLLUP(continent,country,city) ORDER BY GID"
  val select_RowTbl_grouping_grouping_id: String = "SELECT continent,country,city," +
    "SUM(salesamount) as totalsales,GROUPING(continent) as GP_Continent," +
    "GROUPING(country) as GP_Country,GROUPING(city) as GP_City," +
    "GROUPING_ID(continent,country,city) as GID FROM " + rowTbl +
    " GROUP BY ROLLUP(continent,country,city) ORDER BY GID"
  val dropColTbl_grouping_grouping_id : String = dropTbl  + columnTbl
  val dropRowTbl_grouping_grouping_id : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  110. approx_count_dist, 111. mean
    *   116. cume_dist, 117. percent_rank
    */
  val approxcntdist_mean = new Array[String](10)
  approxcntdist_mean(0) = "('James','Sales',3000)"
  approxcntdist_mean(1) = "('Michael','Sales',4600)"
  approxcntdist_mean(2) = "('Robert','Sales',4100)"
  approxcntdist_mean(3) = "('Maria','Finance',3000)"
  approxcntdist_mean(4) = "('James','Sales',3000)"
  approxcntdist_mean(5) = "('Scott','Finance',3300)"
  approxcntdist_mean(6) = "('Jen','Finance',3900)"
  approxcntdist_mean(7) = "('Jeff','Marketing',3000)"
  approxcntdist_mean(8) = "('Kumar','Marketing',2000)"
  approxcntdist_mean(9) = "('Saif','Sales',4100)"
  val createColTypeTbl_approxcntdist_mean_Spark : String = createTbl + columnTbl +
    "(empname string,department string,salary int)"
  val createRowTypeTbl_approxcntdist_mean_Spark : String = createTbl + rowTbl +
    "(empname string,department string,salary int)"
  val createColumnTbl_approxcntdist_mean : String = createTbl + columnTbl +
    "(empname string,department string,salary int) " + usingCol
  val createRowTbl_approxcntdist_mean : String = createTbl + rowTbl +
    "(empname string,department string,salary int) " + usingRow
  val select_ColTbl_approxcntdist_mean : String = "SELECT " +
    "approx_count_distinct(salary),mean(salary),approx_count_distinct(department) FROM " + columnTbl
  val select_RowTbl_approxcntdist_mean: String = "SELECT " +
    "approx_count_distinct(salary),mean(salary),approx_count_distinct(department) FROM " + rowTbl
  /**
    *   116. cume_dist, 117. percent_rank
    */
  val select_ColTbl_cumedist_prank : String = "SELECT *," +
    "cume_dist() OVER(PARTITION BY department ORDER BY salary) cume_dist," +
    "percent_rank() OVER(PARTITION BY department ORDER BY salary) percent_rank FROM " + columnTbl
  val select_RowTbl_cumedist_prank: String = "SELECT *," +
    "cume_dist() OVER(PARTITION BY department ORDER BY salary) cume_dist," +
    "percent_rank() OVER(PARTITION BY department ORDER BY salary) percent_rank FROM " + rowTbl
  val dropColTbl_approxcntdist_mean : String = dropTbl  + columnTbl
  val dropRowTbl_approxcntdist_mean : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  112. printf, 113. md5
    */
  val printf_md5 = new Array[String](3)
  printf_md5(0) = "(1,'TIBCO',' ComputeDB')"
  printf_md5(1) = "(2,'TIBCO',' Spotfire')"
  printf_md5(2) = "(3,'TIBCO',' Data Virtualization')"
  val createColTypeTbl_printf_md5_Spark : String = createTbl + columnTbl +
    "(id int,str1 string,str2 string)"
  val createRowTypeTbl_printf_md5_Spark : String = createTbl + rowTbl +
    "(id int,str1 string,str2 string)"
  val createColumnTbl_printf_md5 : String = createTbl + columnTbl +
    "(id int,str1 string,str2 string) " + usingCol
  val createRowTbl_printf_md5 : String = createTbl + rowTbl +
    "(id int,str1 string,str2 string) " + usingRow
  val select_ColTbl_printf_md5 : String = "SELECT " +
    "id,printf(CONCAT(str1,str2)) as product, md5(CONCAT(str1,str2)) FROM " + columnTbl +
    " ORDER BY id"
  val select_RowTbl_printf_md5: String = "SELECT " +
    "id,printf(CONCAT(str1,str2)) as product, md5(CONCAT(str1,str2)) FROM " + rowTbl +
  " ORDER BY id"
  val dropColTbl_printf_md5 : String = dropTbl  + columnTbl
  val dropRowTbl_printf_md5 : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  114.  assert_true
    */
  val assert_true = new Array[String](3)
  assert_true(0) = "(9,6)"
  assert_true(1) = "(8,8)"
  assert_true(2) = "(5,10)"
  val createColTypeTbl_assert_true_Spark : String = createTbl + columnTbl + "(i1 int,i2 int)"
  val createRowTypeTbl_assert_true_Spark : String = createTbl + rowTbl + "(i1 int,i2 int)"
  val createColumnTbl_assert_true : String = createTbl + columnTbl +
    "(i1 int,i2 int) " + usingCol
  val createRowTbl_assert_true : String = createTbl + rowTbl +
    "(i1 int,i2 int) " + usingRow
  val select_ColTbl_assert_true : String = "SELECT " + "assert_true(i1 > i2) FROM " + columnTbl
  val select_RowTbl_assert_true: String = "SELECT " + "assert_true(i1 > i2) FROM " + rowTbl
  val dropColTbl_assert_true : String = dropTbl  + columnTbl
  val dropRowTbl_assert_true : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  115.  input_file_name
    *  Before merge this to master, change the path to colo machines rather than
    *  local machine.
    */
  val externalTbl : String = "CREATE EXTERNAL TABLE staging_regions USING CSV" +
    " OPTIONS(PATH 'file:////home/cbhatt/NW_1GB/NW_1GB/regions.csv',header 'true')"
  val manageTblCol : String = "CREATE TABLE snappy_regions_col" +  " USING COLUMN" +
    " OPTIONS(BUCKETS '10') AS SELECT * FROM staging_regions"
  val manageTblRow : String = "CREATE TABLE snappy_regions_row" + " USING ROW" +
    " AS SELECT * FROM staging_regions"
  val input_file_name_colTbl : String = "SELECT regionID,regiondescription," +
    "input_file_name() AS filename FROM snappy_regions_col"
  val input_file_name_RowTbl : String = "SELECT regionID,regiondescription," +
    "input_file_name() AS filename FROM snappy_regions_row"
  val input_file_name_externaTbl : String = "SELECT regionID,regiondescription," +
    "input_file_name() AS filename FROM staging_regions"
  /**
    *  Below queries test the functions :
    *  118.  regexp_extract, 119. regexp_replace
    */
  val regexp_extract_replace = new Array[String](3)
  regexp_extract_replace(0) = "(100,'unix','unix@gmail.co.tt')"
  regexp_extract_replace(1) = "(200,'windows','windows@gmail.co.tt')"
  regexp_extract_replace(2) = "(300,'macos','macos@gmail.co.tt')"
  val createColTypeTbl_regexp_extract_replace_Spark : String = createTbl + columnTbl +
    "(empid int,empname string,email string)"
  val createRowTypeTbl_regexp_extract_replace_Spark : String = createTbl + rowTbl +
    "(empid int,empname string,email string)"
  val createColumnTbl_regexp_extract_replace : String = createTbl + columnTbl +
    "(empid int,empname string,email string) " + usingCol
  val createRowTbl_regexp_extract_replace : String = createTbl + rowTbl +
    "(empid int,empname string,email string) " + usingRow
  val select_ColTbl_regexp_extract_replace : String = "SELECT empid,empname," +
    "regexp_extract(email,'@(\\\\w+)') AS domain," +
    "regexp_replace(email,'@(\\\\w+)','@tibco') AS change FROM " + columnTbl
  val select_RowTbl_regexp_extract_replace: String = "SELECT empid,empname," +
    "regexp_extract(email,'@(\\\\w+)') AS domain," +
    "regexp_replace(email,'@(\\\\w+)','@tibco') AS change FROM " + rowTbl
  val dropColTbl_regexp_extract_replace : String = dropTbl  + columnTbl
  val dropRowTbl_regexp_extract_replace : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  121.  json_tuple, 122.  crc32
    */
  val json_tuple_crc32 = new Array[String](3)
  val jsonStr1 = """'{"Name":"TIBCO","Location":20,"Revenue":30.45}'"""
  val jsonStr2 = """'{"Name":"Snappy","Location":2,"Revenue":5.4}'"""
  val jsonStr3 = """'{"Name":"Amazon","Location":32,"Revenue":100.23}'"""
  json_tuple_crc32(0) = "(1,'ComputeDB',".concat(jsonStr1).concat(")")
  json_tuple_crc32(1) = "(2,'SpotFire',".concat(jsonStr2).concat(")")
  json_tuple_crc32(2) = "(3,'AWS',".concat(jsonStr3).concat(")")
  val createColTypeTbl_json_tuple_crc32_Spark : String = createTbl + columnTbl +
    "(id int,checksum string,jsonstr string)"
  val createRowTypeTbl_json_tuple_crc32_Spark : String = createTbl + rowTbl +
    "(id int,checksum string,jsonstr string)"
  val createColumnTbl_json_tuple_crc32 : String = createTbl + columnTbl +
    "(id int,checksum string,jsonstr string) " + usingCol
  val createRowTbl_json_tuple_crc32 : String = createTbl + rowTbl +
    "(id int,checksum string,jsonstr string) " + usingRow
  val select_ColTbl_json_tuple_crc32 : String = "SELECT id," +
    "json_tuple(jsonstr,'Name','Location','Revenue'), crc32(checksum) FROM " + columnTbl +
    " ORDER BY ID"
  val select_RowTbl_json_tuple_crc32: String = "SELECT id," +
    "json_tuple(jsonstr,'Name','Location','Revenue'),crc32(checksum) FROM " + rowTbl +
  " ORDER BY ID"
  val dropColTbl_json_tuple_crc32 : String = dropTbl  + columnTbl
  val dropRowTbl_json_tuple_crc32 : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  123.  like, 124.  rlike
    */
  val like_rlike = new Array[String](15)
  like_rlike(0) = "('Jawaharlal Nehru','Uttar Pradesh','Indian National Congress')"
  like_rlike(1) = "('Gulzarilal Nanda','Gujarat','Indian National Congress')"
  like_rlike(2) = "('Lal Bahadur Shastri','Uttar Pradesh','Indian National Congress')"
  like_rlike(3) = "('Indira Gandhi','Uttar Pradesh','Indian National Congress')"
  like_rlike(4) = "('Morarji Desai','Gujarat','Janata Party')"
  like_rlike(5) = "('Charan Singh','Uttar Pradesh','Janata Party Secular')"
  like_rlike(6) = "('Rajiv Gandhi','Uttar Pradesh','Indian National Congress')"
  like_rlike(7) = "('Vishwanath Pratap Singh','Uttar Pradesh','Janata Dal')"
  like_rlike(8) = "('Chandra Shekhar','Uttar Pradesh','Samajwadi Janata Party')"
  like_rlike(9) = "('P. V. Narasimha Rao','Andhra Pradesh','Indian National Congress(I)')"
  like_rlike(10) = "('Atal Bihari Vajpayee','Uttar Pradesh','Bharatiya Janata Party')"
  like_rlike(11) = "('H. D. Deve Gowda','Karnataka','Janata Dal(U)')"
  like_rlike(12) = "('Inder Kumar Gujral','Bihar','Janata Dal(U)')"
  like_rlike(13) = "('Manmohan Singh','Assam','Indian National Congress(I)')"
  like_rlike(14) = "('Narendra Modi','Uttar Pradesh','Bharatiya Janata Party')"
  val createColTypeTbl_like_rlike_Spark : String = createTbl + columnTbl +
    "(name string,state string,party string)"
  val createRowTypeTbl_like_rlike_Spark : String = createTbl + rowTbl +
    "(name string,state string,party string)"
  val createColumnTbl_like_rlike : String = createTbl + columnTbl +
    "(name string,state string,party string) " + usingCol
  val createRowTbl_like_rlike : String = createTbl + rowTbl +
    "(name string,state string,party string) " + usingRow
  val select_ColTbl_like : String = "SELECT name,state,party FROM " + columnTbl +
  " WHERE state LIKE 'Ut%' AND party LIKE 'Bhar%'"
  val select_RowTbl_like: String = "SELECT name,state,party FROM " + rowTbl +
    " WHERE state LIKE 'Ut%' AND party LIKE 'Bhar%'"
  val select_ColTbl_rlike : String = "SELECT name,state,party FROM " + columnTbl +
    " WHERE state RLIKE '[UG]'"
  val select_RowTbl_rlike: String = "SELECT name,state,party FROM " + rowTbl +
    " WHERE state RLIKE '[UG]'"
  val dropColTbl_like_rlike : String = dropTbl  + columnTbl
  val dropRowTbl_like_rlike : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  126. variance, 127. var_samp, 128. var_pop,
    *  129. stddev, 130. stddev_samp, 131. stddev_pop, 132. std,
    *  135. skewness, 136. kurtosis
    */
  val variance = new Array[String](20)
  variance(0) = "(1,'Andhra Pradesh',2,0)"
  variance(1) = "(2,'Delhi',16,1)"
  variance(2) = "(3,'Haryana',3,14)"
  variance(3) = "(4,'Karnataka',15,0)"
  variance(4) = "(5,'Kerala',26,2)"
  variance(5) = "(6,'Maharashtra',44,3)"
  variance(6) = "(7,'Odisha',1,0)"
  variance(7) = "(8,'Punjab',2,0)"
  variance(8) = "(9,'Rajshthan',5,2)"
  variance(9) = "(10,'Tamilnadu',3,0)"
  variance(10) = "(11,'Telengana',7,9)"
  variance(11) = "(12,'Jammu and Kashmir',4,0)"
  variance(12) = "(13,'Ladakh',10,0)"
  variance(13) = "(14,'Uttar Pradesh',18,1)"
  variance(14) = "(15,'Uttarakhand',1,0)"
  variance(15) = "(16,'West Bengal',1,0)"
  variance(16) = "(17,'Chhattisgarh',1,0)"
  variance(17) = "(18,'Gujarat',2,0)"
  variance(18) = "(19,'Puducherry',1,0)"
  variance(19) = "(20,'Chandigarh',1,0)"
  val createColTypeTbl_variance_Spark : String = createTbl + columnTbl +
    "(id int,state string,indian_nationals_corona int,foreign_national_corana int)"
  val createRowTypeTbl_variance_Spark : String = createTbl + rowTbl +
    "(id int,state string,indian_nationals_corona int,foreign_national_corana int)"
  val createColumnTbl_variance : String = createTbl + columnTbl +
    "(id int,state string,indian_nationals_corona int,foreign_national_corana int) " + usingCol
  val createRowTbl_variance : String = createTbl + rowTbl +
    "(id int,state string,indian_nationals_corona int,foreign_national_corana int) " + usingRow
  val select_ColTbl_variance : String = "SELECT SUM(indian_nationals_corona)," +
    "VAR_POP(indian_nationals_corona),VARIANCE(indian_nationals_corona)," +
    "VAR_SAMP(indian_nationals_corona),STDDEV_POP(indian_nationals_corona)," +
    "STDDEV(indian_nationals_corona),STDDEV_SAMP(indian_nationals_corona)," +
    "STD(indian_nationals_corona),SKEWNESS(indian_nationals_corona)," +
    "KURTOSIS(indian_nationals_corona) FROM " + columnTbl
  val select_RowTbl_variance: String = "SELECT SUM(indian_nationals_corona)," +
    "VAR_POP(indian_nationals_corona),VARIANCE(indian_nationals_corona)," +
    "VAR_SAMP(indian_nationals_corona),STDDEV_POP(indian_nationals_corona)," +
    "STDDEV(indian_nationals_corona),STDDEV_SAMP(indian_nationals_corona)," +
    "STD(indian_nationals_corona),SKEWNESS(indian_nationals_corona)," +
    "KURTOSIS(indian_nationals_corona) FROM " + rowTbl
  val dropColTbl_variance : String = dropTbl  + columnTbl
  val dropRowTbl_variance : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  133. named_struct
    */
  val named_struct = new Array[String](3)
  named_struct(0) = "(1,'TIBCO',43.654,true)"
  named_struct(1) = "(2,'Snappy',12.3,true)"
  named_struct(2) = "(3,'ABCD',NULL,false)"
  val createColTypeTbl_named_struct_Spark : String = createTbl + columnTbl +
    "(id int,v1 string,v2 double,v3 boolean)"
  val createRowTypeTbl_named_struct_Spark : String = createTbl + rowTbl +
    "(id int,v1 string,v2 double,v3 boolean)"
  val createColumnTbl_named_struct : String = createTbl + columnTbl +
    "(id int,v1 string,v2 double,v3 boolean) " + usingCol
  val createRowTbl_named_struct : String = createTbl + rowTbl +
    "(id int,v1 string,v2 double,v3 boolean) " + usingRow
  val select_ColTbl_named_struct : String = "SELECT id," +
    "named_struct('Name',v1,'Revenue',v2,'Equal Employer',v3) FROM " + columnTbl + " ORDER BY ID"
  val select_RowTbl_named_struct: String = "SELECT id," +
    "named_struct('Name',v1,'Revenue',v2,'Equal Employer',v3) FROM " + rowTbl + " ORDER BY ID"
  val dropColTbl_named_struct : String = dropTbl  + columnTbl
  val dropRowTbl_named_struct : String = dropTbl + rowTbl
  /**
    *  Below queries test the functions :
    *  134. dsid() - Returns the unique distributed member ID of executor fetching current row.
    */
  val dsid = new Array[String](4)
  dsid(0) = "(1,'ComputeDB')"
  dsid(1) = "(2,'Spotfire')"
  dsid(2) = "(3,'Data Virutualization')"
  dsid(3) = "(4,'Data Science')"
  val createColumnTbl_dsid : String = createTbl + columnTbl + "(id int,name string) " + usingCol
  val createRowTbl_dsid : String = createTbl + rowTbl + "(id int,name string) " + usingRow
  val select_ColTbl_dsid : String = "SELECT id,name,dsid() FROM " + columnTbl
  val select_RowTbl_dsid: String = "SELECT id,name,dsid() FROM " + rowTbl
  val dropColTbl_dsid : String = dropTbl  + columnTbl
  val dropRowTbl_dsid : String = dropTbl + rowTbl

}
