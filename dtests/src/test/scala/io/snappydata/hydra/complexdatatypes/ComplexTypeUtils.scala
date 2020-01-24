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
package io.snappydata.hydra.complexdatatypes

import scala.util.Random

object ComplexTypeUtils {
  // scalastyle:off println

  val genrateDoubleNum = () => {Random.nextDouble * 100}

  val generateString = () => {
    val chars = ('a' to 'z') ++ ('A' to 'Z')
    val sb = new StringBuilder
    for(i <- 1 to Random.nextInt(15)) {
      val randomchar = Random.nextInt(chars.length)
      sb.append(chars(randomchar))
    }
    sb.toString
  } : String

  /* -----                     Array Type                   ----- */
  /* -----         Snappy Array Type Queries         ----- */
  val Array_Q1 : String = "SELECT * FROM Student ORDER BY id"
  val Array_Q2 : String = "SELECT id, marks[0] AS Maths, marks[1] AS Science," +
    "marks[2] AS English,marks[3] AS Computer, marks[4] AS Music, marks[5] " +
    "FROM Student WHERE id BETWEEN 100 AND 1000"
  val Array_Q3 : String = "SELECT id, name, explode(marks) as Marks FROM Student"
  val Array_View : String = "CREATE VIEW StudentMark AS " +
    "SELECT id,name,explode(marks) AS Marks FROM Student"
  val Array_Q4 : String = "SELECT name,SUM(Marks) AS Total FROM StudentMark " +
    "GROUP BY name ORDER BY Total DESC"
  val Array_Q5 : String =
    "SELECT name,MAX(Marks),MIN(Marks) FROM StudentMark GROUP BY name"

  /* -----   Array Type NULL Value Queries   ----- */
    val createSchemaST = "CREATE SCHEMA ST"
    val dropDatabaseST = "DROP DATABASE ST"
    val createTableLastColumnArrayType = "CREATE TABLE IF NOT EXISTS ST.StudentLast" +
      "(rollno int,name String, adminDate Array<Date>) USING COLUMN"
    val createTableMiddleColumnArrayType = "CREATE TABLE IF NOT EXISTS ST.StudentMiddle" +
      "(rollno int,adminDate Array<Date>,time TimeStamp, class int) USING COLUMN"
    val createTableFirstColumnArrayType = "CREATE TABLE IF NOT EXISTS ST.StudentFirst" +
      "(Total Array<Double>,name String, rollno int) USING COLUMN"
    val createTableInSparkArrTypeLastColumn = "CREATE TABLE IF NOT EXISTS ST.StudentLast" +
      "(rollno int,name String, adminDate Array<Date>)"
      val createTableInSparkArrayTypeMiddleColumn = "CREATE TABLE IF NOT EXISTS ST.StudentMiddle" +
      "(rollno int,adminDate Array<Date>,time TimeStamp, class int)"
    val createTableInSparkArrayTypeFirstColumn = "CREATE TABLE IF NOT EXISTS ST.StudentFirst" +
      "(Total Array<Double>,name String, rollno int)"

    val insertNullInLastColumn = "INSERT INTO ST.StudentLast SELECT 1, 'ABC', null"
    val insertNullInMiddleColumn = "INSERT INTO ST.StudentMiddle SELECT 1,null,null,6"
    val insertNullInFirstColumn = "INSERT INTO ST.StudentFirst SELECT null,'BBB',20"
    val insertNormalDataLastColumn = "INSERT INTO ST.StudentLast SELECT 2,'XYZ',Array('2020-01-21')"
    val insertNormalDataMiddleColumn = "INSERT INTO ST.StudentMiddle SELECT " +
      "1,Array('2020-01-21'), '2020-01-22 12:16:52.598', 5"
    val insertNormalDataFirstColumn = "INSERT INTO ST.StudentFirst SELECT Array(25.6),'AAA',10"

    val selectLastColumn = "SELECT * FROM ST.StudentLast"
    val selectMiddleColumn = "SELECT * FROM ST.StudentMiddle"
    val selectFirstColumn = "SELECT * FROM ST.StudentFirst"

    val dropTableStudentLast = "DROP TABLE ST.StudentLast"
    val dropTableStudentMiddle = "DROP TABLE ST.StudentMiddle"
    val dropTableStudentFirst = "DROP TABLE ST.StudentFirst"

  /* -----                     Map Type                   ----- */
  /* -----         Snappy Map Type Queries         ----- */
  val Map_Q1 : String = "SELECT * FROM StudentMarksRecord ORDER BY id"

  val Map_Q2 : String = "SELECT id, name, " +
    "Maths['maths'],Science['science'] AS SCI ,English['english'] AS ENG," +
    "Computer['computer'],Music['music'],History['history'] " +
    "FROM StudentMarksRecord " +
    "WHERE name = 'JxVJBxYlNT'"

  val Map_Q3 : String = "SELECT name, " +
    "SUM(Maths['maths'] + Science['science'] + English['english'] + " +
    "Computer['computer'] + Music['music'] + History['history']) AS Total " +
    "FROM StudentMarksRecord " +
    "GROUP BY name ORDER BY Total DESC"

  val Map_Q4 : String = "SELECT name, " +
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
    "FROM StudentMarksRecord " +
    "GROUP BY name ORDER BY Total DESC"

  val Map_Q5 : String = "SELECT name, " +
    "(SUM(Maths['maths'] + Science['science'] + English['english'] + " +
    "Computer['computer'] + Music['music'] + " +
    "History['history'])*100.0/600.0) AS Percentage " +
    "FROM StudentMarksRecord " +
    "GROUP BY name ORDER BY Percentage DESC"

  val Map_Q6 : String = "SELECT name, MAX(marks) AS Max, MIN(marks) AS Min FROM " +
    "(SELECT name, Maths['maths'] AS marks FROM StudentMarksRecord " +
    "UNION ALL " +
    "SELECT name, Science['science'] AS marks FROM StudentMarksRecord " +
    "UNION ALL " +
    "SELECT name, English['english'] AS marks FROM StudentMarksRecord " +
    "UNION ALL " +
    "SELECT name, Computer['computer'] AS marks FROM StudentMarksRecord " +
    "UNION ALL " +
    "SELECT name,Music['music'] AS marks FROM StudentMarksRecord " +
    "UNION ALL " +
    "SELECT name,History['history'] AS marks FROM StudentMarksRecord) " +
    "GROUP BY name"

  /* -----   Map Type NULL Value Queries   ----- */
  val createTableLastColumnMapType = "CREATE TABLE IF NOT EXISTS st.MapTypeLast" +
    "(rollno int,name String, test Map<String,String>) USING COLUMN"
  val createTableMiddleColumnMapType = "CREATE TABLE IF NOT EXISTS st.MapTypeMiddle" +
    "(rollno int, test Map<Int,Double>, name String, class int) USING COLUMN"
  val createTableFirstColumnMapType = "CREATE TABLE IF NOT EXISTS st.MapTypeFirst" +
    "(Record Map<String,String>, avg double, match int) USING COLUMN"
  val createTableInSparkMapTypeLastColumn = "CREATE TABLE IF NOT EXISTS st.MapTypeLast" +
    "(rollno int,name String, test Map<String,String>)"
  val createTableInSparkMapTypeMiddleColumn = "CREATE TABLE IF NOT EXISTS st.MapTypeMiddle" +
    "(rollno int, test Map<Int,Double>, name String, class int)"
  val createTableInSparkMapTypeFirstColumn = "CREATE TABLE IF NOT EXISTS st.MapTypeFirst" +
    "(Record Map<String,String>, avg double, match int)"

  val insertNULLMapTypeLast = "INSERT INTO ST.MapTypeLast SELECT 1, 'XXD', null"
  val insertNULLMapTypeMiddle = "INSERT INTO ST.MapTypeMiddle SELECT 1, null, 'ABB', 5"
  val insertNULLMapTypeFirst = "INSERT INTO ST.MapTypeFirst SELECT null, 62.7d, 112"
  val insertNormalDataMapTypeLast = "INSERT INTO ST.MapTypeLast SELECT 2, 'MNO', MAP('HJ','KL')"
  val insertNormalDataMapTypeMiddle = "INSERT INTO ST.MapTypeMiddle " +
    "SELECT 2, MAP(10,55.55d), 'TTT', 9"
  val insertNormalDataMapTypeFirst = "INSERT INTO ST.MapTypeFirst " +
    "SELECT MAP('Sachin', 'RightHand'), 54.6d, 400"

  val selectMapLast = "SELECT * FROM ST.MapTypeLast"
  val selectMapMiddle = "SELECT * FROM ST.MapTypeMiddle"
  val selectMapFirst = "SELECT * FROM ST.MapTypeFirst"

  val dropTableMapLast = "DROP TABLE ST.MapTypeLast"
  val dropTableMapMiddle = "DROP TABLE ST.MapTypeMiddle"
  val dropTableMapFirst = "DROP TABLE ST.MapTypeFirst"

  /* -----                     Struct Type                   ----- */
  /* -----         Snappy Struct Type Queries         ----- */
  val Struct_Q1 : String = "SELECT name, TestRecord.Runs, TestRecord.Avg FROM CricketRecord " +
    "ORDER BY TestRecord.Runs DESC"
  val Struct_Q2 : String = "SELECT SUM(TestRecord.Runs) AS TotalRuns FROM CricketRecord"
  val Struct_Q3 : String = "SELECT name FROM CricketRecord WHERE TestRecord.batStyle = 'LeftHand'"
  val Struct_Q4 : String = "SELECT name, TestRecord.batStyle,TestRecord.Matches," +
    "TestRecord.Runs,TestRecord.Avg " +
    "FROM CricketRecord " +
    "ORDER BY TestRecord.Matches DESC"

  /* -----   Struct Type NULL Value Queries   ----- */
  val createTableLastColumnStructType = "CREATE TABLE IF NOT EXISTS ST.CricketRecordLast(name String,allRounder boolean," +
    "TestRecord STRUCT<batStyle:String,Matches:Long,Runs:Int,Avg:Double>) USING COLUMN"
  val createTableMiddleColumnStructType = "CREATE TABLE IF NOT EXISTS " +
    "ST.CricketRecordMiddle(name String," +
    "TestRecord STRUCT<batStyle:String,Matches:Long,Runs:Int,Avg:Double>," +
    "allRounder boolean) USING COLUMN"
  val createTableFirstColumnStructType = "CREATE TABLE IF NOT EXISTS " +
    "ST.CricketRecordFirst(TestRecord STRUCT<batStyle:String,Matches:Long,Runs:Int,Avg:Double>," +
    "name String,allRounder boolean) USING COLUMN"
  val createTableInSparkStructTypeLastColumn = "CREATE TABLE IF NOT EXISTS " +
    "st.CricketRecordLast(name String,allRounder boolean," +
  "TestRecord STRUCT<batStyle:String,Matches:Long,Runs:Int,Avg:Double>)"
  val createTableInSparkStructTypeMiddleColumn = "CREATE TABLE IF NOT EXISTS " +
    "ST.CricketRecordMiddle(name String," +
    "TestRecord STRUCT<batStyle:String,Matches:Long,Runs:Int,Avg:Double>," +
    "allRounder boolean)"
  val createTableInSparkStructTypeFirstColumn = "CREATE TABLE IF NOT EXISTS " +
    "ST.CricketRecordFirst(TestRecord STRUCT<batStyle:String,Matches:Long,Runs:Int,Avg:Double>," +
    "name String,allRounder boolean)"

  val insertNULLStructTypeLast = "INSERT INTO ST.CricketRecordLast SELECT " +
    "'Sachin Tendulkar', true, null"
  val insertNULLStructTypeMiddle = "INSERT INTO ST.CricketRecordMiddle " +
    "SELECT 'Rahul Drvaid',null,false"
  val insertNULLStructTypeFirst = "INSERT INTO ST.CricketRecordFirst SELECT null, 'Kapil Dev', true"
  val insertNormalDataStructTypeLast = "INSERT INTO ST.CricketRecordLast SELECT " +
  "'Sachin Tendulkar', true, STRUCT('Right Hand',200,15921,53.79)"
  val insertNormalDataStructTypeMiddle = "INSERT INTO ST.CricketRecordMiddle SELECT " +
    "'Rahul Drvaid',STRUCT('Right Hand',164,13288,52.31),false"
  val insertNormalDataStructTypeFirst = "INSERT INTO ST.CricketRecordFirst " +
    "SELECT STRUCT('Right Hand',131,5248,31.05), 'Kapil Dev', true"

  val selectStructLast = "SELECT * FROM ST.CricketRecordLast"
  val selectStructMiddle = "SELECT * FROM ST.CricketRecordMiddle"
  val selectStructFirst = "SELECT * FROM ST.CricketRecordFirst"

  val dropTableStructLast = "DROP TABLE ST.CricketRecordLast"
  val dropTableStructMiddle = "DROP TABLE ST.CricketRecordMiddle"
  val dropTableStructFirst = "DROP TABLE ST.CricketRecordFirst"


  /* -----                     ArrayOfStruct Type                   ----- */
  /* -----         Snappy ArrayOfStruct Type Queries         ----- */
  val ArraysOfStruct_Q1 : String = "SELECT * FROM TwoWheeler"
  val ArraysOfStruct_Q2 : String = "SELECT brand FROM TwoWheeler " +
    "WHERE BikeInfo[0].type = 'Scooter'"
  val ArraysOfStruct_Q3 : String = "SELECT brand,BikeInfo[0].cc FROM TwoWheeler " +
    "WHERE BikeInfo[0].cc >= 149.0 ORDER BY BikeInfo[0].cc DESC"
  val ArraysOfStruct_Q4 : String = "SELECT brand,COUNT(BikeInfo[0].type) FROM TwoWheeler " +
    "WHERE BikeInfo[0].type = 'Cruiser' GROUP BY brand"
  val ArraysOfStruct_Q5 : String = "SELECT brand, BikeInfo[0].type AS Style, " +
    "BikeInfo[0].instock AS Available " +
    "FROM TwoWheeler"

  /* -----                     AllMixed Type                   ----- */
  /* -----         Snappy AllMixed Type Queries         ----- */
  val Mixed_Q1 : String = "SELECT * FROM TwentyTwenty ORDER BY name"
  val Mixed_Q2 : String = "SELECT name, " +
    "SUM(LastThreeMatchPerformance[0] + LastThreeMatchPerformance[1] + " +
    "LastThreeMatchPerformance[2]) AS RunsScored " +
    "FROM TwentyTwenty GROUP BY name"
  val Mixed_Q3 : String = "SELECT name, LastThreeMatchPerformance[2] AS RunsScoredinLastMatch, " +
    "Profile.Matches,Profile.SR,Profile.Runs " +
    "FROM TwentyTwenty WHERE Profile.Runs >= 1000 ORDER BY Profile.Runs DESC"
  val Mixed_Q4 : String = "SELECT COUNT(*) AS AllRounder FROM " +
    "TwentyTwenty WHERE Roll['2'] = 'AllRounder'"
  val Mixed_Q5 : String = "SELECT name, Profile.SR,Profile.Runs " +
    "FROM TwentyTwenty ORDER BY Profile.SR DESC"

  /* -----                     ArraysOfStringInMapAsValue                   ----- */
  /* -----         Snappy ArraysOfStringInMapAsValue Type Queries         ----- */
  val Array_Map_TempView : String = "CREATE TEMPORARY VIEW FamousPeopleView AS " +
  "SELECT country, explode(celebrities) FROM FamousPeople"
  val Array_Map_Q1 : String = "SELECT * FROM FamousPeopleView"
  val Array_Map_Q2 : String = "SELECT country, value[0],value[1],value[2],value[3],value[4]," +
    " value[5],value[6],value[7],value[8],value[9],value[10],value[11]," +
    "value[12],value[13],value[14],value[15] "  +
    "FROM FamousPeopleView WHERE key = 'Prime Ministers'"
  val Array_Map_Q3 : String = "SELECT country, value FROM FamousPeopleView WHERE key = 'Authors'"
}
