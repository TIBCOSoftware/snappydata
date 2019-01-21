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


  /* -----                     ArrayOfStruct Type                   ----- */
  /* -----         Snappy ArrayOfStruct Type Queries         ----- */
  val ArraysOfStruct_Q1 = "SELECT * FROM TwoWheeler"
  val ArraysOfStruct_Q2 = "SELECT brand FROM TwoWheeler " +
    "WHERE BikeInfo[0].type = 'Scooter'"
  val ArraysOfStruct_Q3 = "SELECT brand,BikeInfo[0].cc FROM TwoWheeler " +
    "WHERE BikeInfo[0].cc >= 149.0 ORDER BY BikeInfo[0].cc DESC"
  val ArraysOfStruct_Q4 = "SELECT brand,COUNT(BikeInfo[0].type) FROM TwoWheeler " +
    "WHERE BikeInfo[0].type = 'Cruiser' GROUP BY brand"
  val ArraysOfStruct_Q5 = "SELECT brand, BikeInfo[0].type AS Style, " +
    "BikeInfo[0].instock AS Available " +
    "FROM TwoWheeler"
}
