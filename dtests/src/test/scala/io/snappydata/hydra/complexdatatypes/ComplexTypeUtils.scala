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

}
