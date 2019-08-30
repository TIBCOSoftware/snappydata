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
package io.snappydata.benchmark.snappy.tpch

import java.io.{File, FileOutputStream, PrintStream}
import java.sql.{DriverManager, PreparedStatement}

import io.snappydata.benchmark.TPCH_Queries

object QueryExecutionJdbc {

  def main(args: Array[String]) {

    val avgFileStream: FileOutputStream = new FileOutputStream(new File(s"Snappy_Average.out"))
    val avgPrintStream: PrintStream = new PrintStream(avgFileStream)

    val host = args(0)
    val port = args(1)
    val dbName = "TPCH"

    val dbAddress = "jdbc:snappydata://" + host + ":" + port + "/"
    val conn = DriverManager.getConnection(dbAddress)

    val queries: Array[String] = args(2).split(",")
    // scalastyle:off println
    println(queries.length)
    var isResultCollection: Boolean = args(3).toBoolean
    var warmup: Integer = args(4).toInt
    var runsForAverage: Integer = args(5).toInt
    var isDynamic: Boolean = args(6).toBoolean
    var traceEvents: Boolean = args(7).toBoolean
    val randomSeed: Integer = args(8).toInt

    TPCH_Queries.setRandomSeed(randomSeed)

    for (query <- queries) {
      var prepStatement: PreparedStatement = null
      query match {
        case "1" => {
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery1)
          var parameters = TPCH_Queries.getQ1Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
        }
        case "2" => {
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery2)
          var parameters = TPCH_Queries.getQ2Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setString(2, parameters(1))
          prepStatement.setString(3, parameters(2))
          prepStatement.setString(4, parameters(3))
        }
        case "3" => {
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery3)
          var parameters = TPCH_Queries.getQ3Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setString(2, parameters(1))
          prepStatement.setString(3, parameters(2))
        }
        case "4" => {
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery4)
          var parameters = TPCH_Queries.getQ4Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setString(2, parameters(1))
        }
        case "5" => {
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery5)
          var parameters = TPCH_Queries.getQ5Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setString(2, parameters(1))
          prepStatement.setString(3, parameters(2))
        }
        case "6" => {
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery6)
          var parameters = TPCH_Queries.getQ6Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setString(2, parameters(1))
          prepStatement.setString(3, parameters(2))
          prepStatement.setString(4, parameters(3))
          prepStatement.setString(5, parameters(4))
        }
        case "7" => {
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery7)
          var parameters = TPCH_Queries.getQ7Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setString(2, parameters(1))
          prepStatement.setString(3, parameters(2))
          prepStatement.setString(4, parameters(3))
        }
        case "8" => {
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery8)
          var parameters = TPCH_Queries.getQ8Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setString(2, parameters(1))
          prepStatement.setString(3, parameters(2))
        }
        case "9" => {
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery9)
          var parameters = TPCH_Queries.getQ9Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
        }
        case "10" => {
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery10)
          var parameters = TPCH_Queries.getQ10Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setString(2, parameters(1))
        }
        case "11" => {
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery11)
          var parameters = TPCH_Queries.getQ11Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setString(2, parameters(1))
        }
        case "12" => {
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery12)
          var parameters = TPCH_Queries.getQ12Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setString(2, parameters(1))
          prepStatement.setString(3, parameters(2))
          prepStatement.setString(4, parameters(3))
        }
        case "13" => {
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery13)
          var parameters = TPCH_Queries.getQ13Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setString(2, parameters(1))
        }
        case "14" => {
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery14)
          var parameters = TPCH_Queries.getQ14Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setString(2, parameters(1))
        }
        case "15" => {

        }
        case "16" => {
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery16)
          var parameters = TPCH_Queries.getQ16Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setString(2, parameters(1))
          prepStatement.setString(3, parameters(2))
          prepStatement.setString(4, parameters(3))
          prepStatement.setString(5, parameters(4))
          prepStatement.setString(6, parameters(5))
          prepStatement.setString(7, parameters(6))
          prepStatement.setString(8, parameters(7))
          prepStatement.setString(9, parameters(8))
          prepStatement.setString(10, parameters(9))
          prepStatement.setString(11, parameters(10))

        }
        case "17" => {
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery17)
          var parameters = TPCH_Queries.getQ17Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setString(2, parameters(1))
          prepStatement.setString(3, parameters(2))
        }
        case "18" => {
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery18)
          var parameters = TPCH_Queries.getQ18Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
        }
        case "19" => {
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery19)
          var parameters = TPCH_Queries.getQ19Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setString(2, parameters(1))
          prepStatement.setString(3, parameters(2))
          prepStatement.setString(4, parameters(3))
          prepStatement.setString(5, parameters(4))
          prepStatement.setString(6, parameters(5))
          prepStatement.setString(7, parameters(6))
          prepStatement.setString(8, parameters(7))
          prepStatement.setString(9, parameters(8))
        }
        case "20" => {
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery20)
          var parameters = TPCH_Queries.getQ20Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setString(2, parameters(1))
          prepStatement.setString(3, parameters(2))
          prepStatement.setString(4, parameters(3))
        }
        case "21" => {
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery21)
          var parameters = TPCH_Queries.getQ21Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
        }
        case "22" => {
          prepStatement = conn.prepareStatement(TPCH_Queries.getQuery22)
          var parameters = TPCH_Queries.getQ22Parameter(isDynamic)
          prepStatement.setString(1, parameters(0))
          prepStatement.setString(2, parameters(1))
          prepStatement.setString(3, parameters(2))
          prepStatement.setString(4, parameters(3))
          prepStatement.setString(5, parameters(4))
          prepStatement.setString(6, parameters(5))
          prepStatement.setString(7, parameters(6))
          prepStatement.setString(8, parameters(7))
          prepStatement.setString(9, parameters(8))
          prepStatement.setString(10, parameters(9))
          prepStatement.setString(11, parameters(10))
          prepStatement.setString(12, parameters(11))
          prepStatement.setString(13, parameters(12))
          prepStatement.setString(14, parameters(13))
        }
      }
      QueryExecutor.execute_statement(query, isResultCollection, prepStatement, warmup,
        runsForAverage, avgPrintStream)
      prepStatement.close()
    }

    /* //code for SNAP- 1296
         println("----------------------------------Use of Statement-------------------------------")
         val stmt = conn.createStatement()
         var rs = stmt.executeQuery(TPCH_Snappy.getQuery10)
         var rsmd = rs.getMetaData()
         println(s"KBKB : rsmd : $rsmd")
         var columnsNumber = rsmd.getColumnCount();
         println(s"KBKB : columnsNumber : $columnsNumber")
         var count : Int = 0
         while (rs.next()) {
           count += 1
           for (i <- 1 to columnsNumber) {
             if (i > 1) print(",")
             print(rs.getString(i))
           }
           println()
         }
         println(s"NUmber of results : $count")
         stmt.close()

         println("----------------------------------Use of PreparedStatement-------------------------------")
         var prepStatement = conn.prepareStatement(TPCH_Snappy.getQuery10)
         rs = prepStatement.executeQuery
         rsmd = rs.getMetaData()
         println(s"KBKB : rsmd : $rsmd")
         columnsNumber = rsmd.getColumnCount();
         println(s"KBKB : columnsNumber : $columnsNumber")
//         rs.last()
//         println(s"KBKBKB : totoal result size : ${rs.getRow}")
         count = 0
         while (rs.next()) {
           count += 1
           for (i <- 1 to columnsNumber) {
             if (i > 1) print(",")
             print(rs.getString(i))
           }
           println()
         }
         println(s"NUmber of results : $count")
         prepStatement.close()
*/

    avgPrintStream.close()
    avgFileStream.close()

    QueryExecutor.close

  }
}
