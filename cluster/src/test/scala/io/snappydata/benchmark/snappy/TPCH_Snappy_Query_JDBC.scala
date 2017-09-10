/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
package io.snappydata.benchmark.snappy

import java.io.{File, FileOutputStream, PrintStream}
import java.sql.{DriverManager, PreparedStatement}

import io.snappydata.benchmark.memsql.TPCH_Memsql

object TPCH_Snappy_Query_JDBC {

   def main(args: Array[String]) {

     val avgFileStream: FileOutputStream = new FileOutputStream(new File(s"Snappy_Average.out"))
     val avgPrintStream: PrintStream = new PrintStream(avgFileStream)

     val host = args(0)
     val port = args(1)
     val dbName = "TPCH"

     /* Class.forName("com.mysql.jdbc.Driver")*/

     val dbAddress = "jdbc:snappydata://" + host + ":" + port + "/"
     val conn = DriverManager.getConnection(dbAddress)

     val queries: Array[String] = args(2).split(",")
     // scalastyle:off println
     println(queries)
     var isResultCollection: Boolean = args(3).toBoolean
     var warmup: Integer = args(4).toInt
     var runsForAverage: Integer = args(5).toInt

     for (query <- queries) {
       var prepStatement: PreparedStatement = null
       query match {
         case "1" => {
           prepStatement = conn.prepareStatement(TPCH_Snappy.getQuery1)
         }
         case "2" => {
           prepStatement = conn.prepareStatement(TPCH_Snappy.getQuery2_Original)
         }
         case "3" => {
           prepStatement = conn.prepareStatement(TPCH_Snappy.getQuery3)
         }
         case "4" => {
           prepStatement = conn.prepareStatement(TPCH_Snappy.getQuery4)
         }
         case "5" => {
           prepStatement = conn.prepareStatement(TPCH_Snappy.getQuery5)
         }
         case "6" => {
           prepStatement = conn.prepareStatement(TPCH_Snappy.getQuery6)
         }
         case "7" => {
           prepStatement = conn.prepareStatement(TPCH_Snappy.getQuery7)
         }
         case "8" => {
           prepStatement = conn.prepareStatement(TPCH_Snappy.getQuery8(false))
         }
         case "9" => {
           prepStatement = conn.prepareStatement(TPCH_Snappy.getQuery9(false))
         }
         case "10" => {
           prepStatement = conn.prepareStatement(TPCH_Snappy.getQuery10)
         }
         case "11" => {
           prepStatement = conn.prepareStatement(TPCH_Snappy.getQuery11_Original)
         }
         case "12" => {
           prepStatement = conn.prepareStatement(TPCH_Snappy.getQuery12)
         }
         case "13" => {
           prepStatement = conn.prepareStatement(TPCH_Snappy.getQuery13_Original)
         }
         case "14" => {
           prepStatement = conn.prepareStatement(TPCH_Snappy.getQuery14(false))
         }
         case "15" => {

         }
         case "16" => {
           prepStatement = conn.prepareStatement(TPCH_Snappy.getQuery16)
         }
         case "17" => {
           prepStatement = conn.prepareStatement(TPCH_Snappy.getQuery17_Original)
         }
         case "18" => {
           prepStatement = conn.prepareStatement(TPCH_Snappy.getQuery18)
         }
         case "19" => {
           prepStatement = conn.prepareStatement(TPCH_Snappy.getQuery19(false))
         }
         case "20" => {
           prepStatement = conn.prepareStatement(TPCH_Snappy.getQuery20_Original)
         }
         case "21" => {
           prepStatement = conn.prepareStatement(TPCH_Snappy.getQuery21)
         }
         case "22" => {
           prepStatement = conn.prepareStatement(TPCH_Snappy.getQuery22_Original)
         }
       }
       TPCH_Snappy.execute_statement(query, isResultCollection, prepStatement, warmup,
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

     TPCH_Snappy.close

   }
 }
