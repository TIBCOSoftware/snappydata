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
package io.snappydata.benchmark.memsql

import java.sql.DriverManager

object TPCH_Memsql_Query {

   def main(args: Array[String]) {

     val host = args(0)
     val port = args(1)
     val queries:Array[String] = args(2).split(",")
     val dbName = "TPCH"
     val user = "root"
     val password = ""

     //Class.forName("com.mysql.jdbc.Driver")
     // The new mysql Java connector 8.0 generates a warning if the old driver class name is used.
     Class.forName("com.mysql.cj.jdbc.Driver")
     val dbAddress = "jdbc:mysql://" + host + ":" + port + "/"
     val conn = DriverManager.getConnection(dbAddress, user, password)
     val stmt = conn.createStatement

     var isResultCollection : Boolean = args(3).toBoolean
     var warmup : Integer = args(4).toInt
     var runsForAverage : Integer = args(5).toInt
     var isDynamic : Boolean = args(6).toBoolean
     val randomSeed = args(7).toInt

     stmt.execute("USE " + dbName)

     TPCH_Memsql.setRandomSeed(randomSeed)
     for(query <- queries) {
       TPCH_Memsql.execute(query, isResultCollection, stmt, warmup, runsForAverage, isDynamic)
     }
       /*query match {
         case "1" =>   TPCH_Memsql.execute("q1", isResultCollection, stmt, warmup, runsForAverage)
         case "2" =>   TPCH_Memsql.execute("q2", isResultCollection, stmt, warmup, runsForAverage)
         case "3"=>   TPCH_Memsql.execute("q3", isResultCollection, stmt, warmup, runsForAverage)
         case "4" =>   TPCH_Memsql.execute("q4", isResultCollection, stmt, warmup, runsForAverage)
         case "5" =>   TPCH_Memsql.execute("q5", isResultCollection, stmt, warmup, runsForAverage)
         case "6" =>   TPCH_Memsql.execute("q6", isResultCollection, stmt, warmup, runsForAverage)
         case "7" =>   TPCH_Memsql.execute("q7", isResultCollection, stmt, warmup, runsForAverage)
         case "8" =>   TPCH_Memsql.execute("q8", isResultCollection, stmt, warmup, runsForAverage)
         case "9" =>   TPCH_Memsql.execute("q9", isResultCollection, stmt, warmup, runsForAverage)
         case "10" =>   TPCH_Memsql.execute("q10", isResultCollection, stmt, warmup, runsForAverage)
         case "11" =>   TPCH_Memsql.execute("q11", isResultCollection, stmt, warmup, runsForAverage)
         case "12" =>   TPCH_Memsql.execute("q12", isResultCollection, stmt, warmup, runsForAverage)
         case "13" =>   TPCH_Memsql.execute("q13", isResultCollection, stmt, warmup, runsForAverage)
         case "14" =>   TPCH_Memsql.execute("q14", isResultCollection, stmt, warmup, runsForAverage)
         case "15" =>   TPCH_Memsql.execute("q15", isResultCollection, stmt, warmup, runsForAverage)
         case "16" =>   TPCH_Memsql.execute("q16", isResultCollection, stmt, warmup, runsForAverage)
         case "17" =>   TPCH_Memsql.execute("q17", isResultCollection, stmt, warmup, runsForAverage)
         case "18" =>   TPCH_Memsql.execute("q18", isResultCollection, stmt, warmup, runsForAverage)
         case "19" =>   TPCH_Memsql.execute("q19", isResultCollection, stmt, warmup, runsForAverage)
         case "20" =>   TPCH_Memsql.execute("q20", isResultCollection, stmt, warmup, runsForAverage)
         case "21" =>   TPCH_Memsql.execute("q21", isResultCollection, stmt, warmup, runsForAverage)
         case "22" =>   TPCH_Memsql.execute("q22", isResultCollection, stmt, warmup, runsForAverage)
           println("---------------------------------------------------------------------------------")
       }*/

     stmt.close();
     TPCH_Memsql.close()

   }
 }
