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
package io.snappydata.benchmark.kuduimpala

import java.sql.DriverManager


object TPCH_Impala_Query {

   def main(args: Array[String]) {

     val host = args(0)
     val port = args(1)
     val queries: Array[String] = args(2).split(",")
     var isResultCollection : Boolean = args(3).toBoolean
     var warmup : Integer = args(4).toInt
     var runsForAverage : Integer = args(5).toInt

     Class.forName("com.cloudera.impala.jdbc4.Driver")
     val dbAddress = "jdbc:impala://" + host + ":" + port + "/"
     val conn = DriverManager.getConnection(dbAddress)
     val stmt = conn.createStatement

     stmt.execute("USE TPCH")

     for(query <- queries)
         TPCH_Impala.execute(query, isResultCollection, stmt, warmup, runsForAverage)
     println("I am done with execution")
     stmt.close();
     TPCH_Impala.close()
     println("I should exit now")
   }
 }
