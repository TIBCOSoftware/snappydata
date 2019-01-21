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

import java.io._
import java.sql.DriverManager

import scala.collection.mutable.Map

object TPCH_Memsql_Query_StreamExecution {

  def main(args: Array[String]) {

    val host = args(0)
    val port = args(1)
    val queries:Array[String] = args(2).split(",")
    val dbName = "TPCH"
    val user = "root"
    val password = ""

    Class.forName("com.mysql.jdbc.Driver")
    val dbAddress = "jdbc:mysql://" + host + ":" + port + "/"
    val conn = DriverManager.getConnection(dbAddress, user, password)
    val stmt = conn.createStatement

    var isResultCollection : Boolean = args(3).toBoolean
    var warmUp : Integer = args(4).toInt
    var runsForAverage : Integer = args(5).toInt

    val avgFileStream: FileOutputStream = new FileOutputStream(new File(s"Memsql_Average.out"))
    val avgPrintStream: PrintStream = new PrintStream(avgFileStream)

    stmt.execute("USE " + dbName)

    var avgTime: Map[String, Long]= Map()
    for (i <- 1 to (warmUp + runsForAverage)) {
      for (query <- queries) {
        val executionTime : Long = TPCH_Memsql_StreamExecution.execute(query, isResultCollection, stmt)
        if (!isResultCollection) {
          var out: BufferedWriter = new BufferedWriter(new FileWriter(s"Memsql_$query.out", true));
          out.write( executionTime + "\n")
          out.close()
        }
        if (i > warmUp) {
          if (avgTime contains query) {
            avgTime(query) = avgTime.get(query).get + executionTime
          } else {
            avgTime += (query -> executionTime)
          }
        }
      }
    }
    for (query <- queries) {
      avgPrintStream.println(s"$query,${avgTime.get(query).get / runsForAverage}")
    }
    avgPrintStream.close()
    avgFileStream.close()
    stmt.close();
    TPCH_Memsql.close()

  }
}
