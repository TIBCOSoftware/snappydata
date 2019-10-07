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

package io.snappydata.benchmark.memsql

import java.io.{FileOutputStream, PrintStream}
import java.sql.DriverManager
import java.util.Date

import scala.util.control.NonFatal

object ConcurrentMemsql {

  def main(args: Array[String]): Unit = {

    val host = args(0)
    val port = 3306
    val dbName = "TPCH"
    val user = "root"
    val password = ""

    val readerThread = new Thread(new Runnable {
      def run() {
        Class.forName("com.mysql.jdbc.Driver")
        val dbAddress = "jdbc:mysql://" + host + ":" + port + "/"
        val conn = DriverManager.getConnection(dbAddress, user, password)
        val stmt = conn.createStatement
        stmt.execute("USE " + dbName)
        val avgFileStream = new FileOutputStream(new java.io.File(s"reader.out"))
        val avgPrintStream = new PrintStream(avgFileStream)
        for (i <- 1 to 100000) {

          var starttime = System.nanoTime()
          // val rs = stmt.executeQuery("select count(*) as counter   from PARTSUPP where ps_suppkey =  18692 and Ps_partkey = 7663535; ")
          val rs = stmt.executeQuery("select PS_AVAILQTY  as counter  from PARTSUPP where ps_suppkey =  18692 and PS_partkeY = 653535")
          var count = 0
          while (rs.next()) {
            count = rs.getInt("counter")
            //just iterating over result
            //count+=1
          }
          var timetaken = (System.nanoTime() - starttime)/1000

          avgPrintStream.println(s"Total time taken $timetaken  results : $count ${new Date()} ")

        }
        avgPrintStream.close()
      }
    }).start()

    val writerThread = new Thread(new Runnable {
      def run() {
        Class.forName("com.mysql.jdbc.Driver")
        val dbAddress = "jdbc:mysql://" + host + ":" + port + "/"
        val conn = DriverManager.getConnection(dbAddress, user, password)
        val stmt = conn.createStatement
        stmt.execute("USE " + dbName)
        val avgFileStream = new FileOutputStream(new java.io.File(s"writer.out"))
        val avgPrintStream = new PrintStream(avgFileStream)
        var startCounter = 7653535
        avgPrintStream.println(s"insertion started ${new Date()}")
        for (i <- 1 to 100000) {
          startCounter+=1
          try {
            var starttime = System.nanoTime()
            // val rs = stmt.execute(s"insert into PARTSUPP values ($startCounter, 18692 , 2, 4.11, 'aa') ")
            val rs = stmt.execute(s"update  PARTSUPP set  PS_AVAILQTY = PS_AVAILQTY +1")
          } catch {
            case NonFatal(e) => e.printStackTrace(avgPrintStream)
          }
        }

        avgPrintStream.println(s"insertion ended ${new Date()}")
        avgPrintStream.close()

      }

    }).start()
  }
}
