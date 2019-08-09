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

package io.snappydata.cluster

import java.sql.{Connection, DriverManager, ResultSet}

import io.snappydata.test.dunit.AvailablePortHelper

import org.apache.spark.Logging
import org.apache.spark.sql.collection.Utils

class ConcurrentQueryRoutingDUnitTest(val s: String)
    extends ClusterManagerTestBase(s) with Logging {

  def columnTableRouting(thr: Int, iter: Int, serverHostPort: Int): Int = {
    val tableName = s"order_line_col_${thr}_$iter"

    logInfo(s"ConcurrentQueryRoutingDUnitTest.columnTableRouting-$thr-$iter:" +
        s"network server started at $serverHostPort")
    try {
      ConcurrentQueryRoutingDUnitTest.columnTableRouting(tableName, serverHostPort)
    } catch {
      case e: Exception =>
        logError(s"columnTableRouting failure in $thr-$iter", e)
        throw e
    }
    logInfo(s"ConcurrentQueryRoutingDUnitTest.columnTableRouting-$thr-$iter done")
    1
  }

/*  def rowTableRouting(thr: Int, iter: Int): Int = {
    val tableName = s"order_line_row_${thr}_${iter}"
    val serverHostPort = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", serverHostPort)
    // scalastyle:off println
    println(s"ConcurrentQueryRoutingDUnitTest.rowTableRouting-${thr}-${iter}:" +
        s"network server started at $serverHostPort")
    // scalastyle:on println
    ConcurrentQueryRoutingDUnitTest.rowTableRouting(tableName, serverHostPort)
    // scalastyle:off println
    println(s"ConcurrentQueryRoutingDUnitTest.rowTableRouting-${thr}-${iter} done")
    // scalastyle:on println
    1
  } */

  def testConcurrency(): Unit = {
    val serverHostPort = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", serverHostPort)

    var thrCount1: Integer = 0
    val colThread1 = new Thread(new Runnable {def run() {
      (1 to 5) foreach (i => {
        thrCount1 += columnTableRouting(1, i, serverHostPort)
      })
    }
    })

    var thrCount2: Integer = 0
    val colThread2 = new Thread(new Runnable {def run() {
      (1 to 5) foreach (i => {
        thrCount2 += columnTableRouting(2, i, serverHostPort)
      })
    }
    })

    var thrCount3: Integer = 0
    val rowThread1 = new Thread(new Runnable {def run() {
      (1 to 5) foreach (i => {
        thrCount3 += columnTableRouting(3, i, serverHostPort)
      })
    }
    })

    var thrCount4: Integer = 0
    val rowThread2 = new Thread(new Runnable {def run() {
      (1 to 5) foreach (i => {
        thrCount4 += columnTableRouting(4, i, serverHostPort)
      })
    }
    })

    colThread1.start()
    colThread2.start()
    rowThread1.start()
    rowThread2.start()

    colThread1.join(5 * 60 * 1000)
    // scalastyle:off println
    println(s"ConcurrentQueryRoutingDUnitTest.testConcurrency:" +
        s" columnTableRouting-1 thread done")
    // scalastyle:on println
    rowThread1.join(5 * 60 * 1000)
    // scalastyle:off println
    println(s"ConcurrentQueryRoutingDUnitTest.testConcurrency:" +
        s"rowTableRouting-1 thread done")
    // scalastyle:on println
    colThread2.join(5 * 60 * 1000)
    // scalastyle:off println
    println(s"ConcurrentQueryRoutingDUnitTest.testConcurrency:" +
        s" columnTableRouting-2 thread done")
    // scalastyle:on println
    rowThread2.join(5 * 60 * 1000)
    // scalastyle:off println
    println(s"ConcurrentQueryRoutingDUnitTest.testConcurrency:" +
        s"rowTableRouting-2 thread done")
    // scalastyle:on println

    assert(thrCount1 == 5,
      s"ConcurrentQueryRoutingDUnitTest.testConcurrency:" +
          s" columnTableRoutingCompleted-1=$thrCount1")
    assert(thrCount2 == 5,
      s"ConcurrentQueryRoutingDUnitTest.testConcurrency:" +
          s" rowTableRoutingCompleted-1=$thrCount2")
    assert(thrCount3 == 5,
      s"ConcurrentQueryRoutingDUnitTest.testConcurrency:" +
          s" columnTableRoutingCompleted-2=$thrCount3")
    assert(thrCount4 == 5,
      s"ConcurrentQueryRoutingDUnitTest.testConcurrency:" +
          s" rowTableRoutingCompleted-2=$thrCount4")

    Array(vm0, vm1, vm2).foreach(_.invoke(classOf[ClusterManagerTestBase],
      "validateNoActiveSnapshotTX"))
  }
}


object ConcurrentQueryRoutingDUnitTest {
  def columnTableRouting(tableName: String, serverHostPort: Int): Unit = {
    createColumnTable("testColumnTableRouting-2", serverHostPort, tableName)
    batchInsert("testColumnTableRouting-2", 200, 100, serverHostPort, tableName)
    singleInsert("testColumnTableRouting-2", 200, serverHostPort, tableName)
    query("testColumnTableRouting-2", serverHostPort, tableName, 400, 40)
    dropTable("testColumnTableRouting-2", serverHostPort, tableName)
  }

  def rowTableRouting(tableName: String, serverHostPort: Int): Unit = {
    createRowTable("testRowTableRouting-2", serverHostPort, tableName)
    batchInsert("testRowTableRouting-2", 20, 20, serverHostPort, tableName)
    singleInsert("testRowTableRouting-2", 20, serverHostPort, tableName)
    query("testRowTableRouting-2", serverHostPort, tableName, 40, 4)
    dropTable("testRowTableRouting-2", serverHostPort, tableName)
  }

  def netConnection(netPort: Int): Connection = {
    val driver = "io.snappydata.jdbc.ClientDriver"
    Utils.classForName(driver).newInstance
    val url: String = "jdbc:snappydata://localhost:" + netPort + "/"
    DriverManager.getConnection(url)
  }

  def createColumnTable(testName: String, serverHostPort: Int, tableName: String): Unit = {
    val conn = netConnection(serverHostPort)
    // scalastyle:off println
    println(s"createColumnTable-$testName: Connected to $serverHostPort")
    // scalastyle:on println

    val stmt1 = conn.createStatement()
    try {
      stmt1.execute(s"create table $tableName (ol_int_id  integer," +
          s" ol_int2_id  integer, ol_str_id STRING) using column " +
          "options( partition_by 'ol_int_id, ol_int2_id', buckets '8', COLUMN_BATCH_SIZE '200')")
    } finally {
      stmt1.close()
      conn.close()
    }
  }

  def createRowTable(testName: String, serverHostPort: Int, tableName: String): Unit = {
    val conn = netConnection(serverHostPort)
    // scalastyle:off println
    println(s"createRowTable-$testName: Connected to $serverHostPort")
    // scalastyle:on println

    val stmt1 = conn.createStatement()
    try {
      stmt1.execute(s"create table $tableName (ol_int_id  integer," +
          s" ol_int2_id  integer, ol_str_id STRING) using row " +
          "options( partition_by 'ol_int_id, ol_int2_id', buckets '8')")
    } finally {
      stmt1.close()
      conn.close()
    }
  }

  def dropTable(testName: String, serverHostPort: Int, tableName: String): Unit = {
    val conn = netConnection(serverHostPort)
    // scalastyle:off println
    println(s"dropTable-$testName: Connected to $serverHostPort")
    // scalastyle:on println

    val stmt1 = conn.createStatement()
    try {
      stmt1.execute(s"drop table $tableName")
    } finally {
      stmt1.close()
      conn.close()
    }
  }

  def batchInsert(testName: String, numRows: Int, batchSize: Int, serverHostPort: Int,
      tableName: String): Unit = {
    val conn = netConnection(serverHostPort)
    // scalastyle:off println
    println(s"batchInsert-$testName: Connected to $serverHostPort")
    // scalastyle:on println

    val stmt1 = conn.createStatement()
    try {
      var i = 1
      (1 to numRows).foreach(_ => {
        stmt1.addBatch(s"insert into $tableName values($i, $i, '$i')")
        i += 1
        if (i % batchSize == 0) {
          stmt1.executeBatch()
          i = 0
        }
      })
      stmt1.executeBatch()

      // scalastyle:off println
      println(s"batchInsert-$testName: committed $numRows rows")
      // scalastyle:on println
    } finally {
      stmt1.close()
      conn.close()
    }
  }

  def singleInsert(testName: String, numRows: Int, serverHostPort: Int, tableName: String): Unit = {
    val conn = netConnection(serverHostPort)
    // scalastyle:off println
    println(s"singleInsert-$testName: Connected to $serverHostPort")
    // scalastyle:on println

    val stmt1 = conn.createStatement()
    try {
      (1 to numRows).foreach(i => {
        stmt1.executeUpdate(s"insert into $tableName values($i, $i, '$i')")
      })

      // scalastyle:off println
      println(s"singleInsert-$testName: committed $numRows rows")
      // scalastyle:on println
    } finally {
      stmt1.close()
      conn.close()
    }
  }

  def verifyQuery(testName: String, qryTest: String, stmt_rs: ResultSet, numRows: Int,
      debugNumRows: Int): Unit = {
    val builder = StringBuilder.newBuilder

    var index = 0
    while (stmt_rs.next()) {
      index += 1
      val stmt_i = stmt_rs.getInt(1)
      val stmt_j = stmt_rs.getInt(2)
      val stmt_s = stmt_rs.getString(3)
      if (index % debugNumRows == 0) {
        builder.append(s"verifyQuery-$testName: " +
            s"$qryTest Stmt: row($index) $stmt_i $stmt_j $stmt_s ").append("\n")
      }
    }
    builder.append(s"verifyQuery-$testName: " +
        s"$qryTest Stmt: Total number of rows = $index").append("\n")
    // scalastyle:off println
    println(builder.toString())
    // scalastyle:on println
    assert(index == numRows)
  }

  def query(testName: String, serverHostPort: Int, tableName: String,
      numRows: Int, debugNumRows: Int): Unit = {
    val conn = netConnection(serverHostPort)
    // scalastyle:off println
    println(s"query-$testName: Connected to $serverHostPort")
    // scalastyle:off println

    val stmt1 = conn.createStatement()
    try {
      val qry1 = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ol_int_id < 5000000 " +
          s""
      val rs1 = stmt1.executeQuery(qry1)
      verifyQuery(testName, qry1, rs1, numRows, debugNumRows)
      rs1.close()
      // Thread.sleep(1000000)
    } finally {
      stmt1.close()
      conn.close()
    }
  }
}
