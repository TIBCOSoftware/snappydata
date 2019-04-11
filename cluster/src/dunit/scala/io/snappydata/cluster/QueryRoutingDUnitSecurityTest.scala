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

import java.sql.{BatchUpdateException, Connection, DriverManager, ResultSet, SQLException}

import io.snappydata.cluster.ClusterManagerLDAPTestBase.thriftPort
import io.snappydata.test.dunit.AvailablePortHelper

import org.apache.spark.Logging
import org.apache.spark.sql.collection.Utils

class QueryRoutingDUnitSecurityTest(val s: String)
    extends ClusterManagerLDAPTestBase(s) with Logging {

  def testColumnTableRouting(): Unit = {
    val jdbcUser1 = "gemfire1"
    val jdbcUser2 = "gemfire2"
    val tableName = "order_line_col"

    val serverHostPort = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", serverHostPort)
    // scalastyle:off println
    println(s"QueryRoutingDUnitSecureTest.testColumnTableRouting:" +
        s" network server started at $serverHostPort")
    // scalastyle:on println

    QueryRoutingDUnitSecurityTest.columnTableRouting(jdbcUser1, jdbcUser2, tableName,
      serverHostPort)
  }

  def testRowTableRouting(): Unit = {
    val jdbcUser1 = "gemfire3"
    val jdbcUser2 = "gemfire4"
    val tableName = "order_line_row"
    val serverHostPort = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", serverHostPort)
    // scalastyle:off println
    println(s"QueryRoutingDUnitSecureTest.testRowTableRouting:" +
        s" network server started at $serverHostPort")
    // scalastyle:on println

    QueryRoutingDUnitSecurityTest.rowTableRouting(jdbcUser1, jdbcUser2, tableName, serverHostPort)
  }

  /** Test some queries on the embedded thrift server */
  def testEmbeddedThriftServer(): Unit = {
    val jdbcUser1 = "gemfire1"
    val jdbcUser2 = "gemfire2"

    try {
      DriverManager.getConnection(s"jdbc:hive2://localhost:$thriftPort/app")
    } catch {
      case sqle: SQLException if sqle.getSQLState == "08004" => // expected
    }
    try {
      DriverManager.getConnection(s"jdbc:hive2://localhost:$thriftPort/app",
        "app", "app")
    } catch {
      case sqle: SQLException if sqle.getSQLState == "08004" => // expected
    }
    try {
      DriverManager.getConnection(s"jdbc:hive2://localhost:$thriftPort/$jdbcUser1",
        jdbcUser1, jdbcUser2)
    } catch {
      case sqle: SQLException if sqle.getSQLState == "08004" => // expected
    }
    try {
      DriverManager.getConnection(s"jdbc:hive2://localhost:$thriftPort/$jdbcUser1",
        null, null)
    } catch {
      case sqle: SQLException if sqle.getSQLState == "08004" => // expected
    }

    val conn = DriverManager.getConnection(
      s"jdbc:hive2://localhost:$thriftPort/$jdbcUser1", jdbcUser1, jdbcUser1)
    val stmt = conn.createStatement()

    stmt.execute("create table testTable100 (id int)")
    var rs = stmt.executeQuery("show tables")
    assert(rs.next())
    assert(rs.getString(1) == jdbcUser1)
    assert(rs.getString(2) == "testtable100")
    assert(!rs.getBoolean(3)) // isTemporary
    assert(!rs.next())
    rs.close()

    rs = stmt.executeQuery(s"show tables in $jdbcUser1")
    assert(rs.next())
    assert(rs.getString(1) == jdbcUser1)
    assert(rs.getString(2) == "testtable100")
    assert(!rs.getBoolean(3)) // isTemporary
    assert(!rs.next())
    rs.close()

    rs = stmt.executeQuery("select count(*) from testTable100")
    assert(rs.next())
    assert(rs.getLong(1) == 0)
    assert(!rs.next())
    rs.close()
    stmt.execute("insert into testTable100 select id from range(10000)")
    rs = stmt.executeQuery("select count(*) from testTable100")
    assert(rs.next())
    assert(rs.getLong(1) == 10000)
    assert(!rs.next())
    rs.close()

    stmt.execute("drop table testTable100")
    rs = stmt.executeQuery(s"show tables in $jdbcUser1")
    assert(!rs.next())
    rs.close()

    stmt.close()
    conn.close()
  }
}

object QueryRoutingDUnitSecurityTest {

  def columnTableRouting(jdbcUser1: String, jdbcUser2: String, tableName: String,
      serverHostPort: Int): Unit = {
    try {
      createColumnTable("testColumnTableRouting-1", serverHostPort,
        jdbcUser2 + "." + tableName, jdbcUser1, jdbcUser1)
      assert(false) // fail
    } catch {
      case x: SQLException if x.getSQLState.equals("42507") ||
          x.getSQLState.equals("42508") => // ignore
      case t: Throwable => throw t
    }
    createColumnTable("testColumnTableRouting-2", serverHostPort,
      tableName, jdbcUser2, jdbcUser2)

    try {
      batchInsert("testColumnTableRouting-1", 200, 100,
        serverHostPort, jdbcUser2 + "." + tableName, jdbcUser1, jdbcUser1)
      assert(false) // fail
    } catch {
      case _: BatchUpdateException => // ignore
      // case x: SQLException if x.getSQLState.equals("42500") => // ignore
      case t: Throwable => throw t
    }
    batchInsert("testColumnTableRouting-2", 200, 100,
      serverHostPort, tableName, jdbcUser2, jdbcUser2)

    try {
      singleInsert("testColumnTableRouting-1", 200, serverHostPort,
        jdbcUser2 + "." + tableName, jdbcUser1, jdbcUser1)
      assert(false) // fail
    } catch {
      case x: SQLException if x.getSQLState.equals("42500") => // ignore
      case t: Throwable => throw t
    }
    singleInsert("testColumnTableRouting-2", 200, serverHostPort,
      tableName, jdbcUser2, jdbcUser2)

    // (1 to 5).foreach(d => query())
    try {
      query("testColumnTableRouting-1", serverHostPort,
        jdbcUser2 + "." + tableName, jdbcUser1, jdbcUser1, 400, 40)
      assert(false) // fail
    } catch {
      case x: SQLException if x.getSQLState.equals("42500") => // ignore
      case t: Throwable => throw t
    }
    query("testColumnTableRouting-2", serverHostPort,
      tableName, jdbcUser2, jdbcUser2, 400, 40)

    try {
      dropTable("testColumnTableRouting-1", serverHostPort,
        jdbcUser2 + "." + tableName, jdbcUser1, jdbcUser1)
      assert(false) // fail
    } catch {
      case x: SQLException if x.getSQLState.equals("42507") => // ignore
      case t: Throwable => throw t
    }
    dropTable("testColumnTableRouting-2", serverHostPort,
      tableName, jdbcUser2, jdbcUser2)
  }

  def rowTableRouting(jdbcUser1: String, jdbcUser2: String, tableName: String,
      serverHostPort: Int): Unit = {
    try {
      createRowTable("testRowTableRouting-1", serverHostPort,
        jdbcUser1 + "." + tableName, jdbcUser2, jdbcUser2)
      assert(false) // fail
    } catch {
      case x: SQLException if x.getSQLState.equals("42507") ||
          x.getSQLState.equals("42508") => // ignore
      case t: Throwable => throw t
    }
    createRowTable("testRowTableRouting-2",
      serverHostPort, tableName, jdbcUser1, jdbcUser1)

    try {
      batchInsert("testRowTableRouting-1", 20, 20,
        serverHostPort, jdbcUser1 + "." + tableName, jdbcUser2, jdbcUser2)
      assert(false) // fail
    } catch {
      case _: BatchUpdateException => // ignore
      // case x: SQLException if x.getSQLState.equals("42500") => // ignore
      case t: Throwable => throw t
    }
    batchInsert("testRowTableRouting-2", 20, 20,
      serverHostPort, tableName, jdbcUser1, jdbcUser1)

    try {
      singleInsert("testRowTableRouting-1", 20,
        serverHostPort, jdbcUser1 + "." + tableName, jdbcUser2, jdbcUser2)
      assert(false) // fail
    } catch {
      case x: SQLException if x.getSQLState.equals("42500") => // ignore
      case t: Throwable => throw t
    }
    singleInsert("testRowTableRouting-2", 20,
      serverHostPort, tableName, jdbcUser1, jdbcUser1)

    // (1 to 5).foreach(d => query())
    try {
      query("testRowTableRouting-1", serverHostPort,
        jdbcUser1 + "." + tableName, jdbcUser2, jdbcUser2, 40, 4)
      assert(false) // fail
    } catch {
      case x: SQLException if x.getSQLState.equals("42502") => // ignore
      case t: Throwable => throw t
    }
    query("testRowTableRouting-2", serverHostPort, tableName,
      jdbcUser1, jdbcUser1, 40, 4)

    try {
      dropTable("testRowTableRouting-1", serverHostPort,
        jdbcUser1 + "." + tableName, jdbcUser2, jdbcUser2)
      assert(false) // fail
    } catch {
      case x: SQLException if x.getSQLState.equals("42507") => // ignore
      case t: Throwable => throw t
    }
    dropTable("testRowTableRouting-2", serverHostPort,
      tableName, jdbcUser1, jdbcUser1)
  }

  def netConnection(netPort: Int, user: String, pass: String): Connection = {
    val driver = "io.snappydata.jdbc.ClientDriver"
    Utils.classForName(driver).newInstance
    val url: String = "jdbc:snappydata://localhost:" + netPort + "/"
    DriverManager.getConnection(url, user, pass)
  }

  def createColumnTable(testName: String, serverHostPort: Int, tableName: String,
      user: String, pass: String): Unit = {
    val conn = netConnection(serverHostPort, user, pass)
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

  def createRowTable(testName: String, serverHostPort: Int, tableName: String,
      user: String, pass: String): Unit = {
    val conn = netConnection(serverHostPort, user, pass)
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

  def dropTable(testName: String, serverHostPort: Int, tableName: String,
      user: String, pass: String): Unit = {
    val conn = netConnection(serverHostPort, user, pass)
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
      tableName: String, user: String, pass: String): Unit = {
    val conn = netConnection(serverHostPort, user, pass)
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

  def singleInsert(testName: String, numRows: Int, serverHostPort: Int, tableName: String,
      user: String, pass: String): Unit = {
    val conn = netConnection(serverHostPort, user, pass)
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
      user: String, pass: String, numRows: Int, debugNumRows: Int): Unit = {
    val conn = netConnection(serverHostPort, user, pass)
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
