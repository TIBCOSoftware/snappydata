/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

import io.snappydata.test.dunit.AvailablePortHelper

import org.apache.spark.Logging
import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.collection.Utils

object LeadRestartQueryRoutingDUnitSecureTest{
  val adminUser: String = "gemfire5"
}

class LeadRestartQueryRoutingDUnitSecureTest(val s: String)
    extends ClusterManagerLDAPTestBase(s,
      LeadRestartQueryRoutingDUnitSecureTest.adminUser) with Logging {

  override def setUp(): Unit = {
    super.setUp()
  }

  override def tearDown2(): Unit = {
    super.tearDown2()
  }

  def netConnection(netPort: Int, user: String, pass: String): Connection = {
    val driver = "io.snappydata.jdbc.ClientDriver"
    Utils.classForName(driver).newInstance
    val url: String = "jdbc:snappydata://localhost:" + netPort + "/"
    DriverManager.getConnection(url, user, pass)
  }

  def insertRows1(numRows: Int, serverHostPort: Int, tableName: String,
      user: String, pass: String): Unit = {
    val conn = netConnection(serverHostPort, user, pass)
    // scalastyle:off println
    println(s"insertRows1: Connected to $serverHostPort")
    // scalastyle:on println

    val stmt1 = conn.createStatement()
    try {
      var i = 1
      (1 to numRows).foreach(_ => {
        stmt1.addBatch(s"insert into $tableName values($i, $i, '$i')")
        i += 1
        if (i % 1000 == 0) {
          stmt1.executeBatch()
          i = 0
        }
      })
      stmt1.executeBatch()

      // scalastyle:off println
      println(s"insertRows1: committed $numRows rows")
      // scalastyle:on println
    } finally {
      stmt1.close()
      conn.close()
    }
  }

  def insertRows2(numRows: Int, serverHostPort: Int, tableName: String,
      user: String, pass: String): Unit = {
    val conn = netConnection(serverHostPort, user, pass)
    // scalastyle:off println
    println(s"insertRows2: Connected to $serverHostPort")
    // scalastyle:on println

    val stmt1 = conn.createStatement()
    try {
      (1 to numRows).foreach(i => {
        stmt1.executeUpdate(s"insert into $tableName values($i, $i, '$i')")
      })

      // scalastyle:off println
      println(s"insertRows2: committed $numRows rows")
      // scalastyle:on println
    } finally {
      stmt1.close()
      conn.close()
    }
  }

  def insertRows3(numRows: Int, serverHostPort: Int, tableName: String,
      user: String, pass: String): Unit = {
    val conn = netConnection(serverHostPort, user, pass)
    // scalastyle:off println
    println(s"insertRows3: Connected to $serverHostPort")
    // scalastyle:on println

    val stmt1 = conn.createStatement()
    try {
      var i = 1
      (1 to numRows).foreach(_ => {
        stmt1.addBatch(s"insert into $tableName values($i, $i, '$i')")
        i += 1
        if (i % 10 == 0) {
          stmt1.executeBatch()
          i = 0
        }
      })
      stmt1.executeBatch()

      // scalastyle:off println
      println(s"insertRows3: committed $numRows rows")
      // scalastyle:on println
    } finally {
      stmt1.close()
      conn.close()
    }
  }

  def insertRows4(numRows: Int, serverHostPort: Int, tableName: String,
      user: String, pass: String): Unit = {
    val conn = netConnection(serverHostPort, user, pass)
    // scalastyle:off println
    println(s"insertRows4: Connected to $serverHostPort")
    // scalastyle:on println

    val stmt1 = conn.createStatement()
    try {
      (1 to numRows).foreach(i => {
        stmt1.executeUpdate(s"insert into $tableName values($i, $i, '$i')")
      })

      // scalastyle:off println
      println(s"insertRows4: committed $numRows rows")
      // scalastyle:on println
    } finally {
      stmt1.close()
      conn.close()
    }
  }

  def verifyQuery1(qryTest: String, stmt_rs: ResultSet): Unit = {
    val builder = StringBuilder.newBuilder

    var index = 0
    while (stmt_rs.next()) {
      index += 1
      val stmt_i = stmt_rs.getInt(1)
      val stmt_j = stmt_rs.getInt(2)
      val stmt_s = stmt_rs.getString(3)
      if (index % 10 == 0) {
        builder.append(s"$qryTest Stmt: row($index) $stmt_i $stmt_j $stmt_s ").append("\n")
      }
    }
    builder.append(s"$qryTest Stmt: Total number of rows = $index").append("\n")
    // scalastyle:off println
    println(builder.toString())
    // scalastyle:on println
    assert(index == 400)
  }

  def verifyQuery2(qryTest: String, stmt_rs: ResultSet): Unit = {
    val builder = StringBuilder.newBuilder

    var index = 0
    while (stmt_rs.next()) {
      index += 1
      val stmt_i = stmt_rs.getInt(1)
      val stmt_j = stmt_rs.getInt(2)
      val stmt_s = stmt_rs.getString(3)
      if (index % 5 == 0) {
        builder.append(s"$qryTest Stmt: row($index) $stmt_i $stmt_j $stmt_s ").append("\n")
      }
    }
    builder.append(s"$qryTest Stmt: Total number of rows = $index").append("\n")
    // scalastyle:off println
    println(builder.toString())
    // scalastyle:on println
    assert(index == 40)
  }

  def query1(serverHostPort: Int, tableName: String, user: String, pass: String): Unit = {
    val conn = netConnection(serverHostPort, user, pass)
    // scalastyle:off println
    println(s"query1: Connected to $serverHostPort")
    // scalastyle:off println

    val stmt1 = conn.createStatement()
    try {
      val qry1 = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ol_int_id < 5000000 " +
          s""
      val rs1 = stmt1.executeQuery(qry1)
      verifyQuery1(qry1, rs1)
      rs1.close()
      // Thread.sleep(1000000)
    } finally {
      stmt1.close()
      conn.close()
    }
  }

  def query2(serverHostPort: Int, tableName: String, user: String, pass: String): Unit = {
    val conn = netConnection(serverHostPort, user, pass)
    // scalastyle:off println
    println(s"query2: Connected to $serverHostPort")
    // scalastyle:off println

    val stmt1 = conn.createStatement()
    try {
      val qry1 = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ol_int_id < 5000000 " +
          s""
      val rs1 = stmt1.executeQuery(qry1)
      verifyQuery2(qry1, rs1)
      rs1.close()
      // Thread.sleep(1000000)
    } finally {
      stmt1.close()
      conn.close()
    }
  }

  def createTable1(serverHostPort: Int, tableName: String, user: String, pass: String): Unit = {
    val conn = netConnection(serverHostPort, user, pass)
    // scalastyle:off println
    println(s"createTable1: Connected to $serverHostPort")
    // scalastyle:on println

    val stmt1 = conn.createStatement()
    try {
    stmt1.execute(s"create table $tableName (ol_int_id  integer," +
        s" ol_int2_id  integer, ol_str_id STRING) using column " +
        "options( partition_by 'ol_int_id, ol_int2_id', buckets '5', COLUMN_BATCH_SIZE '200')")
    } finally {
      stmt1.close()
      conn.close()
    }
  }

  def dropTable(serverHostPort: Int, tableName: String, user: String, pass: String): Unit = {
    val conn = netConnection(serverHostPort, user, pass)
    // scalastyle:off println
    println(s"dropTable1: Connected to $serverHostPort")
    // scalastyle:on println

    val stmt1 = conn.createStatement()
    try {
      stmt1.execute(s"drop table $tableName")
    } finally {
      stmt1.close()
      conn.close()
    }
  }

  def createTable2(serverHostPort: Int, tableName: String, user: String, pass: String): Unit = {
    val conn = netConnection(serverHostPort, user, pass)
    // scalastyle:off println
    println(s"createTable2: Connected to $serverHostPort")
    // scalastyle:on println

    val stmt1 = conn.createStatement()
    try {
      stmt1.execute(s"create table $tableName (ol_int_id  integer," +
          s" ol_int2_id  integer, ol_str_id STRING) using row " +
          "options( partition_by 'ol_int_id, ol_int2_id', buckets '5')")
    } finally {
      stmt1.close()
      conn.close()
    }
  }

  def testColumnTableRouting(): Unit = {
    val jdbcUser1 = "gemfire1"
    val jdbcUser2 = "gemfire2"
    val serverHostPort = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", serverHostPort)
    // scalastyle:off println
    println(s"network server started at $serverHostPort")
    // scalastyle:on println

    val tableName = "order_line_col"

    try {
      createTable1(serverHostPort, jdbcUser2 + "." + tableName, jdbcUser1, jdbcUser1)
      assert(false) // fail
    } catch {
      case x: SQLException if x.getSQLState.equals("42507") ||
          x.getSQLState.equals("42508") => // ignore
      case t: Throwable => throw t
    }
    createTable1(serverHostPort, tableName, jdbcUser2, jdbcUser2)

    try {
      insertRows1(200, serverHostPort, jdbcUser2 + "." + tableName, jdbcUser1, jdbcUser1)
      assert(false) // fail
    } catch {
      case x: BatchUpdateException => // ignore
      case t: Throwable => throw t
    }
    insertRows1(200, serverHostPort, tableName, jdbcUser2, jdbcUser2)

    try {
      insertRows2(200, serverHostPort, jdbcUser2 + "." + tableName, jdbcUser1, jdbcUser1)
      assert(false) // fail
    } catch {
      case x: SQLException if x.getSQLState.equals("42500") => // ignore
      case t: Throwable => throw t
    }
    insertRows2(200, serverHostPort, tableName, jdbcUser2, jdbcUser2)

    // (1 to 5).foreach(d => query())
    try {
      query1(serverHostPort, jdbcUser2 + "." + tableName, jdbcUser1, jdbcUser1)
      assert(false) // fail
    } catch {
      case x: SQLException if x.getSQLState.equals("42502") => // ignore
      case t: Throwable => throw t
    }
    query1(serverHostPort, tableName, jdbcUser2, jdbcUser2)

    // stop spark
    val sparkContext = SnappyContext.globalSparkContext
    if(sparkContext != null) sparkContext.stop()
    ClusterManagerTestBase.stopAny()

    ClusterManagerTestBase.startSnappyLead(ClusterManagerTestBase.locatorPort, bootProps)

    // (1 to 5).foreach(d => query())
    try {
      query1(serverHostPort, jdbcUser2 + "." + tableName, jdbcUser1, jdbcUser1)
      assert(false) // fail
    } catch {
      case x: SQLException if x.getSQLState.equals("42502") => // ignore
      case t: Throwable => throw t
    }
    query1(serverHostPort, tableName, jdbcUser2, jdbcUser2)

    try {
      dropTable(serverHostPort, jdbcUser2 + "." + tableName, jdbcUser1, jdbcUser1)
      assert(false) // fail
    } catch {
      case x: SQLException if x.getSQLState.equals("42507") => // ignore
      case t: Throwable => throw t
    }
    dropTable(serverHostPort, tableName, jdbcUser2, jdbcUser2)
  }

  def testRowTableRouting(): Unit = {
    val jdbcUser3 = "gemfire3"
    val jdbcUser4 = "gemfire4"
    val serverHostPort = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", serverHostPort)
    // scalastyle:off println
    println(s"network server started at $serverHostPort")
    // scalastyle:on println

    val tableName = "order_line_row"

    try {
      createTable2(serverHostPort, jdbcUser3 + "." + tableName, jdbcUser4, jdbcUser4)
      assert(false) // fail
    } catch {
      case x: SQLException if x.getSQLState.equals("42507") ||
          x.getSQLState.equals("42508") => // ignore
      case t: Throwable => throw t
    }
    createTable2(serverHostPort, tableName, jdbcUser3, jdbcUser3)

    try {
      insertRows3(20, serverHostPort, jdbcUser3 + "." + tableName, jdbcUser4, jdbcUser4)
      assert(false) // fail
    } catch {
      case x: BatchUpdateException => // ignore
      case t: Throwable => throw t
    }
    insertRows3(20, serverHostPort, tableName, jdbcUser3, jdbcUser3)

    try {
      insertRows4(20, serverHostPort, jdbcUser3 + "." + tableName, jdbcUser4, jdbcUser4)
      assert(false) // fail
    } catch {
      case x: SQLException if x.getSQLState.equals("42500") => // ignore
      case t: Throwable => throw t
    }
    insertRows4(20, serverHostPort, tableName, jdbcUser3, jdbcUser3)

    // (1 to 5).foreach(d => query())
    try {
      query2(serverHostPort, jdbcUser3 + "." + tableName, jdbcUser4, jdbcUser4)
      assert(false) // fail
    } catch {
      case x: SQLException if x.getSQLState.equals("42502") => // ignore
      case t: Throwable => throw t
    }
    query2(serverHostPort, tableName, jdbcUser3, jdbcUser3)

    // stop spark
    val sparkContext = SnappyContext.globalSparkContext
    if(sparkContext != null) sparkContext.stop()
    ClusterManagerTestBase.stopAny()

    ClusterManagerTestBase.startSnappyLead(ClusterManagerTestBase.locatorPort, bootProps)

    // (1 to 5).foreach(d => query())
    try {
      query2(serverHostPort, jdbcUser3 + "." + tableName, jdbcUser4, jdbcUser4)
      assert(false) // fail
    } catch {
      case x: SQLException if x.getSQLState.equals("42502") => // ignore
      case t: Throwable => throw t
    }
    query2(serverHostPort, tableName, jdbcUser3, jdbcUser3)

    try {
      dropTable(serverHostPort, jdbcUser3 + "." + tableName, jdbcUser4, jdbcUser4)
      assert(false) // fail
    } catch {
      case x: SQLException if x.getSQLState.equals("42502") => // ignore
      case t: Throwable => throw t
    }
    dropTable(serverHostPort, tableName, jdbcUser3, jdbcUser3)
  }
}
