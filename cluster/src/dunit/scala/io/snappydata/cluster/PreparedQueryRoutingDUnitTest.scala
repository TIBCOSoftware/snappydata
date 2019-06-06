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

import java.sql._
import java.time.LocalDate
import java.util.concurrent.atomic.AtomicBoolean

import com.pivotal.gemfirexd.TestUtil
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.test.dunit.AvailablePortHelper
import org.apache.spark.Logging
import org.apache.spark.sql.SnappyContext

import scala.util.Random

/**
 * Tests for query routing from JDBC client driver.
 */
class PreparedQueryRoutingDUnitTest(val s: String)
    extends ClusterManagerTestBase(s) with Logging {

  // set default batch size for this test
  private val default_chunk_size = GemFireXDUtils.DML_MAX_CHUNK_SIZE
  var serverHostPort = -1

  override def tearDown2(): Unit = {
    // reset the chunk size on lead node
    setDMLMaxChunkSize(default_chunk_size)
    super.tearDown2()
  }

  def setDMLMaxChunkSize(size: Long): Unit = {
    GemFireXDUtils.DML_MAX_CHUNK_SIZE = size
  }

  def test1_PrepStatementRouting(): Unit = {
    val tableName = "order_line_col_test1"
    serverHostPort = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", serverHostPort)
    // scalastyle:off println
    println(s"test1: network server started at $serverHostPort")
    // scalastyle:on println

    val snc = SnappyContext(sc)
    snc.sql(s"create table $tableName (ol_int_id  decimal(6,2)," +
        s" ol_int2_id  integer, ol_str_id STRING) using column " +
        "options( partition_by 'ol_int_id, ol_int2_id', buckets '2')")

    insertRows_test1(1000, tableName)

    // (1 to 5).foreach(d => query())
    query_test1(tableName)
  }

  def insertRows_test1(numRows: Int, tableName: String): Unit = {

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://localhost:" + serverHostPort)

    val rows = (1 to numRows)
    val stmt = conn.createStatement()
    try {
      var i = 1
      rows.foreach(d => {
        stmt.addBatch(s"insert into $tableName values($i.11, $i, '$i')")
        i += 1
        if (i % 1000 == 0) {
          stmt.executeBatch()
          i = 0
        }
      })
      stmt.executeBatch()
      // scalastyle:off println
      println(s"committed $numRows rows")
      // scalastyle:on println
    } finally {
      stmt.close()
      conn.close()
    }
  }

  def verifyQuery_test1(qryTest: String, prep_rs: ResultSet, stmt_rs: ResultSet,
      expectedNoRows: Int): Unit = {
    val builder = StringBuilder.newBuilder
    var index = 0
    var assertionFailed = false
    while (prep_rs.next() && stmt_rs.next()) {
      val prep_i = prep_rs.getBigDecimal(1)
      val prep_j = prep_rs.getInt(2)
      val prep_s = prep_rs.getString(3)

      val stmt_i = stmt_rs.getBigDecimal(1)
      val stmt_j = stmt_rs.getInt(2)
      val stmt_s = stmt_rs.getString(3)

      builder.append(s"$qryTest Prep: row($index) $prep_i $prep_j $prep_s ").append("\n")
      builder.append(s"$qryTest Stmt: row($index) $stmt_i $stmt_j $stmt_s ").append("\n")

      if (prep_i != stmt_i && !assertionFailed) {
        builder.append(s"Assertion failed at index=$index prep=$prep_i stmt=$stmt_i").append("\n")
        assertionFailed = true
      }

      if (prep_j != stmt_j && !assertionFailed) {
        builder.append(s"Assertion failed at index=$index prep=$prep_j stmt=$stmt_j").append("\n")
        assertionFailed = true
      }

      if (prep_s != stmt_s && !assertionFailed) {
        builder.append(s"Assertion failed at index=$index prep=$prep_s stmt=$stmt_s").append("\n")
        assertionFailed = true
      }

      index += 1
    }

    while (prep_rs.next()) {
      if (!assertionFailed) {
        builder.append(s"Assertion failed at index=$index").append("\n")
        assertionFailed = true
      }

      val prep_i = prep_rs.getBigDecimal(1)
      val prep_j = prep_rs.getInt(2)
      val prep_s = prep_rs.getString(3)
      builder.append(s"$qryTest Prep: row($index) $prep_i $prep_j $prep_s ").append("\n")
      index += 1
    }

    while (stmt_rs.next()) {
      if (!assertionFailed) {
        builder.append(s"Assertion failed at index=$index").append("\n")
        assertionFailed = true
      }

      val stmt_i = stmt_rs.getBigDecimal(1)
      val stmt_j = stmt_rs.getInt(2)
      val stmt_s = stmt_rs.getString(3)
      builder.append(s"$qryTest Stmt: row($index) $stmt_i $stmt_j $stmt_s ").append("\n")
      index += 1
    }

    if (index != expectedNoRows) {
      if (!assertionFailed) {
        builder.append(s"Assertion failed: got number of rows=$index "
            + s"expected=$expectedNoRows").append("\n")
        assertionFailed = true
      }
    }

    if (assertionFailed) {
      // scalastyle:off println
      println(builder.toString())
      // scalastyle:on println
    }

    assert(!assertionFailed)

    prep_rs.close()
    stmt_rs.close()
  }

  def query1_like_clause_test1(limit: Boolean, tableName: String): Unit = {
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://localhost:" + serverHostPort)

    // scalastyle:off println
    println(s"query1_like_clause: Connected to $serverHostPort")
    // scalastyle:on println
    val limitClause = if (limit) "limit 20" else ""
    val stmt = conn.createStatement()
    var prepStatement: java.sql.PreparedStatement = null
    try {
      val qry = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ol_int_id < ? " +
          s" and ol_int2_id > 100 " +
          s" and ol_str_id like ? " +
          s" $limitClause" +
          s""

      prepStatement = conn.prepareStatement(qry)
      prepStatement.setBigDecimal(1, new java.math.BigDecimal("500.11"))
      prepStatement.setString(2, "%0")
      val qry2 = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ol_int_id < 500.11 " +
          s" and ol_int2_id > 100 " +
          s" and ol_str_id LIKE '%0' " +
          s" $limitClause" +
          s""
      verifyQuery_test1("query1_like_clause_test1.1", prepStatement.executeQuery,
        stmt.executeQuery(qry2), if (limit) 20 else 39)

      prepStatement.setBigDecimal(1, new java.math.BigDecimal("800.11"))
      prepStatement.setString(2, "%0")
      val qry3 = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ol_int_id < 800.11 " +
          s" and ol_int2_id > 100 " +
          s" and ol_str_id LIKE '%0' " +
          s" $limitClause" +
          s""
      verifyQuery_test1("query1_like_clause_test1.2", prepStatement.executeQuery,
        stmt.executeQuery(qry3), if (limit) 20 else 69)

      // Thread.sleep(1000000)

    } finally {
      stmt.close()
      if (prepStatement != null) prepStatement.close()
      conn.close()
    }
  }

  def query2_in_clause_test1(limit: Boolean, tableName: String): Unit = {
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://localhost:" + serverHostPort)

    // scalastyle:off println
    println(s"query2_in_clause: Connected to $serverHostPort")
    // scalastyle:on println
    val limitClause = if (limit) "limit 20" else ""
    val stmt = conn.createStatement()
    var prepStatement: java.sql.PreparedStatement = null
    try {
      val qry = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ol_int_id < ? " +
          s" and ol_int2_id in (?, ?, ?) " +
          s" and ol_str_id like ? " +
          s" $limitClause" +
          s""

      prepStatement = conn.prepareStatement(qry)
      prepStatement.setBigDecimal(1, new java.math.BigDecimal("500.11"))
      prepStatement.setInt(2, 100)
      prepStatement.setInt(3, 200)
      prepStatement.setInt(4, 300)
      prepStatement.setString(5, "%0")
      val qry2 = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ol_int_id < 500.11 " +
          s" and ol_int2_id in (100, 200, 300) " +
          s" and ol_str_id LIKE '%0' " +
          s" $limitClause" +
          s""
      verifyQuery_test1("query2_in_clause_test1.1", prepStatement.executeQuery,
        stmt.executeQuery(qry2), 3)

      prepStatement = conn.prepareStatement(qry)
      prepStatement.setBigDecimal(1, new java.math.BigDecimal("300.11"))
      prepStatement.setInt(2, 110)
      prepStatement.setInt(3, 120)
      prepStatement.setInt(4, 130)
      prepStatement.setString(5, "1%")
      val qry3 = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ol_int_id < 300.11 " +
          s" and ol_int2_id in (110, 120, 130) " +
          s" and ol_str_id LIKE '1%' " +
          s" $limitClause" +
          s""
      verifyQuery_test1("query2_in_clause_test1.2", prepStatement.executeQuery,
        stmt.executeQuery(qry3), 3)

      // Thread.sleep(1000000)

    } finally {
      stmt.close()
      if (prepStatement != null) prepStatement.close()
      conn.close()
    }
  }

  def query_test1(tableName: String): Unit = {
    query1_like_clause_test1(true, tableName)
    query1_like_clause_test1(false, tableName)
    query2_in_clause_test1(true, tableName)
    query2_in_clause_test1(false, tableName)
  }

  def insertRows_test2(numRows: Int, tableName: String): Unit = {

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://localhost:" + serverHostPort)

    val rows = (1 to numRows)
    val stmt = conn.createStatement()
    try {
      var i = 1
      val newDate = LocalDate.parse("2017-01-01")
      rows.foreach(d => {
        val e = newDate.plusDays(i)
        stmt.addBatch(s"insert into $tableName values('$e', '$e', '$e')")
        i += 1
        if (i % 1000 == 0) {
          stmt.executeBatch()
          i = 0
        }
      })
      stmt.executeBatch()
      // scalastyle:off println
      println(s"committed $numRows rows")
      // scalastyle:on println
    } finally {
      stmt.close()
      conn.close()
    }
  }

  def verifyQuery_test2(qryTest: String, prep_rs: ResultSet, stmt_rs: ResultSet,
      expectedNoRows: Int): Unit = {
    val builder = StringBuilder.newBuilder
    var index = 0
    var assertionFailed = false
    while (prep_rs.next() && stmt_rs.next()) {
      val prep_i = prep_rs.getDate(1)
      val prep_j = prep_rs.getDate(2)
      val prep_s = prep_rs.getString(3)

      val stmt_i = stmt_rs.getDate(1)
      val stmt_j = stmt_rs.getDate(2)
      val stmt_s = stmt_rs.getString(3)

      builder.append(s"$qryTest Prep: row($index) $prep_i $prep_j $prep_s ").append("\n")
      builder.append(s"$qryTest Stmt: row($index) $stmt_i $stmt_j $stmt_s ").append("\n")

      if (prep_i != stmt_i && !assertionFailed) {
        builder.append(s"Assertion failed at index=$index prep=$prep_i stmt=$stmt_i").append("\n")
        assertionFailed = true
      }

      if (prep_j != stmt_j && !assertionFailed) {
        builder.append(s"Assertion failed at index=$index prep=$prep_j stmt=$stmt_j").append("\n")
        assertionFailed = true
      }

      if (prep_s != stmt_s && !assertionFailed) {
        builder.append(s"Assertion failed at index=$index prep=$prep_s stmt=$stmt_s").append("\n")
        assertionFailed = true
      }

      index += 1
    }

    while (prep_rs.next()) {
      if (!assertionFailed) {
        builder.append(s"Assertion failed at index=$index").append("\n")
        assertionFailed = true
      }

      val prep_i = prep_rs.getDate(1)
      val prep_j = prep_rs.getDate(2)
      val prep_s = prep_rs.getString(3)
      builder.append(s"$qryTest Prep: row($index) $prep_i $prep_j $prep_s ").append("\n")
      index += 1
    }

    while (stmt_rs.next()) {
      if (!assertionFailed) {
        builder.append(s"Assertion failed at index=$index").append("\n")
        assertionFailed = true
      }

      val stmt_i = stmt_rs.getDate(1)
      val stmt_j = stmt_rs.getDate(2)
      val stmt_s = stmt_rs.getString(3)
      builder.append(s"$qryTest Stmt: row($index) $stmt_i $stmt_j $stmt_s ").append("\n")
      index += 1
    }

    if (index != expectedNoRows) {
      if (!assertionFailed) {
        builder.append(s"Assertion failed: got number of rows=$index "
            + s"expected=$expectedNoRows").append("\n")
        assertionFailed = true
      }
    }

    if (assertionFailed) {
      // scalastyle:off println
      println(builder.toString())
      // scalastyle:on println
    }

    assert(!assertionFailed)

    prep_rs.close()
    stmt_rs.close()
  }

  def query1_test2(limit: Boolean, tableName: String): Unit = {
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://localhost:" + serverHostPort)

    // scalastyle:off println
    println(s"query1_test2: Connected to $serverHostPort")
    // scalastyle:on println
    val limitClause = if (limit) "limit 20" else ""
    val stmt = conn.createStatement()
    var prepStatement: java.sql.PreparedStatement = null
    val oneDate = Date.valueOf("2017-03-15")
    val twoDate = Date.valueOf("2017-02-28")
    val threeDate = Date.valueOf("2017-03-31")
    try {
      val qry = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ol_int_id < ? " +
          s" and ol_int2_id > ? " +
          s" and ol_str_id like ? " +
          s" $limitClause" +
          s""

      prepStatement = conn.prepareStatement(qry)
      prepStatement.setString(1, oneDate.toString)
      prepStatement.setDate(2, twoDate)
      prepStatement.setString(3, "%-%")
      val qry2 = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ol_int_id < '${oneDate.toString}' " +
          s" and ol_int2_id > '${twoDate.toString}' " +
          s" and ol_str_id LIKE '%-%' " +
          s" $limitClause" +
          s""
      verifyQuery_test2("query1_test2.1", prepStatement.executeQuery, stmt.executeQuery(qry2), 14)

      prepStatement.setString(1, threeDate.toString)
      prepStatement.setDate(2, twoDate)
      prepStatement.setString(3, "%-%")
      val qry3 = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ol_int_id < '${threeDate.toString}' " +
          s" and ol_int2_id > '${twoDate.toString}' " +
          s" and ol_str_id LIKE '%-%' " +
          s" $limitClause" +
          s""
      verifyQuery_test2("query1_test2.2", prepStatement.executeQuery, stmt.executeQuery(qry3),
        if (limit) 20 else 30)

      // Thread.sleep(1000000)

    } finally {
      stmt.close()
      if (prepStatement != null) prepStatement.close()
      conn.close()
    }
  }

  def query_test2(tableName: String): Unit = {
    query1_test2(true, tableName: String)
    query1_test2(false, tableName: String)
  }

  def test2_date(): Unit = {
    val tableName = "order_line_col_test2"
    serverHostPort = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", serverHostPort)
    // scalastyle:off println
    println(s"test2: network server started at $serverHostPort")
    // scalastyle:on println

    val snc = SnappyContext(sc)
    snc.sql(s"create table $tableName (ol_int_id  date," +
        s" ol_int2_id  date, ol_str_id STRING) using column " +
        "options( partition_by 'ol_int_id, ol_int2_id', buckets '2')")

    insertRows_test2(1000, tableName)
    query_test2(tableName)
  }

  def insertRows_test3(numRows: Int, tableName: String): Unit = {

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://localhost:" + serverHostPort)

    val rows = (1 to numRows)
    val stmt = conn.createStatement()
    try {
      var i = 1
      import java.time.format.DateTimeFormatter
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      val newTimestamp = java.time.LocalDateTime.parse("2017-01-01 10:00:00", formatter)
      rows.foreach(d => {
        val e = newTimestamp.plusDays(i).format(formatter)
        stmt.addBatch(s"insert into $tableName values('$e', '$e', '$e')")
        i += 1
        if (i % 1000 == 0) {
          stmt.executeBatch()
          i = 0
        }
      })
      stmt.executeBatch()
      // scalastyle:off println
      println(s"committed $numRows rows")
      // scalastyle:on println
    } finally {
      stmt.close()
      conn.close()
    }
  }

  def verifyQuery_test3(qryTest: String, prep_rs: ResultSet, stmt_rs: ResultSet,
      expectedNoRows: Int): Unit = {
    val builder = StringBuilder.newBuilder
    var index = 0
    var assertionFailed = false
    while (prep_rs.next() && stmt_rs.next()) {
      val prep_i = prep_rs.getTimestamp(1)
      val prep_j = prep_rs.getTimestamp(2)
      val prep_s = prep_rs.getString(3)

      val stmt_i = stmt_rs.getTimestamp(1)
      val stmt_j = stmt_rs.getTimestamp(2)
      val stmt_s = stmt_rs.getString(3)

      builder.append(s"$qryTest Prep: row($index) $prep_i $prep_j $prep_s ").append("\n")
      builder.append(s"$qryTest Stmt: row($index) $stmt_i $stmt_j $stmt_s ").append("\n")

      if (prep_i != stmt_i && !assertionFailed) {
        builder.append(s"Assertion failed at index=$index prep=$prep_i stmt=$stmt_i").append("\n")
        assertionFailed = true
      }

      if (prep_j != stmt_j && !assertionFailed) {
        builder.append(s"Assertion failed at index=$index prep=$prep_j stmt=$stmt_j").append("\n")
        assertionFailed = true
      }

      if (prep_s != stmt_s && !assertionFailed) {
        builder.append(s"Assertion failed at index=$index prep=$prep_s stmt=$stmt_s").append("\n")
        assertionFailed = true
      }

      index += 1
    }

    while (prep_rs.next()) {
      if (!assertionFailed) {
        builder.append(s"Assertion failed at index=$index").append("\n")
        assertionFailed = true
      }

      val prep_i = prep_rs.getTimestamp(1)
      val prep_j = prep_rs.getTimestamp(2)
      val prep_s = prep_rs.getString(3)
      builder.append(s"$qryTest Prep: row($index) $prep_i $prep_j $prep_s ").append("\n")
      index += 1
    }

    while (stmt_rs.next()) {
      if (!assertionFailed) {
        builder.append(s"Assertion failed at index=$index").append("\n")
        assertionFailed = true
      }

      val stmt_i = stmt_rs.getTimestamp(1)
      val stmt_j = stmt_rs.getTimestamp(2)
      val stmt_s = stmt_rs.getString(3)
      builder.append(s"$qryTest Stmt: row($index) $stmt_i $stmt_j $stmt_s ").append("\n")
      index += 1
    }

    if (index != expectedNoRows) {
      if (!assertionFailed) {
        builder.append(s"Assertion failed: got number of rows=$index "
            + s"expected=$expectedNoRows").append("\n")
        assertionFailed = true
      }
    }

    if (assertionFailed) {
      // scalastyle:off println
      println(builder.toString())
      // scalastyle:on println
    }

    assert(!assertionFailed)

    prep_rs.close()
    stmt_rs.close()
  }

  def query1_test3(limit: Boolean, tableName: String): Unit = {
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://localhost:" + serverHostPort)

    // scalastyle:off println
    println(s"query1_test2: Connected to $serverHostPort")
    // scalastyle:on println
    val limitClause = if (limit) "limit 20" else ""
    val stmt = conn.createStatement()
    var prepStatement: java.sql.PreparedStatement = null
    val oneTs = java.sql.Timestamp.valueOf("2017-03-15 12:02:03")
    val twoTs = java.sql.Timestamp.valueOf("2017-02-28 12:02:04")
    val threeTs = java.sql.Timestamp.valueOf("2017-03-31 12:02:04")
    try {
      val qry = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ol_int_id < ? " +
          s" and ol_int2_id > ? " +
          s" and ol_str_id like ? " +
          s" $limitClause" +
          s""

      prepStatement = conn.prepareStatement(qry)
      prepStatement.setString(1, oneTs.toString)
      prepStatement.setTimestamp(2, twoTs)
      prepStatement.setString(3, "%-%")
      val qry2 = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ol_int_id < '${oneTs.toString}' " +
          s" and ol_int2_id > '${twoTs.toString}' " +
          s" and ol_str_id LIKE '%-%' " +
          s" $limitClause" +
          s""
      verifyQuery_test3("query1_test3.1", prepStatement.executeQuery, stmt.executeQuery(qry2), 15)

      prepStatement.setString(1, threeTs.toString)
      prepStatement.setTimestamp(2, twoTs)
      prepStatement.setString(3, "%-%")
      val qry3 = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ol_int_id < '${threeTs.toString}' " +
          s" and ol_int2_id > '${twoTs.toString}' " +
          s" and ol_str_id LIKE '%-%' " +
          s" $limitClause" +
          s""
      verifyQuery_test3("query1_test3.1", prepStatement.executeQuery, stmt.executeQuery(qry3),
        if (limit) 20 else 31)

      // Thread.sleep(1000000)

    } finally {
      stmt.close()
      if (prepStatement != null) prepStatement.close()
      conn.close()
    }
  }

  def query_test3(tableName: String): Unit = {
    query1_test3(true, tableName: String)
    query1_test3(false, tableName: String)
  }

  def test3_timestamp(): Unit = {
    val tableName = "order_line_col_test3"
    serverHostPort = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", serverHostPort)
    // scalastyle:off println
    println(s"test3: network server started at $serverHostPort")
    // scalastyle:on println

    val snc = SnappyContext(sc)
    snc.sql(s"create table $tableName (ol_int_id  timestamp," +
        s" ol_int2_id  timestamp, ol_str_id STRING) using column " +
        "options( partition_by 'ol_int_id, ol_int2_id', buckets '2')")

    insertRows_test3(1000, tableName)
    query_test3(tableName)
  }

  def test4_update_delete_on_column_table(): Unit = {
    serverHostPort = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", serverHostPort)
    // scalastyle:off println
    println(s"test4_update_delete_on_column_table: network server started at $serverHostPort")
    // scalastyle:on println
    val snc = SnappyContext(sc)
    PreparedQueryRoutingSingleNodeSuite.updateDeleteOnColumnTable(snc, s"localhost:$serverHostPort")
  }

  def test5_equalityOnStringColumn(): Unit = {
    serverHostPort = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", serverHostPort)
    // scalastyle:off println
    println(s"test5_equalityOnStringColumn: network server started at $serverHostPort")
    // scalastyle:on println
    val snc = SnappyContext(sc)
    PreparedQueryRoutingSingleNodeSuite.equalityOnStringColumn(snc, s"localhost:$serverHostPort")
  }

  def test_prepStmntManyThreads(): Unit = {
    serverHostPort = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", serverHostPort)
    // scalastyle:off println
    println(s"test2: network server started at $serverHostPort")
    // scalastyle:on println
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://localhost:" + serverHostPort)

    // scalastyle:off println
    println(s"query1_test2: Connected to $serverHostPort")
    val stmt = conn.createStatement()
    createTableAndPopulateSomeData(conn)
    val keepRunning = new AtomicBoolean(true)
    createNThreadsAndPrepExecute(10, keepRunning)
    assert(keepRunning.get())
  }

  def createTableAndPopulateSomeData(conn: Connection): Boolean = {
    val stmnt = conn.createStatement()
    stmnt.execute("create table test(col1 int, col2 int not null, col3 string, " +
        "col4 long, col5 date, col6 timestamp not null, col7 decimal(4, 2))" +
        " using column options()")
    val prepStmntInsert = conn.prepareCall("insert into test values(?, ?, ?, ?, ?, ?, ?)")
    for ( i <- 0 until 10000) {
      prepStmntInsert.setInt(1, i)
      prepStmntInsert.setInt(2, i*2)
      prepStmntInsert.setString(3, s"aaa$i")
      prepStmntInsert.setLong(4, i*100L)
      prepStmntInsert.setString(5, "2019-05-23")
      prepStmntInsert.setString(6, "2019-05-23 00:01:10")
      prepStmntInsert.setDouble(7, 10.22)
      prepStmntInsert.executeUpdate()
    }
    true
  }

  def createNThreadsAndPrepExecute(i: Int, keepRunning: AtomicBoolean): Unit = {
    val queries = scala.Array("select * from test where col1 < ? and col3 = ?",
      "select col1 from test where col2 in (?, ?, ?)",
      "select col1 from test where col1 = ? and (col2 > ? or col2 < ?)",
      "select * from test where col7 = ?",
      "select avg(col1) from test")

    class Runner extends Runnable {
      override def run(): Unit = {
        var cnt = 0;
        val conn = DriverManager.getConnection(
          "jdbc:snappydata://localhost:" + serverHostPort)
        try {
          while ((cnt < 20) && keepRunning.get()) {
            cnt += 1
            val qNum = Random.nextInt(5)
            val prepquery = conn.prepareCall(queries(qNum))
            qNum match {
              case 0 =>
                prepquery.setInt(1, 10)
                prepquery.setString(2, "aaa10")
                prepquery.execute()
                val rs = prepquery.getResultSet
                assert(rs.next())
                rs.close()
              case 1 =>
                prepquery.setInt(1, 100)
                prepquery.setInt(2, 10)
                prepquery.setInt(3, 1000)
                prepquery.execute()
                val rs = prepquery.getResultSet
                assert(rs != null)
                assert(rs.next())
                assert(rs.next())
                assert(rs.next())
                rs.close()
              case 2 =>
                prepquery.setInt(1, 100)
                prepquery.setInt(2, 10)
                prepquery.setInt(3, 1000)
                prepquery.execute()
                val rs = prepquery.getResultSet
                assert(rs != null)
                rs.close()
              case 3 =>
                prepquery.setInt(1, 1000)
                prepquery.execute()
                val rs = prepquery.getResultSet
                assert(rs != null)
                assert(rs.next())
                rs.close()
              case 4 =>
                prepquery.execute()
                val rs = prepquery.getResultSet
                assert(rs.next())
                rs.close()
            }
          }
        } catch {
          case se: SQLException =>
            logInfo(s"exception got = $se with state ${se.getSQLState}", se)
            keepRunning.set(false)
        }
      }
    }
    val allThreads: scala.Array[Thread] = scala.Array.ofDim(i)
    for ( t <- 0 until i) allThreads(t) = new Thread(new Runner())
    for ( t <- 0 until i) allThreads(t).start()
    for ( t <- 0 until i) allThreads(t).join(180000)
    assert(keepRunning.get())
  }
}
