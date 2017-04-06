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

import java.sql.{Connection, DriverManager, ResultSet}

import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.test.dunit.AvailablePortHelper

import org.apache.spark.Logging
import org.apache.spark.sql.SnappyContext

/**
 * Tests for query routing from JDBC client driver.
 */
class PreparedQueryRoutingDUnitTest(val s: String)
    extends ClusterManagerTestBase(s) with Logging {

  // set default batch size for this test
  private val default_chunk_size = GemFireXDUtils.DML_MAX_CHUNK_SIZE
  var serverHostPort = -1
  val tableName = "order_line_col"

  override def tearDown2(): Unit = {
    // reset the chunk size on lead node
    setDMLMaxChunkSize(default_chunk_size)
    super.tearDown2()
  }

  def setDMLMaxChunkSize(size: Long): Unit = {
    GemFireXDUtils.DML_MAX_CHUNK_SIZE = size
  }

  def test1_PrepStatementRouting(): Unit = {
    serverHostPort = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", serverHostPort)
    println(s"test1: network server started at $serverHostPort")

    val snc = SnappyContext(sc)
    snc.sql(s"create table $tableName (ol_int_id  integer," +
        s" ol_int2_id  integer, ol_str_id STRING) using column " +
        "options( partition_by 'ol_int_id, ol_int2_id', buckets '2')")

    insertRows_test1(1000)

    // (1 to 5).foreach(d => query())
    query_test1()
  }

  def insertRows_test1(numRows: Int): Unit = {

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://localhost:" + serverHostPort)

    val rows = (1 to numRows).toSeq
    val stmt = conn.createStatement()
    try {
      var i = 1
      rows.foreach(d => {
        stmt.addBatch(s"insert into $tableName values($i, $i, '$i')")
        i += 1
        if (i % 1000 == 0) {
          stmt.executeBatch()
          i = 0
        }
      })
      stmt.executeBatch()
      println(s"committed $numRows rows")
    } finally {
      stmt.close()
      conn.close()
    }
  }

  def verifyQuery(qryTest: String, prep_rs: ResultSet, stmt_rs: ResultSet): Unit = {
    val builder = StringBuilder.newBuilder
    var index = 0
    var assertionFailed = false
    while (prep_rs.next() && stmt_rs.next()) {
      val prep_i = prep_rs.getInt(1)
      val prep_j = prep_rs.getInt(2)
      val prep_s = prep_rs.getString(3)

      val stmt_i = stmt_rs.getInt(1)
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

      val prep_i = prep_rs.getInt(1)
      val prep_j = prep_rs.getInt(2)
      val prep_s = prep_rs.getString(3)
      builder.append(s"$qryTest Prep: row($index) $prep_i $prep_j $prep_s ").append("\n")
    }

    while (stmt_rs.next()) {
      if (!assertionFailed) {
        builder.append(s"Assertion failed at index=$index").append("\n")
        assertionFailed = true
      }

      val stmt_i = stmt_rs.getInt(1)
      val stmt_j = stmt_rs.getInt(2)
      val stmt_s = stmt_rs.getString(3)
      builder.append(s"$qryTest Stmt: row($index) $stmt_i $stmt_j $stmt_s ").append("\n")
    }

    if (assertionFailed) {
      println(builder.toString())
    }

    assert(!assertionFailed)
  }

  def query1_like_clause(limitClause: String): Unit = {
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://localhost:" + serverHostPort)

    println(s"query1_like_clause: Connected to $serverHostPort")

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

      val prepStatement = conn.prepareStatement(qry)
      prepStatement.setInt(1, 500)
      prepStatement.setString(2, "%0")
      val rs = prepStatement.executeQuery

      val qry2 = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ol_int_id < 500 " +
          s" and ol_int2_id > 100 " +
          s" and ol_str_id LIKE '%0' " +
          s" $limitClause" +
          s""
      val rs2 = stmt.executeQuery(qry2)

      verifyQuery("query1", rs, rs2)
      rs.close()
      rs2.close()

      // Thread.sleep(1000000)

    } finally {
      stmt.close()
      if (prepStatement != null) prepStatement.close()
      conn.close()
    }
  }

  def query2_in_clause(limitClause: String): Unit = {
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://localhost:" + serverHostPort)

    println(s"query2_in_clause: Connected to $serverHostPort")

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

      val prepStatement = conn.prepareStatement(qry)
      prepStatement.setInt(1, 500)
      prepStatement.setInt(2, 100)
      prepStatement.setInt(3, 200)
      prepStatement.setInt(4, 300)
      prepStatement.setString(5, "%0")
      val rs = prepStatement.executeQuery

      val qry2 = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ol_int_id < 500 " +
          s" and ol_int2_id in (100, 200, 300) " +
          s" and ol_str_id LIKE '%0' " +
          s" $limitClause" +
          s""
      val rs2 = stmt.executeQuery(qry2)

      verifyQuery("query3", rs, rs2)
      rs.close()
      rs2.close()

      // Thread.sleep(1000000)

    } finally {
      stmt.close()
      if (prepStatement != null) prepStatement.close()
      conn.close()
    }
  }

  def query_test1(): Unit = {
    query1_like_clause("limit 20")
    query1_like_clause("")
    query2_in_clause("limit 20")
    query2_in_clause("")
  }
}
