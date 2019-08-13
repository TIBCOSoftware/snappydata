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

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}

import com.pivotal.gemfirexd.TestUtil
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.{SnappyFunSuite, SnappyTableStatsProviderService}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.{SnappyContext, SnappySession}
import org.apache.spark.{Logging, SparkConf}

class PreparedQueryRoutingSingleNodeSuite extends SnappyFunSuite with BeforeAndAfterAll {

  private val default_chunk_size = GemFireXDUtils.DML_MAX_CHUNK_SIZE

  protected override def newSparkConf(addOn: SparkConf => SparkConf): SparkConf = {
    /**
      * Setting local[n] here actually supposed to affect number of reservoir created
      * while sampling.
      *
      * Change of 'n' will influence results if they are dependent on weights - derived
      * from hidden column in sample table.
      */
    new org.apache.spark.SparkConf().setAppName("PreparedQueryRoutingSingleNodeSuite")
        .setMaster("local[6]").
      set(io.snappydata.Property.TestDisableCodeGenFlag.name, "true").
      set(io.snappydata.Property.UseOptimizedHashAggregateForSingleKey.name, "true")
        // .set("spark.logConf", "true")
        // .set("mcast-port", "4958")
  }

  override def beforeAll(): Unit = {
    // System.setProperty("org.codehaus.janino.source_debugging.enable", "true")
    // System.setProperty("spark.testing", "true")
    super.beforeAll()
    // reducing DML chunk size size to force lead node to send
    // results in multiple batches
    setDMLMaxChunkSize(50L)
  }

  override def afterAll(): Unit = {
    // System.clearProperty("org.codehaus.janino.source_debugging.enable")
    // System.clearProperty("spark.testing")
    setDMLMaxChunkSize(default_chunk_size)
    super.afterAll()
  }

  def setDMLMaxChunkSize(size: Long): Unit = {
    GemFireXDUtils.DML_MAX_CHUNK_SIZE = size
  }

  def query0(tableName: String, serverHostPort: String): Unit = {
    // sc.setLogLevel("TRACE")
    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)

    var prepStatement: java.sql.PreparedStatement = null
    var prepStatement1: java.sql.PreparedStatement = null
    var prepStatement2: java.sql.PreparedStatement = null
    var prepStatement3: java.sql.PreparedStatement = null
    var prepStatement4: java.sql.PreparedStatement = null
    var prepStatement5: java.sql.PreparedStatement = null
    try {
      val qry = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ol_int_id < ? " +
          s" and ol_int2_id in (?, ?, ?) " +
          s" and ol_str_id LIKE ? " +
          s" limit 20" +
          s""

      prepStatement = conn.prepareStatement(qry)
      prepStatement.setInt(1, 500)
      prepStatement.setInt(2, 100)
      prepStatement.setInt(3, 200)
      prepStatement.setInt(4, 300)
      prepStatement.setString(5, "%0")
      PreparedQueryRoutingSingleNodeSuite.verifyResults("qry-1", prepStatement.executeQuery,
        Array(100, 200, 300), 1)

      prepStatement.setInt(1, 900)
      prepStatement.setInt(2, 600)
      prepStatement.setInt(3, 700)
      prepStatement.setInt(4, 800)
      prepStatement.setString(5, "%0")
      PreparedQueryRoutingSingleNodeSuite.verifyResults("qry-2", prepStatement.executeQuery,
        Array(600, 700, 800), 1)

      prepStatement.setInt(1, 900)
      prepStatement.setInt(2, 600)
      prepStatement.setInt(3, 700)
      prepStatement.setInt(4, 800)
      prepStatement.setString(5, "%0%")
      PreparedQueryRoutingSingleNodeSuite.verifyResults("qry-2.2", prepStatement.executeQuery,
        Array(600, 700, 800), 2)

      val qry1 = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ol_int_id < ? " +
          s" and ol_int2_id in (?, ?, 300) " +
          s" limit 20" +
          s""

      prepStatement1 = conn.prepareStatement(qry1)
      prepStatement1.setInt(1, 500)
      prepStatement1.setInt(2, 100)
      prepStatement1.setInt(3, 200)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("qry1-1", prepStatement1.executeQuery,
        Array(100, 200, 300), 3)

      prepStatement1.setInt(1, 500)
      prepStatement1.setInt(2, 100)
      prepStatement1.setInt(3, 400)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("qry1-2", prepStatement1.executeQuery,
        Array(100, 400, 300), 3)

      val qry2 = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ol_int_id < ? " +
          s" and ol_int2_id in (?, ?, 800) " +
          s" limit 20" +
          s""
      prepStatement2 = conn.prepareStatement(qry2)
      prepStatement2.setInt(1, 900)
      prepStatement2.setInt(2, 600)
      prepStatement2.setInt(3, 700)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("qry2-1", prepStatement2.executeQuery,
        Array(600, 700, 800), 3)

      prepStatement2.setInt(1, 900)
      prepStatement2.setInt(2, 400)
      prepStatement2.setInt(3, 500)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("qry2-2", prepStatement2.executeQuery,
        Array(400, 500, 800), 3)

      val qry3 = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ol_int_id < ? " +
          s" and ol_int2_id in (?, ?, ?) " +
          s" limit 20" +
          s""

      prepStatement3 = conn.prepareStatement(qry3)
      prepStatement3.setInt(1, 500)
      prepStatement3.setInt(2, 100)
      prepStatement3.setInt(3, 200)
      prepStatement3.setInt(4, 300)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("qry3-1", prepStatement3.executeQuery,
        Array(100, 200, 300), 3)

      prepStatement3.setInt(1, 900)
      prepStatement3.setInt(2, 600)
      prepStatement3.setInt(3, 700)
      prepStatement3.setInt(4, 800)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("qry3-2", prepStatement3.executeQuery,
        Array(600, 700, 800), 3)

      val qry4 = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ? > ol_int_id " +
          s" and ol_int2_id in (?, ?, ?) " +
          s" limit 20" +
          s""

      prepStatement4 = conn.prepareStatement(qry4)
      prepStatement4.setInt(1, 500)
      prepStatement4.setInt(2, 100)
      prepStatement4.setInt(3, 200)
      prepStatement4.setInt(4, 300)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("qry4-1", prepStatement4.executeQuery,
        Array(100, 200, 300), 4)

      prepStatement4.setInt(1, 900)
      prepStatement4.setInt(2, 600)
      prepStatement4.setInt(3, 700)
      prepStatement4.setInt(4, 800)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("qry4-2", prepStatement4.executeQuery,
        Array(600, 700, 800), 4)

      val qry5 = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where cast(ol_int_id as double) < ? " +
          s" and ol_int2_id in (?, ?, ?) " +
          s" limit 20" +
          s""

      prepStatement5 = conn.prepareStatement(qry5)
      prepStatement5.setDouble(1, 500.01)
      prepStatement5.setInt(2, 100)
      prepStatement5.setInt(3, 200)
      prepStatement5.setInt(4, 300)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("qry5-1", prepStatement5.executeQuery,
        Array(100, 200, 300), 5)

      prepStatement5.setDouble(1, 900.01)
      prepStatement5.setInt(2, 600)
      prepStatement5.setInt(3, 700)
      prepStatement5.setInt(4, 800)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("qry5-2", prepStatement5.executeQuery,
        Array(600, 700, 800), 5)

      // Thread.sleep(1000000)
    } finally {
      if (prepStatement != null) prepStatement.close()
      if (prepStatement1 != null) prepStatement1.close()
      if (prepStatement2 != null) prepStatement2.close()
      if (prepStatement3 != null) prepStatement3.close()
      if (prepStatement4 != null) prepStatement4.close()
      conn.close()
    }
  }

  test("test Prepared Statement via JDBC") {
    SnappySession.getPlanCache.invalidateAll()
    assert(SnappySession.getPlanCache.asMap().size() == 0)
    SnappyTableStatsProviderService.TEST_SUSPEND_CACHE_INVALIDATION = true
    val tableName = "order_line_col"
    try {
      snc.sql(s"create table $tableName (ol_int_id  integer," +
          s" ol_int2_id  integer, ol_str_id STRING) using column " +
          "options( partition_by 'ol_int_id, ol_int2_id', buckets '2')")


      val serverHostPort = TestUtil.startNetServer()
      // logInfo("network server started")
      PreparedQueryRoutingSingleNodeSuite.insertRows(tableName, 1000, serverHostPort)
      query0(tableName, serverHostPort)
    } finally {
      snc.sql(s"drop table $tableName")
      SnappyTableStatsProviderService.TEST_SUSPEND_CACHE_INVALIDATION = false
    }
  }

  test("test Metadata for Prepared Statement via JDBC") {
    SnappySession.getPlanCache.invalidateAll()
    assert(SnappySession.getPlanCache.asMap().size() == 0)
    SnappyTableStatsProviderService.TEST_SUSPEND_CACHE_INVALIDATION = true
    val tableName = "order_line_col"
    try {
      snc.sql(s"create table $tableName (ol_int_id  integer," +
          s" ol_int2_id  integer, ol_str_id STRING) using column " +
          "options( partition_by 'ol_int_id, ol_int2_id', buckets '2')")

      val serverHostPort = TestUtil.startNetServer()
      // logInfo("network server started")
      PreparedQueryRoutingSingleNodeSuite.insertRows(tableName, 100, serverHostPort)
      query6(tableName, serverHostPort)
      query7(tableName, serverHostPort)
      query8(tableName, serverHostPort)
      query9(tableName, serverHostPort)
      query10(tableName, serverHostPort)
      query11(tableName, serverHostPort)
      query12(tableName, serverHostPort)
    } finally {
      snc.sql(s"drop table $tableName")
      SnappyTableStatsProviderService.TEST_SUSPEND_CACHE_INVALIDATION = false
    }
  }

  def query1(tableName1: String, tableName2: String, serverHostPort: String): Unit = {
    // sc.setLogLevel("TRACE")
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)

    var prepStatement: java.sql.PreparedStatement = null
    try {
      val qry = s"select ol_1_int_id, ol_2_int2_id, ol_1_str_id " +
          s" from $tableName1 A inner join $tableName2 B " +
          s" on A.ol_1_int_id = B.ol_2_int_id " +
          s" where ol_1_int2_id < ? " +
          s" and ol_2_int2_id in (?, ?, ?) " +
          s" limit 20" +
          s""

      prepStatement = conn.prepareStatement(qry)
      prepStatement.setInt(1, 500)
      prepStatement.setInt(2, 100)
      prepStatement.setInt(3, 200)
      prepStatement.setInt(4, 300)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("query1-1", prepStatement.executeQuery,
        Array(100, 200, 300), 1)

      prepStatement.setInt(1, 900)
      prepStatement.setInt(2, 600)
      prepStatement.setInt(3, 700)
      prepStatement.setInt(4, 800)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("query1-2", prepStatement.executeQuery,
        Array(600, 700, 800), 1)

      // Thread.sleep(1000000)
    } finally {
      if (prepStatement != null) prepStatement.close()
      conn.close()
    }
  }

  def query2(tableName1: String, tableName2: String, serverHostPort: String): Unit = {
    // sc.setLogLevel("TRACE")
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)

    var prepStatement: java.sql.PreparedStatement = null
    try {
      val qry = s"select sum(ol_1_int_id) s, 0, 'a' " +
          s" from $tableName1 " +
          s" group by ol_1_int2_id having sum(ol_1_int_id) in (?, ?, ?) " +
          s" limit 20" +
          s""

      prepStatement = conn.prepareStatement(qry)
      prepStatement.setInt(1, 400)
      prepStatement.setInt(2, 300)
      prepStatement.setInt(3, 200)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("query2-1", prepStatement.executeQuery,
        Array(400, 200, 300), 2)

      prepStatement.setInt(1, 600)
      prepStatement.setInt(2, 800)
      prepStatement.setInt(3, 700)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("query2-2", prepStatement.executeQuery,
        Array(600, 700, 800), 2)

      // Thread.sleep(1000000)
    } finally {
      if (prepStatement != null) prepStatement.close()
      conn.close()
    }
  }

  def query3(tableName1: String, tableName2: String, serverHostPort: String): Unit = {
    // sc.setLogLevel("TRACE")
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)

    var prepStatement: java.sql.PreparedStatement = null
    try {
      val qry = s"select ol_1_int_id, ol_2_int2_id, ol_1_str_id " +
          s" from $tableName1 A inner join $tableName2 B " +
          s" on A.ol_1_int_id = B.ol_2_int_id " +
          s" where ol_1_int2_id < ? " +
          s" and ol_2_int2_id = case when ol_2_int2_id < ? then ? else ? end " +
          s" limit 20" +
          s""

      prepStatement = conn.prepareStatement(qry)
      prepStatement.setInt(1, 500)
      prepStatement.setInt(2, 300)
      prepStatement.setInt(3, 200)
      prepStatement.setInt(4, 400)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("query3-1", prepStatement.executeQuery,
        Array(200, 400), 3)

      prepStatement.setInt(1, 900)
      prepStatement.setInt(2, 700)
      prepStatement.setInt(3, 600)
      prepStatement.setInt(4, 800)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("query3-2", prepStatement.executeQuery,
        Array(600, 800), 3)

      // Thread.sleep(1000000)
    } finally {
      if (prepStatement != null) prepStatement.close()
      conn.close()
    }
  }

  def query4(tableName1: String, tableName2: String, serverHostPort: String): Unit = {
    // sc.setLogLevel("TRACE")
    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)

    var prepStatement: java.sql.PreparedStatement = null
    try {
      val qry = s"select ol_1_int_id, ol_1_int2_id, ol_1_str_id " +
          s" from $tableName1 " +
          s" where ol_1_int2_id = ? " +
          s" union " +
          s" select ol_2_int_id, ol_2_int2_id, ol_2_str_id " +
          s" from $tableName2 " +
          s" where ol_2_int2_id in (?, ?, ?) " +
          s" limit 20" +
          s""

      prepStatement = conn.prepareStatement(qry)
      prepStatement.setInt(1, 100)
      prepStatement.setInt(2, 300)
      prepStatement.setInt(3, 200)
      prepStatement.setInt(4, 400)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("query4-1", prepStatement.executeQuery,
        Array(100, 200, 300, 400), 4)

      prepStatement.setInt(1, 900)
      prepStatement.setInt(2, 600)
      prepStatement.setInt(3, 700)
      prepStatement.setInt(4, 800)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("query4-2", prepStatement.executeQuery,
        Array(900, 600, 700, 800), 4)

      // Thread.sleep(1000000)
    } finally {
      if (prepStatement != null) prepStatement.close()
      conn.close()
    }
  }

  def query5(tableName1: String, tableName2: String, serverHostPort: String): Unit = {
    // sc.setLogLevel("TRACE")
    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)

    var prepStatement: java.sql.PreparedStatement = null
    try {
      val qry = s"select ol_1_int_id, ol_1_int2_id, ol_1_str_id " +
          s" from $tableName1 " +
          s" where ol_1_int_id < ? " +
          s" and ol_1_int2_id in (" +
          s"select ol_2_int_id " +
          s" from $tableName2 " +
          s" where ol_2_int_id in (?, ?, ?) " +
          s") " +
          s" limit 20" +
          s""

      prepStatement = conn.prepareStatement(qry)
      prepStatement.setInt(1, 500)
      prepStatement.setInt(2, 100)
      prepStatement.setInt(3, 200)
      prepStatement.setInt(4, 300)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("query5-1", prepStatement.executeQuery,
        Array(100, 200, 300), 4)

      prepStatement.setInt(1, 900)
      prepStatement.setInt(2, 600)
      prepStatement.setInt(3, 700)
      prepStatement.setInt(4, 800)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("query5-2", prepStatement.executeQuery,
        Array(600, 700, 800), 4)

      // Thread.sleep(1000000)
    } finally {
      if (prepStatement != null) prepStatement.close()
      conn.close()
    }
  }

  def query6(tableName: String, serverHostPort: String): Unit = {
    // sc.setLogLevel("TRACE")
    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)

    var prepStatement: java.sql.PreparedStatement = null
    try {
      val qry = s"select count(ol_int_id) as a , sum(ol_int2_id) as b, ol_str_id as c " +
          s" from $tableName " +
          s" where ol_int_id < ? " +
          s" group by ol_str_id " +
          s" limit 20" +
          s""

      prepStatement = conn.prepareStatement(qry)
      prepStatement.setInt(1, 500)
      assert(prepStatement.getMetaData().getColumnCount() == 3)

      val rs: ResultSet = prepStatement.executeQuery
      assert(prepStatement.getMetaData().getColumnCount() == 3)
      assert(rs.getMetaData().getColumnCount() == 3)

      var index = 0
      while (rs.next()) {
        val i = rs.getInt(1)
        // val j = rs.getInt(2)
        // val s = rs.getString(3)
        // logInfo(s"row($index) $i $j $s ")
        index += 1
      }
      assert(index == 20)

      // logInfo(s"$qryName Number of rows read " + index)
      rs.close()
      // Thread.sleep(1000000)
    } finally {
      if (prepStatement != null) prepStatement.close()
      conn.close()
    }
  }

  def query7(tableName: String, serverHostPort: String): Unit = {
    // sc.setLogLevel("TRACE")
    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)

    var prepStatement: java.sql.PreparedStatement = null
    try {
      val qry = s"select count(ol_int_id) , sum(ol_int2_id), ol_str_id  " +
          s" from $tableName " +
          s" where ol_int_id < ? " +
          s" group by ol_str_id " +
          s""

      prepStatement = conn.prepareStatement(qry)
      prepStatement.setInt(1, 500)
      assert(prepStatement.getMetaData().getColumnCount() == 3)

      val rs: ResultSet = prepStatement.executeQuery
      assert(prepStatement.getMetaData().getColumnCount() == 3)
      assert(rs.getMetaData().getColumnCount() == 3)

      var index = 0
      while (rs.next()) {
        val i = rs.getInt(1)
        // val j = rs.getInt(2)
        // val s = rs.getString(3)
        // logInfo(s"row($index) $i $j $s ")
        index += 1
      }
      assert(index == 100)

      // logInfo(s"$qryName Number of rows read " + index)
      rs.close()
      // Thread.sleep(1000000)
    } finally {
      if (prepStatement != null) prepStatement.close()
      conn.close()
    }
  }

  def query8(tableName: String, serverHostPort: String): Unit = {
    // sc.setLogLevel("TRACE")
    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)

    var prepStatement: java.sql.PreparedStatement = null
    try {
      val qry = s"select ol_int_id as a, ol_int2_id as b, ol_int_id as c" +
          s" from $tableName " +
          s""

      prepStatement = conn.prepareStatement(qry)
      assert(prepStatement.getMetaData().getColumnCount() == 3)

      val rs: ResultSet = prepStatement.executeQuery
      assert(prepStatement.getMetaData().getColumnCount() == 3)
      assert(rs.getMetaData().getColumnCount() == 3)

      var index = 0
      while (rs.next()) {
        val i = rs.getInt(1)
        // val j = rs.getInt(2)
        // val s = rs.getString(3)
        // logInfo(s"row($index) $i $j $s ")
        index += 1
      }
      assert(index == 100)

      // logInfo(s"$qryName Number of rows read " + index)
      rs.close()
      // Thread.sleep(1000000)
    } finally {
      if (prepStatement != null) prepStatement.close()
      conn.close()
    }
  }

  def query9(tableName: String, serverHostPort: String): Unit = {
    // sc.setLogLevel("TRACE")
    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)

    var prepStatement: java.sql.PreparedStatement = null
    try {
      val qry = s"select ol_int_id, ol_int2_id, ol_int_id" +
          s" from $tableName " +
          s" limit 20" +
          s""

      prepStatement = conn.prepareStatement(qry)
      assert(prepStatement.getMetaData().getColumnCount() == 3)

      val rs: ResultSet = prepStatement.executeQuery
      assert(prepStatement.getMetaData().getColumnCount() == 3)
      assert(rs.getMetaData().getColumnCount() == 3)

      var index = 0
      while (rs.next()) {
        val i = rs.getInt(1)
        // val j = rs.getInt(2)
        // val s = rs.getString(3)
        // logInfo(s"row($index) $i $j $s ")
        index += 1
      }
      assert(index == 20)

      // logInfo(s"$qryName Number of rows read " + index)
      rs.close()
      // Thread.sleep(1000000)
    } finally {
      if (prepStatement != null) prepStatement.close()
      conn.close()
    }
  }

  def query10(tableName: String, serverHostPort: String): Unit = {
    // sc.setLogLevel("TRACE")
    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)

    var prepStatement: java.sql.PreparedStatement = null
    try {
      val qry = s"select ol_int_id as a, ol_int2_id as b, ol_int_id as c" +
          s" from $tableName " +
          s" where ol_int_id < ? " +
          s" limit 20" +
          s""

      prepStatement = conn.prepareStatement(qry)
      prepStatement.setInt(1, 500)
      assert(prepStatement.getMetaData().getColumnCount() == 3)

      val rs: ResultSet = prepStatement.executeQuery
      assert(prepStatement.getMetaData().getColumnCount() == 3)
      assert(rs.getMetaData().getColumnCount() == 3)

      var index = 0
      while (rs.next()) {
        val i = rs.getInt(1)
        // val j = rs.getInt(2)
        // val s = rs.getString(3)
        // logInfo(s"row($index) $i $j $s ")
        index += 1
      }
      assert(index == 20)

      // logInfo(s"$qryName Number of rows read " + index)
      rs.close()
      // Thread.sleep(1000000)
    } finally {
      if (prepStatement != null) prepStatement.close()
      conn.close()
    }
  }

  def query11(tableName: String, serverHostPort: String): Unit = {
    // sc.setLogLevel("TRACE")
    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)

    var prepStatement: java.sql.PreparedStatement = null
    try {
      val qry = s"select count(distinct ol_int_id) , sum(ol_int2_id), ol_str_id  " +
          s" from $tableName " +
          s" group by ol_str_id " +
          s""

      prepStatement = conn.prepareStatement(qry)
      assert(prepStatement.getMetaData().getColumnCount() == 3)

      val rs: ResultSet = prepStatement.executeQuery
      assert(prepStatement.getMetaData().getColumnCount() == 3)
      assert(rs.getMetaData().getColumnCount() == 3)

      var index = 0
      while (rs.next()) {
        val i = rs.getInt(1)
        // val j = rs.getInt(2)
        // val s = rs.getString(3)
        // logInfo(s"row($index) $i $j $s ")
        index += 1
      }
      assert(index == 100)

      // logInfo(s"$qryName Number of rows read " + index)
      rs.close()
      // Thread.sleep(1000000)
    } finally {
      if (prepStatement != null) prepStatement.close()
      conn.close()
    }
  }

  def query12(tableName: String, serverHostPort: String): Unit = {
    // sc.setLogLevel("TRACE")
    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)

    var prepStatement: java.sql.PreparedStatement = null
    try {
      val qry = s"select distinct(ol_int_id)  " +
          s" from $tableName " +
          s" limit 20" +
          s""

      prepStatement = conn.prepareStatement(qry)
      assert(prepStatement.getMetaData().getColumnCount() == 1)

      val rs: ResultSet = prepStatement.executeQuery
      assert(prepStatement.getMetaData().getColumnCount() == 1)
      assert(rs.getMetaData().getColumnCount() == 1)

      var index = 0
      while (rs.next()) {
        val i = rs.getInt(1)
        // val j = rs.getInt(2)
        // val s = rs.getString(3)
        // logInfo(s"row($index) $i $j $s ")
        index += 1
      }
      assert(index == 20)

      // logInfo(s"$qryName Number of rows read " + index)
      rs.close()
      // Thread.sleep(1000000)
    } finally {
      if (prepStatement != null) prepStatement.close()
      conn.close()
    }
  }

  test("test Join, SubQuery and Aggragtes") {
    SnappySession.getPlanCache.invalidateAll()
    assert(SnappySession.getPlanCache.asMap().size() == 0)
    SnappyTableStatsProviderService.TEST_SUSPEND_CACHE_INVALIDATION = true
    try {
      val tableName1 = "order_line_1_col"
      val tableName2 = "order_line_2_col"
      snc.sql(s"create table $tableName1 (ol_1_int_id  integer," +
          s" ol_1_int2_id  integer, ol_1_str_id STRING) using column " +
          "options( partition_by 'ol_1_int_id, ol_1_int2_id', buckets '2')")

      snc.sql(s"create table $tableName2 (ol_2_int_id  integer," +
          s" ol_2_int2_id  integer, ol_2_str_id STRING) using column " +
          "options( partition_by 'ol_2_int_id, ol_2_int2_id', buckets '2')")


      val serverHostPort = TestUtil.startNetServer()
      // logInfo("network server started")
      PreparedQueryRoutingSingleNodeSuite.insertRows(tableName1, 1000, serverHostPort)
      PreparedQueryRoutingSingleNodeSuite.insertRows(tableName2, 1000, serverHostPort)
      query1(tableName1, tableName2, serverHostPort)
      query2(tableName1, tableName2, serverHostPort)
      query3(tableName1, tableName2, serverHostPort)
      query4(tableName1, tableName2, serverHostPort)
      query5(tableName1, tableName2, serverHostPort)
    } finally {
      SnappyTableStatsProviderService.TEST_SUSPEND_CACHE_INVALIDATION = false
    }
  }

  test("update delete on column table") {
    val snc = this.snc
    val serverHostPort = TestUtil.startNetServer()
    // logInfo("network server started")
    PreparedQueryRoutingSingleNodeSuite.updateDeleteOnColumnTable(snc, serverHostPort)
  }

  test("SNAP-1981: Equality on string columns") {
    val snc = this.snc
    val serverHostPort = TestUtil.startNetServer()
    // logInfo("network server started")
    PreparedQueryRoutingSingleNodeSuite.equalityOnStringColumn(snc, serverHostPort)
  }

  test("SNAP-1994 Test functions and expressions") {
    SnappyTableStatsProviderService.TEST_SUSPEND_CACHE_INVALIDATION = true
    try {
      testSNAP1994()
    } finally {
      SnappyTableStatsProviderService.TEST_SUSPEND_CACHE_INVALIDATION = false
    }
  }

  private def testSNAP1994(): Unit = {
    snc.sql(s"Drop Table if exists double_tab")
    snc.sql(s"Create Table double_tab (a INT, d Double, s String) " +
        "using column options()")
    snc.sql(s"insert into double_tab values(1, 1.111111, '1a'), (2, 2.222222, '2b')," +
        s" (3, 3.33333, '3c')")
    snc.sql(s"Create Table double_tab_2 (a INT, d Double, s String) " +
        "using column options()")
    snc.sql(s"insert into double_tab_2 values(1, 1.111111, '1a'), (2, 2.222222, '2b')," +
        s" (3, 3.33333, '3c')")
    val cacheMap = SnappySession.getPlanCache.asMap()
    cacheMap.clear()
    assert( cacheMap.size() == 0)
    val serverHostPort = TestUtil.startNetServer()
    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)
    try {
      def close(prepStatement: java.sql.PreparedStatement): Unit = if (prepStatement != null) {
        prepStatement.close()
      }
      val prepStatement0 = conn.prepareStatement(s"select * from double_tab" +
          s" where round(d, 2) < ?")
      assert(cacheMap.size() == 0)
      prepStatement0.setDouble(1, 3.33)
      var update = prepStatement0.executeQuery()
      var index = 0
      while (update.next()) {
        val i = update.getInt(1)
        val j = update.getBigDecimal(2)
        logInfo(s"1-row($index) $i $j")
        index += 1
        assert(i == 1 || i == 2)
      }
      logInfo(s"1-Number of rows read " + index)
      assert(index == 2)
      assert(cacheMap.size() == 1)

      prepStatement0.setDouble(1, 4.3333)
      update = prepStatement0.executeQuery()
      index = 0
      while (update.next()) {
        val i = update.getInt(1)
        val j = update.getBigDecimal(2)
        logInfo(s"2-row($index) $i $j")
        index += 1
        assert(i == 1 || i == 2 || i == 3)
      }
      logInfo(s"2-Number of rows read " + index)
      assert(index == 3)
      assert(cacheMap.size() == 1)
      close(prepStatement0)

      val prepStatement1 = conn.prepareStatement(s"select a + ?, d from double_tab")
      assert(cacheMap.size() == 1)
      prepStatement1.setInt(1, 2)
      update = prepStatement1.executeQuery()
      index = 0
      while (update.next()) {
        val i = update.getInt(1)
        val j = update.getBigDecimal(2)
        logInfo(s"3-row($index) $i $j")
        index += 1
        assert(i > 2 && i < 6)
      }
      logInfo(s"3-Number of rows read " + index)
      assert(index == 3)
      assert(cacheMap.size() == 2)

      prepStatement1.setInt(1, 3)
      update = prepStatement1.executeQuery()
      index = 0
      while (update.next()) {
        val i = update.getInt(1)
        val j = update.getBigDecimal(2)
        logInfo(s"4-row($index) $i $j")
        index += 1
        assert(i > 3 && i < 7)
      }
      logInfo(s"4-Number of rows read " + index)
      assert(index == 3)
      assert(cacheMap.size() == 2)
      close(prepStatement1)

      val prepStatement2 = conn.prepareStatement(s"select a," +
          s" d from double_tab where UPPER(s) = ?")
      assert(cacheMap.size() == 2)
      prepStatement2.setString(1, "1A")
      update = prepStatement2.executeQuery()
      index = 0
      while (update.next()) {
        val i = update.getInt(1)
        val j = update.getString(2)
        logInfo(s"5-row($index) $i $j")
        index += 1
        assert(i == 1)
      }
      logInfo(s"5-Number of rows read " + index)
      assert(index == 1)
      assert(cacheMap.size() == 3)

      prepStatement2.setString(1, "2B")
      update = prepStatement2.executeQuery()
      index = 0
      while (update.next()) {
        val i = update.getInt(1)
        val j = update.getString(2)
        logInfo(s"6-row($index) $i $j")
        index += 1
        assert(i == 2)
      }
      logInfo(s"6-Number of rows read " + index)
      assert(index == 1)
      assert(cacheMap.size() == 3)
      close(prepStatement2)

      val prepStatement3: PreparedStatement = conn.prepareStatement(s"select * from double_tab t1"
          + s" inner join double_tab_2 t2 on t1.a = t2.a where t1.d > ? and " +
          s" t1.a in ( select a from double_tab_2 where d < ? )")
      assert(cacheMap.size() == 3)
      // Anyway TableStatsProviderService.aggregateStats clear stats
      // So clearing here for better assertion in testing
      cacheMap.clear()
      prepStatement3.setInt(1, 1)
      prepStatement3.setInt(2, 3)
      update = prepStatement3.executeQuery()
      index = 0
      while (update.next()) {
        val i = update.getInt(1)
        logInfo(s"7-row($index) $i")
        index += 1
        assert(i == 1 || i == 2)
      }
      logInfo(s"7-Number of rows read " + index)
      assert(index == 2)
      assert(cacheMap.size() == 0)

      prepStatement3.setInt(1, 2)
      prepStatement3.setInt(2, 4)
      update = prepStatement3.executeQuery()
      index = 0
      while (update.next()) {
        val i = update.getInt(1)
        logInfo(s"8-row($index) $i")
        index += 1
        assert(i == 2 || i == 3)
      }
      logInfo(s"8-Number of rows read " + index)
      assert(index == 2)
      assert(cacheMap.size() == 0)
      close(prepStatement3)

      val prepStatement4 = conn.prepareStatement(s"select * from double_tab" +
          s" where round(d, 2) < round(3.33, 2)")
      assert(cacheMap.size() == 0)

      update = prepStatement4.executeQuery()
      index = 0
      while (update.next()) {
        val i = update.getInt(1)
        val j = update.getBigDecimal(2)
        logInfo(s"9-row($index) $i $j")
        index += 1
        assert(i == 1 || i == 2)
      }
      logInfo(s"9-Number of rows read " + index)
      assert(index == 2)
      assert(cacheMap.size() == 1)
      close(prepStatement4)

      val prepStatement5 = conn.prepareStatement(s"select a," +
          s" nvl(d, ?) from double_tab where UPPER(s) = ?")
      assert(cacheMap.size() == 1)
      prepStatement5.setInt(1, 2)
      prepStatement5.setString(2, "1A")
      update = prepStatement5.executeQuery()
      index = 0
      while (update.next()) {
        val i = update.getInt(1)
        val j = update.getBigDecimal(2)
        logInfo(s"10-row($index) $i $j")
        index += 1
        assert(i == 1)
      }
      logInfo(s"10-Number of rows read " + index)
      assert(index == 1)
      assert(cacheMap.size() == 2)
      close(prepStatement5)
      try {
        val faultyPrepStatement = conn.prepareStatement(s"select * from double_tab" +
            s" where round(d, ?) < round(?, ?)")
        fail("PreparedStatement creation should have failed")
      } catch {
        case sqle: SQLException
          if sqle.getMessage.indexOf("cannot have parameterized argument") != -1 =>
        case x: Throwable => throw x
      }
    } finally {
      conn.close()
    }
  }

  test("Test bug SNAP-2446") {
    var conn: Connection = null
    val ddlStr = s"create table MAP(MAP_CONNECTION_ID BIGINT NOT NULL," +
        s" SOURCE_DATA_CONNECTION_CODE INT NOT NULL," +
        s" DESTINATION_DATA_CONNECTION_CODE INT NOT NULL," +
        s" ACTIVE_FLAG BOOLEAN, PRIMARY KEY(MAP_CONNECTION_ID)) USING ROW OPTIONS()"

    snc.sql(ddlStr)
    snc.sql(s"insert into MAP values (-28416, 19375, 424345, true)")
    val serverHostPort = TestUtil.startNetServer()
    conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort + "/route-query=false/")

    val sqlText = s"SELECT DESTINATION_DATA_CONNECTION_CODE," +
        "SOURCE_DATA_CONNECTION_CODE,ACTIVE_FLAG FROM MAP"

    val rs2 = conn.createStatement().executeQuery(sqlText)
    assert(rs2.next())
    assert(rs2.getBoolean(3))
    conn.close()
  }

  test("Test broadcast hash joins and scalar sub-queries") {
    SnappyTableStatsProviderService.TEST_SUSPEND_CACHE_INVALIDATION = true
    var conn: Connection = null
    try {
      val ddlStr = "(YearI INT," + // NOT NULL
          "MonthI INT," + // NOT NULL
          "DayOfMonth INT," + // NOT NULL
          "DayOfWeek INT," + // NOT NULL
          "DepTime INT," +
          "CRSDepTime INT," +
          "ArrTime INT," +
          "CRSArrTime INT," +
          "UniqueCarrier VARCHAR(20)," + // NOT NULL
          "FlightNum INT," +
          "TailNum VARCHAR(20)," +
          "ActualElapsedTime INT," +
          "CRSElapsedTime INT," +
          "AirTime INT," +
          "ArrDelay INT," +
          "DepDelay INT," +
          "Origin VARCHAR(20)," +
          "Dest VARCHAR(20)," +
          "Distance INT," +
          "TaxiIn INT," +
          "TaxiOut INT," +
          "Cancelled INT," +
          "CancellationCode VARCHAR(20)," +
          "Diverted INT," +
          "CarrierDelay INT," +
          "WeatherDelay INT," +
          "NASDelay INT," +
          "SecurityDelay INT," +
          "LateAircraftDelay INT," +
          "ArrDelaySlot INT)"

      val hfile: String = getClass.getResource("/2015.parquet").getPath
      val snContext = snc
      snContext.sql("set spark.sql.shuffle.partitions=6")

      val airlineDF = snContext.read.load(hfile)
      val airlineparquetTable = "airlineparquetTable"
      airlineDF.registerTempTable(airlineparquetTable)

      val colTableName = "airlineColTable"
      snc.sql(s"CREATE TABLE $colTableName $ddlStr" +
          "USING column options()")

      airlineDF.write.insertInto(colTableName)

      def close(prepStatement: java.sql.PreparedStatement): Unit = if (prepStatement != null) {
        prepStatement.close()
      }
      val cacheMap = SnappySession.getPlanCache.asMap()
      cacheMap.clear()
      assert( cacheMap.size() == 0)
      val serverHostPort = TestUtil.startNetServer()
      conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)
      val prepStatement1 = conn.prepareStatement("select avg(taxiin + taxiout)avgTaxiTime," +
          s" count( * ) numFlights, " +
          s" dest, avg(arrDelay) arrivalDelay from $colTableName " +
          s" where (taxiin > ? or taxiout > ?) and dest in  (select dest from $colTableName " +
          s" group by dest having count ( * ) > ?) group by dest order " +
          s" by avgTaxiTime desc")
      assert(cacheMap.size() == 0)
      prepStatement1.setInt(1, 10)
      prepStatement1.setInt(2, 10)
      prepStatement1.setInt(3, 10000)
      var update = prepStatement1.executeQuery()
      var index = 0
      val result1 = List("ORD", "LAX", "LGA", "MIA", "JFK", "DFW", "CLT", "EWR", "MCO", "ATL",
        "DTW", "BOS", "DEN", "CLE", "IAH", "FLL", "PHL", "PHX", "SFO", "IAD", "LAS", "RSW",
        "BNA", "MSP", "SEA", "DCA", "MDW", "RDU", "MKE", "HNL", "SLC", "TPA", "BWI", "AUS",
        "MCI", "STL", "MSY", "SAT", "SNA", "DAL", "PDX", "SMF", "HOU", "SAN", "OAK", "SJC")
      while (update.next()) {
        val s = update.getString(3)
        // logInfo(s"1-row($index) $s ")
        assert(result1.contains(s))
        index += 1
      }
      logInfo(s"1-Number of rows read " + index)
      assert(index == 46)
      assert(cacheMap.size() == 0)

      prepStatement1.setInt(1, 5)
      prepStatement1.setInt(2, 5)
      prepStatement1.setInt(3, 5000)
      update = prepStatement1.executeQuery()
      index = 0
      val result2 = List( "ORD", "LGA", "LAX", "MIA", "JFK", "CLT", "EWR", "DFW", "SJU", "CVG",
        "ATL", "PBI", "DTW", "BOS", "MCO", "CLE", "PHL", "IAH", "IAD", "RIC", "FLL", "DEN",
        "SFO", "MEM", "MSP", "CMH", "JAX", "RSW", "SEA", "DCA", "PHX", "PIT", "MKE", "RDU",
        "IND", "SLC", "BNA", "LAS", "BDL", "TUS", "TPA", "BUF", "OMA", "OKC", "AUS", "MDW",
        "MCI", "OGG", "TUL", "MSY", "BWI", "STL", "ABQ", "SAT", "PDX", "SNA", "HNL", "SAN",
        "SMF", "ONT", "SJC", "OAK", "HOU", "DAL", "BUR")
      while (update.next()) {
        val s = update.getString(3)
        // logInfo(s"2-row($index) $s ")
        assert(result2.contains(s))
        index += 1
      }
      logInfo(s"2-Number of rows read " + index)
      assert(index == 65)
      assert(cacheMap.size() == 0)
      close(prepStatement1)
    }
    finally {
      if (conn != null) {
        conn.close()
      }
      SnappyTableStatsProviderService.TEST_SUSPEND_CACHE_INVALIDATION = false
    }
  }
}

object PreparedQueryRoutingSingleNodeSuite extends Logging {

  def insertRows(tableName: String, numRows: Int, serverHostPort: String): Unit = {

    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)

    val rows = (1 to numRows).toSeq
    val prepareStatement = conn.prepareStatement(s"insert into $tableName values(?, ?, ?)")
    try {
      var i = 1
      rows.foreach(d => {
        prepareStatement.setInt(1, d)
        prepareStatement.setInt(2, d)
        prepareStatement.setString(3, s"$d")
        prepareStatement.addBatch()
        i += 1
        if (i % 1000 == 0) {
          val ret = prepareStatement.executeBatch()
          ret.foreach(r => assert(r == 1))
          assert(ret.length == 999, ret.length)
          i = 0
        }
      })
      val ret = prepareStatement.executeBatch()
      ret.foreach(r => assert(r == 1))
      logInfo(s"committed $numRows rows")
    } finally {
      prepareStatement.close()
      conn.close()
    }
  }

  def verifyResults(qry: String, rs: ResultSet, results: Array[Int],
      cacheMapSize: Int): Unit = {
    val cacheMap = SnappySession.getPlanCache.asMap()

    var index = 0
    while (rs.next()) {
      val i = rs.getInt(1)
      val j = rs.getInt(2)
      val s = rs.getString(3)
      logInfo(s"$qry row($index) $i $j $s ")
      index += 1
      assert(results.contains(i))
    }

    logInfo(s"$qry Number of rows read " + index)
    assert(index == results.length)
    rs.close()

    logInfo(s"cachemapsize = $cacheMapSize and .size = ${cacheMap.size()}")
    assert( cacheMap.size() == cacheMapSize || -1 == cacheMapSize)
  }

  def update_delete_query1(tableName1: String, cacheMapSize: Int, serverHostPort: String): Unit = {
    // sc.setLogLevel("TRACE")
    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)

    var prepStatement0: java.sql.PreparedStatement = null
    var prepStatement1: java.sql.PreparedStatement = null
    var prepStatement2: java.sql.PreparedStatement = null
    var prepStatement3: java.sql.PreparedStatement = null
    var prepStatement4: java.sql.PreparedStatement = null
    var prepStatement5: java.sql.PreparedStatement = null
    try {
      prepStatement0 = conn.prepareStatement( s"select ol_1_int_id, ol_1_int2_id, ol_1_str_id" +
          s" from $tableName1" +
          s" where ol_1_int2_id < ? or ol_1_int_id > ? ")
      prepStatement0.setInt(1, 3)
      prepStatement0.setInt(2, 997)
      verifyResults("update_delete_query1-select0", prepStatement0.executeQuery,
        Array(1, 2, 998, 999, 1000), cacheMapSize)
      prepStatement1 = conn.prepareStatement(s"delete from $tableName1 where ol_1_int2_id < ? ")
      prepStatement1.setInt(1, 400)
      prepStatement1.addBatch()
      prepStatement1.setInt(1, 500)
      prepStatement1.addBatch()
      val delete1 = prepStatement1.executeBatch()
      assert(delete1(0) == 399, delete1(0))
      assert(delete1(1) == 100, delete1(1))

      prepStatement2 = conn.prepareStatement(s"delete from $tableName1 where ol_1_int2_id > ? ")
      prepStatement2.setInt(1, 502)
      val delete2 = prepStatement2.executeUpdate
      assert(delete2 == 498, delete2)

      prepStatement3 =
          conn.prepareStatement(s"update $tableName1 set ol_1_int_id = ? where ol_1_int2_id = ? ")
      prepStatement3.setInt(1, 1000)
      prepStatement3.setInt(2, 500)
      val update1 = prepStatement3.executeUpdate
      assert(update1 == 1, update1)

      prepStatement4 =
        conn.prepareStatement(s"update $tableName1 set ol_1_int_id = ? where ol_1_int2_id > ? ")
      prepStatement4.setInt(1, 2000)
      prepStatement4.setInt(2, 501)
      prepStatement4.addBatch()
      prepStatement4.setInt(1, 2000)
      prepStatement4.setInt(2, 500)
      prepStatement4.addBatch()
      val update2 = prepStatement4.executeBatch()
      assert(update2(0) == 1, update2(0))
      assert(update2(1) == 2, update2(1))

      prepStatement5 = conn.prepareStatement( s"select ol_1_int_id, ol_1_int2_id, ol_1_str_id" +
          s" from $tableName1" +
          " where ol_1_int_id < ?")
      prepStatement5.setInt(1, 10000)
      verifyResults("update_delete_query1-select1", prepStatement5.executeQuery,
        Array(1000, 2000, 2000), cacheMapSize + 1)

      prepStatement3.setInt(1, 4000)
      prepStatement3.setInt(2, 500)
      val update3 = prepStatement3.executeUpdate
      assert(update3 == 1, update3)

      prepStatement4.setInt(1, 5000)
      prepStatement4.setInt(2, 500)
      val update4 = prepStatement4.executeUpdate
      assert(update4 == 2, update4)

      verifyResults("update_delete_query1-select2", prepStatement5.executeQuery,
        Array(4000, 5000, 5000), cacheMapSize + 1)
      // Thread.sleep(1000000)
    } finally {
      def close(prepStatement: java.sql.PreparedStatement) =
        if (prepStatement != null) prepStatement.close()
      close(prepStatement0)
      close(prepStatement1)
      close(prepStatement2)
      close(prepStatement3)
      close(prepStatement4)
      close(prepStatement5)
      conn.close()
    }
  }

  def update_delete_query2(tableName1: String, cacheMapSize: Int, serverHostPort: String): Unit = {
    // sc.setLogLevel("TRACE")
    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)

    var prepStatement0: java.sql.PreparedStatement = null
    var prepStatement1: java.sql.PreparedStatement = null
    val s = conn.createStatement()
    try {
      prepStatement0 =
          conn.prepareStatement(s"update $tableName1 set ol_1_str_id = ? where ol_1_int2_id = ? ")
      prepStatement0.setString(1, "7777")
      prepStatement0.setInt(2, 500)
      val update1 = prepStatement0.executeUpdate
      assert(update1 == 1, update1)

      prepStatement1 = conn.prepareStatement( s"select ol_1_int_id, ol_1_int2_id, ol_1_str_id" +
          s" from $tableName1" +
          " where ol_1_str_id like ?")
      prepStatement1.setString(1, "7777")
      verifyResults("update_delete_query2-select1", prepStatement1.executeQuery, Array(4000),
        cacheMapSize)

      prepStatement0.setString(1, "8888")
      prepStatement0.setInt(2, 501)
      val update2 = prepStatement0.executeUpdate
      assert(update2 == 1, update2)

      prepStatement1.setString(1, "8888")
      verifyResults("update_delete_query2-select1", prepStatement1.executeQuery, Array(5000),
        cacheMapSize)
      // Thread.sleep(1000000)
    } finally {
      if (prepStatement0 != null) prepStatement0.close()
      if (prepStatement1 != null) prepStatement1.close()
      s.close()
      conn.close()
    }
  }

  def updateDeleteOnColumnTable(snc: SnappyContext, serverHostPort: String): Unit = {
    SnappySession.getPlanCache.invalidateAll()
    assert(SnappySession.getPlanCache.asMap().size() == 0)
    SnappyTableStatsProviderService.TEST_SUSPEND_CACHE_INVALIDATION = true
    try {
      val tableName1 = "order_line_1_col_ud"
      val tableName2 = "order_line_2_row_ud"
      snc.sql(s"create table $tableName1 (ol_1_int_id  integer," +
          s" ol_1_int2_id  integer, ol_1_str_id STRING) using column " +
          "options( partition_by 'ol_1_int2_id', buckets '2'," +
          " COLUMN_BATCH_SIZE '100')")

      snc.sql(s"create table $tableName2 (ol_1_int_id  integer," +
          s" ol_1_int2_id  integer, ol_1_str_id STRING) using row " +
          "options( partition_by 'ol_1_int2_id', buckets '2')")


      insertRows(tableName1, 1000, serverHostPort)
      insertRows(tableName2, 1000, serverHostPort)
      update_delete_query1(tableName1, 1, serverHostPort)
      update_delete_query1(tableName2, 3, serverHostPort)
      update_delete_query2(tableName1, 5, serverHostPort)
      update_delete_query2(tableName2, 6, serverHostPort)
    } finally {
      SnappyTableStatsProviderService.TEST_SUSPEND_CACHE_INVALIDATION = false
    }
  }

  def equalityOnStringColumn_query1(tableName1: String, cacheMapSize: Int,
      serverHostPort: String): Unit = {
    // sc.setLogLevel("TRACE")
    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)

    var prepStatement0: java.sql.PreparedStatement = null
    var prepStatement1: java.sql.PreparedStatement = null
    var prepStatement2: java.sql.PreparedStatement = null
    try {
      prepStatement0 = conn.prepareStatement( s"select ol_1_int_id, ol_1_int2_id, ol_1_str_id" +
          s" from $tableName1" +
          s" where ol_1_str_id = ? or ol_1_str_id = ? or ol_1_str_id like ? ")
      prepStatement0.setString(1, "1")
      prepStatement0.setString(2, "2")
      prepStatement0.setString(3, "99%")
      verifyResults("equalityOnStringColumn_query1-select0", prepStatement0.executeQuery,
        Array(1, 2, 99, 990, 991, 992, 993, 994, 995, 996, 997, 998, 999), cacheMapSize)

      prepStatement0.setString(1, "3")
      prepStatement0.setString(2, "4")
      prepStatement0.setString(3, "94%")
      verifyResults("equalityOnStringColumn_query1-select1", prepStatement0.executeQuery,
        Array(3, 4, 94, 940, 941, 942, 943, 944, 945, 946, 947, 948, 949), cacheMapSize)

      prepStatement1 = conn.prepareStatement( s"select ol_1_int_id, ol_1_int2_id, ol_1_str_id" +
          s" from $tableName1" +
          s" where ol_1_str_id = ? or ol_1_str_id = ? or ol_1_str_id like ?" +
          s" limit 20")
      prepStatement1.setString(1, "1")
      prepStatement1.setString(2, "2")
      prepStatement1.setString(3, "99%")
      verifyResults("equalityOnStringColumn_query2-select0", prepStatement1.executeQuery,
        Array(1, 2, 99, 990, 991, 992, 993, 994, 995, 996, 997, 998, 999), cacheMapSize + 1)

      prepStatement1.setString(1, "3")
      prepStatement1.setString(2, "4")
      prepStatement1.setString(3, "94%")
      verifyResults("equalityOnStringColumn_query2-select1", prepStatement1.executeQuery,
        Array(3, 4, 94, 940, 941, 942, 943, 944, 945, 946, 947, 948, 949), cacheMapSize + 1)

      prepStatement2 = conn.prepareStatement( s"select ol_1_int_id, ol_1_int2_id, ol_1_str_id" +
          s" from $tableName1" +
          s" where ol_1_str_id = ? or ol_1_str_id = ?")
      prepStatement2.setString(1, "5")
      prepStatement2.setString(2, "6")
      verifyResults("equalityOnStringColumn_query3-select0", prepStatement2.executeQuery,
        Array(5, 6), cacheMapSize + 2)

      prepStatement2.setString(1, "7")
      prepStatement2.setString(2, "8")
      verifyResults("equalityOnStringColumn_query3-select1", prepStatement2.executeQuery,
        Array(7, 8), cacheMapSize + 2)
    } finally {
      def close(prepStatement: java.sql.PreparedStatement) =
        if (prepStatement != null) prepStatement.close()
      close(prepStatement0)
      close(prepStatement1)
      close(prepStatement2)
      conn.close()
    }
  }

  def equalityOnStringColumn(snc: SnappyContext, serverHostPort: String): Unit = {
    SnappySession.getPlanCache.invalidateAll()
    assert(SnappySession.getPlanCache.asMap().size() == 0)
    SnappyTableStatsProviderService.TEST_SUSPEND_CACHE_INVALIDATION = true
    try {
      val tableName1 = "order_line_1_col_eq"
      val tableName2 = "order_line_2_row_eq"
      snc.sql(s"create table $tableName1 (ol_1_int_id  integer," +
          s" ol_1_int2_id  integer, ol_1_str_id STRING) using column " +
          "options( partition_by 'ol_1_int2_id', buckets '2'," +
          " COLUMN_BATCH_SIZE '100')")

      snc.sql(s"create table $tableName2 (ol_1_int_id  integer," +
          s" ol_1_int2_id  integer, ol_1_str_id STRING) using row " +
          "options( partition_by 'ol_1_int2_id', buckets '2')")

      insertRows(tableName1, 1000, serverHostPort)
      insertRows(tableName2, 1000, serverHostPort)
      equalityOnStringColumn_query1(tableName1, 1, serverHostPort)
      equalityOnStringColumn_query1(tableName2, 4, serverHostPort)
    } finally {
      SnappyTableStatsProviderService.TEST_SUSPEND_CACHE_INVALIDATION = false
    }
  }
}
