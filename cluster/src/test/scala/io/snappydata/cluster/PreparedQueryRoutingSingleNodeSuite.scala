/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import java.sql.{DriverManager, ResultSet}

import com.pivotal.gemfirexd.TestUtil
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.{SnappyFunSuite, SnappyTableStatsProviderService}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SnappyContext, SnappySession}

class PreparedQueryRoutingSingleNodeSuite extends SnappyFunSuite with BeforeAndAfterAll {

  // Logger.getLogger("org").setLevel(Level.DEBUG)

  val default_chunk_size = GemFireXDUtils.DML_MAX_CHUNK_SIZE
  protected override def newSparkConf(addOn: (SparkConf) => SparkConf): SparkConf = {
    /**
      * Setting local[n] here actually supposed to affect number of reservoir created
      * while sampling.
      *
      * Change of 'n' will influence results if they are dependent on weights - derived
      * from hidden column in sample table.
      */
    new org.apache.spark.SparkConf().setAppName("PreparedQueryRoutingSingleNodeSuite")
        .setMaster("local[6]")
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
        Array(100, 200, 300), 0)

      prepStatement.setInt(1, 900)
      prepStatement.setInt(2, 600)
      prepStatement.setInt(3, 700)
      prepStatement.setInt(4, 800)
      prepStatement.setString(5, "%0")
      PreparedQueryRoutingSingleNodeSuite.verifyResults("qry-2", prepStatement.executeQuery,
        Array(600, 700, 800), 0)

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
        Array(100, 200, 300), 1)

      prepStatement1.setInt(1, 500)
      prepStatement1.setInt(2, 100)
      prepStatement1.setInt(3, 400)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("qry1-2", prepStatement1.executeQuery,
        Array(100, 400, 300), 1)

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
        Array(600, 700, 800), 1)

      prepStatement2.setInt(1, 900)
      prepStatement2.setInt(2, 400)
      prepStatement2.setInt(3, 500)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("qry2-2", prepStatement2.executeQuery,
        Array(400, 500, 800), 1)

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
        Array(100, 200, 300), 1)

      prepStatement3.setInt(1, 900)
      prepStatement3.setInt(2, 600)
      prepStatement3.setInt(3, 700)
      prepStatement3.setInt(4, 800)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("qry3-2", prepStatement3.executeQuery,
        Array(600, 700, 800), 1)

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
        Array(100, 200, 300), 2)

      prepStatement4.setInt(1, 900)
      prepStatement4.setInt(2, 600)
      prepStatement4.setInt(3, 700)
      prepStatement4.setInt(4, 800)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("qry4-2", prepStatement4.executeQuery,
        Array(600, 700, 800), 2)

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
        Array(100, 200, 300), 3)

      prepStatement5.setDouble(1, 900.01)
      prepStatement5.setInt(2, 600)
      prepStatement5.setInt(3, 700)
      prepStatement5.setInt(4, 800)
      PreparedQueryRoutingSingleNodeSuite.verifyResults("qry5-2", prepStatement5.executeQuery,
        Array(600, 700, 800), 3)

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
    SnappyTableStatsProviderService.suspendCacheInvalidation = true
    try {
      val tableName = "order_line_col"
      snc.sql(s"create table $tableName (ol_int_id  integer," +
          s" ol_int2_id  integer, ol_str_id STRING) using column " +
          "options( partition_by 'ol_int_id, ol_int2_id', buckets '2')")


      val serverHostPort = TestUtil.startNetServer()
      // println("network server started")
      PreparedQueryRoutingSingleNodeSuite.insertRows(tableName, 1000, serverHostPort)
      query0(tableName, serverHostPort)
    } finally {
      SnappyTableStatsProviderService.suspendCacheInvalidation = false
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

  test("test Join, SubQuery and Aggragtes") {
    SnappySession.getPlanCache.invalidateAll()
    assert(SnappySession.getPlanCache.asMap().size() == 0)
    SnappyTableStatsProviderService.suspendCacheInvalidation = true
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
      // println("network server started")
      PreparedQueryRoutingSingleNodeSuite.insertRows(tableName1, 1000, serverHostPort)
      PreparedQueryRoutingSingleNodeSuite.insertRows(tableName2, 1000, serverHostPort)
      query1(tableName1, tableName2, serverHostPort)
      query2(tableName1, tableName2, serverHostPort)
      query3(tableName1, tableName2, serverHostPort)
      query4(tableName1, tableName2, serverHostPort)
      query5(tableName1, tableName2, serverHostPort)
    } finally {
      SnappyTableStatsProviderService.suspendCacheInvalidation = false
    }
  }

  test("update delete on column table") {
    val serverHostPort = TestUtil.startNetServer()
    // println("network server started")
    PreparedQueryRoutingSingleNodeSuite.updateDeleteOnColumnTable(snc, serverHostPort)
  }

  test("SNAP-1981: Equality on string columns") {
    val serverHostPort = TestUtil.startNetServer()
    // println("network server started")
    PreparedQueryRoutingSingleNodeSuite.equalityOnStringColumn(snc, serverHostPort)
  }
}

object PreparedQueryRoutingSingleNodeSuite{

  def insertRows(tableName: String, numRows: Int, serverHostPort: String): Unit = {

    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)

    val rows = (1 to numRows).toSeq
    val stmt = conn.createStatement()
    try {
      var i = 1
      rows.foreach(d => {
        stmt.addBatch(s"insert into $tableName values($d, $d, '$d')")
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

  def verifyResults(qry: String, rs: ResultSet, results: Array[Int], cacheMapSize: Int): Unit = {
    val cacheMap = SnappySession.getPlanCache.asMap()

    var index = 0
    while (rs.next()) {
      val i = rs.getInt(1)
      val j = rs.getInt(2)
      val s = rs.getString(3)
      // scalastyle:off println
      println(s"$qry row($index) $i $j $s ")
      // scalastyle:on println
      index += 1
      assert(results.contains(i))
    }

    // scalastyle:off println
    println(s"$qry Number of rows read " + index)
    // scalastyle:on println
    assert(index == results.length)
    rs.close()

    // scalastyle:off println
    println(s"cachemapsize = ${cacheMapSize} and .size = ${cacheMap.size()}")
    // scalastyle:on println
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
      val delete1 = prepStatement1.executeUpdate
      assert(delete1 == 399, delete1)
      prepStatement1.setInt(1, 500)
      val delete2 = prepStatement1.executeUpdate
      assert(delete2 == 100, delete2)

      prepStatement2 = conn.prepareStatement(s"delete from $tableName1 where ol_1_int2_id > ? ")
      prepStatement2.setInt(1, 502)
      val delete3 = prepStatement2.executeUpdate
      assert(delete3 == 498, delete3)

      prepStatement3 =
          conn.prepareStatement(s"update $tableName1 set ol_1_int_id = ? where ol_1_int2_id = ? ")
      prepStatement3.setInt(1, 1000)
      prepStatement3.setInt(2, 500)
      val update1 = prepStatement3.executeUpdate
      assert(update1 == 1, update1)

      prepStatement4 =
          conn.prepareStatement(s"update $tableName1 set ol_1_int_id = ? where ol_1_int2_id > ? ")
      prepStatement4.setInt(1, 2000)
      prepStatement4.setInt(2, 500)
      val update2 = prepStatement4.executeUpdate
      assert(update2 == 2, update2)

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
    SnappyTableStatsProviderService.suspendCacheInvalidation = true
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
      update_delete_query2(tableName1, 4, serverHostPort)
      update_delete_query2(tableName2, 4, serverHostPort)
    } finally {
      SnappyTableStatsProviderService.suspendCacheInvalidation = false
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
        Array(1, 2, 99, 990, 991, 992, 993, 994, 995, 996, 997, 998, 999), cacheMapSize - 1)

      prepStatement0.setString(1, "3")
      prepStatement0.setString(2, "4")
      prepStatement0.setString(3, "94%")
      verifyResults("equalityOnStringColumn_query1-select1", prepStatement0.executeQuery,
        Array(3, 4, 94, 940, 941, 942, 943, 944, 945, 946, 947, 948, 949), cacheMapSize - 1)

      prepStatement1 = conn.prepareStatement( s"select ol_1_int_id, ol_1_int2_id, ol_1_str_id" +
          s" from $tableName1" +
          s" where ol_1_str_id = ? or ol_1_str_id = ? or ol_1_str_id like ?" +
          s" limit 20")
      prepStatement1.setString(1, "1")
      prepStatement1.setString(2, "2")
      prepStatement1.setString(3, "99%")
      verifyResults("equalityOnStringColumn_query2-select0", prepStatement1.executeQuery,
        Array(1, 2, 99, 990, 991, 992, 993, 994, 995, 996, 997, 998, 999), cacheMapSize - 1)

      prepStatement1.setString(1, "3")
      prepStatement1.setString(2, "4")
      prepStatement1.setString(3, "94%")
      verifyResults("equalityOnStringColumn_query2-select1", prepStatement1.executeQuery,
        Array(3, 4, 94, 940, 941, 942, 943, 944, 945, 946, 947, 948, 949), cacheMapSize - 1)

      prepStatement2 = conn.prepareStatement( s"select ol_1_int_id, ol_1_int2_id, ol_1_str_id" +
          s" from $tableName1" +
          s" where ol_1_str_id = ? or ol_1_str_id = ?")
      prepStatement2.setString(1, "5")
      prepStatement2.setString(2, "6")
      verifyResults("equalityOnStringColumn_query3-select0", prepStatement2.executeQuery,
        Array(5, 6), cacheMapSize)

      prepStatement2.setString(1, "7")
      prepStatement2.setString(2, "8")
      verifyResults("equalityOnStringColumn_query3-select1", prepStatement2.executeQuery,
        Array(7, 8), cacheMapSize)
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
    SnappyTableStatsProviderService.suspendCacheInvalidation = true
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
      equalityOnStringColumn_query1(tableName2, 2, serverHostPort)
    } finally {
      SnappyTableStatsProviderService.suspendCacheInvalidation = false
    }
  }
}
