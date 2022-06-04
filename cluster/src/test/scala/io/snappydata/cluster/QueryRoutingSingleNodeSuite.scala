/*
 * Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
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
import io.snappydata.gemxd.SnappySessionPerConnection
import io.snappydata.{SnappyFunSuite, SnappyTableStatsProviderService}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.store.ColumnTableBatchInsertTest
import org.junit.Assert._
import org.apache.spark.SnappyJavaUtils.snappyJavaUtil

class QueryRoutingSingleNodeSuite extends SnappyFunSuite with BeforeAndAfterAll {

  val default_chunk_size = GemFireXDUtils.DML_MAX_CHUNK_SIZE
  var serverHostPort = ""
  val tableName = "order_line_col"

  override def beforeAll(): Unit = {
    super.beforeAll()
    // reducing DML chunk size size to force lead node to send
    // results in multiple batches
    setDMLMaxChunkSize(50L)
    serverHostPort = TestUtil.startNetServer()
    logInfo("network server started")
  }

  override def afterAll(): Unit = {
    setDMLMaxChunkSize(default_chunk_size)
    TestUtil.stopNetServer()
    logInfo("network server stopped")
    super.afterAll()
  }

  def setDMLMaxChunkSize(size: Long): Unit = {
    GemFireXDUtils.DML_MAX_CHUNK_SIZE = size
  }

  def insertRows(numRows: Int): Unit = {

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)

    val rows = (1 to numRows).toSeq
    val stmt = conn.createStatement()
    try {
      var i = 1
      rows.foreach(d => {
        stmt.addBatch(s"insert into $tableName values($i, '1')")
        i += 1
        if (i % 1000 == 0) {
          stmt.executeBatch()
          i = 0
        }
      })
      stmt.executeBatch()
      logInfo(s"committed $numRows rows")
    } finally {
      stmt.close()
      conn.close()
    }
  }

  def query(): Unit = {
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)

    val stmt = conn.createStatement()
    try {
      val rs = stmt.executeQuery(
        s"select ol_w_id  from $tableName ")
      var index = 0
      while (rs.next()) {
        rs.getInt(1)
        index += 1
      }
      logInfo("Number of rows read " + index)
      rs.close()
    } finally {
      stmt.close()
      conn.close()
    }
  }

  test("test serialization with lesser dml chunk size") {

    snc.sql("create table order_line_col (ol_w_id  integer,ol_d_id STRING) using column " +
        "options( partition_by 'ol_w_id, ol_d_id', buckets '8')")

    insertRows(1000)

    (1 to 5).foreach(d => query())
  }
  def insertRows(tableName: String, numRows: Int, serverHostPort: String): Unit = {

    val conn: java.sql.Connection = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)

    val rows = (1 to numRows).toSeq
    val stmt: java.sql.Statement = conn.createStatement()
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
      logInfo(s"committed $numRows rows")
    } finally {
      stmt.close()
      conn.close()
    }
  }

  private def verifyResults(qry: String, rs: ResultSet, results: Array[Int],
      cacheMapSize: Int): Unit = {
    val cacheMap = SnappySession.getPlanCache.asMap()
    assert(cacheMap.size() == cacheMapSize || -1 == cacheMapSize)

    var index = 0
    while (rs.next()) {
      val i = rs.getInt(1)
      val j = rs.getInt(2)
      val s = rs.getString(3)
      logInfo(s"$qry row($index) $i $j $s")
      index += 1

      assert(results.contains(i))
    }

    logInfo(s"$qry Number of rows read " + index)
    assert(index == results.length)
    rs.close()
  }

  // TODO: After Fix remove this comment
  // This test do not work with 100, 200 but works with 300
  def query1(tableName1: String, tableName2: String, serverHostPort: String): Unit = {
    // sc.setLogLevel("TRACE")
    val conn: java.sql.Connection = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val stmt: java.sql.Statement = conn.createStatement()
    try {
      val qry = s"select ol_1_int_id, ol_1_int2_id, ol_1_str_id " +
          s" from $tableName1 " +
          s" where ol_1_int_id < 500 " +
          s" and ol_1_int2_id in (" +
          s"select ol_2_int_id " +
          s" from $tableName2 " +
          s" where ol_2_int_id = 100 " +
          s") " +
          s" limit 20" +
          s""
      verifyResults("query1-1", stmt.executeQuery(qry), Array(100),
        -1) // TODO pass a number than -1
    } finally {
      stmt.close()
      conn.close()
    }
  }

  def query2(tableName1: String, tableName2: String, serverHostPort: String): Unit = {
    // sc.setLogLevel("TRACE")
    val conn: java.sql.Connection = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val stmt: java.sql.Statement = conn.createStatement()
    try {
      val qry = s"select ol_1_int_id, ol_1_int2_id, ol_1_str_id " +
          s" from $tableName1 " +
          s" where ol_1_int_id < 500 " +
          s" and ol_1_int2_id in (" +
          s"select ol_2_int_id " +
          s" from $tableName2 " +
          s" where ol_2_int_id in (100, 200, 300) " +
          s") " +
          s" limit 20" +
          s""
      verifyResults("query2-1", stmt.executeQuery(qry), Array(100, 200, 300), 0)

      val qry2 = s"select ol_1_int_id, ol_1_int2_id, ol_1_str_id " +
          s" from $tableName1 " +
          s" where ol_1_int_id < 900 " +
          s" and ol_1_int2_id in (" +
          s"select ol_2_int_id " +
          s" from $tableName2 " +
          s" where ol_2_int_id in (600, 700, 800) " +
          s") " +
          s" limit 20" +
          s""
      verifyResults("query2-2", stmt.executeQuery(qry2), Array(600, 700, 800), 0)
    } finally {
      stmt.close()
      conn.close()
    }
  }

  def query2snc(tableName1: String, tableName2: String, serverHostPort: String, iter: Int): Unit = {
    val qry = s"select ol_1_int_id, ol_1_int2_id, ol_1_str_id " +
        s" from $tableName1 " +
        s" where ol_1_int_id < 500 " +
        s" and ol_1_int2_id in (" +
        s"select ol_2_int_id " +
        s" from $tableName2 " +
        s" where ol_2_int_id in (100, 200, 300) " +
        s") " +
        s" limit 20" +
        s""
    logInfo(s"Iter $iter QUERY = $qry")
    val df1 = snc.sql(qry)
    val res1 = df1.collect()
    logInfo(s"Iter $iter with query = $qry")
    logInfo(res1.mkString("\n"))
    logInfo(s"Iter $iter query end and res1 size = ${res1.length}")
    assert(res1.length == 3)

    val qry2 = s"select ol_1_int_id, ol_1_int2_id, ol_1_str_id " +
        s" from $tableName1 " +
        s" where ol_1_int_id < 900 " +
        s" and ol_1_int2_id in (" +
        s"select ol_2_int_id " +
        s" from $tableName2 " +
        s" where ol_2_int_id in (600, 700, 800) " +
        s") " +
        s" limit 20" +
        s""
    val df2 = snc.sql(qry2)
    val res2 = df2.collect()
    logInfo(s"Iter $iter with query2 = $qry2")
    logInfo(res2.mkString("\n"))
    logInfo(s"Iter $iter query2 end with res size = ${res2.length}")
    assert(!res1.sameElements(res2))
    assert(res2.length == 3)
  }

  test("Tokenization test with IN SubQuery") {
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

      insertRows(tableName1, 1000, serverHostPort)
      insertRows(tableName2, 1000, serverHostPort)
      query1(tableName1, tableName2, serverHostPort)
      (0 to 5).foreach(i => query2snc(tableName1, tableName2, serverHostPort, i))
      query2(tableName1, tableName2, serverHostPort)
    } finally {
      SnappyTableStatsProviderService.TEST_SUSPEND_CACHE_INVALIDATION = false
    }
  }

  test("SNAP-1615") {
    val tName = "table1615"
    snc.sql(s"create table $tName (id int, price decimal(38,18), name varchar(10)) using column")

    val conn: java.sql.Connection = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val stmt: java.sql.Statement = conn.createStatement()
    try {
      stmt.addBatch(s"insert into $tName values(1,10.4,'abc')")
      stmt.addBatch(s"insert into $tName values(2,112.4,'aaa')")
      stmt.addBatch(s"insert into $tName values(3,1452.4,'bbb')")
      stmt.addBatch(s"insert into $tName values(4,16552.4,'ccc')")
      stmt.addBatch(s"insert into $tName values(5,null,'ddd')")
      stmt.addBatch(s"insert into $tName values(6,10.6,'ddd')")
      stmt.executeBatch()
      logInfo(s"inserted rows")
    } finally {
      stmt.close()
      conn.close()
    }

    (1 to 5).foreach(_ => query1615(tName, serverHostPort))
  }

  def query1615(tName: String, sHostPort: String): Unit = {
    val conn = DriverManager.getConnection("jdbc:snappydata://" + sHostPort)
    val stmt = conn.createStatement()
    try {
      val rs = stmt.executeQuery(s"select avg(price),name from $tName group by name")
      var index = 0
      var sum: BigDecimal = 0
      while (rs.next()) {
        sum += rs.getBigDecimal(1)
        assert(rs.getString(2) != null)
        index += 1
      }
      logInfo(s"Number of rows read $index sum=$sum")
      assert(index == 5, index)
      assert(sum - 18138.2 == 0, sum)
      rs.close()
    } finally {
      stmt.close()
      conn.close()
    }
  }

  def insertBooleanRows(numRows: Int): Unit = {

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)

    val rows = (1 to numRows).toSeq
    val stmt = conn.createStatement()
    try {
      var i = 1
      rows.foreach(d => {
        stmt.addBatch(s"insert into order_line_row_bool values(${i % 2 == 0}, $i)")
        i += 1
        if (i % 1000 == 0) {
          stmt.executeBatch()
          i = 0
        }
      })
      stmt.executeBatch()
      logInfo(s"committed $numRows rows")
    } finally {
      stmt.close()
      conn.close()
    }
  }

  def queryBooleanRows(): Unit = {
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)

    val stmt = conn.createStatement()
    try {
      val query = s"select distinct ol_w_id  from order_line_row_bool"
      val count = snc.sql(query).collect().length
      assert(count == 2)
      logInfo("snc: Number of rows read " + count)

      val rs = stmt.executeQuery(query)
      var index = 0
      while (rs.next()) {
        rs.getInt(1)
        index += 1
      }
      logInfo("jdbc: Number of rows read " + index)
      assert(index == 2)
      rs.close()
    } finally {
      stmt.close()
      conn.close()
    }
  }

  test("1655: test Boolean in Row Table") {

    snc.sql("create table order_line_row_bool (ol_w_id  Boolean, ol_d_id Long) using row " +
        "options( partition_by 'ol_w_id, ol_d_id', buckets '8')")

    insertBooleanRows(1000)

    (1 to 5).foreach(d => queryBooleanRows())
  }

  test("1737: Failure to convert UTF8String error with index") {

    snc.sql("CREATE TABLE IF NOT EXISTS app.ds_property (" +
        "ds_name VARCHAR(250) NOT NULL," +
        "ds_column VARCHAR(150) NOT NULL," +
        "property VARCHAR(150) NOT NULL," +
        "ds_class_id CHAR(1) NOT NULL," +
        "string_value VARCHAR(1024)," +
        "long_value BIGINT," +
        "double_value DOUBLE," +
        "updated_ts TIMESTAMP NOT NULL," +
        "PRIMARY KEY (ds_name, ds_column, property))" +
        "USING ROW OPTIONS (" +
        "PARTITION_BY 'ds_name'," +
        "buckets '2'," +
        "PERSISTENT 'SYNCHRONOUS')")

    snc.sql("CREATE INDEX app.ds_property_colprop_idx ON app.ds_property(ds_column, property)")
    snc.sql("CREATE INDEX app.ds_property_property_idx ON app.ds_property(property)")
    snc.sql("CREATE INDEX app.ds_property_dsnameprop_idx ON app.ds_property(ds_name, property)")

    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)
    val stmt = conn.createStatement()
    try {
      stmt.execute(s"insert into app.ds_property " +
          s"values ('a', 'b', 'c', 'x', 'd', 1, 1.1, '1995-12-30 11:12:30')")
      stmt.execute(s"insert into app.ds_property values " +
          s"('a', '-', 'FAMILY', 'C', 'FindDatasetTestFamily_1', 1, 0.1, '1995-12-30 11:12:30')")
      stmt.execute(s"insert into app.ds_property values " +
          s"('b', '-', 'DOUBLE_PROP', 'C', 'FindDatasetTestFamily_1', 1, 0.3," +
          s"'1995-12-30 11:12:30')")


      val query = s"SELECT p.ds_name,p.ds_column,p.property,p.ds_class_id,p.string_value," +
          s" p.long_value,p.double_value FROM app.ds_property p " +
          s" WHERE (p.ds_column = '-' AND p.property = 'FAMILY' AND p.string_value =" +
          s" 'FindDatasetTestFamily_1') OR" +
          s" (p.ds_column = '-' AND p.property = 'DOUBLE_PROP' AND p.double_value <= 0.2) OR " +
          s" (p.ds_class_id = 'C' AND p.property = 'DOUBLE_PROP' AND p.double_value > 0.2) OR " +
          s" (p.ds_class_id = 'C' AND p.property = 'DOUBLE_PROP' AND p.double_value < 0.2)"

      val count = snc.sql(query).collect().length
      assert(count == 2)
      logInfo("snc: Number of rows read " + count)

      val rs = stmt.executeQuery(query)
      var index = 0
      while (rs.next()) {
        index += 1
        logInfo(s"$index: ${rs.getString(1)} ${rs.getString(2)} ${rs.getString(3)} " +
            s"${rs.getString(4)} ${rs.getString(5)} ${rs.getLong(6)} ${rs.getBigDecimal(7)}")
      }
      logInfo("jdbc: Number of rows read " + index)
      assert(index == 2)
      rs.close()
    } finally {
      stmt.close()
      conn.close()
    }
  }

  def insertRows(tableName: String, numRows: Int): Unit = {

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)

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
      logInfo(s"committed $numRows rows")
    } finally {
      stmt.close()
      conn.close()
    }
  }

  def update_delete_query1(tableName1: String, cacheMapSize: Int): Unit = {
    // sc.setLogLevel("TRACE")
    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)
    val s = conn.createStatement()
    try {
      val delete1 = s.executeUpdate(s"delete from $tableName1 where ol_1_int2_id < 400 ")
      assert(delete1 == 399, delete1)
      val delete2 = s.executeUpdate(s"delete from $tableName1 where ol_1_int2_id < 500 ")
      assert(delete2 == 100, delete2)

      val delete3 = s.executeUpdate(s"delete from $tableName1 where ol_1_int2_id > 502 ")
      assert(delete3 == 498, delete3)

      val update1 =
        s.executeUpdate(s"update $tableName1 set ol_1_int_id = 1000 where ol_1_int2_id = 500 ")
      assert(update1 == 1, update1)

      val update2 =
        s.executeUpdate(s"update $tableName1 set ol_1_int_id = 2000 where ol_1_int2_id > 500 ")
      assert(update2 == 2, update2)

      val selectQry = s"select ol_1_int_id, ol_1_int2_id, ol_1_str_id from $tableName1 limit 20"
      verifyResults("update_delete_query1-select1", s.executeQuery(selectQry),
        Array(1000, 2000, 2000), cacheMapSize)

      val update3 =
        s.executeUpdate(s"update $tableName1 set ol_1_int_id = 4000 where ol_1_int2_id = 500 ")
      assert(update3 == 1, update3)

      val update4 =
        s.executeUpdate(s"update $tableName1 set ol_1_int_id = 5000 where ol_1_int2_id > 500 ")
      assert(update4 == 2, update4)

      verifyResults("update_delete_query1-select2", s.executeQuery(selectQry),
        Array(4000, 5000, 5000), cacheMapSize)

      // Thread.sleep(1000000)
    } finally {
      s.close()
      conn.close()
    }
  }

  def update_delete_query2(tableName1: String, cacheMapSize: Int): Unit = {
    // sc.setLogLevel("TRACE")
    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)
    val s = conn.createStatement()
    try {
      val update1 = s.executeUpdate(s"UPDATE $tableName1 SET ol_1_int_id = ol_1_int_id + 1 " +
            s" WHERE ol_1_int2_id IN (SELECT max(ol_1_int2_id) from $tableName1)")
      assert(update1 == 1, update1)

      val delete1 = s.executeUpdate(s"delete from $tableName1 where ol_1_int2_id in "
            + s"(SELECT min(ol_1_int2_id) from $tableName1)")
      assert(delete1 == 1, delete1)

      val selectQry1 = s"select ol_1_int_id, ol_1_int2_id, ol_1_str_id from $tableName1 limit 20"
      verifyResults("update_delete_query2-select1",
        s.executeQuery(selectQry1), Array(5000, 5001), cacheMapSize)
    } finally {
      s.close()
      conn.close()
    }
  }

  def update_delete_query3(tableName1: String, cacheMapSize: Int, numPartition: Int): Unit = {
    // sc.setLogLevel("TRACE")
    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)
    val s = conn.createStatement()
    try {
      val update1 = snc.sql(s"UPDATE $tableName1 SET ol_1_int_id = ol_1_int_id + 1 " +
            s" WHERE ol_1_int2_id IN (SELECT max(ol_1_int2_id) from $tableName1)")
      val sum_update1 = update1.collect().map(_.get(0).asInstanceOf[Number].longValue).sum
      val count_update1 = update1.count()
      assert(sum_update1 == 1)
      assert(count_update1 == numPartition)

      val delete1 = snc.sql(s"delete from $tableName1 where ol_1_int2_id in "
            + s"(SELECT min(ol_1_int2_id) from $tableName1)")
      val sum_delete1 = delete1.collect().map(_.get(0).asInstanceOf[Number].longValue).sum
      val count_delete1 = delete1.count()
      assert(sum_delete1 == 1)
      assert(count_delete1 == numPartition)

      val selectQry1 = s"select ol_1_int_id, ol_1_int2_id, ol_1_str_id from $tableName1 limit 20"
      verifyResults("update_delete_query3-select1",
        s.executeQuery(selectQry1), Array(5002), cacheMapSize)
    } finally {
      s.close()
      conn.close()
    }
  }

  test("update delete on column table") {
    SnappySession.getPlanCache.invalidateAll()
    assert(SnappySession.getPlanCache.asMap().size() == 0)
    SnappyTableStatsProviderService.TEST_SUSPEND_CACHE_INVALIDATION = true
    try {
      val tableName1 = "order_line_1_col_ud"
      val tableName2 = "order_line_2_row_ud"
      snc.sql(s"create table $tableName1 (ol_1_int_id  integer," +
          s" ol_1_int2_id  integer, ol_1_str_id STRING) using column " +
          "options( partition_by 'ol_1_int2_id', buckets '2')")

      snc.sql(s"create table $tableName2 (ol_1_int_id  integer," +
          s" ol_1_int2_id  integer, ol_1_str_id STRING) using row " +
          "options( partition_by 'ol_1_int2_id', buckets '2')")

      insertRows(tableName1, 1000)
      insertRows(tableName2, 1000)
      update_delete_query1(tableName1, 1)
      update_delete_query2(tableName1, 1)
      update_delete_query3(tableName1, 1, 2)

      update_delete_query1(tableName2, 1)
      update_delete_query2(tableName2, 1)
      update_delete_query3(tableName2, 1, 1)
    } finally {
      SnappyTableStatsProviderService.TEST_SUSPEND_CACHE_INVALIDATION = false
    }
  }

  def insertRows2(tableName: String, numRows: Int): Unit = {
    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)
    val rows = (1 to numRows).toSeq
    val stmt = conn.createStatement()
    try {
      var i = 1
      rows.foreach(d => {
        val d1 = d + 1
        stmt.addBatch(s"insert into $tableName values($d, $d1, '$d1')")
        i += 1
        if (i % 1000 == 0) {
          stmt.executeBatch()
          i = 0
        }
      })
      stmt.executeBatch()
      logInfo(s"insertRows2: committed $numRows rows")
    } finally {
      stmt.close()
      conn.close()
    }
  }

  def insertInto(tableName1: String, tableName2: String, rowsExpected: Int): Unit = {
    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)
    val stmt = conn.createStatement()
    try {
      val numRows = stmt.executeUpdate(s"insert into $tableName1 select * from $tableName2")
      logInfo(s"insertInto $numRows rows")
      assert(numRows == rowsExpected)
    } finally {
      stmt.close()
      conn.close()
    }
  }

  def putInto(tableName1: String, tableName2: String, rowsExpected: Int): Unit = {
    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)
    val stmt = conn.createStatement()
    try {
      val numRows = stmt.executeUpdate(s"put into $tableName1 select * from $tableName2")
      logInfo(s"putInto $numRows rows")
      assert(numRows == rowsExpected)
    } finally {
      stmt.close()
      conn.close()
    }
  }

  test("put into on row table") {
    SnappySession.getPlanCache.invalidateAll()
    assert(SnappySession.getPlanCache.asMap().size() == 0)
    SnappyTableStatsProviderService.TEST_SUSPEND_CACHE_INVALIDATION = true

    def createTable(tableName: String): Unit =
      snc.sql(s"create table $tableName (ol_1_int_id  integer primary key," +
        s" ol_1_int2_id  integer, ol_1_str_id STRING) using row " +
          "options( partition_by 'ol_1_int_id', buckets '2')" +
          // TODO SNAP-1945: This leads to  duplicate key value error
          // "options( partition_by 'ol_1_int2_id', buckets '2')" +
          "")

    try {
      val tableName1 = "order_line_1_row_pi"
      val tableName2 = "order_line_2_row_pi"
      val tableName3 = "order_line_3_row_pi"
      createTable(tableName1)
      createTable(tableName2)
      createTable(tableName3)

      insertRows(tableName1, 10)
      insertInto(tableName3, tableName1, 10)

      insertRows2(tableName2, 5)
      putInto(tableName3, tableName2, 5)

      val df = snc.sql(s"select * from $tableName3")
      assert(df.count() == 10)
      var assertionNotFailed = true
      df.foreach(r => {
        val col1 = r.getInt(0)
        val col2 = r.getInt(1)
        if (col1 < 6) {
          assertionNotFailed = assertionNotFailed && (col1 + 1 == col2)
        } else {
          assertionNotFailed = assertionNotFailed && (col1 == col2)
        }
      })
      assert(assertionNotFailed)
    } finally {
      SnappyTableStatsProviderService.TEST_SUSPEND_CACHE_INVALIDATION = false
    }
  }

  test("Spark caching using SQL") {
    // first test using session
    val sc = this.sc
    val session = this.snc.snappySession
    ColumnTableBatchInsertTest.testSparkCachingUsingSQL(sc, session.sql, session.catalog.isCached,
      df => session.sharedState.cacheManager.lookupCachedData(df).isDefined)

    // next using JDBC connection
    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)
    try {
      val stmt = conn.createStatement()
      // dummy query to create session for connection
      stmt.executeQuery("show tables")
      val allSessions = SnappySessionPerConnection.getAllSessions
      // only one connection session should be present
      assert(allSessions.length === 1)
      val connSession = allSessions.head
      // skip the "isCached" checks with JDBC since session is different for JDBC connection
      ColumnTableBatchInsertTest.testSparkCachingUsingSQL(sc,
        SnappyFunSuite.resultSetToDataset(connSession, stmt), connSession.catalog.isCached,
        df => connSession.sharedState.cacheManager.lookupCachedData(df).isDefined)
      stmt.close()
    } finally {
      conn.close()
    }
  }

  test("Test Bug SNAP-2707 with jdbc connection") {

    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)
    val stmt = conn.createStatement()
    snc.sql("drop table if exists t")
    snc.sql("create table t(id integer primary key, str string) using row")
    stmt.execute("put into t values(100, 'aa')")
    stmt.execute("put into t   (id, str) values    (101, 'bb')      ")
    stmt.execute("put into t values(102, 'cc')")
    stmt.execute("put into t values(102, 'dd')")
    assertEquals(3, snc.sql("select * from t").count())
    val rs = snc.sql("select str from t where id = 102")
    val rows = rs.collect()
    for (row <- rows) {
      assertEquals("dd", row.getAs[String]("str"))
    }

    snc.sql("drop table if exists t1")
    snc.sql("create table t1(id integer, id2 string) using column options(key_columns 'id')")
    stmt.execute("put into t1 values(100, 'aa')      ")
    stmt.execute("put into t1   (id, id2) values(101, 'sb')      ")
    stmt.execute("put into t1 values(102, 'cc')")
    stmt.execute("put into t1 values(102, 'dd')")
    assertEquals(3, snc.sql("select * from t1").count())
    val rs1 = snc.sql("select id2 from t1 where id = 102")
    val rows1 = rs1.collect()
    for (row <- rows1) {
      assertEquals("dd", row.getAs[String]("id2"))
    }

    snc.sql("drop table if exists t2")
    snc.sql("create table t2(id integer, id2 string) using column " +
        "options(key_columns 'id', COLUMN_MAX_DELTA_ROWS '1', buckets '1')")
    for (i <- 1 to 10) {
      stmt.execute("insert into t2 values(" + i + ",'test" + i + "')")
    }

    for (i <- 1 to 10) {
      stmt.execute("put into t2 values(" + i + ",'test" + i + 1 + "')")
    }

    val rs2 = snc.sql("select * from t2 order by id")
    assertEquals(10, rs2.count())
    val rows2 = rs2.collect()
    var i = 1
    for (row <- rows2) {
      assertEquals("test" + i + 1, row.getAs[String]("id2"))
      i = i + 1
    }

    snc.sql("drop table if exists columntable")
    snc.sql("CREATE TABLE columnTable (bigIntCol BIGINT," +
        " binaryCol1 BINARY, boolCol BOOLEAN , byteCol BYTE," +
        " charCol CHAR( 30 ) , dateCol DATE , decimalCol DECIMAL( 10, 2 ) ," +
        " doubleCol DOUBLE , floatCol FLOAT , intCol INT , integerCol INTEGER," +
        " longVarcharCol LONG , numericCol NUMERIC, numeric1Col NUMERIC(10,2)," +
        " doublePrecisionCol DOUBLE PRECISION, realCol REAL, stringCol STRING," +
        " timestampCol TIMESTAMP , varcharCol VARCHAR( 20 ))" +
        " using COLUMN options(BUCKETS '8', key_columns 'bigIntcol');")
    stmt.execute("put into columntable values(-10, NULL, true, 56, 'ABC456'," +
        " current_date, -66, 0.0111, -2.225E-307, -10, 10, 123456, -1, 1," +
        " 123.56, 0.089, 'abcd', current_timestamp, 'SNAPPY')")
    stmt.execute("put into columntable (bigIntCol, binaryCol1, boolCol, byteCol," +
        " charCol, dateCol , decimalCol , doubleCol , floatCol , intCol)" +
        " values (1000, 1010, FALSE, 97,'1234567890abcdefghij'," +
        " date('1970-01-08'), 66, 2.2, 1.0E8, 1000)")
    assertEquals(2, snc.sql("select * from columntable").count())
  }

  test("Test Bug SNAP-2707 with snappy session") {

    snc.sql("drop table if exists t")
    snc.sql("create table t(id integer primary key, STR string) using row           ")
    snc.sql("put into t values(100, 'aa')")
    snc.sql("put into t   (id, str) values    (101, 'bb')      ")
    snc.sql("put into t   (id) values    (104)      ")
    snc.sql("put into t values(102, 'cc')")
    snc.sql("put into t values(102, 'dd')")
    assertEquals(4, snc.sql("select * from t").count())
    val rs = snc.sql("select STR from t where id = 102")
    val rows = rs.collect()
    for (row <- rows) {
      assertEquals("dd", row.getAs[String]("str"))
    }

    snc.sql("drop table if exists t1")
    snc.sql("create table t1(id integer, ID2 string) using column options(key_columns 'id')")
    snc.sql("put into t1   (id, id2) values    (101, 'bb')      ")
    snc.sql("put into t1 values       (100, 'aa')      ")
    snc.sql("put into t1   (id) values    (104)      ")
    snc.sql("put into t1 values(102, 'cc')")
    snc.sql("put into t1 values(102, 'dd')")
    snc.sql("put into t1 values(103, NULL)")
    assertEquals(5, snc.sql("select * from t1").count())
    val rs1 = snc.sql("select id2 from t1 where id = 102")
    val rows1 = rs1.collect()
    for (row <- rows1) {
      assertEquals("dd", row.getAs[String]("id2"))
    }


    snc.sql("drop table if exists t2")
    snc.sql("create table t2(id integer, ID2 string) using column " +
        "options(key_columns 'id', COLUMN_MAX_DELTA_ROWS '1', buckets '1')")
    for (i <- 1 to 10) {
      snc.sql("insert into t2 values(" + i + ",'test" + i + "')")
    }

    for (i <- 1 to 10) {
      snc.sql("put into t2 values(" + i + ",'test" + i + 1 + "')")
    }

    val rs2 = snc.sql("select * from t2 order by id")
    assertEquals(10, rs2.count())
    val rows2 = rs2.collect()
    var i = 1
    for (row <- rows2) {
      assertEquals("test" + i + 1, row.getAs[String]("id2"))
      i = i + 1
    }

    snc.sql("drop table if exists columntable")
    snc.sql("CREATE TABLE columnTable (bigIntCol BIGINT," +
        " binaryCol1 BINARY, boolCol BOOLEAN , byteCol BYTE," +
        " charCol CHAR( 30 ) , dateCol DATE , decimalCol DECIMAL( 10, 2 ) ," +
        " doubleCol DOUBLE , floatCol FLOAT , intCol INT , integerCol INTEGER," +
        " longVarcharCol LONG , numericCol NUMERIC, numeric1Col NUMERIC(10,2)," +
        " doublePrecisionCol DOUBLE PRECISION, realCol REAL, stringCol STRING," +
        " timestampCol TIMESTAMP , varcharCol VARCHAR( 20 ))" +
        " using COLUMN options(BUCKETS '8', key_columns 'bigIntcol');")
    snc.sql("put into columntable values(-10, NULL, true, 56, 'ABC456'," +
        " current_date, -66, 0.0111, -2.225E-307, -10, 10, 123456, -1, 1," +
        " 123.56, 0.089, 'abcd', current_timestamp, 'SNAPPY')")
    snc.sql("put into columntable (bigIntCol, binaryCol1, boolCol, byteCol," +
        " charCol, dateCol , decimalCol , doubleCol , floatCol , intCol)" +
        " values (1000, 1010, FALSE, 97,'1234567890abcdefghij'," +
        " date('1970-01-08'), 66, 2.2, 1.0E8, 1000)")
    assertEquals(2, snc.sql("select * from columntable").count())
  }

  test("Test Bug SNAP-3038 with jdbc connection") {

    val conn = DriverManager.getConnection("jdbc:snappydata://" + serverHostPort)
    val stmt = conn.createStatement()
    snc.sql("drop schema if exists std1")
    snc.sql("create schema std1")
    snc.sql("drop table if exists std1.t")
    snc.sql("create table std1.t(id integer primary key, str string) using row")
    stmt.execute("put into std1.t values(100, 'aa')")
    stmt.execute("put into std1.t   (id, str) values    (101, 'bb')      ")
    stmt.execute("put into std1.t values(102, 'cc')")
    stmt.execute("put into std1.t values(102, 'dd')")
    assertEquals(3, snc.sql("select * from std1.t").count())
    val rs = snc.sql("select str from std1.t where id = 102")
    val rows = rs.collect()
    for (row <- rows) {
      assertEquals("dd", row.getAs[String]("str"))
    }
    
    snc.sql("drop table if exists std1.t1")
    snc.sql("create table std1.t1(id integer, id2 string) using column options(key_columns 'id')")
    stmt.execute("put into std1.t1 values(100, 'aa')      ")
    stmt.execute("put into std1.t1   (id, id2) values(101, 'sb')      ")
    stmt.execute("put into std1.t1 values(102, 'cc')")
    stmt.execute("put into std1.t1 values(102, 'dd')")
    assertEquals(3, snc.sql("select * from std1.t1").count())
    val rs1 = snc.sql("select id2 from std1.t1 where id = 102")
    val rows1 = rs1.collect()
    for (row <- rows1) {
      assertEquals("dd", row.getAs[String]("id2"))
    }

    snc.sql("drop table if exists std1.t2")
    snc.sql("create table std1.t2(id integer, id2 string) using column " +
        "options(key_columns 'id', COLUMN_MAX_DELTA_ROWS '1', buckets '1')")
    for (i <- 1 to 10) {
      stmt.execute("insert into std1.t2 values(" + i + ",'test" + i + "')")
    }

    for (i <- 1 to 10) {
      stmt.execute("put into std1.t2 values(" + i + ",'test" + i + 1 + "')")
    }

    val rs2 = snc.sql("select * from std1.t2 order by id")
    assertEquals(10, rs2.count())
    val rows2 = rs2.collect()
    var i = 1
    for (row <- rows2) {
      assertEquals("test" + i + 1, row.getAs[String]("id2"))
      i = i + 1
    }

    snc.sql("drop table if exists std1.columntable")
    snc.sql("CREATE TABLE std1.columnTable (bigIntCol BIGINT," +
        " binaryCol1 BINARY, boolCol BOOLEAN , byteCol BYTE," +
        " charCol CHAR( 30 ) , dateCol DATE , decimalCol DECIMAL( 10, 2 ) ," +
        " doubleCol DOUBLE , floatCol FLOAT , intCol INT , integerCol INTEGER," +
        " longVarcharCol LONG , numericCol NUMERIC, numeric1Col NUMERIC(10,2)," +
        " doublePrecisionCol DOUBLE PRECISION, realCol REAL, stringCol STRING," +
        " timestampCol TIMESTAMP , varcharCol VARCHAR( 20 ))" +
        " using COLUMN options(BUCKETS '8', key_columns 'bigIntcol');")
    snc.sql("put into std1.columntable values(-10, NULL, true, 56, 'ABC456'," +
        " current_date, -66, 0.0111, -2.225E-307, -10, 10, 123456, -1, 1," +
        " 123.56, 0.089, 'abcd', current_timestamp, 'SNAPPY')")
    snc.sql("put into std1.columntable (bigIntCol, binaryCol1, boolCol, byteCol," +
        " charCol, dateCol , decimalCol , doubleCol , floatCol , intCol)" +
        " values (1000, 1010, FALSE, 97,'1234567890abcdefghij'," +
        " date('1970-01-08'), 66, 2.2, 1.0E8, 1000)")
    assertEquals(2, snc.sql("select * from std1.columntable").count())
  }

  test("Test Bug SNAP-3038 with snappy session") {

    snc.sql("drop schema if exists std2")
    snc.sql("create schema std2")
    snc.sql("drop table if exists std2.t")
    snc.sql("create table std2.t(id integer primary key, STR string) using row           ")
    snc.sql("put into std2.t values(100, 'aa')")
    snc.sql("put into std2.t   (id, str) values    (101, 'bb')      ")
    snc.sql("put into std2.t   (id) values    (104)      ")
    snc.sql("put into std2.t values(102, 'cc')")
    snc.sql("put into std2.t values(102, 'dd')")
    assertEquals(4, snc.sql("select * from std2.t").count())
    val rs = snc.sql("select STR from std2.t where id = 102")
    val rows = rs.collect()
    for (row <- rows) {
      assertEquals("dd", row.getAs[String]("str"))
    }

    snc.sql("drop table if exists std2.t1")
    snc.sql("create table std2.t1(id integer, ID2 string) using column options(key_columns 'id')")
    snc.sql("put into std2.t1   (id, id2) values    (101, 'bb')      ")
    snc.sql("put into std2.t1 values       (100, 'aa')      ")
    snc.sql("put into std2.t1   (id) values    (104)      ")
    snc.sql("put into std2.t1 values(102, 'cc')")
    snc.sql("put into std2.t1 values(102, 'dd')")
    snc.sql("put into std2.t1 values(103, NULL)")
    assertEquals(5, snc.sql("select * from std2.t1").count())
    val rs1 = snc.sql("select id2 from std2.t1 where id = 102")
    val rows1 = rs1.collect()
    for (row <- rows1) {
      assertEquals("dd", row.getAs[String]("id2"))
    }


    snc.sql("drop table if exists std2.t2")
    snc.sql("create table std2.t2(id integer, ID2 string) using column " +
        "options(key_columns 'id', COLUMN_MAX_DELTA_ROWS '1', buckets '1')")
    for (i <- 1 to 10) {
      snc.sql("insert into std2.t2 values(" + i + ",'test" + i + "')")
    }

    for (i <- 1 to 10) {
      snc.sql("put into std2.t2 values(" + i + ",'test" + i + 1 + "')")
    }

    val rs2 = snc.sql("select * from std2.t2 order by id")
    assertEquals(10, rs2.count())
    val rows2 = rs2.collect()
    var i = 1
    for (row <- rows2) {
      assertEquals("test" + i + 1, row.getAs[String]("id2"))
      i = i + 1
    }

    snc.sql("drop table if exists std2.columntable")
    snc.sql("CREATE TABLE std2.columntable (bigIntCol BIGINT," +
        " binaryCol1 BINARY, boolCol BOOLEAN , byteCol BYTE," +
        " charCol CHAR( 30 ) , dateCol DATE , decimalCol DECIMAL( 10, 2 ) ," +
        " doubleCol DOUBLE , floatCol FLOAT , intCol INT , integerCol INTEGER," +
        " longVarcharCol LONG , numericCol NUMERIC, numeric1Col NUMERIC(10,2)," +
        " doublePrecisionCol DOUBLE PRECISION, realCol REAL, stringCol STRING," +
        " timestampCol TIMESTAMP , varcharCol VARCHAR( 20 ))" +
        " using COLUMN options(BUCKETS '8', key_columns 'bigIntcol');")
    snc.sql("put into std2.columntable values(-10, NULL, true, 56, 'ABC456'," +
        " current_date, -66, 0.0111, -2.225E-307, -10, 10, 123456, -1, 1," +
        " 123.56, 0.089, 'abcd', current_timestamp, 'SNAPPY')")
    snc.sql("put into std2.columntable (bigIntCol, binaryCol1, boolCol, byteCol," +
        " charCol, dateCol , decimalCol , doubleCol , floatCol , intCol)" +
        " values (1000, 1010, FALSE, 97,'1234567890abcdefghij'," +
        " date('1970-01-08'), 66, 2.2, 1.0E8, 1000)")
    assertEquals(2, snc.sql("select * from std2.columntable").count())
  }
}
