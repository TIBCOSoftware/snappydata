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

import java.io.File
import java.sql.{Connection, DatabaseMetaData, DriverManager, ResultSet, SQLException, Statement}

import scala.collection.mutable

import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.Constant._
import io.snappydata.test.dunit.{AvailablePortHelper, SerializableRunnable}
import junit.framework.TestCase
import org.apache.commons.io.FileUtils
import org.junit.Assert

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.{IndexTest, SaveMode, SingleNodeTest, SnappyContext, TPCHUtils}
import org.apache.spark.util.Benchmark

/**
 * Tests for query routing from JDBC client driver.
 */
class QueryRoutingDUnitTest(val s: String)
    extends ClusterManagerTestBase(s) with Logging {

  private val default_chunk_size = GemFireXDUtils.DML_MAX_CHUNK_SIZE

  override def tearDown2(): Unit = {
    // reset the chunk size on lead node
    setDMLMaxChunkSize(default_chunk_size)
    super.tearDown2()
  }

  def _testQueryRouting(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    createTableAndInsertData()
    val conn = getANetConnection(netPort1)
    val s = conn.createStatement()
    s.execute("select col1 from TEST.ColumnTableQR")
    var rs = s.getResultSet
    var cnt = 0
    while (rs.next()) {
      cnt += 1
    }
    assert(cnt == 5)

    var md = rs.getMetaData
    logInfo("metadata col cnt = " + md.getColumnCount + " col name = " +
        md.getColumnName(1) + " col table name = " + md.getTableName(1))
    assert(md.getColumnCount == 1)
    assert(md.getColumnName(1).equals("COL1"))
    assert(md.getTableName(1).equals("COLUMNTABLEQR"))

    // 2nd query which compiles in gemxd too but needs to be routed
    s.execute("select * from TEST.ColumnTableQR")
    rs = s.getResultSet
    cnt = 0
    while (rs.next()) {
      cnt += 1
    }
    assert(cnt == 5)
    md = rs.getMetaData
    logInfo("2nd metadata col cnt = " + md.getColumnCount + " col name = " +
        md.getColumnName(1) + " col table name = " + md.getTableName(1))
    assert(md.getColumnCount == 3)
    assert(md.getColumnName(1).equals("COL1"))
    assert(md.getColumnName(2).equals("COL2"))
    assert(md.getColumnName(3).equals("COL3"))
    assert(md.getTableName(1).equals("COLUMNTABLEQR"))
    assert(md.getTableName(2).equals("COLUMNTABLEQR"))
    assert(md.getTableName(3).equals("COLUMNTABLEQR"))

    vm1.invoke(new SerializableRunnable() {
      override def run(): Unit = {
        val catalog = Misc.getMemStore.getExternalCatalog
        assert(catalog.isColumnTable("TEST", "ColumnTableQR", false))
      }
    })

    // Now give a syntax error which will give parse error on spark sql side as well
    try {
      s.execute("select ** from sometable")
    } catch {
      case sqe: SQLException =>
        if ("42X01" != sqe.getSQLState && "38000" != sqe.getSQLState) {
          throw sqe
        }
    }
    s.execute("select col1, col2 from TEST.ColumnTableQR")
    rs = s.getResultSet
    cnt = 0
    while (rs.next()) {
      cnt += 1
    }
    assert(cnt == 5)
    md = rs.getMetaData
    logInfo("3rd metadata col cnt = " + md.getColumnCount + " col name = " +
        md.getColumnName(1) + " col table name = " + md.getTableName(1))
    assert(md.getColumnCount == 2)

    s.execute("select * from TEST.ColumnTableQR where col1 > 4")
    rs = s.getResultSet
    cnt = 0
    while (rs.next()) {
      cnt += 1
    }
    assert(cnt == 3)

    s.execute(
      "select col1 from TEST.ColumnTableQR where col1 > 0 order by col1 desc")
    rs = s.getResultSet
    cnt = 0
    // 1, 7, 9, 4, 5
    while (rs.next()) {
      cnt += 1
      cnt match {
        case 1 => assert(9 == rs.getInt(1), s"Expected 9 but found ${rs.getInt(1)}")
        case 2 => assert(7 == rs.getInt(1),s"Expected 7 but found ${rs.getInt(1)}")
        case 3 => assert(5 == rs.getInt(1),s"Expected 5 but found ${rs.getInt(1)}")
        case 4 => assert(4 == rs.getInt(1),s"Expected 4 but found ${rs.getInt(1)}")
        case 5 => assert(1 == rs.getInt(1),s"Expected 1 but found ${rs.getInt(1)}")
      }
    }
    assert(cnt == 5)

    // reducing DML chunk size size to force lead node to send
    // results in multiple batches
    setDMLMaxChunkSize(50L)
    val expectedResult: Array[Int] = Array(1, 7, 9, 4, 5)
    val actualResult: Array[Int] = new Array[Int](5)
    s.execute("select col1 from TEST.ColumnTableQR order by col1")
    rs = s.getResultSet
    cnt = 0
    while (rs.next()) {
      actualResult(cnt) = rs.getInt(1)
      logInfo("----" + rs.getInt(1))
      cnt += 1
    }
    assert(cnt == 5)
    // actualResult.foreach(println)
    assert(expectedResult.sorted.sameElements(actualResult))
    setDMLMaxChunkSize(default_chunk_size)

    // Check that update and delete on column table returns exception
    try {
      s.executeUpdate("update TEST.ColumnTableQR set col1 = 10")
      TestCase.fail("update on column table should have failed")
    } catch {
      case sqe: SQLException =>
        if ("42Y62" != sqe.getSQLState) {
          throw sqe
        }
    }

    try {
      s.executeUpdate("delete from TEST.ColumnTableQR")
      TestCase.fail("delete on column table should have failed")
    } catch {
      case sqe: SQLException =>
        if ("42Y62" != sqe.getSQLState) {
          throw sqe
        }
    }

    conn.close()
  }

  def testQueryRoutingWithSchema(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    val conn1 = getANetConnection(netPort1)
    val conn2 = getANetConnection(netPort1)
    val conn3 = getANetConnection(netPort1)
    val columnTable = "columnTable"
    val rowTable = "rowTable"
    conn1.createStatement().executeUpdate("create schema test1")
    conn1.createStatement().executeUpdate("set schema test1")

    conn2.createStatement().executeUpdate("create schema test2")
    conn2.createStatement().executeUpdate("set schema test2")

    // tables are created under schema test1
    conn1.createStatement().executeUpdate(s"create table $columnTable ( x int) using column")
    conn1.createStatement().executeUpdate(s"create table $rowTable ( x int) using row")

    // tables are created under schema test2
    conn2.createStatement().executeUpdate(s"create table $columnTable ( x int) using column")
    conn2.createStatement().executeUpdate(s"create table $rowTable ( x int) using row")

    // tables are created under schema APP
    conn3.createStatement().executeUpdate(s"create table $columnTable ( x int) using column")
    conn3.createStatement().executeUpdate(s"create table $rowTable ( x int) using row")

    // insert data under schema test1
    conn1.createStatement().executeUpdate(s" insert into $columnTable values (1)")
    conn1.createStatement().executeUpdate(s" insert into $rowTable values (2)")

    // insert data under schema test2
    conn2.createStatement().executeUpdate(s" insert into $columnTable values (1)")
    conn2.createStatement().executeUpdate(s" insert into $rowTable values (2)")

    // insert data under schema APP
    conn3.createStatement().executeUpdate(s" insert into $columnTable values (1)")
    conn3.createStatement().executeUpdate(s" insert into $rowTable values (2)")

    // verify data under each column table
    var rs = conn1.createStatement().executeQuery(s"select count(*) from APP.$columnTable")
    assert(rs.next())
    assert(rs.getInt(1) == 1)
    rs = conn1.createStatement().executeQuery(s"select count(*) from TEST1.$columnTable")
    assert(rs.next())
    assert(rs.getInt(1) == 1)
    rs = conn1.createStatement().executeQuery(s"select count(*) from TEST2.$columnTable")
    assert(rs.next())
    assert(rs.getInt(1) == 1)

    // verify data under each row table
    rs = conn1.createStatement().executeQuery(s"select count(*) from APP.$rowTable")
    assert(rs.next())
    assert(rs.getInt(1) == 1)
    rs = conn1.createStatement().executeQuery(s"select count(*) from TEST1.$rowTable")
    assert(rs.next())
    assert(rs.getInt(1) == 1)
    rs = conn1.createStatement().executeQuery(s"select count(*) from TEST2.$rowTable")
    assert(rs.next())
    assert(rs.getInt(1) == 1)

    // truncate tables
    conn1.createStatement().executeUpdate(s" truncate table $columnTable")
    conn1.createStatement().executeUpdate(s" truncate table $rowTable")

    conn2.createStatement().executeUpdate(s" truncate table $columnTable")
    conn2.createStatement().executeUpdate(s" truncate table $rowTable")

    conn3.createStatement().executeUpdate(s" truncate table $columnTable")
    conn3.createStatement().executeUpdate(s" truncate table $rowTable")

    // verify that all tables are empty
    rs = conn1.createStatement().executeQuery(s"select count(*) from APP.$rowTable")
    assert(rs.next())
    assert(rs.getInt(1) == 0)
    rs = conn1.createStatement().executeQuery(s"select count(*) from TEST1.$rowTable")
    assert(rs.next())
    assert(rs.getInt(1) == 0)
    rs = conn1.createStatement().executeQuery(s"select count(*) from TEST2.$rowTable")
    assert(rs.next())
    assert(rs.getInt(1) == 0)

    // drop all tables
    conn1.createStatement().executeUpdate(s" drop table $columnTable")
    conn1.createStatement().executeUpdate(s" drop table $rowTable")

    conn2.createStatement().executeUpdate(s" drop table $columnTable")
    conn2.createStatement().executeUpdate(s" drop table $rowTable")

    conn3.createStatement().executeUpdate(s" drop table $columnTable")
    conn3.createStatement().executeUpdate(s" drop table $rowTable")
  }

  def testSnap1296_1297(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    createTableAndInsertData

    val conn = getANetConnection(netPort1)
    val ps = conn.prepareStatement("select * from TEST.ColumnTableQR")
    val rs = ps.executeQuery
    val md = rs.getMetaData

    assert(md.getColumnCount == 3, "column count is = " + md.getColumnCount)
    assert(md.getColumnName(1).equals("COL1"))
    assert(md.getColumnName(2).equals("COL2"))
    assert(md.getColumnName(3).equals("COL3"))
    assert(md.getTableName(1).equals("COLUMNTABLEQR"))
    assert(md.getTableName(2).equals("COLUMNTABLEQR"))
    assert(md.getTableName(3).equals("COLUMNTABLEQR"))

    var cnt = 0
    while (rs.next()) {
      val col1 = rs.getString(1)
      val col2 = rs.getString(2)
      val col3 = rs.getString(3)
      println(s"col1 = $col1, col2 = $col2, col3 = $col3")
      cnt += 1
    }
    assert(cnt == 5)
    ps.close()

    val ps2 = conn.prepareStatement("select * from TEST.ColumnTableQR where col1 = ?")
    ps2.setInt(1, 1)
    ps2.execute
    val rs2 = ps2.getResultSet
    val md2 = rs2.getMetaData
    assert(md2.getColumnCount == 3)
    assert(md2.getColumnName(1).equals("COL1"))
    assert(md2.getColumnName(2).equals("COL2"))
    assert(md2.getColumnName(3).equals("COL3"))
    assert(md2.getTableName(1).equals("COLUMNTABLEQR"))
    assert(md2.getTableName(2).equals("COLUMNTABLEQR"))
    assert(md2.getTableName(3).equals("COLUMNTABLEQR"))

    var cnt2 = 0
    while (rs2.next()) {
      val col1 = rs2.getInt(1)
      val col2 = rs2.getString(2)
      val col3 = rs2.getString(3)
      println(s"col1 = $col1, col2 = $col2, col3 = $col3")
      assert(col1 == 1)
      cnt2 += 1
    }
    assert(cnt2 == 1)
    ps2.close()
  }

  def testSNAP193_607_8_9(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    createTableAndInsertData2(netPort1)
    val conn = getANetConnection(netPort1)
    val stmt = conn.createStatement()

    val numExpectedRows = 188894
    var rs = stmt.executeQuery("select count(UniqueCarrier) from Airline")
    assert(rs.next())
    val numRows = rs.getInt(1)
    assert(numRows == numExpectedRows, s"got rows=$numRows")
    assert(!rs.next())

    val md = rs.getMetaData
    logInfo("metadata colCount=" + md.getColumnCount + " colName=" +
        md.getColumnName(1) + " tableName=" + md.getTableName(1))
    assert(md.getColumnCount == 1)
    assert(md.getColumnName(1) == "count(UNIQUECARRIER)",
      "columnName=" + md.getColumnName(1))

    // check successful run with larger number (>8) of columns (SNAP-607)
    rs.close()
    rs = stmt.executeQuery("select YEARI, MONTHI, DAYOFMONTH, DAYOFWEEK, " +
        "DEPTIME, CRSDEPTIME, ARRTIME, CRSARRTIME, UNIQUECARRIER " +
        "from AIRLINE limit 10")
    var nrows = 0
    while (rs.next()) {
      nrows += 1
    }
    rs.close()
    Assert.assertEquals(10, nrows)

    // check no hang with decent number of runs (SNAP-608)
    rs.close()
    for (_ <- 0 until 20) {
      rs = stmt.executeQuery("select YEARI, MONTHI, DAYOFMONTH, DAYOFWEEK, " +
          "DEPTIME, CRSDEPTIME, UNIQUECARRIER " +
          "from AIRLINE limit 2")
      var nrows = 0
      while (rs.next()) {
        nrows += 1
      }
      rs.close()
      Assert.assertEquals(2, nrows)
    }

    // below hangs in CREATE TABLE (SNAP-609)
    stmt.execute("CREATE TABLE airline2 USING column AS " +
        "(select * from airline limit 10000)")
    rs = stmt.executeQuery("select count(*) from Airline2")
    assert(rs.next())
    assert(rs.getInt(1) == 10000, "got rows=" + rs.getInt(1))
    assert(!rs.next())

    // now check for ClassCastException with a "select *"
    rs = stmt.executeQuery("select * from Airline2")
    var cnt = 0
    while (rs.next()) {
      cnt += 1
    }
    rs.close()
    Assert.assertEquals(10000, cnt)

    conn.close()
  }

  def testSystablesQueries(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    val filePath = "/tmp/parquetdata"
    val dataDir = new File(filePath)
    val conn = getANetConnection(netPort1)
    var newConn: Connection = null
    try {
      val s = conn.createStatement()
      val colTable = "COLUMNTABLE"
      val rowTable = "ROWTABLE"

      // SYSTABLES queries
      s.execute(s"CREATE TABLE $colTable (Col1 INT, Col2 INT, Col3 INT) " +
          "USING column")
      s.execute(s"select * from sys.systables where tablename='$colTable'")
      var rs = s.getResultSet
      assert(rs.next())
      var tableType = rs.getString("tabletype")
      assert("C".equals(tableType))
      var schemaname = rs.getString("tableschemaname")
      assert("APP".equals(schemaname))

      s.execute(s"CREATE TABLE $rowTable (Col1 INT, Col2 INT, Col3 INT) USING row")
      s.execute(s"select * from sys.systables where tablename='$rowTable'")
      rs = s.getResultSet
      assert(rs.next())
      tableType = rs.getString("tabletype")
      assert("T".equals(tableType))
      schemaname = rs.getString("tableschemaname")
      assert("APP".equals(schemaname))

      val dbmd = conn.getMetaData
      val rSet = dbmd.getTables(null, "APP", null,
        Array[String]("TABLE", "SYSTEM TABLE", "COLUMN TABLE",
          "EXTERNAL TABLE", "STREAM TABLE"))
      assert(rSet.next())

      s.execute(s"drop table $rowTable")

      // Ensure systables, members can be queried (SNAP-215)
      doQueries(s, dbmd, colTable)

      // Ensure systables, members can be queried (SNAP-215) on a new connection too.
      newConn = getANetConnection(netPort1)
      doQueries(newConn.createStatement(), newConn.getMetaData, colTable)

      // Ensure parquet table can be dropped (SNAP-215)
      val parquetTable = "PARQUETTABLE"
      dataDir.mkdir()
      s.execute(s"CREATE EXTERNAL TABLE APP_PARQUET.$parquetTable " +
          s"(Col1 INT, Col2 INT, Col3 INT) USING parquet OPTIONS (path '$filePath')")

      // check meta-data
      val schemaMd = dbmd.getSchemas
      val results = new mutable.HashSet[String]()
      while (schemaMd.next()) {
        results += schemaMd.getString(1)
      }
      assert(results.contains("APP"))
      assert(results.contains("APP_PARQUET"))
      results.clear()

      val tableMd = dbmd.getTables(null, "APP%", null,
        Array[String]("TABLE", "SYSTEM TABLE", "COLUMN TABLE",
          "EXTERNAL TABLE", "STREAM TABLE"))
      while (tableMd.next()) {
        results += tableMd.getString(2) + '.' + tableMd.getString(3)
      }
      // 2 for column table and 1 for parquet external table
      assert(results.size == 3, s"Got size = ${results.size} but expected 3")
      assert(results.contains(s"APP.$colTable"))
      assert(results.contains(s"APP_PARQUET.$parquetTable"))
      results.clear()

      // check the columns
      val columnsMd = dbmd.getColumns(null, "APP_PARQUET", null, null)
      while (columnsMd.next()) {
        results += columnsMd.getString(4)
      }
      assert(results.size == 3, s"Got columns = ${results.size} but expected 3")
      assert(results.contains("COL1"), s"columns = $results")
      assert(results.contains("COL2"))
      assert(results.contains("COL3"))
      results.clear()

      s.execute(s"DROP TABLE APP_PARQUET.$parquetTable")

    } finally {
      conn.close()
      if (newConn != null) {
        newConn.close()
      }
      FileUtils.deleteDirectory(dataDir)
    }
  }

  def testPrepStatementRouting(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    createTableAndInsertData()
    val conn = getANetConnection(netPort1)
    try {
      val ps = conn.prepareStatement("select col1 from TEST.ColumnTableQR " +
          "where col1 >? and col1 < ?")
      ps.setInt(1, 1)
      ps.setInt(2, 1000)
      val rs = ps.executeQuery()
      var cnt = 0
      while (rs.next()) {
        cnt += 1
      }
      assert(cnt == 4)

      val md = rs.getMetaData
      assert(md.getColumnCount == 1)
      assert(md.getColumnName(1).equalsIgnoreCase("col1"))
      //      assert(md.getSchemaName(1).equalsIgnoreCase("test"))
      assert(md.getTableName(1).equalsIgnoreCase("columnTableqr"))

      // Test zero parameter
      val ps2 = conn.prepareStatement("select col1 from TEST.ColumnTableQR " +
          "where col1 > 1 and col1 < 500")
      val rs2 = ps2.executeQuery()
      var cnt2 = 0
      while (rs2.next()) {
        cnt2 += 1
      }
      assert(cnt2 == 4)
    } finally {
      conn.close()
    }
  }

  private def doQueries(s: Statement, dbmd: DatabaseMetaData, t: String): Unit = {
    s.execute("select * from sys.members")
    assert(s.getResultSet.next())
    s.execute("select * from sys.systables")
    assert(s.getResultSet.next())
    s.execute("select * from sys.systables where tableschemaname='APP'")
    assert(s.getResultSet.next())

    // Simulates 'SHOW TABLES' of ij
    var rSet = dbmd.getTables(null, "APP", null,
      Array[String]("TABLE", "SYSTEM TABLE", "COLUMN TABLE",
        "EXTERNAL TABLE", "STREAM TABLE"))

    var foundTable = false
    while (rSet.next()) {
      if (t.equalsIgnoreCase(rSet.getString("TABLE_NAME"))) {
        foundTable = true
        assert(rSet.getString("TABLE_TYPE").equalsIgnoreCase("COLUMN TABLE"))
      }
    }
    assert(foundTable)

    val rSet2 = dbmd.getTables(null, SHADOW_SCHEMA_NAME, null,
      Array[String]("TABLE", "SYSTEM TABLE", "COLUMN TABLE",
        "EXTERNAL TABLE", "STREAM TABLE"))

    foundTable = false
    while (rSet2.next()) {
      if (s"APP____${t + SHADOW_TABLE_SUFFIX}".
          equalsIgnoreCase(rSet2.getString("TABLE_NAME"))) {
        foundTable = true
        assert(rSet2.getString("TABLE_TYPE").equalsIgnoreCase("TABLE"))
      }
    }
    assert(foundTable)

    // Simulates 'SHOW MEMBERS' of ij
    rSet = s.executeQuery("SELECT * FROM SYS.MEMBERS ORDER BY ID ASC")
    assert(rSet.next())
  }

  def createTableAndInsertData(): Unit = {
    val snc = SnappyContext(sc)
    val tableName: String = "TEST.ColumnTableQR"
    snc.sql(s" drop table if exists $tableName")

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3),
      Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s =>
      Data(s.head, s(1).toString, Decimal(s(1).toString + '.' + s(2))))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(tableName, "column", dataDF.schema,
      Map.empty[String, String])
    dataDF.write.format("column").mode(SaveMode.Append)
        .saveAsTable(tableName)
  }

  def createTableAndInsertData2(netPort1: Int): Unit = {
    val snc = SnappyContext(sc)
    val tableName: String = "Airline"

    val hfile = getClass.getResource("/2015-trimmed.parquet").getPath
    val dataDF = snc.read.load(hfile)
    snc.createTable(tableName, "column", dataDF.schema,
      Map.empty[String, String])
    val conn = getANetConnection(netPort1)
    val stmt = conn.createStatement()

    val numExpectedRows = 0
    var rs = stmt.executeQuery("select count(UniqueCarrier) from Airline")
    assert(rs.next())
    val numRows = rs.getInt(1)
    assert(numRows == numExpectedRows, s"got rows=$numRows")
    assert(!rs.next())

    dataDF.write.format("column").mode(SaveMode.Append)
        .saveAsTable(tableName)
  }

  def setDMLMaxChunkSize(size: Long): Unit = {
    GemFireXDUtils.DML_MAX_CHUNK_SIZE = size
  }

  def testGemXDURL(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    val conn = getANetConnection(netPort1, useGemXDURL = true)
    val s = conn.createStatement()
    s.execute("CREATE TABLE T1(COL1 INT, COL2 INT) PERSISTENT REPLICATE")
    s.execute("INSERT INTO T1 VALUES(1, 1), (2, 2), (3, 3),(4, 4), (5, 5)")
    s.execute("SELECT * FROM T1")
    val rs = s.getResultSet
    var cnt = 0
    while (rs.next()) {
      cnt += 1
    }
    assert(cnt == 5)

    try {
      s.execute("CREATE TABLE colTable(Col1 INT ,Col2 INT, Col3 INT)" +
          "USING column " +
          "options " +
          "(" +
          "BUCKETS '1'," +
          "REDUNDANCY '0')")
      Assert.fail(
        "Should have thrown an exception as gemxd URL does not route query")
    } catch {
      case sqe: SQLException =>
        if ("42X01" != sqe.getSQLState) {
          throw sqe
        }
    }

  }

  def DISABLED_SNAP_1597_testNodesPruning(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val snc = SnappyContext(sc)
    SingleNodeTest.testNodesPruning(snc)
  }

  def testTPCHNodesPruning(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val snc = SnappyContext(sc)

    try {
      val queries = Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11",
        "12", "13", "14", "15", "16", "17", "18", "19",
        "20", "21", "22")

      TPCHUtils.createAndLoadTables(snc, true)

      snc.sql(
        s"""CREATE INDEX idx_orders_cust ON orders(o_custkey)
             options (COLOCATE_WITH 'customer')
          """)

      snc.sql(
        s"""CREATE INDEX idx_lineitem_part ON lineitem(l_partkey)
             options (COLOCATE_WITH 'part')
          """)

      val tables = Seq("nation", "region", "supplier", "customer", "orders", "lineitem", "part",
        "partsupp")

      val tableSizes = tables.map { tableName =>
        (tableName, snc.table(tableName).count())
      }.toMap

      tableSizes.foreach(println)

      val i = new IndexTest
      i.runBenchmark("select o_orderkey from orders where o_orderkey = 1", tableSizes, 2)
      i.runBenchmark("select o_orderkey from orders where o_orderkey = 32", tableSizes)
      i.runBenchmark("select o_orderkey from orders where o_orderkey = 801", tableSizes)
      i.runBenchmark("select o_orderkey from orders where o_orderkey = 1409", tableSizes)
      // queries.foreach(q => i.benchmark(q, tableSizes))
    } finally {
      snc.sql(s"DROP INDEX idx_orders_cust")
      snc.sql(s"DROP INDEX idx_lineitem_part")
    }

  }

  def testLimitStatementRouting(): Unit = {
    val serverHostPort = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", serverHostPort)
    println(s"network server started at $serverHostPort")

    val tableName = "order_line_col"
    val snc = SnappyContext(sc)
    snc.sql(s"create table $tableName (ol_int_id  integer," +
        s" ol_int2_id  integer, ol_str_id STRING) using column " +
        "options( partition_by 'ol_int_id, ol_int2_id', buckets '2')")

    limitInsertRows(1000, serverHostPort, tableName)

    // (1 to 5).foreach(d => query())
    limitQuery(serverHostPort, tableName)
  }

  def limitInsertRows(numRows: Int, serverHostPort: Int, tableName: String): Unit = {

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

  def limitQuery(serverHostPort: Int, tableName: String): Unit = {
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://localhost:" + serverHostPort)

    println(s"Connected to $serverHostPort")

    val stmt1 = conn.createStatement()
    val stmt2 = conn.createStatement()
    try {
      val qry1 = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ol_int_id < 500 " +
          s" and ol_int2_id in (100, 200, 300) " +
          " and ol_str_id LIKE '%0' " +
          s" limit 20" +
          s""
      val rs1 = stmt1.executeQuery(qry1)

      val qry2 = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ol_int_id < 500 " +
          s" and ol_int2_id in (100, 200, 300) " +
          s" and ol_str_id LIKE '%0' " +
          s""
      val rs2 = stmt2.executeQuery(qry2)
      verifyQuery("query", rs1 , rs2)
      rs1.close()
      rs2.close()

      // Thread.sleep(1000000)

    } finally {
      stmt1.close()
      stmt2.close()
      conn.close()
    }
  }
}
