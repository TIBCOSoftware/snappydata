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

import java.sql.{Connection, DriverManager, SQLException}

import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.test.dunit.{AvailablePortHelper, SerializableRunnable}

import org.apache.spark.sql.collection.Utils

class DistributedDDLRoutingDUnitTest(val s: String) extends ClusterManagerTestBase(s) {

  private def getANetConnection(netPort: Int): Connection = {
    val driver = "io.snappydata.jdbc.ClientDriver"
    Utils.classForName(driver).newInstance
    val url = "jdbc:snappydata://localhost:" + netPort + "/"
    DriverManager.getConnection(url)
  }

  def testColumnTableRouting(): Unit = {
    val tableName: String = "TEST.ColumnTableQR"
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)

    // first fail a statement
    failCreateTableXD(conn, tableName, doFail = true, " column ")

    createTableXD(conn, tableName, " column ")
    tableMetadataAssertColumnTable("TEST", "ColumnTableQR")
    // Test create table - error for recreate
    failCreateTableXD(conn, tableName, doFail = false, " column ")

    // Drop Table and Recreate
    dropTableXD(conn, tableName)
    createTableXD(conn, tableName, " column ")

    insertDataXD(conn, tableName)
    queryData(tableName)

    truncateTableXD(conn, tableName)
    insertDataXD(conn, tableName)
    queryData(tableName)

    createTempTableXD(conn)

    queryDataXD(conn, tableName)
    dropTableXD(conn, tableName)
  }

  def _testRowTableRouting(): Unit = {
    val tableName: String = "RowTableQR"

    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)

    // first fail a statement
    failCreateTableXD(conn, tableName, true, " row ")

    createTableXD(conn, tableName, " row ")
    tableMetadataAssertRowTable("APP", tableName)
    // Test create table - error for recreate
    failCreateTableXD(conn, tableName, false, " row ")

    // Drop Table and Recreate
    dropTableXD(conn, tableName)
    createTableXD(conn, tableName, " row ")

    insertDataXD(conn, tableName)
    queryData(tableName)

    truncateTableXD(conn, tableName)
    insertDataXD(conn, tableName)
    queryData(tableName)

    createTempTableXD(conn)

    queryDataXD(conn, tableName)
    dropTableXD(conn, tableName)
  }

  def _testRowTableByDefaultRouting(): Unit = {
    val tableName: String = "TEST.DefaultRowTableQR"

    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)

    createTableByDefaultXD(conn, tableName)
    tableMetadataAssertRowTable("TEST", "DefaultRowTableQR")

    // Drop Table and Recreate
    dropTableXD(conn, tableName)
    createTableByDefaultXD(conn, tableName)

    insertDataXD(conn, tableName)
    queryData(tableName)

    createTempTableXD(conn)

    queryDataXD(conn, tableName)
    dropTableXD(conn, tableName)

    Snap319(conn)
  }

  def _testHang_SNAP_961(): Unit = {
    val tableName: String = "TEST.ColumnTableQR"
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)

    val s = conn.createStatement()
    var options = "OPTIONS(PERSISTENT 'async', DISKSTORE 'd1')"
    try {
      s.execute(s"CREATE TABLE $tableName (Col1 INT, Col2 INT, Col3 INT) " +
          s"USING column $options")
    } catch {
      case sqle: SQLException => if (sqle.getSQLState != "38000" ||
          !sqle.getMessage.contains("Disk store D1 not found")) throw sqle
    }

    // should succeed after creating diskstore
    s.execute("CREATE DISKSTORE d1")
    s.execute(s"CREATE TABLE $tableName (Col1 INT, Col2 INT, Col3 INT) " +
        s"USING column $options")

    dropTableXD(conn, tableName)

    // offheap has been removed
    options = "OPTIONS(OFFHEAP 'true')"
    try {
      s.execute(s"CREATE TABLE $tableName (Col1 INT, Col2 INT, Col3 INT) " +
          s"USING column $options")
    } catch {
      case sqle: SQLException => if (sqle.getSQLState != "38000" ||
          !sqle.getMessage.contains("Unknown option")) {
        throw sqle
      }
    }

    s.execute("DROP DISKSTORE d1")
  }

  def createTableXD(conn: Connection, tableName: String,
      usingStr: String): Unit = {
    val s = conn.createStatement()
    val options = ""
    s.execute(s"CREATE TABLE $tableName (Col1 INT, Col2 INT, Col3 STRING) " +
        s"USING $usingStr $options")
  }

  def createTableByDefaultXD(conn: Connection, tableName: String): Unit = {
    val s = conn.createStatement()
    s.execute("set spark.sql.shuffle.partitions=5")
    s.execute("CREATE TABLE " + tableName + " (Col1 INT, Col2 INT, Col3 STRING) ")
  }

  def Snap319(conn: Connection): Unit = {
    {
      val snc = org.apache.spark.sql.SnappyContext(sc)
      snc.sql("set spark.sql.shuffle.partitions=10")
      val val1 = snc.getAllConfs.getOrElse("spark.sql.shuffle.partitions", "0")
      assert(val1.equals("10"), "Expect 10 but got " + val1)

      {
        // Change by DRDA has no effects
        val s = conn.createStatement()
        s.execute("set spark.sql.shuffle.partitions=5")
        val val2 = snc.getAllConfs.getOrElse("spark.sql.shuffle.partitions", "0")
        assert(val2.equals("10"), "Expect 10 but got " + val2)
      }
    }

    {
      // This setting has no effect in other Snappy Context
      val snc3 = org.apache.spark.sql.SnappyContext(sc)
      val val3 = snc3.getAllConfs.getOrElse("spark.sql.shuffle.partitions", "0")
      assert(val3.equals("0"), "Expect 0 but got " + val3)
    }
  }

  def failCreateTableXD(conn: Connection, tableName: String, doFail: Boolean,
      usingStr: String): Unit = {
    try {
      val s = conn.createStatement()
      val options = ""
      s.execute("CREATE TABLE " + tableName + " (Col1 INT, Col2 INT, " +
          "Col3 INT) " + (if (doFail) "fail" orElse "") + " USING " +
          usingStr + " " + options)
      // println("Successfully Created ColumnTable = " + tableName)
    } catch {
      case e: Exception => getLogWriter.error("create: Caught exception " +
          e.getMessage + " for ColumnTable = " + tableName, e)
      // println("Exception stack. create. ex=" + e.getMessage + " ,stack=" +
      //   ExceptionUtils.getFullStackTrace(e))
    }
  }

  def tableMetadataAssertColumnTable(schemaName: String,
      tableName: String): Unit = {
    vm0.invoke(new SerializableRunnable() {
      override def run(): Unit = {
        val catalog = Misc.getMemStore.getExternalCatalog
        assert(catalog.isColumnTable(schemaName, tableName, false))
      }
    })
  }

  def tableMetadataAssertRowTable(schemaName: String, tableName: String): Unit = {
    vm0.invoke(new SerializableRunnable() {
      override def run(): Unit = {
        val catalog = Misc.getMemStore.getExternalCatalog
        assert(!catalog.isColumnTable(schemaName, tableName, false))
      }
    })
  }

  def insertDataXD(conn: Connection, tableName: String): Unit = {
    val s = conn.createStatement()
    s.execute("insert into " + tableName + " values(10, 200, '3') ")
    s.execute("insert into " + tableName
        + " values(70, 800, '9'),(90, 200, '3'),(40, 200, '3'),(50, 600, '7') ")
  }

  def dropTableXD(conn: Connection, tableName: String): Unit = {
    val s = conn.createStatement()
    s.execute("drop table " + tableName)
  }

  def truncateTableXD(conn: Connection, tableName: String): Unit = {
    val s = conn.createStatement()
    s.execute("truncate table " + tableName)
  }

  def createTempTableXD(conn: Connection): Unit = {
    try {
      val s = conn.createStatement()
      s.execute("CREATE EXTERNAL TABLE airlineRef_temp(Code VARCHAR(25), " +
          "Description VARCHAR(25)) USING parquet OPTIONS()")
    } catch {
      case e: java.sql.SQLException =>
      // println("Exception stack. create. ex=" + e.getMessage +
      //   " ,stack=" + ExceptionUtils.getFullStackTrace(e))
    }
    // println("Created ColumnTable = " + tableName)
  }

  def queryData(tableName: String): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)
    // println("Firing select on ColumnTable = " + tableName)
    val dataDF = snc.sql("Select * from " + tableName)
    // dataDF.map(t => "Select Query: Col1: " + t(0) + " Col2: " + t(1) +
    //   " Col3: " + t(2)).collect().foreach(println)

    assert(dataDF.rdd.map(t => t(0)).count() == 5)
    dataDF.rdd.map(t => t(0)).collect().foreach(verifyData)
  }

  def verifyData(v: Any): Unit = {
    assert(Seq(10, 70, 90, 40, 50).contains(v))
  }

  def queryDataXD(conn: Connection, tableName: String): Unit = {
    val s = conn.createStatement()
    val rs = s.executeQuery("Select col1 from " + tableName)
    var cnt = 0
    while (rs.next()) {
      cnt += 1
      assert(Seq(10, 70, 90, 40, 50).contains(rs.getInt(1)))
    }
    assert(cnt == 5, cnt)
  }
}

case class insertData(col1: Int, col2: Int, col3: Int)
