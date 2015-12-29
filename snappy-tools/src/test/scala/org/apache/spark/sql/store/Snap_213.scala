package org.apache.spark.sql.store

import java.sql.{Connection, DriverManager}

import com.pivotal.gemfirexd.TestUtil
import io.snappydata.{Constant, SnappyFunSuite}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry

class Snap_213
    extends SnappyFunSuite
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestUtil.stopNetServer()
    super.afterAll()
  }

  test("Test to verify long bytes as parameters works in insert") {
    DriverRegistry.register(Constant.JDBC_CLIENT_DRIVER)
    val hostPort = TestUtil.startNetServer()

    println("server  started ")
    val conn: Connection = DriverManager.getConnection(
      "jdbc:snappydata://" + hostPort)

    val tableName = "TEST_TABLE"

    conn.createStatement().execute("create table " + tableName +
        " (fr varchar(23), id integer, b1 blob, b2 blob, b3 blob, b4 blob, " +
        "b5 blob, b6 blob, b7 blob, secid integer) partition by column(id)")

    val b1 = Array.fill[Byte](10000)(0)
    val b2 = Array.fill[Byte](10000)(0)
    val b3 = Array.fill[Byte](15000)(0)
    val b4 = Array.fill[Byte](25000)(0)
    val b5 = Array.fill[Byte](14000)(0)
    val b6 = Array.fill[Byte](10000)(0)
    val b7 = Array.fill[Byte](20000)(0)
    val stmt = conn.prepareStatement("insert into " + tableName +
        " values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
    stmt.setString(1, "firstRow")
    stmt.setInt(2, 10)
    stmt.setBytes(3, b1)
    stmt.setBytes(4, b2)
    stmt.setBytes(5, b3)
    stmt.setBytes(6, b4)
    stmt.setBytes(7, b5)
    stmt.setBytes(8, b6)
    stmt.setBytes(9, b7)
    stmt.setInt(10, 20)
    stmt.execute()
    conn.close()
  }
}
