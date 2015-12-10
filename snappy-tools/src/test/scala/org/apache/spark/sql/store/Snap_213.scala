package org.apache.spark.sql.store

import java.sql.{Connection, DriverManager}
import java.util.Properties

import scala.language.implicitConversions

import com.gemstone.gemfire.internal.AvailablePortHelper
import io.snappydata.{Constant, Server, ServiceManager}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry


class Snap_213
  extends FunSuite
  with BeforeAndAfter
  with BeforeAndAfterAll {

  test("Test gemxd simple Prepare Statement with bytes") {
    val props: Properties = new Properties()
    props.put("host-data", "true")
    val server: Server = ServiceManager.getServerInstance
    server.start(props)
    val netPort = AvailablePortHelper.getRandomAvailableTCPPort
    server.startNetworkServer("localhost", netPort, null)

    println("server  started ")
    DriverRegistry.register(Constant.JDBC_CLIENT_DRIVER)
    val conn: Connection = DriverManager.getConnection("jdbc:snappydata://localhost:" + netPort)

    val tableName = "TEST_TABLE"

    conn.createStatement().execute("create table " + tableName + " " +
        "(fr varchar(23), id integer, b1 blob, b2 blob, b3 blob, b4 blob, " +
        "b5 blob, b6 blob, b7 blob, secid integer)" + "partition by column(id)")

    val b1 = Array.fill[Byte](10000)(0)
    val b2 = Array.fill[Byte](10000)(0)
    val b3 = Array.fill[Byte](15000)(0)
    val b4 = Array.fill[Byte](25000)(0)
    val b5 = Array.fill[Byte](14000)(0)
    val b6 = Array.fill[Byte](10000)(0)
    val b7 = Array.fill[Byte](20000)(0)
    val stmt = conn.prepareStatement("insert into " + tableName + " values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
    stmt.setString(1, "firstRow");
    stmt.setInt(2, 10);
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
    server.stop(null)
  }
}
