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
import java.util
import java.util.Properties

import com.pivotal.gemfirexd.TestUtil
import io.snappydata.SnappyFunSuite
import io.snappydata.jdbc.ClientDriver
import org.apache.commons.crypto.utils.Utils
import org.junit.Before
import org.scalatest.BeforeAndAfterAll

class JDBCConnectionPoolTestSuite extends SnappyFunSuite with BeforeAndAfterAll {

  val driverName = "io.snappydata.jdbc.ClientPoolDriver"

 test("Test JDBC connection pool with null properties") {
    snc
    val serverHostPort = TestUtil.startNetServer()

    val url = s"jdbc:snappydata:pool://$serverHostPort"
    // scalastyle:off
    Class.forName(driverName)
    val properties = null
    for(i <- 1 to 3) {
      val conn = DriverManager.getConnection(url, properties)
      assert(null != conn)
      conn.close()
    }
  }

  test("Test connection pool with pool and connection properties") {
    snc
    val serverHostPort = TestUtil.startNetServer()
    val properties = new Properties
    properties.setProperty("pool-maxActive", "5")
    properties.setProperty("pool-initialSize", "5")
    properties.setProperty("user", "app")
    properties.setProperty("password", "app")
    val url = s"jdbc:snappydata:pool://$serverHostPort"
    // scalastyle:off
    Class.forName(driverName)
    for(i <- 1 to 3){
      val conn = DriverManager.getConnection(url, properties)
      assert(null != conn)
      conn.close()
    }
  }

  test("Test connection pool with random property") {
    snc
    val serverHostPort = TestUtil.startNetServer()
    val properties = new Properties
    properties.setProperty("pool-maxActive", "5")
    properties.setProperty("pool-initialSize", "5")
    properties.setProperty("user", "app")
    properties.setProperty("password", "app")
    val url = s"jdbc:snappydata:pool://$serverHostPort"
    // scalastyle:off
    Class.forName(driverName)
    for(i <- 1 to 3){
      val conn = DriverManager.getConnection(url, properties)
      assert(null != conn)
      conn.close()
    }
  }

  test("Test JDBC connection pool with empty   ") {
    val serverHostPort = TestUtil.startNetServer()
    val url = s"jdbc:snappydata:pool://$serverHostPort"
    // scalastyle:off
    Class.forName(driverName)
    val properties = new Properties
    properties.setProperty("pool-initialSize", "5")
    properties.setProperty("pool-maxIdle", "0")
    properties.setProperty("pool-maxActive", "5")
    val conn = DriverManager.getConnection(url, properties)
    val conn1 = DriverManager.getConnection(url, properties)
    assert(null != conn && null != conn1)
    conn.close()
    conn1.close()
  }

  test("Test connection pool with max connection call than the initial size") {
    snc
    val serverHostPort = TestUtil.startNetServer()
    val properties = new Properties
    properties.setProperty("pool-maxActive", "5")
    properties.setProperty("pool-initialSize", "5")
    properties.setProperty("user", "app")
    properties.setProperty("password", "app")
    val url = s"jdbc:snappydata:pool://$serverHostPort"
    // scalastyle:off
    Class.forName(driverName)
    for(i <- 1 to 10){
      val conn = DriverManager.getConnection(url, properties)
      assert(null != conn)
      conn.close()
    }
  }

  test("Test connection pool without passing any property") {
    val serverHostPort = TestUtil.startNetServer()
    val url = s"jdbc:snappydata:pool://$serverHostPort"
    // scalastyle:off
    Class.forName(driverName)
    val conn = DriverManager.getConnection(url)
    assert(null != conn)
    conn.close()
  }

  test("Test JDBC connection of pool to create, insert and read Query ") {
    snc
    val serverHostPort = TestUtil.startNetServer()
    val url = s"jdbc:snappydata:pool://$serverHostPort"
    // scalastyle:off
    Class.forName(driverName)
    val properties = new Properties
    properties.setProperty("user", "app")
    properties.setProperty("password", "app")
    val conn = DriverManager.getConnection(url, properties)
    val stmt = conn.createStatement()
    var sql = "DROP TABLE IF EXISTS TEST_JDBC_DRIVER_POOL"
    stmt.executeUpdate(sql)
    sql = "CREATE TABLE TEST_JDBC_DRIVER_POOL (id INTEGER , " +
      "col1 VARCHAR(255), col2 VARCHAR(255)," + " age INTEGER );"
    stmt.executeUpdate(sql)

    val preparedStatement = conn.prepareStatement("insert into " +
      "TEST_JDBC_DRIVER_POOL VALUES (?,?,?,?)")
    var i = 1
    while (i < 1000) {
      preparedStatement.setInt(1, i)
      preparedStatement.setString(2, "Col_1_Value_" + i)
      preparedStatement.setString(3, "Col_2_Value_" + i)
      preparedStatement.setInt(4, i)
      preparedStatement.execute
      i += 1
    }

    sql = "select count(*) from TEST_JDBC_DRIVER_POOL"
    val rs = stmt.executeQuery(sql)
    var count = 0
    while  (rs.next()){
      count = rs.getInt(1)
    }
    assert(count == 999)

    stmt.close()
    conn.close()
  }

  test("Test JDBC connection pool to drop table") {
    snc
    val serverHostPort = TestUtil.startNetServer()
    val url = s"jdbc:snappydata:pool://$serverHostPort"
    // scalastyle:off
    Class.forName(driverName)
    val properties = new Properties
    properties.setProperty("user", "app")
    properties.setProperty("password", "app")
    val conn = DriverManager.getConnection(url, properties)
    val stmt = conn.createStatement()
    val sql = "DROP TABLE IF EXISTS TEST_JDBC_DRIVER_POOL"
    assert(0 == stmt.executeUpdate(sql))
  }

  ignore("Test connection pool to test pool exhaus") {

    try {
      snc
      val serverHostPort = TestUtil.startNetServer()
      val properties = new Properties
      properties.setProperty("pool-maxIdle", "1")
      properties.setProperty("pool-maxWait", "5")
      properties.setProperty("pool-removeAbandoned", "true")
      properties.setProperty("pool-removeAbandonedTimeout", "15")
      properties.setProperty("pool-minIdle", "1")
      properties.setProperty("pool-maxActive", "3")
      properties.setProperty("pool-initialSize", "1")
      properties.setProperty("pool-user", "app")
      properties.setProperty("pool-password", "app")

      val url = s"jdbc:snappydata:pool://$serverHostPort"
      // scalastyle:off
      Class.forName(driverName)
      // max active is 3 and trying to use more than that
      for (i <- 1 to 5) {
        val conn = DriverManager.getConnection(url, properties)
        // conn.close()
      }
      assert(false)

    } catch {
      case e: org.apache.tomcat.jdbc.pool.PoolExhaustedException => {
        assert(true)
      }
      case e: Exception => {
        assert(false)
      }
    }
  }

}