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

import com.pivotal.gemfirexd.{Attribute, TestUtil}
import com.pivotal.gemfirexd.security.LdapTestServer
import io.snappydata.test.dunit.AvailablePortHelper

import org.apache.spark.Logging
import org.apache.spark.sql.collection.Utils

class SecureQueryRoutingDUnitTest(val s: String)
    extends ClusterManagerTestBase(s) with Logging {

  override def setUp(): Unit = {
    val ldapServer = LdapTestServer.getInstance(getClass.getResource("/auth.ldif").getPath)
    if (!ldapServer.isServerStarted) {
      ldapServer.startServer()
    }
    setSecurityProps(ldapServer)
    super.setUp()
  }

  override def tearDown2(): Unit = {
    super.tearDown2()
    val ldapServer = LdapTestServer.getInstance()
    if (ldapServer.isServerStarted) {
      ldapServer.stopService()
    }
  }

  val ldapAdminUser = "gemfire3"
  val snappyAdminUser = "gemfire2"
  val jdbcUser = "gemfire1"

  def setSecurityProps(ldapServer: LdapTestServer): Unit = {
    import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE, AUTH_LDAP_SEARCH_DN, AUTH_LDAP_SEARCH_PW}
    var props = scala.collection.mutable.Map[String, String]()
    props += (Attribute.AUTH_PROVIDER ->
        com.pivotal.gemfirexd.Constants.AUTHENTICATION_PROVIDER_LDAP)
    props += (AUTH_LDAP_SERVER -> s"ldap://localhost:${ldapServer.getServerPort}/")
    props += (AUTH_LDAP_SEARCH_BASE -> "ou=ldapTesting,dc=pune,dc=gemstone,dc=com")
    props += (AUTH_LDAP_SEARCH_DN ->
        s"uid=$ldapAdminUser,ou=ldapTesting,dc=pune,dc=gemstone,dc=com")
    props += (AUTH_LDAP_SEARCH_PW -> ldapAdminUser)
    for ((k, v) <- props) System.setProperty(k, v)

    props += (Attribute.USERNAME_ATTR -> snappyAdminUser)
    props += (Attribute.PASSWORD_ATTR -> snappyAdminUser)
    for ((k, v) <- props) locatorNetProps.setProperty(k, v)
    for ((k, v) <- props) bootProps.setProperty(k, v)
  }

  def netConnection(netPort: Int, user: String, pass: String): Connection = {
    val driver = "io.snappydata.jdbc.ClientDriver"
    Utils.classForName(driver).newInstance
    var url: String = "jdbc:snappydata://localhost:" + netPort + "/"
    DriverManager.getConnection(url, user, pass)
  }

  def insertRows1(numRows: Int, serverHostPort: Int, tableName: String,
      user: String, pass: String): Unit = {
    val conn = netConnection(serverHostPort, user, pass)
    // scalastyle:off println
    println(s"insertRows1: Connected to $serverHostPort")
    // scalastyle:on println

    val stmt1 = conn.createStatement()
    val rows = (1 to numRows).toSeq
    try {
      var i = 1
      rows.foreach(d => {
        stmt1.addBatch(s"insert into $tableName values($i, $i, '$i')")
        i += 1
        if (i % 1000 == 0) {
          stmt1.executeBatch()
          i = 0
        }
      })
      stmt1.executeBatch()
      // scalastyle:off println
      println(s"committed $numRows rows")
      // scalastyle:on println
    } finally {
      stmt1.close()
      conn.close()
    }
  }

  def verifyQuery1(qryTest: String, stmt_rs: ResultSet): Unit = {
    val builder = StringBuilder.newBuilder

    var index = 0
    while (stmt_rs.next()) {
      index += 1
      val stmt_i = stmt_rs.getInt(1)
      val stmt_j = stmt_rs.getInt(2)
      val stmt_s = stmt_rs.getString(3)
      if (index % 1000 == 0) {
        builder.append(s"$qryTest Stmt: row($index) $stmt_i $stmt_j $stmt_s ").append("\n")
      }
    }
    builder.append(s"$qryTest Stmt: Total number of rows = $index").append("\n")
    // scalastyle:off println
    println(builder.toString())
    // scalastyle:on println
    assert(index == 20000)
  }

  def query1(serverHostPort: Int, tableName: String, user: String, pass: String): Unit = {
    val conn = netConnection(serverHostPort, user, pass)
    // scalastyle:off println
    println(s"query1: Connected to $serverHostPort")
    // scalastyle:off println

    val stmt1 = conn.createStatement()
    try {
      val qry1 = s"select ol_int_id, ol_int2_id, ol_str_id " +
          s" from $tableName " +
          s" where ol_int_id < 5000000 " +
          s""
      val rs1 = stmt1.executeQuery(qry1)
      verifyQuery1(qry1, rs1)
      rs1.close()
      // Thread.sleep(1000000)
    } finally {
      stmt1.close()
      conn.close()
    }
  }

  def createTable1(serverHostPort: Int, tableName: String, user: String, pass: String): Unit = {
    val conn = netConnection(serverHostPort, user, pass)
    // scalastyle:off println
    println(s"createTable1: Connected to $serverHostPort")
    // scalastyle:on println

    val stmt1 = conn.createStatement()
    try {
    stmt1.execute(s"create table $tableName (ol_int_id  integer," +
        s" ol_int2_id  integer, ol_str_id STRING) using column " +
        "options( partition_by 'ol_int_id, ol_int2_id', buckets '5', COLUMN_BATCH_SIZE '200')")
    } finally {
      stmt1.close()
      conn.close()
    }
  }

  def dropTable(serverHostPort: Int, tableName: String, user: String, pass: String): Unit = {
    val conn = netConnection(serverHostPort, user, pass)
    // scalastyle:off println
    println(s"dropTable1: Connected to $serverHostPort")
    // scalastyle:on println

    val stmt1 = conn.createStatement()
    try {
      stmt1.execute(s"drop table $tableName")
    } finally {
      stmt1.close()
      conn.close()
    }
  }

  def createTable2(serverHostPort: Int, tableName: String, user: String, pass: String): Unit = {
    val conn = netConnection(serverHostPort, user, pass)
    // scalastyle:off println
    println(s"createTable2: Connected to $serverHostPort")
    // scalastyle:on println

    val stmt1 = conn.createStatement()
    try {
      stmt1.execute(s"create table $tableName (ol_int_id  integer," +
          s" ol_int2_id  integer, ol_str_id STRING) using row " +
          "options( partition_by 'ol_int_id, ol_int2_id', buckets '5')")
    } finally {
      stmt1.close()
      conn.close()
    }
  }
  
  def testColumnTableRouting(): Unit = {
    val serverHostPort = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", serverHostPort)
    // scalastyle:off println
    println(s"network server started at $serverHostPort")
    // scalastyle:on println

    val tableName = "order_line_col"
    createTable1(serverHostPort, tableName, jdbcUser, jdbcUser)
    insertRows1(20000, serverHostPort, tableName, jdbcUser, jdbcUser)

    // (1 to 5).foreach(d => query())
    query1(serverHostPort, tableName, jdbcUser, jdbcUser)
    dropTable(serverHostPort, tableName, jdbcUser, jdbcUser)
  }

  def testRowTableRouting(): Unit = {
    val serverHostPort = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", serverHostPort)
    // scalastyle:off println
    println(s"network server started at $serverHostPort")
    // scalastyle:on println

    val tableName = "order_line_row"
    createTable2(serverHostPort, tableName, jdbcUser, jdbcUser)
    insertRows1(20000, serverHostPort, tableName, jdbcUser, jdbcUser)

    // (1 to 5).foreach(d => query())
    query1(serverHostPort, tableName, jdbcUser, jdbcUser)
    dropTable(serverHostPort, tableName, jdbcUser, jdbcUser)
  }
}
