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
package org.apache.spark.sql.policy

import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties

import com.pivotal.gemfirexd.internal.iapi.error.StandardException
import com.pivotal.gemfirexd.security.{LdapTestServer, SecurityTestUtils}
import com.pivotal.gemfirexd.{Attribute, TestUtil}
import io.snappydata.{Constant, SnappyFunSuite}
import org.junit.Assert.assertEquals
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.apache.spark.sql.SnappyContext
import org.apache.spark.{Logging, SparkConf}

class RestrictTableCreationPolicyTest extends SnappyFunSuite
    with Logging
    with BeforeAndAfter
    with BeforeAndAfterAll {
  val user1 = "gemfire1"
  val user2 = "gemfire2"
  val user3 = "gemfire3"
  val tableCreator = user1
  val ownerLdapGroup = "gemGroup1" // users gem1, gem2, gem3
  val otherLdapGroup = "gemGroup3" // users gem6, gem7, gem8
  val otherUser = "gemfire6"
  val props = Map.empty[String, String]
  val schema = "tax"
  val numElements = 100
  val colTableName: String = s"$schema.ColumnTable"
  val rowTableName: String = s"$schema.RowTable"
  var ownerContext: SnappyContext = _

  private val sysUser = "gemfire10"
  var serverHostPort: String = _


  override def beforeAll(): Unit = {
    this.stopAll()
    System.setProperty("snappydata.RESTRICT_TABLE_CREATION", "true")
    super.beforeAll()
    val seq = for (i <- 0 until numElements) yield {
      (s"name_$i", i)
    }
    val rdd = sc.parallelize(seq)
    ownerContext = snc.newSession()
    serverHostPort = TestUtil.startNetServer()
    ownerContext.snappySession.conf.set(Attribute.USERNAME_ATTR, tableCreator)
    ownerContext.snappySession.conf.set(Attribute.PASSWORD_ATTR, tableCreator)
    val dataDF = ownerContext.createDataFrame(rdd)
    val adminContext = snc.newSession()
    adminContext.snappySession.conf.set(Attribute.USERNAME_ATTR, sysUser)
    adminContext.snappySession.conf.set(Attribute.PASSWORD_ATTR, sysUser)
    val adminConn = getConnection(Some(sysUser))
    val adminStmt = adminConn.createStatement()
    try {
      adminStmt.execute(s"create schema $schema authorization ldapgroup:$ownerLdapGroup")
    } finally {
      adminConn.close()
    }
    ownerContext.sql(s"CREATE TABLE $colTableName (name String, id Int) " +
        s" USING column ")

    ownerContext.sql(s"CREATE TABLE $rowTableName (name String, id Int) " +
        s" USING row ")
    ownerContext.sql(s"grant select on table $colTableName to ldapgroup:$otherLdapGroup")
    ownerContext.sql(s"grant select on table $rowTableName to ldapgroup:$otherLdapGroup")

    ownerContext.sql(s"alter table $colTableName enable row level security")
    ownerContext.sql(s"alter table $rowTableName enable row level security")

    dataDF.write.insertInto(colTableName)
    dataDF.write.insertInto(rowTableName)
  }

  protected override def newSparkConf(addOn: (SparkConf) => SparkConf): SparkConf = {
    val ldapProperties = SecurityTestUtils.startLdapServerAndGetBootProperties(0, 0, sysUser,
      getClass.getResource("/auth.ldif").getPath)
    import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SEARCH_BASE, AUTH_LDAP_SERVER}
    for (k <- List(Attribute.AUTH_PROVIDER, AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE)) {
      System.setProperty(k, ldapProperties.getProperty(k))
    }
    System.setProperty(Constant.STORE_PROPERTY_PREFIX + Attribute.USERNAME_ATTR, sysUser)
    System.setProperty(Constant.STORE_PROPERTY_PREFIX + Attribute.PASSWORD_ATTR, sysUser)
    val conf = new org.apache.spark.SparkConf()
        .setAppName("Test")
        .setMaster("local[3]")
        .set(Attribute.AUTH_PROVIDER, ldapProperties.getProperty(Attribute.AUTH_PROVIDER))
        .set(Constant.STORE_PROPERTY_PREFIX + Attribute.USERNAME_ATTR, sysUser)
        .set(Constant.STORE_PROPERTY_PREFIX + Attribute.PASSWORD_ATTR, sysUser)

    if (addOn != null) {
      addOn(conf)
    } else {
      conf
    }
  }

  override def afterAll(): Unit = {
    ownerContext.dropTable(colTableName, true)
    ownerContext.dropTable(rowTableName, true)
    this.stopAll()
    super.afterAll()
    val ldapServer = LdapTestServer.getInstance()
    if (ldapServer.isServerStarted) {
      ldapServer.stopService()
    }
    import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SEARCH_BASE, AUTH_LDAP_SERVER}
    for (k <- List(Attribute.AUTH_PROVIDER, AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE)) {
      System.clearProperty(k)
      System.clearProperty("gemfirexd." + k)
      System.clearProperty(Constant.STORE_PROPERTY_PREFIX + k)
    }
    System.clearProperty(Constant.STORE_PROPERTY_PREFIX + Attribute.USERNAME_ATTR)
    System.clearProperty(Constant.STORE_PROPERTY_PREFIX + Attribute.PASSWORD_ATTR)
    System.setProperty("gemfirexd.authentication.required", "false")
    System.clearProperty("snappydata.RESTRICT_TABLE_CREATION")
  }


  test("Policy creation on a column table using jdbc client") {
    this.testPolicy(colTableName)
  }

  test("Policy creation on a row table using jdbc client") {
    this.testPolicy(rowTableName)
  }

  private def testPolicy(tableName: String) {
    val conn = getConnection(Some(tableCreator))
    val stmt = conn.createStatement()
    val conn1 = getConnection(Some(otherUser))
    val conn2 = getConnection(Some(user2))
    val conn3 = getConnection(Some(user3))
    try {
      stmt.execute(s"create policy $schema.testPolicy1 on  " +
          s"$tableName for select to current_user using id < 0")
      var rs = stmt.executeQuery(s"select * from $tableName")
      var rsSize = 0
      while (rs.next()) rsSize += 1
      assertEquals(numElements, rsSize)
      rsSize = 0
      val stmt1 = conn1.createStatement()
      rs = stmt1.executeQuery(s"select * from $tableName")
      while (rs.next()) rsSize += 1
      assertEquals(0, rsSize)

      // users gemfire2 & gemfire3 should also not get policy applied on them
      val stmt2 = conn2.createStatement()
      rs = stmt2.executeQuery(s"select * from $tableName")
      rsSize = 0
      while (rs.next()) rsSize += 1
      assertEquals(numElements, rsSize)
      rsSize = 0
      val stmt3 = conn3.createStatement()
      rs = stmt3.executeQuery(s"select * from $tableName")
      rsSize = 0
      while (rs.next()) rsSize += 1
      assertEquals(numElements, rsSize)
      rsSize = 0
      // let user2 drop the policy
      stmt2.execute(s"drop policy $schema.testPolicy1")
    } finally {
      conn.close()
      conn1.close()
      conn2.close()
      conn3.close()
    }
  }

  test("users of other ldap group not allowed to create or drop policies") {
    val conn = getConnection(Some(tableCreator))
    val stmt = conn.createStatement()
    val conn1 = getConnection(Some(otherUser))
    val conn2 = getConnection(Some(user2))
    val conn3 = getConnection(Some(user3))
    try {
      val stmt1 = conn1.createStatement()
      try {
        stmt1.execute(s"create policy $schema.testPolicy1 on  " +
            s"$colTableName for select to current_user using id < 0")
        fail("other user cannot create policy in other's schema")
      } catch {
        case _: SQLException =>
        case _: StandardException =>
        case th: Throwable => throw th
      }

      stmt.execute(s"create policy $schema.testPolicy1 on  " +
          s"$colTableName for select to current_user using id < 0")
      // let other user drop the policy
      try {
        stmt1.execute(s"drop policy $schema.testPolicy1")
        fail("other user cannot drop policy in other's schema")
      } catch {
        case _: SQLException =>
        case _: StandardException =>
        case th: Throwable => throw th
      }
      val stmt2 = conn2.createStatement()
      stmt2.execute(s"drop policy $schema.testPolicy1")
    } finally {
      conn.close()
      conn1.close()
      conn2.close()
      conn3.close()
    }
  }

  test("check toggle row level security behaviour for ldap groups") {
    val conn = getConnection(Some(tableCreator))
    val stmt = conn.createStatement()
    val conn1 = getConnection(Some(otherUser))
    val conn2 = getConnection(Some(user2))
    val conn3 = getConnection(Some(user3))
    try {
      val stmt1 = conn1.createStatement()
      try {
        stmt1.execute(s"alter table $colTableName disable row level security")
      } catch {
        case _: SQLException =>
        case _: StandardException =>
        case th: Throwable => throw th
      }
      val stmt2 = conn2.createStatement()
      stmt2.execute(s"alter table $colTableName enable row level security")
    } finally {
      conn.close()
      conn1.close()
      conn2.close()
      conn3.close()
    }

  }

  private def getConnection(user: Option[String] = None): Connection = {
    val props = new Properties()
    if (user.isDefined) {
      props.put(Attribute.USERNAME_ATTR, user.get)
      props.put(Attribute.PASSWORD_ATTR, user.get)
    }
    DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort", props)
  }

}
