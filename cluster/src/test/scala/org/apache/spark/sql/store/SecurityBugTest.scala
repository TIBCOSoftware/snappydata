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
package org.apache.spark.sql.store

import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.pivotal.gemfirexd.{Attribute, TestUtil}
import com.pivotal.gemfirexd.security.{LdapTestServer, SecurityTestUtils}
import io.snappydata.util.TestUtils
import io.snappydata.{Constant, PlanTest, Property, SnappyFunSuite}
import org.scalatest.BeforeAndAfterAll
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}

import org.apache.spark.SparkConf

class SecurityBugTest extends SnappyFunSuite with BeforeAndAfterAll {
  private val sysUser = "gemfire10"
  var serverHostPort: String = _
  override def beforeAll(): Unit = {
    this.stopAll()
    super.beforeAll()
    snc
    serverHostPort = TestUtil.startNetServer()
  }
  
  protected override def newSparkConf(addOn: (SparkConf) => SparkConf): SparkConf = {
    val ldapProperties = SecurityTestUtils.startLdapServerAndGetBootProperties(0, 0, sysUser,
      getClass.getResource("/auth.ldif").getPath)
    import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE}
    for (k <- List(Attribute.AUTH_PROVIDER, AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE)) {
      System.setProperty(k, ldapProperties.getProperty(k))
    }
    System.setProperty(Constant.STORE_PROPERTY_PREFIX + Attribute.USERNAME_ATTR, sysUser)
    System.setProperty(Constant.STORE_PROPERTY_PREFIX + Attribute.PASSWORD_ATTR, sysUser)
    val conf = new org.apache.spark.SparkConf()
        .setAppName("BugTest")
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
    this.stopAll()
    TestUtil.stopNetServer()
    val ldapServer = LdapTestServer.getInstance()
    if (ldapServer.isServerStarted) {
      ldapServer.stopService()
    }
    import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE}
    for (k <- List(Attribute.AUTH_PROVIDER, AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE)) {
      System.clearProperty(k)
      System.clearProperty("gemfirexd." + k)
      System.clearProperty(Constant.STORE_PROPERTY_PREFIX  + k)
    }
    System.clearProperty(Constant.STORE_PROPERTY_PREFIX + Attribute.USERNAME_ATTR)
    System.clearProperty(Constant.STORE_PROPERTY_PREFIX + Attribute.PASSWORD_ATTR)
    System.setProperty("gemfirexd.authentication.required", "false")
  }

  test("Bug SNAP-2255 connection pool exhaustion") {
    val user1 = "gemfire1"
    val user2 = "gemfire2"

    val snc1 = snc.newSession()
    snc1.snappySession.conf.set(Attribute.USERNAME_ATTR, user1)
    snc1.snappySession.conf.set(Attribute.PASSWORD_ATTR, user1)

    snc1.sql(s"create table test (id  integer," +
        s" name STRING) using column")
    snc1.sql("insert into test values (1, 'name1')")
    snc1.sql(s"GRANT select ON TABLE  test TO  $user2")

    // TODO : Use the actual connection pool limit
    val limit = 500

    for (i <- 1 to limit) {
      val snc2 = snc.newSession()
      snc2.snappySession.conf.set(Attribute.USERNAME_ATTR, user2)
      snc2.snappySession.conf.set(Attribute.PASSWORD_ATTR, user2)


      val rs = snc2.sql(s"select * from $user1.test").collect()
      assertEquals(1, rs.length)
    }
  }

  test("Bug SNAP-2827 admin is unable to drop schema") {
    val user1 = "gemfire1"
    val adminSnc = snc.newSession()
    adminSnc.snappySession.conf.set(Attribute.USERNAME_ATTR, sysUser)
    adminSnc.snappySession.conf.set(Attribute.PASSWORD_ATTR, sysUser)
    adminSnc.sql("create schema test_schema1 authorization ldapgroup:gemGroup1")
    val snc1 = snc.newSession()
    snc1.snappySession.conf.set(Attribute.USERNAME_ATTR, user1)
    snc1.snappySession.conf.set(Attribute.PASSWORD_ATTR, user1)
    snc1.sql(s"create table test_schema1.test (id  integer," +
      s" name STRING) using column")
    adminSnc.sql("drop table test_schema1.test")
    adminSnc.sql("drop schema test_schema1")
    adminSnc.sql("create schema test_schema2 authorization ldapgroup:gemGroup1")
    adminSnc.sql("drop schema test_schema2")

    val adminConn = getConnection(Some(sysUser))
    val adminStmt = adminConn.createStatement
    adminStmt.execute("create schema test_schema3 authorization ldapgroup:gemGroup1")
    val userConn = getConnection(Some(user1))
    val userStmt = userConn.createStatement
    userStmt.execute("create table test_schema3.test (id  integer, name STRING) using column")
    adminStmt.execute("drop table test_schema3.test")
    adminStmt.execute("drop schema test_schema3")
    adminStmt.execute("create schema test_schema4 authorization ldapgroup:gemGroup1")
    adminStmt.execute("drop schema test_schema4")
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
