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
package org.apache.spark.sql.policy

import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties

import com.pivotal.gemfirexd.internal.iapi.error.StandardException
import com.pivotal.gemfirexd.internal.iapi.reference.Property
import com.pivotal.gemfirexd.{Attribute, TestUtil}
import org.junit.Assert.assertEquals

import org.apache.spark.SparkConf
import org.apache.spark.sql.SnappySession

class RestrictTableCreationPolicyTest extends PolicyTestBase {

  val user1 = "gemfire1"
  val user2 = "gemfire2"
  val user3 = "gemfire3"
  val tableCreator: String = user1
  val ownerLdapGroup = "gemGroup1" // users gem1, gem2, gem3
  val otherLdapGroup = "gemGroup3" // users gem6, gem7, gem8
  val otherUser = "gemfire6"
  val props = Map.empty[String, String]
  val schema = "tax"
  val numElements = 100
  val colTableName: String = s"$schema.ColumnTable"
  val rowTableName: String = s"$schema.RowTable"
  var ownerSession: SnappySession = _

  var serverHostPort: String = _

  override def beforeAll(): Unit = {
    System.setProperty(Property.SNAPPY_RESTRICT_TABLE_CREATE, "true")
    super.beforeAll()
    val seq = for (i <- 0 until numElements) yield {
      (s"name_$i", i)
    }
    val rdd = sc.parallelize(seq)
    ownerSession = snc.snappySession.newSession()
    serverHostPort = TestUtil.startNetServer()
    ownerSession.conf.set(Attribute.USERNAME_ATTR, tableCreator)
    ownerSession.conf.set(Attribute.PASSWORD_ATTR, tableCreator)
    val dataDF = ownerSession.createDataFrame(rdd)

    // check failure in CREATE SCHEMA with authorization when using user session
    try {
      ownerSession.sql(s"create schema $schema authorization ldapgroup:$ownerLdapGroup")
      fail("Expected security failure")
    } catch {
      case se: SQLException if se.getSQLState == "42508" => // expected
    }
    // CREATE SCHEMA with authorization should work with admin privileges
    val adminSession = snc.snappySession.newSession()
    adminSession.conf.set(Attribute.USERNAME_ATTR, sysUser)
    adminSession.conf.set(Attribute.PASSWORD_ATTR, sysUser)
    adminSession.sql(s"create schema $schema authorization ldapgroup:$ownerLdapGroup")

    ownerSession.sql(s"CREATE TABLE $colTableName (name String, id Int) " +
        s" USING column ")

    ownerSession.sql(s"CREATE TABLE $rowTableName (name String, id Int) " +
        s" USING row ")
    ownerSession.sql(s"grant select on table $colTableName to ldapgroup:$otherLdapGroup")
    ownerSession.sql(s"grant select on table $rowTableName to ldapgroup:$otherLdapGroup")

    ownerSession.sql(s"alter table $colTableName enable row level security")
    ownerSession.sql(s"alter table $rowTableName enable row level security")

    dataDF.write.insertInto(colTableName)
    dataDF.write.insertInto(rowTableName)
  }

  protected override def newSparkConf(addOn: SparkConf => SparkConf): SparkConf = {
    newLDAPSparkConf(addOn)
  }

  override def afterAll(): Unit = {
    ownerSession.dropTable(colTableName, ifExists = true)
    ownerSession.dropTable(rowTableName, ifExists = true)
    super.afterAll()
    System.clearProperty(Property.SNAPPY_RESTRICT_TABLE_CREATE)
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
