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

import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.pivotal.gemfirexd.{Attribute, TestUtil}
import io.snappydata.SnappyFunSuite
import io.snappydata.core.Data
import org.junit.Assert.assertEquals
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.Logging
import org.apache.spark.sql.{SaveMode, SnappyContext}

class PolicyJdbcClientTest extends SnappyFunSuite
    with Logging
    with BeforeAndAfter
    with BeforeAndAfterAll {


  var serverHostPort: String = _

  val props = Map.empty[String, String]
  val tableOwner = "ashahid"
  val numElements = 100
  val colTableName: String = s"$tableOwner.ColumnTable"
  val rowTableName: String = s"${tableOwner}.RowTable"
  var ownerContext: SnappyContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val seq = for (i <- 0 until numElements) yield {
      (s"name_$i", i)
    }
    val rdd = sc.parallelize(seq)
    ownerContext = snc.newSession()
    serverHostPort = TestUtil.startNetServer()
    ownerContext.snappySession.conf.set(Attribute.USERNAME_ATTR, tableOwner)

    val dataDF = ownerContext.createDataFrame(rdd)

    ownerContext.sql(s"CREATE TABLE $colTableName (name String, id Int) " +
        s" USING column ")

    ownerContext.sql(s"CREATE TABLE $rowTableName (name String, id Int) " +
        s" USING row ")
    dataDF.write.insertInto(colTableName)
    dataDF.write.insertInto(rowTableName)
    val conn = getConnection()
    try {
      val stmt = conn.createStatement()
      stmt.execute(s"alter table $colTableName enable row level security")
      stmt.execute(s"alter table $rowTableName enable row level security")
    } finally {
      conn.close
    }

  }

  override def afterAll(): Unit = {
    ownerContext.dropTable(colTableName, true)
    ownerContext.dropTable(rowTableName, true)
    super.afterAll()

  }

  test("Policy creation on a column table using jdbc client") {
    this.testPolicy(colTableName)
  }

  test("Policy creation on a row table using jdbc client") {
    this.testPolicy(rowTableName)
  }

  private def testPolicy(tableName: String) {
    val conn = getConnection(Some(tableOwner))
    val stmt = conn.createStatement()
    val conn1 = getConnection(Some("UserX"))
    try {
      stmt.execute(s"create policy testPolicy1 on  " +
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
      stmt.execute("drop policy testPolicy1")
    } finally {
      conn.close()
      conn1.close()
    }
  }

  test("test multiple policies application using snappy context on column table") {
    this.testMultiplePolicy(colTableName)
  }

  test("test multiple policies application using snappy context on row table") {
    this.testMultiplePolicy(rowTableName)
  }

  test("Test plan invalidation when queries & policy creation are mixed on column table") {
    this.testMultiplePolicyCreationWithQuery(colTableName)
  }

  test("Test plan invalidation when queries & policy creation are mixed on row table") {
    this.testMultiplePolicyCreationWithQuery(rowTableName)
  }

  test("test policy recreation on column table ENT:38") {
    this.testPolicyRecreation(colTableName)
  }

  test("test policy recreation on row table ENT:38") {
    this.testPolicyRecreation(colTableName)
  }

  def testPolicyRecreation(tableName: String): Unit = {
    val conn = getConnection(Some(tableOwner))
    val stmt = conn.createStatement()
    val conn1 = getConnection(Some("UserX"))
    val stmt1 = conn1.createStatement()
    try {

      var rs = stmt.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      var rsSize = 0
      while (rs.next()) rsSize += 1
      assertEquals(numElements - 1 - 25 + 10, rsSize)

      rsSize = 0
      rs = stmt1.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      while (rs.next()) rsSize += 1
      assertEquals(numElements - 1 - 25 + 10, rsSize)


      stmt.execute(s"create policy testPolicy1 on  " +
          s"$tableName for select to current_user using id > 10")

      rs = stmt.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      rsSize = 0
      while (rs.next()) rsSize += 1
      assertEquals(numElements - 1 - 25 + 10, rsSize)
      rsSize = 0

      rs = stmt1.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      while (rs.next()) rsSize += 1
      assertEquals(numElements - 1 - 25, rsSize)

      stmt.execute(s"alter table $tableName disable row level security")
      stmt.execute("drop policy testPolicy1")
      rsSize = 0
      rs = stmt1.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      while (rs.next()) rsSize += 1
      assertEquals(numElements - 1 - 25 + 10, rsSize)

      stmt.execute(s"create policy testPolicy1 on  " +
          s"$tableName for select to current_user using id > 10")

      stmt.execute(s"alter table $tableName enable row level security")
      rsSize = 0
      rs = stmt1.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      while (rs.next()) rsSize += 1
      assertEquals(numElements - 1 - 25, rsSize)

      stmt.execute("drop policy testPolicy1")

    } finally {
      conn.close()
      conn1.close()
    }
  }


  def testMultiplePolicyCreationWithQuery(tableName: String): Unit = {
    val conn = getConnection(Some(tableOwner))
    val stmt = conn.createStatement()
    val conn1 = getConnection(Some("UserX"))
    val stmt1 = conn1.createStatement()
    try {

      var rs = stmt.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      var rsSize = 0
      while (rs.next()) rsSize += 1
      assertEquals(numElements - 1 - 25 + 10, rsSize)

      rsSize = 0
      rs = stmt1.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      while (rs.next()) rsSize += 1
      assertEquals(numElements - 1 - 25 + 10, rsSize)


      stmt.execute(s"create policy testPolicy1 on  " +
          s"$tableName for select to current_user using id > 10")

      rs = stmt.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      rsSize = 0
      while (rs.next()) rsSize += 1
      assertEquals(numElements - 1 - 25 + 10, rsSize)
      rsSize = 0

      rs = stmt1.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      while (rs.next()) rsSize += 1
      assertEquals(numElements - 1 - 25, rsSize)


      stmt.execute(s"create policy testPolicy2 on  " +
          s"$tableName for select to current_user using id < 30")

      rs = stmt.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      rsSize = 0
      while (rs.next()) rsSize += 1
      assertEquals(numElements - 1 - 25 + 10, rsSize)
      rsSize = 0

      rs = stmt1.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      while (rs.next()) rsSize += 1
      assertEquals(4, rsSize)
      stmt.execute("drop policy testPolicy1")
      stmt.execute("drop policy testPolicy2")
    } finally {
      conn.close()
      conn1.close()
    }
  }

  private def testMultiplePolicy(tableName: String) {
    val conn = getConnection(Some(tableOwner))
    val stmt = conn.createStatement()
    val conn1 = getConnection(Some("UserX"))
    try {
      stmt.execute(s"create policy testPolicy1 on  " +
          s"$tableName for select to current_user using id > 10")

      stmt.execute(s"create policy testPolicy2 on  " +
          s"$tableName for select to current_user using id < 30")

      var rs = stmt.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      var rsSize = 0
      while (rs.next()) rsSize += 1
      assertEquals(numElements - 1 - 25 + 10, rsSize)
      rsSize = 0
      val stmt1 = conn1.createStatement()
      rs = stmt1.executeQuery(s"select * from $tableName where id > 25 or id < 10 ")
      while (rs.next()) rsSize += 1
      assertEquals(4, rsSize)
      stmt.execute("drop policy testPolicy1")
      stmt.execute("drop policy testPolicy2")
    } finally {
      conn.close()
      conn1.close()
    }
  }

  test("sys.syspolicies table") {
    val conn = getConnection(Some(tableOwner))
    val stmt = conn.createStatement()
    try {
      stmt.execute(s"create policy testPolicy1 on  " +
          s"$colTableName for select to current_user using id > 10")

      stmt.execute(s"create policy testPolicy2 on  " +
          s"$rowTableName for select to current_user using id < 30")

      stmt.executeQuery("select * from sys.syspolicies")
      stmt.execute("drop policy testPolicy1")
      stmt.execute("drop policy testPolicy2")
    } finally {
      conn.close()

    }
  }

  test("old query plan invalidation on enabling rls on column table using jdbc client") {
    this.testQueryPlanInvalidationOnRLSEnbaling(colTableName)
  }

  test("old query plan invalidation on enabling rls on row table using jdbc client") {
    this.testQueryPlanInvalidationOnRLSEnbaling(rowTableName)
  }

  private def testQueryPlanInvalidationOnRLSEnbaling(tableName: String): Unit = {
    // first disable RLS
    ownerContext.sql(s"alter table $tableName disable row level security")
    // now create a policy
    ownerContext.sql(s"create policy testPolicy1 on  " +
        s"$tableName for select to current_user using id < 30")
    val conn = getConnection(Some(tableOwner))

    val conn1 = getConnection(Some("UserX"))
    try {

      val q = s"select * from $tableName where id > 70"
      val stmt1 = conn1.createStatement()
      var rs = stmt1.executeQuery(q)
      var numRows = 0
      while (rs.next()) numRows += 1
      assertEquals(29, numRows)
      // fire again
      rs = stmt1.executeQuery(q)
      numRows = 0
      while (rs.next()) numRows += 1
      assertEquals(29, numRows)
      // fire again
      rs = stmt1.executeQuery(q)
      numRows = 0
      while (rs.next()) numRows += 1
      assertEquals(29, numRows)

      val stmt = conn.createStatement()
      rs = stmt.executeQuery(q)
      numRows = 0
      while (rs.next()) numRows += 1
      assertEquals(29, numRows)

      // fire again
      rs = stmt1.executeQuery(q)
      numRows = 0
      while (rs.next()) numRows += 1
      assertEquals(29, numRows)

      // Now enable RLS

      stmt.execute(s"alter table $tableName enable row level security")
      rs = stmt.executeQuery(q)
      numRows = 0
      while (rs.next()) numRows += 1
      assertEquals(29, numRows)

      rs = stmt1.executeQuery(q)
      numRows = 0
      while (rs.next()) numRows += 1
      assertEquals(0, numRows)
      ownerContext.sql("drop policy testPolicy1")
    } finally {
      conn1.close()
      conn.close()
    }

  }



  private def getConnection(user: Option[String] = None): Connection = {
    val props = new Properties()
    if (user.isDefined) {
      props.put(Attribute.USERNAME_ATTR, user.get)
    }
    DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort", props)
  }

}


