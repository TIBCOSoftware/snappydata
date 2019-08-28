/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
import org.junit.Assert.{assertFalse, assertTrue}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SnappyContext

class SecurityEnabledJdbcClientPolicyTest extends PolicyTestBase {

  val user1 = "gemfire1"
  val user2 = "gemfire2"

  val props = Map.empty[String, String]
  val tableOwner: String = user1
  val numElements = 100
  val colTableName: String = s"$tableOwner.ColumnTable"
  val rowTableName: String = s"$tableOwner.RowTable"
  var ownerContext: SnappyContext = _

  var serverHostPort: String = _


  override def beforeAll(): Unit = {
    super.beforeAll()
    val seq = for (i <- 0 until numElements) yield {
      (s"name_$i", i)
    }
    val rdd = sc.parallelize(seq)
    ownerContext = snc.newSession()
    serverHostPort = TestUtil.startNetServer()
    ownerContext.snappySession.conf.set(Attribute.USERNAME_ATTR, tableOwner)
    ownerContext.snappySession.conf.set(Attribute.PASSWORD_ATTR, tableOwner)
    val dataDF = ownerContext.createDataFrame(rdd)

    ownerContext.sql(s"CREATE TABLE $colTableName (name String, id Int) " +
        s" USING column ")

    ownerContext.sql(s"CREATE TABLE $rowTableName (name String, id Int) " +
        s" USING row ")
    ownerContext.sql(s"grant select on table $colTableName to $user2")
    ownerContext.sql(s"grant select on table $rowTableName to $user2")
    dataDF.write.insertInto(colTableName)
    dataDF.write.insertInto(rowTableName)
  }

  protected override def newSparkConf(addOn: SparkConf => SparkConf): SparkConf = {
    newLDAPSparkConf(addOn)
  }

  override def afterAll(): Unit = {
    ownerContext.dropTable(colTableName, ifExists = true)
    ownerContext.dropTable(rowTableName, ifExists = true)
    super.afterAll()
  }


  test("test bug causing recursion with query having filter using col table - ENT-40") {
    this.testRecursionBug(colTableName)
  }

  test("test bug causing recursion with query having filter using row table - ENT-40") {
    this.testRecursionBug(rowTableName)
  }

  private def testRecursionBug(tableName: String): Unit = {
    ownerContext.sql(s"create policy testPolicy1 on  " +
        s"$tableName for select to $user2 using id < 30")

    val conn1 = getConnection(Some(user2))
    try {
      val q = s"select * from $tableName where id < 20 and name = 'name_3'"
      val rs = conn1.createStatement().executeQuery(q)
      assertTrue(rs.next())
      assertFalse(rs.next())
      ownerContext.sql("drop policy testPolicy1")
    } finally {
      conn1.close()
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
