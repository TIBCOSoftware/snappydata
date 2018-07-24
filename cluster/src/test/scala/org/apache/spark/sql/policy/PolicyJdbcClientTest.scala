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
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

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
    val conn = getConnection(Some(tableOwner))
    val stmt = conn.createStatement()
    val conn1 = getConnection(Some("UserX"))
    try {
      stmt.execute(s"create policy testPolicy1 on  " +
          s"$colTableName for select to current_user using id < 0")
      var rs = stmt.executeQuery(s"select * from $colTableName")
      var rsSize = 0
      while(rs.next()) rsSize += 1
      assertEquals(numElements, rsSize)

      rsSize = 0

      val stmt1 = conn1.createStatement()


      rs = stmt1.executeQuery(s"select * from $colTableName")
      while(rs.next()) rsSize += 1
      assertEquals(0, rsSize)
      stmt.execute("drop policy testPolicy1")
    } finally {
      conn.close()
    }

  }



  ignore("ignore for now") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3),
      Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => Data(s.head, s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.format("column").mode(SaveMode.Append).options(props)
        .saveAsTable(colTableName)
    snc.sql(s"create policy testPolicy1 on  $colTableName for select to current_user" +
        s" using col1 > 0")
  }

  private def getConnection(user: Option[String] = None): Connection = {
    val props = new Properties()
    if (user.isDefined) {
      props.put(Attribute.USERNAME_ATTR, user.get)
    }
    DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort", props)
  }

}


