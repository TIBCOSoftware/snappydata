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

import java.sql.SQLException

import com.pivotal.gemfirexd.Attribute
import io.snappydata.{Property, SnappyFunSuite}
import io.snappydata.core.Data
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SaveMode, SnappyContext, SnappySession}
import org.apache.spark.unsafe.types.UTF8String

class PolicyTest extends SnappyFunSuite
    with Logging
    with BeforeAndAfter
    with BeforeAndAfterAll {


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
    ownerContext.snappySession.conf.set(Attribute.USERNAME_ATTR, tableOwner)

    val dataDF = ownerContext.createDataFrame(rdd)

    ownerContext.sql(s"CREATE TABLE $colTableName (name String, id Int) " +
        s" USING column ")

    ownerContext.sql(s"CREATE TABLE $rowTableName (name String, id Int) " +
        s" USING row ")
    dataDF.write.insertInto(colTableName)
    dataDF.write.insertInto(rowTableName)
    ownerContext.sql(s"alter table $colTableName enable row level security")
    ownerContext.sql(s"alter table $rowTableName enable row level security")
  }

  override def afterAll(): Unit = {
    ownerContext.dropTable(colTableName, true)
    ownerContext.dropTable(rowTableName, true)
    super.afterAll()
  }

  test("Policy creation on a column table using snappy context") {
    this.testPolicy(colTableName)
  }

  test("Policy creation on a row table using snappy context") {
    this.testPolicy(rowTableName)
  }

  private def testPolicy(tableName: String) {
    ownerContext.sql(s"create policy testPolicy1 on  " +
        s"$tableName for select to current_user using id < 0")
    var rs = ownerContext.sql(s"select * from $tableName").collect()
    assertEquals(numElements, rs.length)

    val snc2 = snc.newSession()
    snc2.snappySession.conf.set(Attribute.USERNAME_ATTR, "UserX")

    rs = snc2.sql(s"select * from $tableName").collect()
    assertEquals(0, rs.length)
    ownerContext.sql("drop policy testPolicy1")
  }

  test("Check Policy Filter applied to the plan only once") {
    ownerContext.sql(s"create policy testPolicy2 on  " +
        s"$colTableName for select to current_user using id > 0")

    val snc2 = snc.newSession()
    snc2.snappySession.conf.set(Attribute.USERNAME_ATTR, "UserX")

    val df = snc2.sql(s"select * from $colTableName")
    val allFilters = df.queryExecution.analyzed.collect {
      case f: Filter => f
    }
    assertEquals(1, allFilters.map(_.condition).flatMap(ex => {
      ex.collect {
        case x@EqualTo(Literal(l1, StringType), Literal(l2, StringType))
          if l1.equals(UTF8String.fromString(PolicyProperties.rlsConditionString)) &&
              l2.equals(UTF8String.fromString(PolicyProperties.rlsConditionString)) => x
      }
    }).length)

    ownerContext.sql("drop policy testPolicy2")
  }

  test("test multiple policies application using snappy context on column table") {
    this.testMultiplePolicy(colTableName)
  }

  test("test multiple policies application using snappy context on row table") {
    this.testMultiplePolicy(rowTableName)
  }

  test("old query plan invalidation on creation of policy on column table using snappy context") {
    this.testQueryPlanInvalidation(colTableName)
  }

  test("old query plan invalidation on creation of policy on row table using snappy context") {
    this.testQueryPlanInvalidation(rowTableName)
  }

  private def testQueryPlanInvalidation(tableName: String): Unit = {

    val snc2 = snc.newSession()
    snc2.snappySession.conf.set(Attribute.USERNAME_ATTR, "UserX")
    val q = s"select * from $tableName where id > 70"
    var rs = snc2.sql(q)
    assertEquals(29, rs.collect().length)
    // fire again
    rs = snc2.sql(q)
    assertEquals(29, rs.collect().length)
    // fire again
    rs = ownerContext.sql(q)
    assertEquals(29, rs.collect().length)

    // fire again
    rs = snc2.sql(q)
    assertEquals(29, rs.collect().length)



    // now create a policy
    ownerContext.sql(s"create policy testPolicy1 on  " +
        s"$tableName for select to current_user using id < 30")

    rs = ownerContext.sql(q)
    assertEquals(29, rs.collect().length)

    rs = snc2.sql(q)
    assertEquals(0, rs.collect().length)
    ownerContext.sql("drop policy testPolicy1")

  }

  test("old query plan invalidation on enabling rls on column table using snappy context") {
    this.testQueryPlanInvalidationOnRLSEnbaling(colTableName)
  }

  test("old query plan invalidation on enabling rls on row table using snappy context") {
    this.testQueryPlanInvalidationOnRLSEnbaling(rowTableName)
  }

  private def testQueryPlanInvalidationOnRLSEnbaling(tableName: String): Unit = {
    // first disable RLS
    ownerContext.sql(s"alter table $tableName disable row level security")
    // now create a policy
    ownerContext.sql(s"create policy testPolicy1 on  " +
        s"$tableName for select to current_user using id < 30")

    val snc2 = snc.newSession()
    snc2.snappySession.conf.set(Attribute.USERNAME_ATTR, "UserX")
    val q = s"select * from $tableName where id > 70"

    var rs = snc2.sql(q)
    assertEquals(29, rs.collect().length)
    // fire again
    rs = snc2.sql(q)
    assertEquals(29, rs.collect().length)
    // fire again
    rs = ownerContext.sql(q)
    assertEquals(29, rs.collect().length)

    rs = ownerContext.sql(q)
    assertEquals(29, rs.collect().length)

    // fire again
    rs = snc2.sql(q)
    assertEquals(29, rs.collect().length)

   // Now enable RLS

    ownerContext.sql(s"alter table $tableName enable row level security")


    rs = ownerContext.sql(q)
    assertEquals(29, rs.collect().length)

    rs = snc2.sql(q)
    assertEquals(0, rs.collect().length)
    ownerContext.sql("drop policy testPolicy1")

  }
  test("test bug causing recursion with query having filter using col table") {
    this.testRecursionBug(colTableName)
  }

  test("test bug causing recursion with query having filter using row table") {
    this.testRecursionBug(rowTableName)
  }

  private def testRecursionBug(tableName: String): Unit = {
    ownerContext.sql(s"create policy testPolicy1 on  " +
        s"$tableName for select to userX using id < 30 and name = 'name_1'")
    val snc2 = snc.newSession()
    snc2.snappySession.conf.set(Attribute.USERNAME_ATTR, "UserX")
    val q = s"select * from $tableName where id < 20 and name = 'name_1'"
    val x = snc2.sql(q)
    val rs = x.collect()
    assertEquals(1, rs.length)
    ownerContext.sql("drop policy testPolicy1")

  }

  private def testMultiplePolicy(tableName: String) {
    ownerContext.sql(s"create policy testPolicy1 on  " +
        s"$tableName for select to current_user using id > 10")
    ownerContext.sql(s"create policy testPolicy2 on  " +
        s"$tableName for select to current_user using id < 20")
    var rs = ownerContext.sql(s"select * from $tableName").collect()
    assertEquals(numElements, rs.length)

    val snc2 = snc.newSession()
    snc2.snappySession.conf.set(Attribute.USERNAME_ATTR, "UserX")

    rs = snc2.sql(s"select * from $tableName").collect()
    assertEquals(9, rs.length)
    ownerContext.sql("drop policy testPolicy1")
    ownerContext.sql("drop policy testPolicy2")
  }

  test("Policy creation & dropping allowed by all users if security is disabled") {
    val snc2 = snc.newSession()
    snc2.snappySession.conf.set(Attribute.USERNAME_ATTR, "UserX")
    snc2.sql(s"create policy testPolicy2 on  " +
        s"$colTableName for select to current_user using id > 10")
    snc2.sql("drop policy testPolicy2")

  }

}
