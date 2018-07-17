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
  }

  override def afterAll(): Unit = {
    ownerContext.snappySession.conf.set(Attribute.USERNAME_ATTR, tableOwner)
    ownerContext.dropTable(colTableName, true)
    ownerContext.dropTable(rowTableName, true)
    super.afterAll()

  }

  test("Policy creation on a column table using snappy context") {
    ownerContext.sql(s"create policy testPolicy1 on  " +
        s"$colTableName for select to current using id < 0")
    var rs = ownerContext.sql(s"select * from $colTableName").collect()
    assertEquals(numElements, rs.length)

    val snc2 = snc.newSession()
    snc2.snappySession.conf.set(Attribute.USERNAME_ATTR, "UserX")

    rs = snc2.sql(s"select * from $colTableName").collect()
    assertEquals(0, rs.length)
    ownerContext.sql("drop policy testPolicy1")
  }

  test("Check Policy Filter applied to the plan only once") {
    ownerContext.sql(s"create policy testPolicy2 on  " +
        s"$colTableName for select to current using id > 0")

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

  ignore("ignore for now") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3),
      Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => Data(s.head, s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.format("column").mode(SaveMode.Append).options(props)
        .saveAsTable(colTableName)
    snc.sql(s"create policy testPolicy1 on  $colTableName for select to current using col1 > 0")
  }

}
