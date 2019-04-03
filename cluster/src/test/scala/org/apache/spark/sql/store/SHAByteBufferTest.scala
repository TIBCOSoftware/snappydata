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

import java.io.{BufferedReader, FileReader}
import java.sql.{DriverManager, SQLException}
import java.util.Properties
import com.pivotal.gemfirexd.TestUtil
import io.snappydata.{Property, SnappyFunSuite}
import org.scalatest.BeforeAndAfterAll
import org.junit.Assert._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.SnappySession

class SHAByteBufferTest extends SnappyFunSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("simple aggregate query") {
    snc
    snc.sql("drop table if exists test1")
    snc.sql("create table test1 (col1 int, col2 int, col3 int, col4 int) " +
      "using column ")
    val range = 50
    val groupingDivisor = 10
    val insertDF = snc.range(50).selectExpr("id", "id*2", "id * 3",
      s"id % ${groupingDivisor} ")
    insertDF.write.insertInto("test1")
    val rs = snc.sql("select col4, sum(col1) as summ1, sum(col2) as summ2 " +
      " from test1 group by col4")
    import org.apache.spark.sql.execution.debug._
    rs.debugCodegen()
    val results = rs.collect()
    assertTrue(results.size > 0)
    rs.foreach( row => {
      val groupKey = row.getInt(0)
      val n = (range/groupingDivisor)
      val sum1 = ((n/2.0f)* ((2 * groupKey) + (n - 1)* groupingDivisor)).toLong
      val sum2 = ((n/2.0f)* ((2 *2* groupKey) + (n - 1) * 2 * groupingDivisor)).toLong
      val sumAgg1 = row.getLong(1)
      val sumAgg2 = row.getLong(2)
      assertEquals(sum1, sumAgg1)
      assertEquals(sum2, sumAgg2)
    })
    snc.dropTable("test1")
  }

  test("multiple aggregates query") {
    snc
    snc.sql("drop table if exists test1")
    snc.sql("create table test1 (col1 int, col2 int, col3 int, col4 String) " +
      "using column ")
    val range = 50
    val groupingDivisor = 10
    val insertDF = snc.range(50).selectExpr("id", "id*2", "id * 3",
      s"Concat( 'test', Cast(id % ${groupingDivisor} as string) ) ")
    insertDF.write.insertInto("test1")
    val rs = snc.sql("select col4, sum(col1) as summ1, sum(col2) as summ2 " +
      " from test1 group by col4")
    import org.apache.spark.sql.execution.debug._
    rs.debugCodegen()
    val results = rs.collect()
    assertTrue(results.size > 0)
    rs.foreach( row => {
      val groupKey = row.getString(0).substring("test".length).toInt
      val n = (range/groupingDivisor)
      val sum1 = ((n/2.0f)* ((2 * groupKey) + (n - 1)* groupingDivisor)).toLong
      val sum2 = ((n/2.0f)* ((2 *2* groupKey) + (n - 1) * 2 * groupingDivisor)).toLong
      val sumAgg1 = row.getLong(1)
      val sumAgg2 = row.getLong(2)
      assertEquals(sum1, sumAgg1)
      assertEquals(sum2, sumAgg2)
    })
    snc.dropTable("test1")
  }

  test("multiple aggregates query with null grouping keys") {
    snc
    snc.sql("drop table if exists test1")
    snc.sql("create table test1 (col1 int, col2 int, col3 int, col4 String) " +
      "using column ")
    val range = 50
    val groupingDivisor = 10
    val insertDF = snc.range(50).selectExpr("id", "id*2", "id * 3",
      s"Concat( 'test', Cast(id % ${groupingDivisor} as string) ) ")
    insertDF.write.insertInto("test1")
    val rs = snc.sql("select col4, sum(col1) as summ1, sum(col2) as summ2 " +
      " from test1 group by col4")
    import org.apache.spark.sql.execution.debug._
    rs.debugCodegen()
    val results = rs.collect()
    assertTrue(results.size > 0)
    rs.foreach( row => {
      val groupKey = row.getString(0).substring("test".length).toInt
      val n = (range/groupingDivisor)
      val sum1 = ((n/2.0f)* ((2 * groupKey) + (n - 1)* groupingDivisor)).toLong
      val sum2 = ((n/2.0f)* ((2 *2* groupKey) + (n - 1) * 2 * groupingDivisor)).toLong
      val sumAgg1 = row.getLong(1)
      val sumAgg2 = row.getLong(2)
      assertEquals(sum1, sumAgg1)
      assertEquals(sum2, sumAgg2)
    })
    snc.dropTable("test1")
  }
}
