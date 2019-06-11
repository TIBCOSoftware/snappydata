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
package org.apache.spark.sql.execution

import java.sql.DriverManager

import com.pivotal.gemfirexd.TestUtil
import io.snappydata.SnappyFunSuite
import io.snappydata.core.Data
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.Logging
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Row, SnappyContext, SnappySession}

case class DataWithMultipleKeys(pk1: Int, col1: Int, pk2: String, col2: Long)
case class DataDiffCol(column1: Int, column2: Int, column3: Int)

case class DataDiffColMultipleKeys(pk1: Int, col1: Int, pk2: String, col2: Long)

case class DataWithMatchingKeyColumns(column1: Int, column2: Int, column3: Int)

case class DataStrCol(column1: Int, column2: String, column3: Int)


case class RData(C_CustKey: Int, C_Name: String,
    C_Address: String, C_NationKey: Int,
    C_Phone: String, C_AcctBal: Decimal,
    C_MktSegment: String, C_Comment: String, skip: String)

class SnappyTableMutableAPISuite extends SnappyFunSuite with Logging with BeforeAndAfter
    with BeforeAndAfterAll {

  val data1 = Seq(Seq(1, 22, 3), Seq(7, 81, 9), Seq(9, 23, 3), Seq(4, 24, 3),
    Seq(5, 6, 7), Seq(88, 88, 88))

  val data2 = Seq(Seq(1, 22, 3), Seq(7, 81, 9), Seq(9, 23, 3), Seq(4, 24, 3),
    Seq(5, 6, 7), Seq(8, 8, 8), Seq(88, 88, 90))

  val data3 = Seq(Seq(1, "22", 3), Seq(7, "81", 9), Seq(9, "23", 3), Seq(4, null, 3),
    Seq(5, "6", 7), Seq(88, "88", 88))

  val data4 = Seq(Seq(1, "22", 3), Seq(7, "81", 9), Seq(9, "23", 3), Seq(4, null, 3),
    Seq(5, "6", 7), Seq(8, "8", 8), Seq(88, "88", 90))

  val data5 = Seq(Seq(1, 22, "str1", 3L), Seq(7, 81, "str7", 9L), Seq(9, 23, "str9", 3L),
    Seq(4, 24, "str4", 3L), Seq(5, 6, "str5", 7L), Seq(8, 8, "str8", 8L), Seq(88, 88, "str88", 88L))

  val data6 = Seq(Seq(1, 22, "str1", 3L), Seq(7, 81, "str7", 9L), Seq(9, 23, "str9", 3L),
    Seq(4, 24, "str4", 3L), Seq(5, 6, "str5", 7L), Seq(88, 88, "str88", 88L))

  private var netPort = 0

  after {
    snc.dropTable("col_table", ifExists = true)
    snc.dropTable("row_table", ifExists = true)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    assert(this.snc !== null)
    // start a local network server
    netPort = TestUtil.startNetserverAndReturnPort()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestUtil.stopNetServer()
  }

  test("PutInto with sql") {
    val snc = new SnappySession(sc)
    val rdd = sc.parallelize(data1, 2).map(s => Data(s(0), s(1), s(2)))
    val df1 = snc.createDataFrame(rdd)

    val rdd2 = sc.parallelize(data2, 2).map(s => Data(s(0), s(1), s(2)))
    val df2 = snc.createDataFrame(rdd2)

    val props = Map("BUCKETS" -> "2", "PARTITION_BY" -> "col1", "key_columns" -> "col2")
    val props1 = Map.empty[String, String]

    snc.createTable("col_table", "column", df1.schema, props)
    snc.createTable("row_table", "row", df2.schema, props1)

    df1.write.insertInto("col_table")
    df2.write.insertInto("row_table")

    snc.sql("put into table col_table" +
        "   select * from row_table")

    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 7)
    assert(resultdf.contains(Row(8, 8, 8)))
    assert(resultdf.contains(Row(88, 88, 90)))
  }

  ignore("Multiple update with correlated subquery") {
    val snc = new SnappySession(sc)
    val rdd = sc.parallelize(data1, 2).map(s => Data(s(0), s(1), s(2)))
    val df1 = snc.createDataFrame(rdd)

    val rdd2 = sc.parallelize(data2, 2).map(s => Data(s(0), s(1), s(2)))
    val df2 = snc.createDataFrame(rdd2)

    val props = Map("BUCKETS" -> "2", "PARTITION_BY" -> "col1", "key_columns" -> "col2")
    val props1 = Map.empty[String, String]

    snc.createTable("col_table", "column", df1.schema, props)
    snc.createTable("row_table", "row", df2.schema, props1)

    df1.write.insertInto("col_table")
    df2.write.insertInto("row_table")

    snc.sql("update col_table  set col3 = "  +
        " (select col3 from row_table  where col_table.col2 = row_table.col2 )")

    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 7)
    assert(resultdf.contains(Row(8, 8, 8)))
    assert(resultdf.contains(Row(88, 88, 90)))
  }

  test("Single column update with join") {
    val snc = new SnappySession(sc)
    val rdd = sc.parallelize(data1, 2).map(s => Data(s(0), s(1), s(2)))
    val df1 = snc.createDataFrame(rdd)

    val rdd2 = sc.parallelize(data2, 2).map(s => Data(s(0), s(1), s(2)))
    val df2 = snc.createDataFrame(rdd2)

    val props = Map("BUCKETS" -> "2", "PARTITION_BY" -> "col1", "key_columns" -> "col2")
    val props1 = Map.empty[String, String]

    snc.createTable("col_table", "column", df1.schema, props)
    snc.createTable("row_table", "row", df2.schema, props1)

    df1.write.insertInto("col_table")
    df2.write.insertInto("row_table")

    snc.sql("update col_table set a.col3 = b.col3 from " +
        "col_table a join row_table b on a.col2 = b.col2")

    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 6)
    assert(resultdf.contains(Row(88, 88, 90)))
  }

  test("Multiple columns update with join") {
    val snc = new SnappySession(sc)
    snc.sql("create table col_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using column options(BUCKETS '2', PARTITION_BY 'col1') ")
    snc.sql("create table row_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using row options(BUCKETS '2', PARTITION_BY 'col1') ")

    snc.insert("row_table", Row(1, "1", "100", 100))
    snc.insert("row_table", Row(2, "2", "200", 200))
    snc.insert("row_table", Row(4, "4", "400", 4))

    snc.insert("col_table", Row(1, "1", "1", 1))
    snc.insert("col_table", Row(2, "2", null, 2))
    snc.insert("col_table", Row(4, "4", "3", 3))

    snc.sql("update col_table set a.col3 = b.col3, a.col4 = b.col4 from " +
        "col_table a join row_table b on a.col2 = b.col2")

    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 3)
    assert(resultdf.contains(Row(1, "1", "100", 100)))
    assert(resultdf.contains(Row(2, "2", "200", 200)))
    assert(resultdf.contains(Row(4, "4", "400", 4)))
  }

  test("Multiple columns update with join syntax simpler-sybase") {
    val snc = new SnappySession(sc)
    snc.sql("create table col_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using column options(BUCKETS '2', PARTITION_BY 'col1') ")
    snc.sql("create table row_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using row options(BUCKETS '2', PARTITION_BY 'col1') ")

    snc.insert("row_table", Row(1, "1", "100", 100))
    snc.insert("row_table", Row(2, "2", "200", 200))
    snc.insert("row_table", Row(4, "4", "400", 4))

    snc.insert("col_table", Row(1, "1", "1", 1))
    snc.insert("col_table", Row(2, "2", null, 2))
    snc.insert("col_table", Row(4, "4", "3", 3))

    snc.sql("update col_table set a.col3 = b.col3, a.col4 = b.col4 from " +
        "col_table a, row_table b where a.col2 = b.col2")

    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 3)
    assert(resultdf.contains(Row(1, "1", "100", 100)))
    assert(resultdf.contains(Row(2, "2", "200", 200)))
    assert(resultdf.contains(Row(4, "4", "400", 4)))
  }


  ignore("Multiple columns update with join : Row PR tables") {
    val snc = new SnappySession(sc)
    snc.sql("create table col_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using column options(BUCKETS '2', PARTITION_BY 'col1') ")
    snc.sql("create table row_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using row options(BUCKETS '2', PARTITION_BY 'col1') ")

    snc.insert("col_table", Row(1, "1", "100", 100))
    snc.insert("col_table", Row(2, "2", "200", 200))
    snc.insert("col_table", Row(4, "4", "400", 4))

    snc.insert("row_table", Row(1, "1", "1", 1))
    snc.insert("row_table", Row(1, "5", "30", 30))
    snc.insert("row_table", Row(2, "2", null, 2))
    snc.insert("row_table", Row(4, "4", "3", 3))

    snc.sql("update row_table set a.col3 = b.col3, a.col4 = b.col4 from " +
        "row_table a join col_table b on a.col2 = b.col2")

    val resultdf = snc.table("row_table").collect()
    assert(resultdf.length == 4)
    assert(resultdf.contains(Row(1, "1", "100", 100)))
    assert(resultdf.contains(Row(2, "2", "200", 200)))
    assert(resultdf.contains(Row(4, "4", "400", 4)))
    assert(resultdf.contains(Row(1, "5", "30", 30))) // Unchanged
  }

  test("Multiple columns update with join : Column tables") {
    val snc = new SnappySession(sc)
    snc.sql("create table col_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using column options(BUCKETS '2', PARTITION_BY 'col1') ")
    snc.sql("create table row_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using row options(BUCKETS '2', PARTITION_BY 'col1') ")

    snc.insert("row_table", Row(1, "1", "100", 100))
    snc.insert("row_table", Row(2, "2", "200", 200))
    snc.insert("row_table", Row(4, "4", "400", 4))

    snc.insert("col_table", Row(1, "1", "1", 1))
    snc.insert("col_table", Row(1, "5", "30", 30))
    snc.insert("col_table", Row(2, "2", null, 2))
    snc.insert("col_table", Row(4, "4", "3", 3))

    snc.sql("update col_table set a.col3 = b.col3, a.col4 = b.col4 from " +
        "col_table a join row_table b on a.col2 = b.col2")

    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 4)
    assert(resultdf.contains(Row(1, "1", "100", 100)))
    assert(resultdf.contains(Row(2, "2", "200", 200)))
    assert(resultdf.contains(Row(4, "4", "400", 4)))
    assert(resultdf.contains(Row(1, "5", "30", 30))) // Unchanged
  }


  ignore("Multiple columns update with join : Row RR tables") {
    val snc = new SnappySession(sc)
    snc.sql("create table col_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using column options(BUCKETS '2', PARTITION_BY 'col1') ")
    snc.sql("create table row_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using row ")
    snc.insert("col_table", Row(1, "1", "100", 100))
    snc.insert("col_table", Row(2, "2", "200", 200))
    snc.insert("col_table", Row(4, "4", "400", 4))

    snc.insert("row_table", Row(1, "1", "1", 1))
    snc.insert("row_table", Row(1, "5", "30", 30))
    snc.insert("row_table", Row(2, "2", null, 2))
    snc.insert("row_table", Row(4, "4", "3", 3))

    snc.sql("update row_table set a.col3 = b.col3, a.col4 = b.col4 from " +
        "row_table a join col_table b on a.col2 = b.col2")

    val resultdf = snc.table("row_table").collect()
    assert(resultdf.length == 4)
    assert(resultdf.contains(Row(1, "1", "100", 100)))
    assert(resultdf.contains(Row(2, "2", "200", 200)))
    assert(resultdf.contains(Row(4, "4", "400", 4)))
    assert(resultdf.contains(Row(1, "5", "30", 30))) // Unchanged
  }

  test("Single column update with subquery : Row RR tables") {
    val snc = new SnappySession(sc)
    snc.sql("create table col_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using column options(BUCKETS '2', PARTITION_BY 'col1') ")
    snc.sql("create table row_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using row ")
    snc.insert("col_table", Row(1, "1", "100", 100))
    snc.insert("col_table", Row(2, "2", "200", 200))
    snc.insert("col_table", Row(4, "4", "400", 4))

    snc.insert("row_table", Row(1, "1", "1", 1))
    snc.insert("row_table", Row(1, "5", "30", 30))
    snc.insert("row_table", Row(2, "2", null, 2))
    snc.insert("row_table", Row(4, "4", "3", 3))

    val df = snc.sql("update row_table set col3 = '5' where col2 in (select col2 from col_table)")
    df.collect()

    val resultdf = snc.table("row_table").collect()
    assert(resultdf.length == 4)
    assert(resultdf.contains(Row(1, "1", "5", 1)))
    assert(resultdf.contains(Row(2, "2", "5", 2)))
    assert(resultdf.contains(Row(4, "4", "5", 3)))
    assert(resultdf.contains(Row(1, "5", "30", 30))) // Unchanged
  }

  ignore("Single column update with subquery with avg : Row RR tables") {
    val snc = new SnappySession(sc)
    snc.sql("create table col_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using column options(BUCKETS '2', PARTITION_BY 'col1') ")
    snc.sql("create table row_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using row ")
    snc.insert("col_table", Row(1, "2", "100", 100))
    snc.insert("col_table", Row(2, "2", "200", 200))
    snc.insert("col_table", Row(4, "2", "400", 4))

    snc.insert("row_table", Row(1, "1", "1", 1))
    snc.insert("row_table", Row(1, "5", "30", 30))
    snc.insert("row_table", Row(2, "2", null, 2))
    snc.insert("row_table", Row(4, "4", "3", 3))

    snc.sql("update row_table set col3 = '5' where col2 > (select avg(col2) from col_table)")

    val resultdf = snc.table("row_table").collect()
    assert(resultdf.length == 4)
    assert(resultdf.contains(Row(4, "4", "5", 3)))
    assert(resultdf.contains(Row(1, "5", "5", 30))) // Unchanged
  }

  test("row table without child") {
    val snc = new SnappySession(sc)
    snc.sql("create table col_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using column options(BUCKETS '2', PARTITION_BY 'col1') ")
    snc.sql("create table row_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using row options(BUCKETS '2', PARTITION_BY 'col1') ")

    snc.insert("col_table", Row(1, "1", "100", 100))
    snc.insert("col_table", Row(2, "2", "200", 200))
    snc.insert("col_table", Row(4, "4", "400", 4))

    snc.insert("row_table", Row(1, "1", "1", 1))
    snc.insert("row_table", Row(2, "2", null, 2))
    snc.insert("row_table", Row(4, "4", "3", 3))

    snc.sql("update row_table set col3 = '4' ")

  }


  test("PutInto with API") {
    val snc = new SnappySession(sc)
    val rdd = sc.parallelize(data1, 2).map(s => Data(s(0), s(1), s(2)))
    val df1 = snc.createDataFrame(rdd)
    val rdd2 = sc.parallelize(data2, 2).map(s => Data(s(0), s(1), s(2)))
    val df2 = snc.createDataFrame(rdd2)

    snc.createTable("col_table", "column", df1.schema, Map("key_columns" -> "col2"))

    df1.write.insertInto("col_table") // insert initial data
    df2.cache()
    df2.count()
    df2.write.putInto("col_table") // update & insert subsequent data

    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 7)
    assert(resultdf.contains(Row(8, 8, 8)))
    assert(resultdf.contains(Row(88, 88, 90)))
  }

  test("PutInto Cache Test") {
    val snc = new SnappySession(sc)
    val rdd = sc.parallelize(data1, 2).map(s => Data(s(0), s(1), s(2)))
    val df1 = snc.createDataFrame(rdd)
    val rdd2 = sc.parallelize(data2, 2).map(s => Data(s(0), s(1), s(2)))
    val df2 = snc.createDataFrame(rdd2)
    snc.sql("set spark.sql.defaultSizeInBytes=1000")
    snc.createTable("col_table", "column", df1.schema, Map("key_columns" -> "col2"))

    df1.write.insertInto("col_table") // insert initial data
    df2.write.putInto("col_table") // update & insert subsequent data
    df2.write.putInto("col_table")
    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 7)
    assert(resultdf.contains(Row(8, 8, 8)))
    assert(resultdf.contains(Row(88, 88, 90)))
  }

  test("PutInto with API for pure inserts") {
    val snc = new SnappySession(sc)
    val rdd = sc.parallelize(data1, 2).map(s => Data(s(0), s(1), s(2)))
    val df1 = snc.createDataFrame(rdd)
    val df2 = snc.range(100, 110).selectExpr("id", "id", "id")
    snc.sql("set spark.sql.defaultSizeInBytes=1000")
    snc.createTable("col_table", "column", df1.schema,
      Map("key_columns" -> "col2", "buckets" -> "4"))

    df1.write.insertInto("col_table") // insert initial data
    df2.write.putInto("col_table") // update & insert subsequent data

    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 16)
    assert(resultdf.contains(Row(100, 100, 100)))
  }

  test("PutInto Cache test for intermediate joins") {
    val snc = new SnappySession(sc)
    val rdd = sc.parallelize(data1, 2).map(s => Data(s(0), s(1), s(2)))
    val df1 = snc.createDataFrame(rdd)
    val df2 = snc.range(100, 110).selectExpr("id", "id", "id")
    snc.sql("set spark.sql.defaultSizeInBytes=1000")
    snc.createTable("col_table", "column", df1.schema,
      Map("key_columns" -> "col2", "buckets" -> "4"))

    df1.write.insertInto("col_table") // insert initial data
    snc.sharedState.cacheManager.clearCache()
    df2.write.putInto("col_table") // update & insert subsequent data
    assert(snc.sharedState.cacheManager.isEmpty)

    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 16)
    assert(resultdf.contains(Row(100, 100, 100)))
  }

  test("PutInto with different column names") {
    val snc = new SnappySession(sc)
    val rdd = sc.parallelize(data1, 2).map(s => Data(s(0), s(1), s(2)))
    val df1 = snc.createDataFrame(rdd)
    val rdd2 = sc.parallelize(data2, 2).map(s => DataDiffCol(s(0), s(1), s(2)))
    val df2 = snc.createDataFrame(rdd2)

    snc.createTable("col_table", "column", df1.schema, Map("key_columns" -> "col2"))

    df1.write.insertInto("col_table") // insert initial data
    df2.write.putInto("col_table") // update & insert subsequent data

    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 7)
    assert(resultdf.contains(Row(8, 8, 8)))
    assert(resultdf.contains(Row(88, 88, 90)))
  }

  test("PutInto with null key values") {
    val snc = new SnappySession(sc)
    snc.sql("create table col_table(col1 INT, col2 STRING, col3 INT)" +
        " using column options(BUCKETS '2', PARTITION_BY 'col1', key_columns 'col2') ")
    snc.sql("create table row_table(col1 INT, col2 STRING, col3 INT)")

    snc.insert("row_table", Row(1, "1", 1))
    snc.insert("row_table", Row(2, "2", 2))
    snc.insert("row_table", Row(3, null, 3))

    snc.insert("col_table", Row(1, "1", 1))
    snc.insert("col_table", Row(2, "2", 2))
    snc.insert("col_table", Row(3, null, 3))

    snc.sql("put into table col_table" +
        "   select * from row_table")

    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 4)
    // TODO What should be the behaviour ?
    assert(resultdf.filter(r => r.equals(Row(3, null, 3))).size == 2)
  }

  test("PutInto op changed row count validation") {
    val snc = new SnappySession(sc)
    snc.sql("create table col_table(col1 INT, col2 STRING, col3 INT)" +
        " using column options(BUCKETS '4', PARTITION_BY 'col1', key_columns 'col2') ")
    snc.sql("create table row_table(col1 INT, col2 STRING, col3 INT)")

    snc.insert("row_table", Row(1, "1", 11))
    snc.insert("row_table", Row(9, "9", 99))
    snc.insert("row_table", Row(2, "2", 22))
    snc.insert("row_table", Row(3, "4", 3))

    snc.insert("col_table", Row(1, "1", 1))
    snc.insert("col_table", Row(9, "9", 9))
    snc.insert("col_table", Row(2, "2", 2))
    snc.insert("col_table", Row(3, "5", 3))

    val df = snc.sql("put into table col_table" +
        "   select * from row_table")

    assert(df.collect()(0)(0) == 4)
    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 5)
  }

  test("PutInto with only key values") {
    val snc = new SnappySession(sc)
    snc.sql("create table col_table(col1 INT)" +
        " using column options(BUCKETS '2', PARTITION_BY 'col1', key_columns 'col1') ")
    snc.sql("create table row_table(col1 INT)")

    snc.insert("row_table", Row(1))
    snc.insert("row_table", Row(2))
    snc.insert("row_table", Row(3))

    snc.insert("col_table", Row(1))
    snc.insert("col_table", Row(2))
    snc.insert("col_table", Row(3))

    intercept[AnalysisException]{
      snc.sql("put into table col_table" +
          "   select * from row_table")
    }

  }

  test("Bug - Incorrect updateExpresion") {
    val snc = new SnappySession(sc)
    snc.sql("create table col_table (col1 int, col2 int, col3 int)" +
        " using column options(partition_by 'col2', key_columns 'col2') ")
    snc.sql("create table row_table " +
        "(col1 int, col2 int, col3 int) using row options(partition_by 'col2')")

    snc.insert("row_table", Row(1, 1, 1))
    snc.insert("row_table", Row(2, 2, 2))
    snc.insert("row_table", Row(3, 3, 3))
    snc.sql("put into table col_table" +
        "   select * from row_table")

    snc.sql("put into table col_table" +
        "   select * from row_table")

    val df = snc.table("col_table").collect()

    assert(df.contains(Row(1, 1, 1)))
    assert(df.contains(Row(2, 2, 2)))
    assert(df.contains(Row(3, 3, 3)))
  }


  test("PutInto with multiple column key") {
    val snc = new SnappySession(sc)

    snc.sql("create table col_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using column options(BUCKETS '2', PARTITION_BY 'col1', key_columns 'col2, col3') ")

    snc.sql("create table row_table(col1 INT, col2 STRING, col3 String, col4 Int)")


    snc.insert("row_table", Row(1, "1", "1", 100))
    snc.insert("row_table", Row(2, "2", "2", 2))
    snc.insert("row_table", Row(4, "4", "4", 4))

    snc.insert("col_table", Row(1, "1", "1", 1))
    snc.insert("col_table", Row(2, "2", "2", 2))
    snc.insert("col_table", Row(3, "3", "3", 3))

    snc.sql("put into table col_table" +
        "   select * from row_table")

    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 4)
    assert(resultdf.contains(Row(1, "1", "1", 100)))
    assert(resultdf.contains(Row(4, "4", "4", 4)))
  }

  test("PutInto with multiple column key and null values") {
    val snc = new SnappySession(sc)

    snc.sql("create table col_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using column options(BUCKETS '2', PARTITION_BY 'col1', key_columns 'col2, col3') ")

    snc.sql("create table row_table(col1 INT, col2 STRING, col3 String, col4 Int)")


    snc.insert("row_table", Row(1, "1", "1", 100))
    snc.insert("row_table", Row(2, "2", "2", 2))
    snc.insert("row_table", Row(4, "4", "4", 4))

    snc.insert("col_table", Row(1, "1", "1", 1))
    snc.insert("col_table", Row(2, "2", null, 2))
    snc.insert("col_table", Row(3, "3", "3", 3))

    snc.sql("put into table col_table" +
        "   select * from row_table")

    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 5)
    assert(resultdf.contains(Row(2, "2", null, 2)))
    assert(resultdf.contains(Row(2, "2", "2", 2)))
  }

  test("PutInto selecting from same table") {
    val snc = new SnappySession(sc)

    snc.sql("create table col_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using column options(BUCKETS '2', PARTITION_BY 'col1', key_columns 'col2, col3') ")

    snc.insert("col_table", Row(1, "1", "1", 1))
    snc.insert("col_table", Row(2, "2", "2", 2))
    snc.insert("col_table", Row(3, "3", "3", 3))

    intercept[AnalysisException]{
      snc.sql("put into table col_table" +
          "   select * from col_table")
    }
    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 3)
  }

  test("PutInto Key columns validation") {
    val snc = new SnappySession(sc)
    snc.sql("create table col_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using column options(BUCKETS '2', PARTITION_BY 'col1') ")
    snc.sql("create table row_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using row options(BUCKETS '2', PARTITION_BY 'col1') ")

    snc.insert("row_table", Row(1, "1", "1", 100))
    snc.insert("row_table", Row(2, "2", "2", 2))
    snc.insert("row_table", Row(4, "4", "4", 4))

    snc.insert("col_table", Row(1, "1", "1", 1))
    snc.insert("col_table", Row(2, "2", null, 2))
    snc.insert("col_table", Row(3, "3", "3", 3))

    intercept[AnalysisException]{
      snc.sql("put into table col_table" +
          "   select * from row_table")
    }
  }

  test("deleteFrom table exists syntax with alias") {
    val snc = new SnappySession(sc)
    snc.sql("create table col_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using column options(BUCKETS '2', PARTITION_BY 'col1') ")
    snc.sql("create table row_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using row options(BUCKETS '2', PARTITION_BY 'col1') ")

    snc.insert("row_table", Row(1, "1", "1", 100))
    snc.insert("row_table", Row(2, "2", "2", 2))
    snc.insert("row_table", Row(4, "4", "4", 4))

    snc.insert("col_table", Row(1, "1", "1", 1))
    snc.insert("col_table", Row(2, "2", null, 2))
    snc.insert("col_table", Row(3, "3", "3", 3))
    snc.sql("delete from col_table a where exists (select 1 from row_table b where a.col2 = " +
        "b.col2 and a.col3 = b.col3)")

    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 2)
    assert(resultdf.contains(Row(3, "3", "3", 3)))
    assert(resultdf.contains(Row(2, "2", null, 2)))
  }

  test("deleteFrom table exists syntax without alias") {
    val snc = new SnappySession(sc)
    snc.sql("create table col_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using column options(BUCKETS '2', PARTITION_BY 'col1') ")
    snc.sql("create table row_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using row options(BUCKETS '2', PARTITION_BY 'col1') ")

    snc.insert("row_table", Row(1, "1", "1", 100))
    snc.insert("row_table", Row(2, "2", "2", 2))
    snc.insert("row_table", Row(4, "4", "4", 4))

    snc.insert("col_table", Row(1, "1", "1", 1))
    snc.insert("col_table", Row(2, "2", null, 2))
    snc.insert("col_table", Row(3, "3", "3", 3))
    snc.sql("delete from col_table where exists (select 1 from row_table  where col_table.col2 = " +
        "row_table.col2 and col_table.col3 = row_table.col3)")

    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 2)
    assert(resultdf.contains(Row(3, "3", "3", 3)))
    assert(resultdf.contains(Row(2, "2", null, 2)))
  }

  test("deleteFrom table where(a,b) select a,b syntax") {
    val snc = new SnappySession(sc)
    snc.sql("create table col_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using column options(BUCKETS '2', PARTITION_BY 'col1') ")
    snc.sql("create table row_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using row options(BUCKETS '2', PARTITION_BY 'col1') ")

    snc.insert("row_table", Row(1, "1", "1", 100))
    snc.insert("row_table", Row(2, "2", "2", 2))
    snc.insert("row_table", Row(4, "4", "4", 4))

    snc.insert("col_table", Row(1, "1", "1", 1))
    snc.insert("col_table", Row(2, "2", null, 2))
    snc.insert("col_table", Row(3, "3", "3", 3))
    snc.sql("delete from col_table where (col2, col3) in  (select col2, col3 from row_table)")

    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 2)
    assert(resultdf.contains(Row(3, "3", "3", 3)))
    assert(resultdf.contains(Row(2, "2", null, 2)))
  }

  ignore("deleteFrom table where(a,b) select a,b syntax Row table") {
    val snc = new SnappySession(sc)
    snc.sql("create table col_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using column options(BUCKETS '2', PARTITION_BY 'col1') ")
    snc.sql("create table row_table(col1 INT, col2 STRING, col3 String, col4 Int)" +
        " using row options(BUCKETS '2', PARTITION_BY 'col1') ")

    snc.insert("row_table", Row(1, "5", "5", 100))
    snc.insert("row_table", Row(1, "1", "1", 100))
    snc.insert("row_table", Row(2, "2", "2", 2))
    snc.insert("row_table", Row(4, "4", "4", 4))

    snc.insert("col_table", Row(9, "1", "1", 1))
    snc.insert("col_table", Row(2, "2", null, 2))
    snc.insert("col_table", Row(3, "3", "3", 3))
    snc.sql("delete from row_table where (col2, col3) in  (select col2, col3 from col_table)")

    val resultdf = snc.table("row_table").collect()
    assert(resultdf.length == 3)
    assert(resultdf.contains(Row(1, "5", "5", 100)))
    assert(resultdf.contains(Row(4, "4", "4", 4)))
    assert(resultdf.contains(Row(2, "2", "2", 2)))
  }

  test("DeleteFrom dataframe API: column tables") {
    val snc = new SnappySession(sc)
    val rdd = sc.parallelize(data5, 2).map(s => DataWithMultipleKeys(s(0).asInstanceOf[Int],
      s(1).asInstanceOf[Int], s(2).asInstanceOf[String], s(3).asInstanceOf[Long]))
    val df1 = snc.createDataFrame(rdd)
    val rdd2 = sc.parallelize(data6, 2).map(s => DataDiffColMultipleKeys(s(0).asInstanceOf[Int],
      s(1).asInstanceOf[Int], s(2).asInstanceOf[String], s(3).asInstanceOf[Long]))
    val df2 = snc.createDataFrame(rdd2)

    snc.createTable("col_table", "column",
      df1.schema, Map("key_columns" -> "pk2,pk1"))

    df1.write.insertInto("col_table")
    df2.write.deleteFrom("col_table")

    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 1)
    assert(resultdf.contains(Row(8, 8, "str8", 8)))
  }

  test("DeleteFrom empty Key columns validation: column tables") {
    val snc = new SnappySession(sc)
    val rdd = sc.parallelize(data5, 2).map(s => DataWithMultipleKeys(s(0).asInstanceOf[Int],
      s(1).asInstanceOf[Int], s(2).asInstanceOf[String], s(3).asInstanceOf[Long]))
    val df1 = snc.createDataFrame(rdd)
    val rdd2 = sc.parallelize(data6, 2).map(s => DataDiffColMultipleKeys(s(0).asInstanceOf[Int],
      s(1).asInstanceOf[Int], s(2).asInstanceOf[String], s(3).asInstanceOf[Long]))
    val df2 = snc.createDataFrame(rdd2)

    snc.createTable("col_table", "column",
      df1.schema, Map.empty[String, String])

    df1.write.insertInto("col_table")

    val message = intercept[AnalysisException] {
      df2.write.deleteFrom("col_table")
    }.getMessage
    assert(message.contains("DeleteFrom operation requires key " +
        "columns(s) or primary key defined on table."))
  }

  test("DeleteFrom missing Key columns validation: column tables") {
    val snc = new SnappySession(sc)
    val rdd = sc.parallelize(data5, 2).map(s => DataWithMultipleKeys(s(0).asInstanceOf[Int],
      s(1).asInstanceOf[Int], s(2).asInstanceOf[String], s(3).asInstanceOf[Long]))
    val df1 = snc.createDataFrame(rdd)
    val rdd2 = sc.parallelize(data6, 2).map(s => DataDiffColMultipleKeys(s(0).asInstanceOf[Int],
      s(1).asInstanceOf[Int], s(2).asInstanceOf[String], s(3).asInstanceOf[Long]))
    val df2 = snc.createDataFrame(rdd2)

    snc.sql("create table col_table(pk1 int, col1 int, pk3 varchar(50), col2 int)" +
        " using column options(key_columns 'PK3,PK1')")

    df1.write.insertInto("col_table")

    val message = intercept[AnalysisException] {
      df2.write.deleteFrom("col_table")
    }.getMessage
    assert(message.contains("column `pk3` cannot be resolved on the right side of the operation."))
  }

  test("Bug - SNAP-2157") {
    snc.sql("CREATE TABLE app.customer (C_CustKey int NOT NULL,C_Name varchar(64)," +
        "C_Address varchar(64),C_NationKey int,C_Phone varchar(64),C_AcctBal decimal(13,2)," +
        "C_MktSegment varchar(64),C_Comment varchar(120)," +
        " skip varchar(64), PRIMARY KEY (C_CustKey))")

    val data = (1 to 10).map(i => RData(i, s"$i name",
      s"$i addr",
      i,
      s"$i phone",
      Decimal(1),
      s"$i mktsegment",
      s"$i comment",
      s"$i skip"))

    val rdd = sc.parallelize(data, 2)
    val df1 = snc.createDataFrame(rdd)
    df1.write.insertInto("app.customer")

    df1.write.deleteFrom("app.customer")
    val df0 = snc.table("app.customer")
    assert(df0.count() == 0)
  }

  test("DeleteFrom dataframe API: row tables") {
    val snc = new SnappySession(sc)
    val rdd = sc.parallelize(data5, 2).map(s => DataWithMultipleKeys(s(0).asInstanceOf[Int],
      s(1).asInstanceOf[Int], s(2).asInstanceOf[String], s(3).asInstanceOf[Long]))
    val df1 = snc.createDataFrame(rdd)
    val rdd2 = sc.parallelize(data6, 2).map(s => DataDiffColMultipleKeys(s(0).asInstanceOf[Int],
      s(1).asInstanceOf[Int], s(2).asInstanceOf[String], s(3).asInstanceOf[Long]))
    val df2 = snc.createDataFrame(rdd2)

    snc.sql("create table row_table(pk1 int , col1 int, pk2 varchar(50), col2 int," +
        " primary key (pk2,pk1)) using row options(partition_by 'pk2,col1')")
    df1.write.insertInto("row_table")
    df2.write.deleteFrom("row_table")

    val resultdf = snc.table("row_table").collect()
    assert(resultdf.length == 1)
    assert(resultdf.contains(Row(8, 8, "str8", 8)))
  }

  test("DeleteFrom dataframe API Row replicated tables") {
    val snc = new SnappySession(sc)
    val rdd = sc.parallelize(data5, 2).map(s => DataWithMultipleKeys(s(0).asInstanceOf[Int],
      s(1).asInstanceOf[Int], s(2).asInstanceOf[String], s(3).asInstanceOf[Long]))
    val df1 = snc.createDataFrame(rdd)
    val rdd2 = sc.parallelize(data6, 2).map(s => DataDiffColMultipleKeys(s(0).asInstanceOf[Int],
      s(1).asInstanceOf[Int], s(2).asInstanceOf[String], s(3).asInstanceOf[Long]))
    val df2 = snc.createDataFrame(rdd2)

    snc.sql("create table row_table(pk1 int, col1 int, pk2 varchar(50), col2 int," +
        " primary key(pk2,pk1))  using row")
    df1.write.insertInto("row_table")
    df2.write.deleteFrom("row_table")

    val resultdf = snc.table("row_table").collect()
    assert(resultdf.length == 1)
    assert(resultdf.contains(Row(8, 8, "str8", 8)))
  }

  test("DeleteFrom empty Key columns validation: row tables") {
    val snc = new SnappySession(sc)
    val rdd = sc.parallelize(data5, 2).map(s => DataWithMultipleKeys(s(0).asInstanceOf[Int],
      s(1).asInstanceOf[Int], s(2).asInstanceOf[String], s(3).asInstanceOf[Long]))
    val df1 = snc.createDataFrame(rdd)
    val rdd2 = sc.parallelize(data6, 2).map(s => DataDiffColMultipleKeys(s(0).asInstanceOf[Int],
      s(1).asInstanceOf[Int], s(2).asInstanceOf[String], s(3).asInstanceOf[Long]))
    val df2 = snc.createDataFrame(rdd2)

    snc.createTable("row_table", "row",
      df1.schema, Map.empty[String, String])

    df1.write.insertInto("row_table")

    val message = intercept[AnalysisException]{
      df2.write.deleteFrom("row_table")
    }.getMessage

    assert(message.contains("DeleteFrom operation requires " +
        "key columns(s) or primary key defined on table."))
  }


  test("DeleteFrom missing Key columns validation: row tables") {
    val snc = new SnappySession(sc)
    val rdd = sc.parallelize(data5, 2).map(s => DataWithMultipleKeys(s(0).asInstanceOf[Int],
      s(1).asInstanceOf[Int], s(2).asInstanceOf[String], s(3).asInstanceOf[Long]))
    val df1 = snc.createDataFrame(rdd)
    val rdd2 = sc.parallelize(data6, 2).map(s => DataDiffColMultipleKeys(s(0).asInstanceOf[Int],
      s(1).asInstanceOf[Int], s(2).asInstanceOf[String], s(3).asInstanceOf[Long]))
    val df2 = snc.createDataFrame(rdd2)

    snc.sql("create table row_table(pk1 int, col1 int, pk3 varchar(50), col2 int" +
        ", primary key(PK1, PK3)) using row")

    df1.write.insertInto("row_table")

    val message = intercept[AnalysisException]{
      df2.write.deleteFrom("row_table")
    }.getMessage

    assert(message.contains("column `pk3` cannot be resolved on the right side of the operation."))
  }

  test("Delete From SQL using JDBC: row tables") {
    val snc = this.snc
    val rdd = sc.parallelize(data5, 2).map(s => DataWithMultipleKeys(s(0).asInstanceOf[Int],
      s(1).asInstanceOf[Int], s(2).asInstanceOf[String], s(3).asInstanceOf[Long]))
    val df1 = snc.createDataFrame(rdd)
    val rdd2 = sc.parallelize(data6, 2).map(s => DataDiffColMultipleKeys(s(0).asInstanceOf[Int],
      s(1).asInstanceOf[Int], s(2).asInstanceOf[String], s(3).asInstanceOf[Long]))
    val df2 = snc.createDataFrame(rdd2)

    snc.sql("create table row_table(pk1 int, col1 int, pk2 varchar(50), col2 int," +
        " primary key(pk2,pk1))  using row")
    df1.write.insertInto("row_table")
    val conn = DriverManager.getConnection(s"jdbc:snappydata://localhost:$netPort")
    try {
      df2.createGlobalTempView("delete_df")
      val stmt = conn.createStatement()
      stmt.execute("DELETE FROM row_table SELECT pk1, pk2 from delete_df")
      stmt.close()
    } finally {
      snc.dropTable("delete_df")
      conn.close()
    }
    val resultdf = snc.table("row_table").collect()
    assert(resultdf.length == 1)
    assert(resultdf.contains(Row(8, 8, "str8", 8)))
  }

  test("Delete From SQL using JDBC: column tables") {
    val snc = this.snc
    val rdd = sc.parallelize(data5, 2).map(s => DataWithMultipleKeys(s(0).asInstanceOf[Int],
      s(1).asInstanceOf[Int], s(2).asInstanceOf[String], s(3).asInstanceOf[Long]))
    val df1 = snc.createDataFrame(rdd)
    val rdd2 = sc.parallelize(data6, 2).map(s => DataDiffColMultipleKeys(s(0).asInstanceOf[Int],
      s(1).asInstanceOf[Int], s(2).asInstanceOf[String], s(3).asInstanceOf[Long]))
    val df2 = snc.createDataFrame(rdd2)

    snc.sql("create table col_table(pk1 int, col1 int, pk2 varchar(50), col2 int) " +
        "using column options(key_columns 'pk2,pk1')")

    df1.write.insertInto("col_table")

    val conn = DriverManager.getConnection(s"jdbc:snappydata://localhost:$netPort")
    try {
      df2.createGlobalTempView("delete_df")
      val stmt = conn.createStatement()
      stmt.execute("DELETE FROM col_table SELECT pk1, pk2 from delete_df")
      stmt.close()
    } finally {
      conn.close()
      snc.dropTable("delete_df")
    }
    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 1)
    assert(resultdf.contains(Row(8, 8, "str8", 8)))
  }

  test("Delete From SQL with key column aliasing") {
    val snc = this.snc
    val rdd = sc.parallelize(data5, 2).map(s => DataWithMultipleKeys(s(0).asInstanceOf[Int],
      s(1).asInstanceOf[Int], s(2).asInstanceOf[String], s(3).asInstanceOf[Long]))
    val df1 = snc.createDataFrame(rdd)
    val rdd2 = sc.parallelize(data6, 2).map(s => DataDiffColMultipleKeys(s(0).asInstanceOf[Int],
      s(1).asInstanceOf[Int], s(2).asInstanceOf[String], s(3).asInstanceOf[Long]))
    val df2 = snc.createDataFrame(rdd2)

    snc.sql("create table col_table(pk1 int, col1 int, pk3 varchar(50), col2 int) " +
        "using column options(key_columns 'pk3,pk1')")

    df1.write.insertInto("col_table")

    val conn = DriverManager.getConnection(s"jdbc:snappydata://localhost:$netPort")
    try {
      df2.createGlobalTempView("delete_df")
      val stmt = conn.createStatement()
      stmt.execute("DELETE FROM col_table SELECT pk1, pk2 as pk3 from delete_df")
      stmt.close()
    } finally {
      conn.close()
      snc.dropTable("delete_df")
    }
    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 1)
    assert(resultdf.contains(Row(8, 8, "str8", 8)))
  }

  private def bug2348Test(): Unit = {
    var snc = new SnappySession(sc)
    snc.sql("create table t1(id long," +
        "datekey int," +
        "checkin_date int," +
        "checkout_date int," +
        "crawl_time int," +
        "batch tinyint," +
        "source tinyint," +
        "is_high_star tinyint," +
        "mt_poi_id bigint," +
        "mt_room_id bigint," +
        "mt_breakfast tinyint," +
        "mt_goods_id bigint," +
        "mt_bd_id int," +
        "mt_goods_vendor_id long," +
        "mt_business_type tinyint," +
        "mt_room_status tinyint," +
        "mt_poi_uv int," +
        "mt_price1 int," +
        "mt_price2 int," +
        "mt_price3 int," +
        "mt_price4 int," +
        "mt_price5 int," +
        "mt_price6 int," +
        "mt_price7 int," +
        "mt_price8 int," +
        "mt_flag1 tinyint," +
        "mt_flag2 tinyint," +
        "mt_flag3 tinyint," +
        "comp_site_id int," +
        "comp_poi_id varchar(200)," +
        "comp_room_id long," +
        "comp_breakfast tinyint," +
        "comp_goods_id varchar(200)," +
        "comp_goods_vendor varchar(200)," +
        "comp_room_status tinyint," +
        "comp_is_promotion tinyint," +
        "comp_pay_type tinyint," +
        "comp_goods_type tinyint," +
        "comp_price1 int," +
        "comp_price2 int," +
        "comp_price3 int," +
        "comp_price4 int," +
        "comp_price5 int," +
        "comp_price6 int," +
        "comp_price7 int," +
        "comp_price8 int," +
        "comp_flag1 tinyint," +
        "comp_flag2 tinyint," +
        "comp_flag3 tinyint," +
        "valid_status tinyint," +
        "gmt_time timestamp," +
        "version timestamp," +
        "interval_days int," +
        "real_batch bigint," +
        "start_time_long bigint," +
        "end_time_long bigint," +
        "start_time bigint," +
        "end_time bigint," +
        "start_real_batch bigint," +
        "end_real_batch bigint," +
        "flag int," +
        "insert_time bigint) " +
        "USING column OPTIONS (PARTITION_BY 'mt_poi_id'," +
        "REDUNDANCY '0',BUCKETS '1'," +
        "PERSISTENCE 'ASYNC', OVERFLOW 'true')")

    snc.sql("create table t2(id bigint," +
        "datekey int," +
        "checkin_date int," +
        "checkout_date int," +
        "crawl_time bigint," +
        "batch int," +
        "source int," +
        "is_high_star int," +
        "mt_poi_id bigint," +
        "mr_room_id bigint," +
        "mt_breakfast int," +
        "mt_goods_id bigint," +
        "mt_bd_id int," +
        "mt_goods_vendor_id bigint," +
        "mr_business_type int," +
        "mt_room_status int," +
        "mt_poi_uv int," +
        "mt_price1 int," +
        "mt_price2 int," +
        "mt_price3 int," +
        "mt_price4 int," +
        "mt_price5 int," +
        "mt_price6 int," +
        "mt_price7 int," +
        "mt_price8 int," +
        "mt_flag1 int," +
        "mt_flag2 int," +
        "mt_flag3 int," +
        "comp_site_id int," +
        "comp_poi_id varchar(200)," +
        "comp_room_id long," +
        "comp_breakfast tinyint," +
        "comp_goods_id varchar(200)," +
        "comp_goods_vendor varchar(200)," +
        "comp_room_status tinyint," +
        "comp_is_promotion tinyint," +
        "comp_pay_type tinyint," +
        "comp_goods_type tinyint," +
        "comp_price1 int," +
        "comp_price2 int," +
        "comp_price3 int," +
        "comp_price4 int," +
        "comp_price5 int," +
        "comp_price6 int," +
        "comp_price7 int," +
        "comp_price8 int," +
        "comp_flag1 tinyint," +
        "comp_flag2 tinyint," +
        "comp_flag3 tinyint," +
        "valid_status tinyint," +
        "gmt_time timestamp," +
        "version timestamp," +
        "interval_days int," +
        "real_batch bigint," +
        "start_time_long bigint," +
        "end_time_long bigint," +
        "start_time bigint," +
        "end_time bigint," +
        "start_real_batch bigint," +
        "end_real_batch bigint," +
        "flag int," +
        "insert_time bigint) USING " +
        "column OPTIONS (PARTITION_BY 'mt_poi_id'," +
        "REDUNDANCY '0',BUCKETS '1'," +
        "PERSISTENCE 'ASYNC',OVERFLOW 'true')")

    val table1 = snc.table("t1")
    val df1 = DataGenerator.generateDataFrame(snc, table1.schema, 20000)
    df1.write.insertInto("t1")
    val table2 = snc.table("t2")
    val df2 = DataGenerator.generateDataFrame(snc, table2.schema, 20000)
    df2.write.insertInto("t2")
    snc.sql("update t1 set a.flag = 0 from t1 a join" +
        " t2 b on a.mt_poi_id = b.mt_poi_id and a.comp_goods_id = b.comp_goods_id " +
        "and a.mt_goods_id = b.mt_goods_id and a.datekey = b.datekey and" +
        " a.CHECKIN_DATE = b.CHECKIN_DATE and b.start_time_long > a.end_time_long where a.flag = 1")

    snc.sql("update t1 set a.flag = 0,a.end_time_long = b.start_time_long," +
        " a.end_time = b.start_time,a.end_real_batch = b.start_real_batch" +
        " from t1 a join t2 b on a.mt_poi_id = b.mt_poi_id" +
        " and a.comp_goods_id = b.comp_goods_id and a.mt_goods_id = b.mt_goods_id" +
        " and a.datekey = b.datekey and a.CHECKIN_DATE = b.CHECKIN_DATE" +
        " and b.start_time_long <= a.end_time_long AND" +
        " (a.comp_price1 <> b.comp_price1 or a.comp_price3 <> b.comp_price3" +
        " or a.mt_price1 <> b.mt_price1 or a.mt_price3 <> b.mt_price3 or" +
        " a.mt_price4 <> b.mt_price4 or a.mt_price5 <> b.mt_price5 or" +
        " a.mt_room_status <> b.mt_room_status or" +
        " a.comp_room_status <> b.comp_room_status) where a.flag = 1")

    snc.sql("update t1 set a.end_time_long = b.start_time_long + 2 * 60 * 60 * 1000," +
        "a.end_time= b.start_time,a.end_real_batch = b.start_real_batch" +
        " from t1 a join t2 b on a.mt_poi_id = b.mt_poi_id and" +
        " a.comp_goods_id = b.comp_goods_id and a.mt_goods_id = b.mt_goods_id" +
        " and a.datekey = b.datekey and a.CHECKIN_DATE = b.CHECKIN_DATE and" +
        " b.start_time_long <= a.end_time_long AND a.comp_price1 = b.comp_price1" +
        " and a.comp_price3 = b.comp_price3 and a.mt_price1 = b.mt_price1" +
        " and a.mt_price3 = b.mt_price3 and a.mt_price4 = b.mt_price4 and" +
        " a.mt_price5 = b.mt_price5 and a.mt_room_status = b.mt_room_status" +
        " and a.comp_room_status = b.comp_room_status where a.flag = 1")

    SnappyContext.globalSparkContext.stop()

    snc = new SnappySession(sc)
    snc.sql("select count(1) from t1").collect()
  }

  test("Bug-2348 : Invalid stats bitmap") {
    try {
      bug2348Test()
    } catch {
      case t: Throwable => throw t
    } finally {
      val snc = new SnappySession(sc)
      snc.dropTable("t1", ifExists = true)
      snc.dropTable("t2", ifExists = true)
    }

  }

  test("Bug-2369 : Incorrect Filtering on join predicate") {
    var snc = new SnappySession(sc)
    snc.sql("CREATE TABLE SNAPPY_COL_TABLE3(r1 Integer, r2 Integer) " +
        "USING COLUMN OPTIONS(PARTITION_BY 'R1');")
    snc.sql("CREATE TABLE SNAPPY_COL_TABLE4(r1 Integer, r2 Integer) " +
        "USING COLUMN OPTIONS(PARTITION_BY 'R1');")

    snc.insert("SNAPPY_COL_TABLE3", Row(1, 1))
    snc.insert("SNAPPY_COL_TABLE3", Row(2, 2))
    snc.insert("SNAPPY_COL_TABLE3", Row(3, 3))
    snc.insert("SNAPPY_COL_TABLE3", Row(5, 5))

    snc.insert("SNAPPY_COL_TABLE4", Row(1, 1))
    snc.insert("SNAPPY_COL_TABLE4", Row(2, 2))
    snc.insert("SNAPPY_COL_TABLE4", Row(3, 3))
    snc.insert("SNAPPY_COL_TABLE4", Row(4, 4))
    snc.insert("SNAPPY_COL_TABLE4", Row(5, 5))

    val dfLeftJoin = snc.sql("SELECT * FROM " +
        "SNAPPY_COL_TABLE4 t LEFT OUTER JOIN SNAPPY_COL_TABLE3 tt " +
        "ON tt.R1 = t.R2").collect()
    assert(dfLeftJoin.length == 5)
    assert(dfLeftJoin.contains(Row(4, 4, null, null)))

    val df = snc.sql("SELECT * FROM SNAPPY_COL_TABLE4 t INNER JOIN SNAPPY_COL_TABLE3 tt " +
        "ON tt.R1 = t.R2").collect()
    assert(df.length == 4)

    val df1 = snc.sql("SELECT * FROM SNAPPY_COL_TABLE4 t INNER JOIN SNAPPY_COL_TABLE3 tt " +
        "ON tt.R1 = t.R2 " +
        "WHERE t.R2 >= 3 AND t.R2 < 5").collect()
    assert(df1.length == 1)
    assert(df1.contains(Row(3, 3, 3, 3)))

    val df2 = snc.sql("SELECT * FROM SNAPPY_COL_TABLE4 t INNER JOIN SNAPPY_COL_TABLE3 tt " +
        "ON tt.R1 = t.R2 " +
        "WHERE t.R2 >= 3 AND t.R2 < 5 " +
        " AND " +
        "tt.R1 >= 3 AND tt.R1 < 5;").collect()
    assert(df2.length == 1)
    assert(df2.contains(Row(3, 3, 3, 3)))
  }
}
