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
package org.apache.spark.sql.execution

import io.snappydata.SnappyFunSuite
import io.snappydata.core.Data
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.Logging
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.{AnalysisException, Row, SnappySession}

case class DataDiffCol(column1: Int, column2: Int, column3: Int)

case class DataStrCol(column1: Int, column2: String, column3: Int)

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

  after {
    snc.dropTable("col_table", ifExists = true)
    snc.dropTable("row_table", ifExists = true)
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

  test("Multiple update with correlated subquery") {
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

    val df = snc.sql("update col_table a  set col3 = "  +
        " (select col3 from row_table b  where a.col2 = b.col2 )")
    println(df.queryExecution.executedPlan)
    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 6)
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

  test("Multiple columns update with join : Row PR tables") {
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


  test("Multiple columns update with join : Row RR tables") {
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

    snc.sql("update row_table set col3 = '5' where col2 in (select col2 from col_table)")

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
    df2.write.putInto("col_table") // update & insert subsequent data

    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 7)
    assert(resultdf.contains(Row(8, 8, 8)))
    assert(resultdf.contains(Row(88, 88, 90)))
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

  test("deleteFrom table where(a,b) select a,b syntax Row table") {
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
    snc.sql("delete from row_table where (col2, col3) in  (select col2, col3 from col_table)")

    val resultdf = snc.table("row_table").collect()
    assert(resultdf.length == 2)
    assert(resultdf.contains(Row(4, "4", "4", 4)))
    assert(resultdf.contains(Row(2, "2", "2", 2)))
  }

  test("DeleteFrom dataframe API") {
    val snc = new SnappySession(sc)
    val rdd = sc.parallelize(data2, 2).map(s => Data(s(0), s(1), s(2)))
    val df1 = snc.createDataFrame(rdd)
    val rdd2 = sc.parallelize(data1, 2).map(s => DataDiffCol(s(0), s(1), s(2)))
    val df2 = snc.createDataFrame(rdd2)

    snc.createTable("col_table", "column",
      df1.schema, Map("key_columns" -> "col2"))

    df1.write.insertInto("col_table")
    df2.write.deleteFrom("col_table")

    val resultdf = snc.table("col_table").collect()
    assert(resultdf.length == 1)
    assert(resultdf.contains(Row(8, 8, 8)))
  }

  test("DeleteFrom Key columns validation") {
    val snc = new SnappySession(sc)
    val rdd = sc.parallelize(data2, 2).map(s => Data(s(0), s(1), s(2)))
    val df1 = snc.createDataFrame(rdd)
    val rdd2 = sc.parallelize(data1, 2).map(s => DataDiffCol(s(0), s(1), s(2)))
    val df2 = snc.createDataFrame(rdd2)

    snc.createTable("col_table", "column",
      df1.schema, Map.empty[String, String])

    df1.write.insertInto("col_table")

    intercept[AnalysisException]{
      df2.write.deleteFrom("col_table")
    }
  }
}
