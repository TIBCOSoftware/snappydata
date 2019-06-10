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

import java.sql.{Connection, Date, DriverManager, SQLException, SQLType, Timestamp, Types}

import scala.collection.mutable

import com.pivotal.gemfirexd.{Attribute, TestUtil}
import com.pivotal.gemfirexd.security.SecurityTestUtils
import io.snappydata.{Constant, Property, SnappyFunSuite}
import org.scalatest.BeforeAndAfterAll
import org.junit.Assert._

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SaveMode, SnappyContext, SnappySession}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.benchmark.ColumnCacheBenchmark
import org.apache.spark.sql.execution.{SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.types.{ArrayType, CalendarIntervalType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.CalendarInterval

class SHAByteBufferTest extends SnappyFunSuite with BeforeAndAfterAll {

  var serverHostPort2: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    serverHostPort2 = TestUtil.startNetServer()
  }

  protected override def newSparkConf(addOn: (SparkConf) => SparkConf): SparkConf = {

    System.setProperty("spark.testing", "true")
    super.newSparkConf((conf: SparkConf) => {
      conf.set("spark.sql.codegen.maxFields", "110")
      conf.set("spark.sql.codegen.fallback", "false")
      conf.set("snappydata.sql.useBBMapInSHAFor1StringGroupBy", "true")
      conf
    })
  }

  override def afterAll(): Unit = {
    TestUtil.stopNetServer()
    super.afterAll()
    System.clearProperty("spark.testing")
  }

  test("simple aggregate query") {
    snc
    snc.sql("drop table if exists test1")
    snc.sql("create table test1 (col1 int, col2 int, col3 int, col4 int) " +
      "using column ")
    val range = 50
    val groupingDivisor = 10
    val insertDF = snc.range(50).selectExpr("id", "id*2", "id * 3",
      s"id % $groupingDivisor ")
    insertDF.write.insertInto("test1")
    val rs = snc.sql("select col4, sum(col1) as summ1, sum(col2) as summ2 " +
      " from test1 group by col4")
    val results = rs.collect()
   // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    assertEquals(groupingDivisor, results.length)
    results.foreach(row => {
      val groupKey = row.getInt(0)
      val n = range / groupingDivisor
      val sum1 = ((n / 2.0f) * ((2 * groupKey) + (n - 1) * groupingDivisor)).toLong
      val sum2 = ((n / 2.0f) * ((2 * 2 * groupKey) + (n - 1) * 2 * groupingDivisor)).toLong
      val sumAgg1 = row.getLong(1)
      val sumAgg2 = row.getLong(2)
      assertEquals(sum1, sumAgg1)
      assertEquals(sum2, sumAgg2)
    })
    snc.dropTable("test1")
  }

  test("not nullable multiple strings as grouping key") {
    snc
    snc.sql("drop table if exists test1")
    snc.sql("create table test1 (col1 int not null, col2 String not null , " +
      "col3 int not null, col4 String  not null) using column ")
    val range = 50
    val groupingDivisor = 10
    val groupingDivisor1 = 5
    val ps = getSqlConnection.prepareStatement("insert into test1 values (?, ?, ?, ?) ")
    for (i <- 0 until range) {
      for (j <- 0 until range ) {
        ps.setInt(1, i * j)
        ps.setString(2, s"col2_${j % groupingDivisor1}")
        ps.setInt(3, j)
        ps.setString(4, s"col4_${i % groupingDivisor}")
        ps.addBatch()
      }
    }
    ps.executeBatch()
    val rs = snc.sql("select col4, col2 , sum(col1) as summ1  from test1 group by col4, col2")
    // import org.apache.spark.sql.execution.debug._
    // rs.debugCodegen()
    val results = rs.collect()
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    assertEquals(groupingDivisor * groupingDivisor1, results.length)

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
      s"Concat( 'test', Cast(id % $groupingDivisor as string) ) ")
    insertDF.write.insertInto("test1")
    val rs = snc.sql("select col4, sum(col1) as summ1, sum(col2) as summ2 " +
      " from test1 group by col4")
    // import org.apache.spark.sql.execution.debug._
    // rs.debugCodegen()
    val results = rs.collect()
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    assertEquals(groupingDivisor, results.length)
    results.foreach(row => {
      val groupKey = row.getString(0).substring("test".length).toInt
      val n = range / groupingDivisor
      val sum1 = ((n / 2.0f) * ((2 * groupKey) + (n - 1) * groupingDivisor)).toLong
      val sum2 = ((n / 2.0f) * ((2 * 2 * groupKey) + (n - 1) * 2 * groupingDivisor)).toLong
      val sumAgg1 = row.getLong(1)
      val sumAgg2 = row.getLong(2)
      assertEquals(sum1, sumAgg1)
      assertEquals(sum2, sumAgg2)
    })
    snc.dropTable("test1")
  }

  test("multiple aggregates query with null grouping keys") {
    snc
    val conn = getSqlConnection
    snc.sql("drop table if exists test1")
    snc.sql("create table test1 (col1 int, col2 int, col3 int, col4 String) " +
      "using column ")
    val range = 200
    val groupingDivisor = 10
    val insertPs = conn.prepareStatement("insert into test1 values(?,?,?,?)")
    for (i <- 0 until range) {
      insertPs.setInt(1, i)
      insertPs.setInt(2, i * 2)
      insertPs.setInt(3, i * 3)
      if (i % groupingDivisor == 0) {
        insertPs.setNull(4, Types.VARCHAR)
      } else {
        insertPs.setString(4, s"test${i % groupingDivisor}")
      }
      insertPs.addBatch()
    }
    insertPs.executeBatch()

    val rs = snc.sql("select col4, sum(col1) as summ1, sum(col2) as summ2 " +
      " from test1 group by col4")
    // import org.apache.spark.sql.execution.debug._
    // rs.debugCodegen()
    val results = rs.collect()
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    assertEquals(groupingDivisor, results.length)
    results.foreach(row => {
      val groupKey = if (row.isNullAt(0)) {
        0
      } else {
        row.getString(0).substring("test".length).toInt
      }
      val n = range / groupingDivisor
      val sum1 = ((n / 2.0f) * ((2 * groupKey) + (n - 1) * groupingDivisor)).toLong
      val sum2 = ((n / 2.0f) * ((2 * 2 * groupKey) + (n - 1) * 2 * groupingDivisor)).toLong
      val sumAgg1 = row.getLong(1)
      val sumAgg2 = row.getLong(2)
      assertEquals(sum1, sumAgg1)
      assertEquals(sum2, sumAgg2)
    })
    snc.dropTable("test1")
  }

  test("multiple aggregates query with null and two grouping keys") {
    snc
    val conn = getSqlConnection
    snc.sql("drop table if exists test1")
    snc.sql("create table test1 (col1 int, col2 int, col3 int, col4 String, col5 String) " +
      "using column ")
    val range = 50
    val groupingDivisor1 = 10
    val groupingDivisor2 = 10
    val insertPs = conn.prepareStatement("insert into test1 values(?,?,?,?,?)")
    for (i <- 0 until range; j <- 0 until groupingDivisor2) {
      insertPs.setInt(1, i)
      insertPs.setInt(2, i * 2)
      insertPs.setInt(3, i * 3)
      if (i % groupingDivisor1 == 0) {
        insertPs.setNull(4, Types.VARCHAR)
      } else {
        insertPs.setString(4, s"test${i % groupingDivisor1}")
      }

      if (j % groupingDivisor2 == 0) {
        insertPs.setNull(5, Types.VARCHAR)
      } else {
        insertPs.setString(5, s"test${j % groupingDivisor2}")
      }
      insertPs.addBatch()
    }
    insertPs.executeBatch()

    val rs = snc.sql("select col4, col5, sum(col1) as summ1, sum(col2) as summ2 " +
      " from test1 group by col4, col5")
    // import org.apache.spark.sql.execution.debug._
    // rs.debugCodegen()
    val results = rs.collect()
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    assertEquals(groupingDivisor1 * groupingDivisor2, results.length)
    results.foreach(row => {
      val groupKey = if (row.isNullAt(0)) {
        0
      } else {
        row.getString(0).substring("test".length).toInt
      }
      val n = range / groupingDivisor1
      val sum1 = ((n / 2.0f) * ((2 * groupKey) + (n - 1) * groupingDivisor1)).toLong
      val sum2 = ((n / 2.0f) * ((2 * 2 * groupKey) + (n - 1) * 2 * groupingDivisor1)).toLong
      val sumAgg1 = row.getLong(2)
      val sumAgg2 = row.getLong(3)
      assertEquals(sum1, sumAgg1)
      assertEquals(sum2, sumAgg2)
    })
    snc.dropTable("test1")
  }


  test("Test incremental addition of grouping columns with the last grouping column as null") {
    val numKeyCols = 100
    snc
    val conn = getSqlConnection

    snc.sql("drop table if exists test1")

    val createTableStr = "create table test1 ( num1 int, num2 int," +
      (for (i <- 3 until numKeyCols + 3) yield s"col$i string").mkString(",") + ")"

    snc.sql(s" $createTableStr using column ")


    val insertStr = "insert into test1 values(?, ?," +
      (for (i <- 0 until numKeyCols) yield "?").mkString(",") + ")"

    val insertPs = conn.prepareStatement(insertStr)

    for (i <- 3 until numKeyCols + 3) {
      val groupingCols = (for (j <- 3 to i) yield s"col$j").mkString(", ")
      val sqlStr = s"select sum(num1) as summ1, sum(num2) as summ2, $groupingCols " +
        s" from test1 group by $groupingCols"
      // println(s"executing query making $i grouping column as null")
      insertPs.setInt(1, i)
      insertPs.setInt(2, i * 2)
      for (j <- 3 until i) {
        insertPs.setString(j, s"col$j")
      }
      insertPs.setNull(i, Types.VARCHAR)
      insertPs.addBatch()
      insertPs.executeBatch()
      // now query

      val rs = snc.sql(sqlStr)
      // import org.apache.spark.sql.execution.debug._
      //  rs.debugCodegen()
      val results = rs.collect()
      // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
      assertEquals(1, results.length)
      for (j <- 2 until i - 1) {
        assertEquals(s"col${j + 1}", results(0).getString(j))
      }
      assertTrue(results(0).isNullAt(i - 1))
      snc.sql("delete from test1")
    }
    snc.dropTable("test1")
  }

  test("null grouping key bit masking for 1 to 100 columns in group by") {
    val numKeyCols = 100
    snc
    val conn = getSqlConnection

    snc.sql("drop table if exists test1")

    val createTableStr = "create table test1 ( num1 int, num2 int," +
      (for (i <- 3 until numKeyCols + 3) yield s"col$i string").mkString(",") + ")"

    snc.sql(s" $createTableStr using column ")
    val range = 100
    val groupingDivisor = 10
    val insertStr = "insert into test1 values(?, ?," +
      (for (_ <- 0 until numKeyCols) yield "?").mkString(",") + ")"
    val insertPs = conn.prepareStatement(insertStr)
    for (i <- 0 until range) {
      insertPs.setInt(1, i)
      insertPs.setInt(2, i * 2)
      for (j <- 3 until numKeyCols + 3) {
        if (j == i % numKeyCols) {
          insertPs.setNull(j, Types.VARCHAR)
        } else {
          insertPs.setString(j, s"col$j-${i % groupingDivisor}")
        }
      }
      insertPs.addBatch()
    }

    insertPs.executeBatch()

    for (i <- 3 until numKeyCols + 3) {
      //  println(s"executing query number ${i -2} ")
      val groupingCols = (for (j <- 3 to i) yield s"col$j").mkString(", ")
      val sqlStr = s"select sum(num1) as summ1, sum(num2) as summ2, $groupingCols " +
        s" from test1 group by $groupingCols"
      val rs = snc.sql(sqlStr)
      // import org.apache.spark.sql.execution.debug._
      // rs.debugCodegen()
      val results = rs.collect()
      // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
      assertTrue(results.length > 0)
    }


    for (i <- 3 until numKeyCols + 3) {
      //  println(s"executing query number ${i -2} ")
      snc.sql("delete from test1")
      // insert 100 rows with num1 as 1 and num2 as 2
      // string cols as col-n & rest as null
      val numRowsInBatch = 100
      val const1 = 1
      val const2 = 2
      val const4 = 4
      val const8 = 8
      for (_ <- 0 until numRowsInBatch) {
        insertPs.setInt(1, const1)
        insertPs.setInt(2, const2)
        for (k <- 3 to i) {
          insertPs.setString(k, s"col-$k")
        }
        for (p <- i + 1 until numKeyCols + 3) {
          insertPs.setNull(p, Types.VARCHAR)
        }
        insertPs.addBatch()
      }

      // insert 100 rows with num1 as 4 and num2 as 8
      // string cols as col-n for n - 1 cols, & the nth col as null

      for (_ <- 0 until numRowsInBatch) {
        insertPs.setInt(1, const4)
        insertPs.setInt(2, const8)
        for (k <- 3 until i) {
          insertPs.setString(k, s"col-$k")
        }
        for (p <- i until numKeyCols + 3) {
          insertPs.setNull(p, Types.VARCHAR)
        }
        insertPs.addBatch()
      }
      insertPs.executeBatch()

      val groupingCols = (for (j <- 3 to i) yield s"col$j").mkString(", ")
      val sqlStr = s"select sum(num1) as summ1, sum(num2) as summ2, $groupingCols " +
        s" from test1 group by $groupingCols"
      val rs = snc.sql(sqlStr)
      val results = rs.collect()
      // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
      assertEquals(2, results.length)
      val row1 = results(0)
      val row2 = results(1)
      var foundNullKey = false
      if (row1.isNullAt(i - 1)) {
        foundNullKey = true
        assertEquals(numRowsInBatch * const4, row1.getLong(0))
        for (j <- 2 until i - 1) {
          assertEquals(s"col-${j + 1}", row1.getString(j))
        }
      } else {
        assertEquals(numRowsInBatch * const1, row1.getLong(0))
        for (j <- 2 until i) {
          assertEquals(s"col-${j + 1}", row1.getString(j))
        }
      }

      if (row2.isNullAt(i - 1)) {
        foundNullKey = true
        assertEquals(numRowsInBatch * const4, row2.getLong(0))
        for (j <- 2 until i - 1) {
          assertEquals(s"col-${j + 1}", row2.getString(j))
        }
      } else {
        assertEquals(numRowsInBatch * const1, row2.getLong(0))
        for (j <- 2 until i) {
          assertEquals(s"col-${j + 1}", row2.getString(j))
        }
      }
      assert(foundNullKey)

    }

    snc.dropTable("test1")
  }


  // missing types = struct type, maptype,userdefinedtype, hivestringtype,
  // array type, calendarinterval
  test("aggregate functions & grouping on each of spark data type") {

    snc
    val conn = getSqlConnection

    type DataMap = Map[Int, Any]

    snc.sql("drop table if exists test1")

    val numCols = 14

    val createTableStr = s"create table test1 ( col000 int, " +
      s"col${Types.TINYINT.toString.replaceAll("-", "_")} byte," +
      s" col${Types.SMALLINT.toString.replaceAll("-", "_")} short, " +
      s"col${Types.INTEGER.toString.replaceAll("-", "_")} int," +
      s"col${Types.BIGINT.toString.replaceAll("-", "_")} long, " +
      s"col${Types.FLOAT.toString.replaceAll("-", "_")} float, " +
      s"col${Types.DOUBLE.toString.replaceAll("-", "_")} double, " +
      s"col${Types.DECIMAL.toString.replaceAll("-", "_")}_1 decimal(12, 5)," +
      s" col${Types.DECIMAL.toString.replaceAll("-", "_")}_2 decimal(28, 25)," +
      s"col${Types.TIMESTAMP.toString.replaceAll("-", "_")} timestamp," +
      s" col${Types.VARCHAR.toString.replaceAll("-", "_")} string, " +
      s"col${Types.BOOLEAN.toString.replaceAll("-", "_")} boolean, " +
      s"col${Types.DATE.toString.replaceAll("-", "_")} date, " +
      s"col${Types.BINARY.toString.replaceAll("-", "_")} binary) using column"


    snc.sql(createTableStr)


    val insertStr = s"insert into test1 " +
      s"values (${(for (_ <- Range(0, numCols, 1)) yield "?").mkString(",")} )"

    val insertPs = conn.prepareStatement(insertStr)

    val posToTypeMapping = Map[Int, Int](
      1 -> Types.INTEGER, 2 -> Types.TINYINT, 3 -> Types.SMALLINT,
      4 -> Types.INTEGER, 5 -> Types.BIGINT, 6 -> Types.FLOAT,
      7 -> Types.DOUBLE, 8 -> Types.DECIMAL, 9 -> Types.DECIMAL, 10 -> Types.TIMESTAMP,
      11 -> Types.VARCHAR, 12 -> Types.BOOLEAN, 13 -> Types.DATE, 14 -> Types.BINARY
    )

    val typeMapping: Map[Int, (Int, Any) => Unit] =
      Map(
        Types.TINYINT -> ((i: Int, o: Any) => insertPs.setByte(i, o.asInstanceOf[Byte])),
        Types.SMALLINT -> ((i: Int, o: Any) => insertPs.setShort(i, o.asInstanceOf[Short])),
        Types.INTEGER -> ((i: Int, o: Any) => insertPs.setInt(i, o.asInstanceOf[Int])),
        Types.BIGINT -> ((i: Int, o: Any) => insertPs.setLong(i, o.asInstanceOf[Long])),
        Types.FLOAT -> ((i: Int, o: Any) => insertPs.setFloat(i, o.asInstanceOf[Float])),
        Types.DOUBLE -> ((i: Int, o: Any) => insertPs.setDouble(i, o.asInstanceOf[Double])),
        Types.DECIMAL -> ((i: Int, o: Any) =>
          insertPs.setBigDecimal(i, o.asInstanceOf[java.math.BigDecimal])),
        Types.TIMESTAMP -> ((i: Int, o: Any) =>
          insertPs.setTimestamp(i, o.asInstanceOf[Timestamp])),
        Types.VARCHAR -> ((i: Int, o: Any) => insertPs.setString(i, o.asInstanceOf[String])),
        Types.BOOLEAN -> ((i: Int, o: Any) => insertPs.setBoolean(i, o.asInstanceOf[Boolean])),
        Types.DATE -> ((i: Int, o: Any) => insertPs.setDate(i, o.asInstanceOf[Date])),
        Types.BINARY -> ((i: Int, o: Any) => insertPs.setBytes(i, o.asInstanceOf[Array[Byte]]))
      )

    def setInInsertStatement(dataMap: Map[Int, Any]): Unit = {
      for (entry <- dataMap) {
        val pos = entry._1
        val value = entry._2
        typeMapping(posToTypeMapping(pos))(pos, value)
      }

      for (i <- 1 until numCols + 1) {
        if (!dataMap.contains(i)) {
          insertPs.setNull(i, posToTypeMapping(i))
        }
      }
    }

    def colName(pos: Int): String = if (pos == 1) "col000"
    else s"col${posToTypeMapping(pos).toString.replaceAll("-", "_")}"

    var expectedResult: mutable.Map[Any, Any] = null

    // check behaviour of byte as aggregate column
    for (i <- 0 until 10) {
      val dataMap: DataMap = Map(1 -> i, 2 -> i.toByte, 11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    var q = s"select sum(${colName(2)}), ${colName(11)} from test1 group by ${colName(11)} "
    expectedResult = mutable.Map[Any, Any]("col0" -> 10L, "col1" -> 35L)
    var rs = snc.sql(q)
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    var rows = rs.collect
    assertEquals(2, rows.length)
    rows.foreach(row => {
      assertEquals(expectedResult(row.getString(1)), row.getLong(0))
      expectedResult.remove(row.getString(1))
    })
    assertTrue(expectedResult.isEmpty)
    snc.sql("delete from test1")

    // check behaviour of short as aggregate column
    for (i <- 0 until 10) {
      val dataMap: DataMap = Map(1 -> i, 3 -> i.toShort, 11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    q = s"select sum(${colName(3)}), ${colName(11)} from test1 group by ${colName(11)} "
    expectedResult = mutable.Map("col0" -> 10L, "col1" -> 35L)
    rs = snc.sql(q)
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    rows = rs.collect
    assertEquals(2, rows.length)
    rows.foreach(row => {
      assertEquals(expectedResult(row.getString(1)), row.getLong(0))
      expectedResult.remove(row.getString(1))
    })
    assertTrue(expectedResult.isEmpty)
    snc.sql("delete from test1")

    // check behaviour of long as aggregate column
    for (i <- 0 until 10) {
      val dataMap: DataMap = Map(1 -> i, 5 -> i.toLong, 11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    q = s"select sum(${colName(5)}), ${colName(11)} from test1 group by ${colName(11)} "
    expectedResult = mutable.Map("col0" -> 10L, "col1" -> 35L)
    rs = snc.sql(q)
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    rows = rs.collect
    assertEquals(2, rows.length)
    rows.foreach(row => {
      assertEquals(expectedResult(row.getString(1)), row.getLong(0))
      expectedResult.remove(row.getString(1))
    })
    assertTrue(expectedResult.isEmpty)
    snc.sql("delete from test1")


    // check behaviour of float as aggregate column
    for (i <- 0 until 10) {
      val dataMap: DataMap = Map(1 -> i, 6 -> i.toFloat, 11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    q = s"select sum(${colName(6)}), ${colName(11)} from test1 group by ${colName(11)} "
    expectedResult = mutable.Map("col0" -> 10.toDouble, "col1" -> 35.toDouble)
    rs = snc.sql(q)
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    rows = rs.collect
    assertEquals(2, rows.length)
    rows.foreach(row => {
      assertEquals(expectedResult(row.getString(1)), row.getDouble(0))
      expectedResult.remove(row.getString(1))
    })
    assertTrue(expectedResult.isEmpty)
    snc.sql("delete from test1")


    // check behaviour of double as aggregate column
    for (i <- 0 until 10) {
      val dataMap: DataMap = Map(1 -> i, 7 -> i.toDouble, 11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    q = s"select sum(${colName(7)}), ${colName(11)} from test1 group by ${colName(11)} "
    expectedResult = mutable.Map("col0" -> 10.toDouble, "col1" -> 35.toDouble)
    rs = snc.sql(q)
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    rows = rs.collect
    assertEquals(2, rows.length)
    rows.foreach(row => {
      assertEquals(expectedResult(row.getString(1)), row.getDouble(0))
      expectedResult.remove(row.getString(1))
    })
    assertTrue(expectedResult.isEmpty)
    snc.sql("delete from test1")

    // check behaviour of Big Decimal with precision < 18 as aggregate column
    for (i <- 0 until 10) {
      val dataMap: DataMap = Map(1 -> i, 8 -> new java.math.BigDecimal(s"${.3 * i}"),
        11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    q = s"select sum(${colName(8)}_1), ${colName(11)} from test1 group by ${colName(11)} "
    expectedResult = mutable.Map("col0" -> new java.math.BigDecimal(s"${.3 * 10}"),
      "col1" -> new java.math.BigDecimal(s"${.3 * 35}"))
    rs = snc.sql(q)
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    rows = rs.collect
    assertEquals(2, rows.length)
    rows.foreach(row => {
      assertTrue(
        Math.abs(expectedResult(row.getString(1)).asInstanceOf[java.math.BigDecimal].
          subtract(row.getDecimal(0)).doubleValue()) < .1)
      expectedResult.remove(row.getString(1))
    })
    assertTrue(expectedResult.isEmpty)
    snc.sql("delete from test1")

    // check behaviour of Big Decimal with precision > 18 as aggregate column
    for (i <- 0 until 10) {
      val dataMap: DataMap = Map(1 -> i, 9 -> new java.math.BigDecimal(s"${.3 * i}"),
        11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    q = s"select sum(${colName(9)}_2), ${colName(11)} from test1 group by ${colName(11)} "
    expectedResult = mutable.Map("col0" -> new java.math.BigDecimal(s"${.3 * 10}"),
      "col1" -> new java.math.BigDecimal(s"${.3 * 35}"))
    rs = snc.sql(q)
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    rows = rs.collect
    assertEquals(2, rows.length)
    rows.foreach(row => {
      assertTrue(
        Math.abs(expectedResult(row.getString(1)).asInstanceOf[java.math.BigDecimal].
          subtract(row.getDecimal(0)).doubleValue()) < .1)
      expectedResult.remove(row.getString(1))
    })
    assertTrue(expectedResult.isEmpty)
    snc.sql("delete from test1")


    // check behaviour of Timestamp as aggregate column
    for (i <- 0 until 10) {
      val dataMap: DataMap = Map(1 -> i, 10 -> new Timestamp(1234567 * i),
        11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    val expected1 = (for (i <- 0 until 5) yield {
      DateTimeUtils.fromJavaTimestamp(new Timestamp(1234567 * i))
    }).foldLeft(0L)(_ + _) / 1000000d

    val expected2 = (for (i <- 5 until 10) yield {
      DateTimeUtils.fromJavaTimestamp(new Timestamp(1234567 * i))
    }).foldLeft(0L)(_ + _) / 1000000d

    q = s"select sum(${colName(10)}), ${colName(11)} from test1 group by ${colName(11)} "
    expectedResult = mutable.Map("col0" -> expected1,
      "col1" -> expected2)
    rs = snc.sql(q)
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    rows = rs.collect
    assertEquals(2, rows.length)
    rows.foreach(row => {
      assertTrue(Math.abs(expectedResult(
        row.getString(1)).asInstanceOf[Double] - row.getDouble(0)) < 1)
      expectedResult.remove(row.getString(1))
    })
    assertTrue(expectedResult.isEmpty)
    snc.sql("delete from test1")


    // check behaviour of byte as grouping column with null & two not nulls
    for (i <- 0 until 10) {
      val dataMap: DataMap = Map(1 -> i, 2 -> (i / 5).toByte, 11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    for (i <- 10 until 15) {
      val dataMap: DataMap = Map(1 -> i, 11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    q = s"select sum(${colName(1)}), ${colName(2)} from test1 group by ${colName(2)} "
    expectedResult = mutable.Map(0 -> 10L, 1 -> 35L, "null" -> 60L)
    rs = snc.sql(q)
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    rows = rs.collect
    assertEquals(3, rows.length)
    rows.foreach(row => {
      assertEquals(expectedResult(if (row.isNullAt(1)) "null" else row.getByte(1)), row.getLong(0))
      expectedResult.remove(if (row.isNullAt(1)) "null" else row.getByte(1))
    })
    assertTrue(expectedResult.isEmpty)
    snc.sql("delete from test1")

    // check behaviour of short as grouping column with null & two not nulls
    for (i <- 0 until 10) {
      val dataMap: DataMap = Map(1 -> i, 3 -> (i / 5).toShort, 11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    for (i <- 10 until 15) {
      val dataMap: DataMap = Map(1 -> i, 11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    q = s"select sum(${colName(1)}), ${colName(3)} from test1 group by ${colName(3)} "
    expectedResult = mutable.Map(0 -> 10L, 1 -> 35L, "null" -> 60L)
    rs = snc.sql(q)
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    rows = rs.collect
    assertEquals(3, rows.length)
    rows.foreach(row => {
      assertEquals(expectedResult(if (row.isNullAt(1)) "null" else row.getShort(1)), row.getLong(0))
      expectedResult.remove(if (row.isNullAt(1)) "null" else row.getShort(1))
    })
    assertTrue(expectedResult.isEmpty)
    snc.sql("delete from test1")

    // check behaviour of int as grouping column with null & two not nulls
    for (i <- 0 until 10) {
      val dataMap: DataMap = Map(1 -> i, 4 -> (i / 5), 11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    for (i <- 10 until 15) {
      val dataMap: DataMap = Map(1 -> i, 11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    q = s"select sum(${colName(1)}), ${colName(4)} from test1 group by ${colName(4)} "
    expectedResult = mutable.Map(0 -> 10L, 1 -> 35L, "null" -> 60L)
    rs = snc.sql(q)
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    rows = rs.collect
    assertEquals(3, rows.length)
    rows.foreach(row => {
      assertEquals(expectedResult(if (row.isNullAt(1)) "null" else row.getInt(1)), row.getLong(0))
      expectedResult.remove(if (row.isNullAt(1)) "null" else row.getInt(1))
    })
    assertTrue(expectedResult.isEmpty)
    snc.sql("delete from test1")

    // check behaviour of long as grouping column with null & two not nulls
    for (i <- 0 until 10) {
      val dataMap: DataMap = Map(1 -> i, 5 -> (i / 5).toLong, 11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    for (i <- 10 until 15) {
      val dataMap: DataMap = Map(1 -> i, 11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    q = s"select sum(${colName(1)}), ${colName(5)} from test1 group by ${colName(5)} "
    expectedResult = mutable.Map(0 -> 10L, 1 -> 35L, "null" -> 60L)
    rs = snc.sql(q)
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    rows = rs.collect
    assertEquals(3, rows.length)
    rows.foreach(row => {
      assertEquals(expectedResult(if (row.isNullAt(1)) "null" else row.getLong(1)), row.getLong(0))
      expectedResult.remove(if (row.isNullAt(1)) "null" else row.getLong(1))
    })
    assertTrue(expectedResult.isEmpty)
    snc.sql("delete from test1")

    // check behaviour of float as grouping column with null & two not nulls
    for (i <- 0 until 10) {
      val dataMap: DataMap = Map(1 -> i, 6 -> (i / 5) * .7F, 11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    for (i <- 10 until 15) {
      val dataMap: DataMap = Map(1 -> i, 11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    q = s"select sum(${colName(1)}), ${colName(6)} from test1 group by ${colName(6)} "
    expectedResult = mutable.Map(0 -> 10L, .7f -> 35L, "null" -> 60L)
    rs = snc.sql(q)
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    rows = rs.collect
    assertEquals(3, rows.length)
    rows.foreach(row => {
      assertEquals(expectedResult(if (row.isNullAt(1)) "null" else row.getFloat(1)), row.getLong(0))
      expectedResult.remove(if (row.isNullAt(1)) "null" else row.getFloat(1))
    })
    assertTrue(expectedResult.isEmpty)
    snc.sql("delete from test1")

    // check behaviour of double as grouping column with null & two not nulls
    for (i <- 0 until 10) {
      val dataMap: DataMap = Map(1 -> i, 7 -> (i / 5) * .7D, 11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    for (i <- 10 until 15) {
      val dataMap: DataMap = Map(1 -> i, 11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    q = s"select sum(${colName(1)}), ${colName(7)} from test1 group by ${colName(7)} "
    expectedResult = mutable.Map(0 -> 10L, .7D -> 35L, "null" -> 60L)
    rs = snc.sql(q)
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    rows = rs.collect
    assertEquals(3, rows.length)
    rows.foreach(row => {
      assertEquals(expectedResult(if (row.isNullAt(1)) "null" else row.getDouble(1)),
        row.getLong(0))
      expectedResult.remove(if (row.isNullAt(1)) "null" else row.getDouble(1))
    })
    assertTrue(expectedResult.isEmpty)
    snc.sql("delete from test1")

    // check behaviour of BigDecimal as grouping column with null & two not nulls
    for (i <- 0 until 10) {
      val dataMap: DataMap = Map(1 -> i, 8 -> new java.math.BigDecimal(s"${12.3E+3 + i / 5 + 1}"),
        11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    for (i <- 10 until 15) {
      val dataMap: DataMap = Map(1 -> i, 11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    q = s"select sum(${colName(1)}), ${colName(8)}_1 from test1 group by ${colName(8)}_1 "
    expectedResult = mutable.Map(new java.math.BigDecimal(s"${12.3E+3 + 1}").doubleValue() -> 10L,
      new java.math.BigDecimal(s"${12.3E+3 + 2}").doubleValue() -> 35L, "null" -> 60L)
    rs = snc.sql(q)
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    rows = rs.collect
    assertEquals(3, rows.length)
    rows.foreach(row => {
      assertEquals(expectedResult(if (row.isNullAt(1)) "null" else row.getDecimal(1).doubleValue()),
        row.getLong(0))
      expectedResult.remove(if (row.isNullAt(1)) "null" else row.getDecimal(1).doubleValue())
    })
    assertTrue(expectedResult.isEmpty)
    snc.sql("delete from test1")

    // check behaviour of binary byte array as grouping column with null & two not nulls
    for (i <- 0 until 10) {
      val dataMap: DataMap = Map(1 -> i, 14 -> Array.fill[Byte](100)((i / 5).toByte),
        11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    for (i <- 10 until 15) {
      val dataMap: DataMap = Map(1 -> i, 11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    q = s"select sum(${colName(1)}), ${colName(14)} from test1 group by ${colName(14)} "
    expectedResult = mutable.Map(Array.fill[Byte](100)(0.toByte).sum -> 10L,
      Array.fill[Byte](100)(1.toByte).sum -> 35L, "null" -> 60L)
    rs = snc.sql(q)
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    rows = rs.collect
    assertEquals(3, rows.length)
    rows.foreach(row => {
      assertEquals(expectedResult(
        if (row.isNullAt(1)) {
          "null"
        } else {
          row.getAs[Array[Byte]](1).sum
        }), row.getLong(0))
      expectedResult.remove(
        if (row.isNullAt(1)) {
          "null"
        } else {
          row.getAs[Array[Byte]](1).sum
        })
    })
    assertTrue(expectedResult.isEmpty)
    snc.sql("delete from test1")

    // check behaviour of boolean as grouping column with null & two not nulls
    for (i <- 0 until 10) {
      val dataMap: DataMap = Map(1 -> i, 12 -> (i / 5 == 1),
        11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    for (i <- 10 until 15) {
      val dataMap: DataMap = Map(1 -> i, 11 -> s"col${i / 5}")
      setInInsertStatement(dataMap)
      insertPs.executeUpdate()
    }
    q = s"select sum(${colName(1)}), ${colName(12)} from test1 group by ${colName(12)} "
    expectedResult = mutable.Map(false -> 10L,
      true -> 35L, "null" -> 60L)
    rs = snc.sql(q)
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    rows = rs.collect
    assertEquals(3, rows.length)
    rows.foreach(row => {
      assertEquals(expectedResult(
        if (row.isNullAt(1)) {
          "null"
        } else {
          row.getBoolean(1)
        }), row.getLong(0))
      expectedResult.remove(
        if (row.isNullAt(1)) {
          "null"
        } else {
          row.getBoolean(1)
        })
    })
    assertTrue(expectedResult.isEmpty)
    snc.sql("delete from test1")

    snc.dropTable("test1")
  }


  test("simple aggregate query with struct type as grouping key") {
    snc.sql("drop table if exists test1")
    val data = for (i <- 0 until 15) yield {
      Row(i, Row(s"col${i / 5}", i / 5))
    }
    val strucType1 = StructType(Seq(StructField("col2_1", StringType),
      StructField("col2_2", IntegerType)))
    val structType = StructType(Seq(StructField("col1", IntegerType),
      StructField("col2", strucType1)))
    val df = snc.createDataFrame(snc.sparkContext.parallelize(data), structType)
    df.write.format("column").mode(SaveMode.Overwrite).saveAsTable("test1")

    def runTest(session: SnappyContext): Unit = {
      val rs = session.sql("select col2, sum(col1) as summ1 from test1 group by col2")
      val results = rs.collect()
      // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
      val expectedResult = mutable.Map[(String, Int), Int](("col0", 0) -> 10,
        ("col1", 1) -> 35, ("col2", 2) -> 60)
      assertEquals(expectedResult.size, results.length)
      results.foreach(row => {
        val str = row.getStruct(0)
        val key = (str.getString(0), str.getInt(1))
        assertEquals(expectedResult.getOrElse(key, fail("key does not exist")), row.getLong(1))
        expectedResult.remove(key)
      })
      assertTrue(expectedResult.isEmpty)
    }

    val newSession1 = snc.newSession()
    newSession1.setConf(Property.TestExplodeComplexDataTypeInSHA.name, true.toString)
    runTest(newSession1)

    val newSession2 = snc.newSession()
    newSession2.setConf(Property.TestExplodeComplexDataTypeInSHA.name, false.toString)
    runTest(newSession2)
    snc.dropTable("test1")

  }

  test("test array type field as grouping key") {
    snc.sql("drop table if exists test1")

    val structType = StructType(Seq(StructField("col1", IntegerType),
      StructField("stringarray", ArrayType(StringType)),
      StructField("longarray", ArrayType(LongType))))
    val data = for (i <- 0 until 15) yield {
      Row(i, Array.fill[String](10)(s"col${i / 5}"), Array.fill[Long](10)(i / 5.toLong))
    }

    val data1 = for (i <- 15 until 20) yield {
      Row(i, Array.tabulate[String](7)(indx => if (indx % 2 == 0) null else s"col${i / 5}"),
        Array.fill[Long](10)(i / 5.toLong))
    }

    val data2 = for (i <- 20 until 25) yield {
      Row(i, Array.tabulate[String](100)(indx => if (indx % 2 == 0) null else s"col${i / 5}"),
        Array.fill[Long](10)(i / 5.toLong))
    }

    val df = snc.createDataFrame(snc.sparkContext.parallelize(data ++ data1 ++ data2), structType)
    df.write.format("column").mode(SaveMode.Overwrite).saveAsTable("test1")

    def runTest1(session: SnappyContext): Unit = {
      val rs = session.sql("select longarray,  sum(col1) as summ1 from test1 group by longarray")
      val results = rs.collect()
      // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))

      val expectedResult1 = mutable.Map[Long, Int](0L -> 10, 1L * 10 -> 35,
        2L * 10 -> 60, 3L * 10 -> 85, 4L * 10 -> 110)
      assertEquals(expectedResult1.size, results.length)

      results.foreach(row => {
        val key = row.getSeq[Long](0).foldLeft(0L)(_ + _)
        assertEquals(expectedResult1.getOrElse(key, fail("key does not exist")), row.getLong(1))
        expectedResult1.remove(key)
      })
      assertTrue(expectedResult1.isEmpty)
    }

    val newSession1 = snc.newSession()
    newSession1.setConf(Property.TestExplodeComplexDataTypeInSHA.name, true.toString)
    runTest1(newSession1)

    val newSession2 = snc.newSession()
    newSession2.setConf(Property.TestExplodeComplexDataTypeInSHA.name, false.toString)
    runTest1(newSession2)


    def runTest2(session: SnappyContext): Unit = {
      val rs = session.sql("select stringarray, longarray," +
        "  sum(col1) as summ1 from test1 group by stringarray, longarray")

      val results = rs.collect()
      // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
      val expectedResult2 = mutable.Map[(String, Long), Long](
        (Array.fill[String](10)("col0").mkString(","), 10 * 0L) -> 10L,
        (Array.fill[String](10)("col1").mkString(","), 10 * 1L) -> 35L,
        (Array.fill[String](10)("col2").mkString(","), 10 * 2L) -> 60L,
        (Array.tabulate[String](7)(indx => if (indx % 2 == 0) "null" else "col3").
          mkString(","), 10 * 3L) -> 85L,
        (Array.tabulate[String](100)(indx => if (indx % 2 == 0) "null" else "col4").
          mkString(","), 10 * 4L) -> 110L
      )

      assertEquals(expectedResult2.size, results.length)
      results.foreach(row => {
        val key = (row.getSeq[String](0).map(x => if (x == null) "null" else x).mkString(","),
          row.getSeq[Long](1).foldLeft(0L)(_ + _))
        assertEquals(expectedResult2.getOrElse(key, fail("key does not exist")), row.getLong(2))
        expectedResult2.remove(key)
      })

      assertTrue(expectedResult2.isEmpty)
    }

    runTest2(newSession1)
    runTest2(newSession2)


    snc.dropTable("test1")
  }

  test("aggregate query with nested struct type as grouping key") {
    snc.sql("drop table if exists test1")
    val structType2 = StructType(Seq(StructField("wife", StringType),
      StructField("numOffsprings", IntegerType)))
    val strucType1 = StructType(Seq(StructField("name", StringType),
      StructField("zip", IntegerType), StructField("family", structType2)))
    val structType = StructType(Seq(StructField("id", IntegerType),
      StructField("details", strucType1)))

    val data = for (i <- 0 until 15) yield {
      val num = i / 5
      Row(i, Row(s"name$num", num, Row(s"spouse$num", num)))
    }

    val df = snc.createDataFrame(snc.sparkContext.parallelize(data), structType)
    df.write.format("column").mode(SaveMode.Overwrite).saveAsTable("test1")

    def runTest(session: SnappyContext): Unit = {
      val rs = session.sql("select details, sum(id) as summ1 from test1 group by details")
      val results = rs.collect()
      // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
      val expectedResult = mutable.Map[(String, Int, (String, Int)), Int](
        ("name0", 0, ("spouse0", 0)) -> 10,
        ("name1", 1, ("spouse1", 1)) -> 35, ("name2", 2, ("spouse2", 2)) -> 60)
      assertEquals(expectedResult.size, results.length)
      results.foreach(row => {
        val str1 = row.getStruct(0)
        val str2 = str1.getStruct(2)
        val key = (str1.getString(0), str1.getInt(1), (str2.getString(0), str2.getInt(1)))
        assertEquals(expectedResult.getOrElse(key, fail("key does not exist")), row.getLong(1))
        expectedResult.remove(key)
      })
      assertTrue(expectedResult.isEmpty)
    }

    val newSession1 = snc.newSession()
    newSession1.setConf(Property.TestExplodeComplexDataTypeInSHA.name, true.toString)
    runTest(newSession1)

    val newSession2 = snc.newSession()
    newSession2.setConf(Property.TestExplodeComplexDataTypeInSHA.name, false.toString)
    runTest(newSession2)
    snc.dropTable("test1")
  }

  test("grouping columns and aggregate with expressions") {
    snc
    snc.sql("drop table if exists test1")
    snc.sql("create table test1 (col1 int, col2 int, col3 int, col4 int, col5 string) " +
      "using column ")
    val range = 50
    val groupingDivisor = 10
    val insertDF = snc.range(50).selectExpr("id", "id * 2", "id * 3",
      s"id % $groupingDivisor", s"concat('col_',id % $groupingDivisor)")

    insertDF.write.insertInto("test1")
    val rs = snc.sql(s"select " +
      s"concat(Cast((cast(col2 / 2 as int)) % $groupingDivisor as string), col5) as x," +
      s" col5,  sum(col1) as summ1  from test1 group by x, col5")
    val results = rs.collect()
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    assertEquals(groupingDivisor, results.length)
    rs.foreach(row => {
      val x = row.getString(1)
      val groupKey = x.substring("col_".length).toInt
      val n = range / groupingDivisor
      val sum1 = ((n / 2.0f) * ((2 * groupKey) + (n - 1) * groupingDivisor)).toLong
      val sumAgg1 = row.getLong(2)
      val firstCol = s"${groupKey}col_$groupKey"
      assertEquals(firstCol, row.getString(0))
      assertEquals(sum1, sumAgg1)
    })
    snc.dropTable("test1")
  }


  test("test large data with rehash of ByteBufferHashMap") {
    val hfileTaxi: String = getClass.getResource("/trip_fare_ParquetEmptyData").getPath
    // TODO: Use following with full data
    // val hfile: String = "../../data/NYC_trip_ParquetData"
    // val hfile1: String = "../../data/trip_fare_ParquetData"

    val taxiFare = "taxifare"
    val tripFareDF = snc.read.parquet(hfileTaxi)

    tripFareDF.registerTempTable(taxiFare)
    tripFareDF.cache()

    var rs = snc.sql(s"SELECT hack_license, sum(fare_amount) as daily_fare_amount" +
      s" FROM $taxiFare group by hack_license")
    rs.collect()
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))

    rs = snc.sql(s"SELECT hack_license, to_date(pickup_datetime) as pickup_date," +
      s" sum(fare_amount) as daily_fare_amount  FROM $taxiFare " +
      s"group by hack_license, pickup_date")
    rs.collect()
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    snc.dropTable(taxiFare, true)
    tripFareDF.unpersist()
  }

  test("bug negative size of string grouping key") {
    val hfile: String = getClass.getResource("/2015.parquet").getPath
    val snContext = snc
    snContext.sql("set spark.sql.shuffle.partitions=6")
    val airlineTable = "airline"
    val airlineDF = snContext.read.load(hfile)
    airlineDF.registerTempTable(airlineTable)
    val rs = snc.sql(s"select dest from $airlineTable " +
      s" group by dest having count(*) > 1000000")

    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    rs.collect()
    snc.dropTable(airlineTable, true)
  }

  test("group by query without having aggregate functions") {
    snc
    snc.sql("drop table if exists test1")
    snc.sql("create table test1 (col1 int, col2 int, col3 int, col4 String) " +
      "using column ")
    val range = 50
    val groupingDivisor = 10
    val insertDF = snc.range(50).selectExpr("id", "id*2", "id * 3",
      s"Concat( 'test', Cast(id % $groupingDivisor as string) ) ")
    insertDF.write.insertInto("test1")
    val rs = snc.sql("select distinct col4 from test1 ")
    // import org.apache.spark.sql.execution.debug._
    // rs.debugCodegen()
    val results = rs.collect()
    val expectedResults = Array.tabulate[String](groupingDivisor)(i => s"test$i").toBuffer
    // assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    assertEquals(groupingDivisor, results.length)
    results.foreach(row => {
      assertTrue(expectedResults.exists(_ == row.getString(0)))
      expectedResults.remove(expectedResults.indexOf(row.getString(0)))
    })
    assertTrue(expectedResults.isEmpty)
    snc.dropTable("test1")
  }

  test("GITHUB-534") {
    val session = SnappyContext(sc).snappySession
    session.sql("CREATE TABLE yes_with(device_id VARCHAR(200), " +
      "sdk_version VARCHAR(200)) USING COLUMN OPTIONS(PARTITION_BY 'device_id')")
    session.insert("yes_with", Row("id1", "v1"), Row("id1", "v2"),
      Row("id2", "v1"), Row("id2", "v1"), Row("id2", "v3"))
    val rs = session.sql("select sdk_version, count(distinct device_id) from (" +
      "select sdk_version,device_id from YES_WITH group by sdk_version, " +
      "device_id) a group by sdk_version")
  //  assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))

    ColumnCacheBenchmark.collect(rs,
      Seq(Row("v1", 2), Row("v2", 1), Row("v3", 1)))
    snc.dropTable("yes_with")
  }

  test("oom") {
    snc
    snc.sql("drop table if exists part")
    snc.sql("CREATE EXTERNAL TABLE part using csv " +
      "options(path '/Users/asifshahid/workspace/TPCH_APP/GB1/part.csv'," +
      " header 'true', inferSchema 'true');")

    snc.sql("drop table if exists supplier")
    snc.sql("CREATE EXTERNAL TABLE supplier using csv " +
      "options(path '/Users/asifshahid/workspace/TPCH_APP/GB1/supplier.csv'," +
      " header 'true', inferSchema 'true');")

    snc.sql("drop table if exists partsupp")
    snc.sql("CREATE EXTERNAL TABLE partsupp using csv " +
      "options(path '/Users/asifshahid/workspace/TPCH_APP/GB1/partsupp.csv'," +
      " header 'true', inferSchema 'true');")

    snc.sql("drop table if exists customer")
    snc.sql("CREATE EXTERNAL TABLE customer using csv " +
      "options(path '/Users/asifshahid/workspace/TPCH_APP/GB1/customer.csv'," +
      " header 'true', inferSchema 'true');")

    snc.sql("drop table if exists orders")
    snc.sql("CREATE EXTERNAL TABLE orders using csv " +
      "options(path '/Users/asifshahid/workspace/TPCH_APP/GB1/orders.csv'," +
      " header 'true', inferSchema 'true');")

    snc.sql("drop table if exists lineitem")
    snc.sql("CREATE EXTERNAL TABLE lineitem using csv " +
      "options(path '/Users/asifshahid/workspace/TPCH_APP/GB1/lineitem.csv'," +
      " header 'true', inferSchema 'true');")

    snc.sql("drop table if exists nation")
    snc.sql("CREATE EXTERNAL TABLE nation using csv " +
      "options(path '/Users/asifshahid/workspace/TPCH_APP/GB1/nation.csv'," +
      " header 'true', inferSchema 'true');")

    snc.sql("drop table if exists region")
    snc.sql("CREATE EXTERNAL TABLE region using csv " +
      "options(path '/Users/asifshahid/workspace/TPCH_APP/GB1/region.csv'," +
      " header 'true', inferSchema 'true');")
    /*
    val q1_1 = "SELECT  l_returnflag,  l_linestatus,  sum(l_quantity) as sum_qty," +
      " sum(l_extendedprice) as sum_base_price,  sum(l_extendedprice * (1 - l_discount)) as sum_disc_price," +
      " sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,  avg(l_quantity) as avg_qty," +
      " avg(l_extendedprice) as avg_price,  avg(l_discount) as avg_disc," +
      " count(*) as count_order  FROM   lineitem WHERE  l_shipdate <= to_date('1998-12-01') - interval 90 day " +
      " GROUP BY   l_returnflag,   l_linestatus ORDER BY   l_returnflag, l_linestatus"

    val q1_2 = "SELECT  l_returnflag,  l_linestatus,  sum(l_quantity) as sum_qty," +
      " sum(l_extendedprice) as sum_base_price,  sum(l_extendedprice * (1 - l_discount)) as sum_disc_price," +
      " sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,  avg(l_quantity) as avg_qty," +
      " avg(l_extendedprice) as avg_price,  avg(l_discount) as avg_disc," +
      " count(*) as count_order  FROM   lineitem WHERE  l_shipdate <= to_date('1998-12-02') - interval 60 day " +
      " GROUP BY   l_returnflag,   l_linestatus ORDER BY   l_returnflag, l_linestatus"

    snc.sql(q1_1).collect()
    snc.sql(q1_2).collect()

    */
    val q2_1 = "select\ns_acctbal,\ns_name,\nn_name,\np_partkey,\np_mfgr,\ns_address,\ns_phone,\ns_comment\nfrom\npart," +
      "\nsupplier,\npartsupp,\nnation,\nregion\nwhere\np_partkey = ps_partkey\nand s_suppkey = ps_suppkey\nand " +
      "p_size = 10\nand p_type like '%STEEL'\nand s_nationkey = n_nationkey\nand n_regionkey = r_regionkey\nand r_name = 'EUROPE'\nand ps_supplycost = (\nselect min(ps_supplycost)\nfrom\npartsupp, supplier,\nnation, region\nwhere\np_partkey = ps_partkey\nand s_suppkey = ps_suppkey\nand s_nationkey = n_nationkey\nand n_regionkey = r_regionkey\nand r_name = 'EUROPE'\n)\norder by\ns_acctbal desc,\nn_name,\ns_name,\np_partkey"

    val q2_2 = "select\ns_acctbal,\ns_name,\nn_name,\np_partkey,\np_mfgr,\ns_address,\ns_phone,\ns_comment\nfrom\npart," +
      "\nsupplier,\npartsupp,\nnation,\nregion\nwhere\np_partkey = ps_partkey\nand s_suppkey = ps_suppkey\nand " +
      "p_size = 20\nand p_type like '%BRASS'\nand s_nationkey = n_nationkey\nand n_regionkey = r_regionkey\nand r_name = 'ASIA'\nand ps_supplycost = (\nselect min(ps_supplycost)\nfrom\npartsupp, supplier,\nnation, region\nwhere\np_partkey = ps_partkey\nand s_suppkey = ps_suppkey\nand s_nationkey = n_nationkey\nand n_regionkey = r_regionkey\nand r_name = 'ASIA'\n)\norder by\ns_acctbal desc,\nn_name,\ns_name,\np_partkey"


    snc.sql(q2_1).collect()
    snc.sql(q2_2).collect()

    val q2_prep = "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from part," +
      " supplier, partsupp, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and " +
      " p_size = ? and p_type like '%?' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = ? " +
      " and ps_supplycost = ( select min(ps_supplycost) from\npartsupp, supplier, nation," +
      " region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey " +
      " and n_regionkey = r_regionkey and r_name = ? ) order by s_acctbal desc, n_name, s_name, p_partkey"

    val conn = getSqlConnection
    val ps2 = conn.prepareStatement(q2_prep)
    ps2.setInt(1, 10)
    ps2.setString(2, "STEEL")
    ps2.setString(3, "EUROPE")
    ps2.setString(4, "EUROPE")
    ps2.executeQuery()

    ps2.setInt(1, 20)
    ps2.setString(2, "BRASS")
    ps2.setString(3, "ASIA")
    ps2.setString(4, "ASIA")
    ps2.executeQuery()

    ps2.setString(1, "30")
    ps2.setString(2, "NICKEL")
    ps2.setString(3, "AMERICA")
    ps2.setString(4, "AMERICA")
    ps2.executeQuery()


    /*
    val q3_1 = "select\nl_orderkey,\nsum(l_extendedprice*(1-l_discount)) as revenue,\no_orderdate,\no_shippriority\nfrom\ncustomer,\norders,\nlineitem\nwhere\nc_mktsegment = 'AUTOMOBILE'\nand " +
      "c_custkey = o_custkey\nand l_orderkey = o_orderkey\nand o_orderdate < to_date ('1995-03-07') \nand l_shipdate > to_date( '1995-03-14')\ngroup by\nl_orderkey,\no_orderdate,\no_shippriority\norder by\nrevenue desc,\no_orderdate"

    val q3_2 = "select\nl_orderkey,\nsum(l_extendedprice*(1-l_discount)) as revenue,\no_orderdate,\no_shippriority\nfrom\ncustomer,\norders,\nlineitem\nwhere\nc_mktsegment = 'FURNITURE'\nand " +
      "c_custkey = o_custkey\nand l_orderkey = o_orderkey\nand o_orderdate < to_date ('1995-03-08') \nand l_shipdate > to_date( '1995-03-16')\ngroup by\nl_orderkey,\no_orderdate,\no_shippriority\norder by\nrevenue desc,\no_orderdate"


    snc.sql(q3_1).collect()
    snc.sql(q3_2).collect()

    val q4_1 = "select\no_orderpriority,\ncount(*) as order_count\nfrom\norders\nwhere\no_orderdate >= to_date ('1993-08-01')\nand o_orderdate < to_date( '1993-08-01') + interval 3 month\nand exists (\nselect\n*\nfrom\nlineitem\nwhere\nl_orderkey = o_orderkey\nand l_commitdate < l_receiptdate\n)\ngroup by\no_orderpriority\norder by\no_orderpriority"
    val q4_2 = "select\no_orderpriority,\ncount(*) as order_count\nfrom\norders\nwhere\no_orderdate >= to_date ('1994-08-01')\nand o_orderdate < to_date( '1994-08-01') + interval 3 month\nand exists (\nselect\n*\nfrom\nlineitem\nwhere\nl_orderkey = o_orderkey\nand l_commitdate < l_receiptdate\n)\ngroup by\no_orderpriority\norder by\no_orderpriority"

    snc.sql(q4_1).collect()
    snc.sql(q4_2).collect()


    val q5_1 = "select\nn_name,\nsum(l_extendedprice * (1 - l_discount)) as revenue\nfrom\ncustomer,\norders,\nlineitem,\nsupplier,\nnation,\nregion\nwhere\nc_custkey = o_custkey\nand l_orderkey = o_orderkey\nand l_suppkey = s_suppkey\nand c_nationkey = s_nationkey\nand s_nationkey = n_nationkey\nand n_regionkey = r_regionkey\nand r_name = 'ASIA'\nand o_orderdate >= to_date( '1995-01-01')\nand o_orderdate < to_date( '1995-01-01') + interval 1 year\ngroup by\nn_name\norder by\nrevenue desc;"
    val q5_2 = "select\nn_name,\nsum(l_extendedprice * (1 - l_discount)) as revenue\nfrom\ncustomer,\norders,\nlineitem,\nsupplier,\nnation,\nregion\nwhere\nc_custkey = o_custkey\nand l_orderkey = o_orderkey\nand l_suppkey = s_suppkey\nand c_nationkey = s_nationkey\nand s_nationkey = n_nationkey\nand n_regionkey = r_regionkey\nand r_name = 'EUROPE'\nand o_orderdate >= to_date( '1996-01-01')\nand o_orderdate < to_date( '1996-01-01') + interval 1 year\ngroup by\nn_name\norder by\nrevenue desc;"
    snc.sql(q5_1).collect()
    snc.sql(q5_2).collect()

    val q6_1 = "select\nsum(l_extendedprice*l_discount) as revenue\nfrom\nlineitem\nwhere\nl_shipdate >= to_date ('1996-01-01')\nand l_shipdate < to_date ('1996-01-01') + interval 1 year\nand l_discount between 0.04 - 0.01 and 0.04 + 0.01\nand l_quantity < 24;"
    val q6_2 = "select\nsum(l_extendedprice*l_discount) as revenue\nfrom\nlineitem\nwhere\nl_shipdate >= to_date ('1994-01-01')\nand l_shipdate < to_date ('1994-01-01') + interval 1 year\nand l_discount between 0.06 - 0.01 and 0.06 + 0.01\nand l_quantity < 25;"

    snc.sql(q6_1).collect()
    snc.sql(q6_2).collect()

    val q7_1 = "select\nsupp_nation,\ncust_nation,\nl_year, sum(volume) as revenue\nfrom (\nselect\nn1.n_name as supp_nation,\nn2.n_name as cust_nation,\nyear( l_shipdate) as l_year,\nl_extendedprice * (1 - l_discount) as volume\nfrom\nsupplier,\nlineitem,\norders,\ncustomer,\nnation n1,\nnation n2\nwhere\ns_suppkey = l_suppkey\nand o_orderkey = l_orderkey\nand c_custkey = o_custkey\nand s_nationkey = n1.n_nationkey\nand c_nationkey = n2.n_nationkey\nand (\n(n1.n_name = 'INDIA' and n2.n_name = 'GERMANY')\nor (n1.n_name = 'GERMANY' and n2.n_name = 'INDIA')\n)\nand l_shipdate between to_date( '1995-01-01') and to_date('1996-12-31') \n) as shipping\ngroup by\nsupp_nation,\ncust_nation,\nl_year\norder by\nsupp_nation,\ncust_nation,\nl_year;"
    val q7_2 = "select\nsupp_nation,\ncust_nation,\nl_year, sum(volume) as revenue\nfrom (\nselect\nn1.n_name as supp_nation,\nn2.n_name as cust_nation,\nyear( l_shipdate) as l_year,\nl_extendedprice * (1 - l_discount) as volume\nfrom\nsupplier,\nlineitem,\norders,\ncustomer,\nnation n1,\nnation n2\nwhere\ns_suppkey = l_suppkey\nand o_orderkey = l_orderkey\nand c_custkey = o_custkey\nand s_nationkey = n1.n_nationkey\nand c_nationkey = n2.n_nationkey\nand (\n(n1.n_name = 'CANADA' and n2.n_name = 'FRANCE')\nor (n1.n_name = 'GERMANY' and n2.n_name = 'INDIA')\n)\nand l_shipdate between to_date( '1995-01-01') and to_date('1996-12-31') \n) as shipping\ngroup by\nsupp_nation,\ncust_nation,\nl_year\norder by\nsupp_nation,\ncust_nation,\nl_year;"

    snc.sql(q7_1).collect()
    snc.sql(q7_2).collect()


    val q8_1 = "select\no_year,\nsum(case\nwhen nation = 'GERMANY'\nthen volume\nelse 0\nend) / sum(volume) as mkt_share\nfrom (\nselect\nyear(o_orderdate) as o_year,\nl_extendedprice * (1-l_discount) as volume,\nn2.n_name as nation\nfrom\npart,\nsupplier,\nlineitem,\norders,\ncustomer,\nnation n1,\nnation n2,\nregion\nwhere\np_partkey = l_partkey\nand s_suppkey = l_suppkey\nand l_orderkey = o_orderkey\nand o_custkey = c_custkey\nand c_nationkey = n1.n_nationkey\nand n1.n_regionkey = r_regionkey\nand r_name = 'EUROPE'\nand s_nationkey = n2.n_nationkey\nand o_orderdate between to_date( '1995-01-01') and to_date( '1996-12-31') \nand p_type = 'ECONOMY ANODIZED STEEL'\n) as all_nations\ngroup by\no_year\norder by\no_year;"
    val q8_2 = "select\no_year,\nsum(case\nwhen nation = 'IRAN'\nthen volume\nelse 0\nend) / sum(volume) as mkt_share\nfrom (\nselect\nyear(o_orderdate) as o_year,\nl_extendedprice * (1-l_discount) as volume,\nn2.n_name as nation\nfrom\npart,\nsupplier,\nlineitem,\norders,\ncustomer,\nnation n1,\nnation n2,\nregion\nwhere\np_partkey = l_partkey\nand s_suppkey = l_suppkey\nand l_orderkey = o_orderkey\nand o_custkey = c_custkey\nand c_nationkey = n1.n_nationkey\nand n1.n_regionkey = r_regionkey\nand r_name = 'ASIA'\nand s_nationkey = n2.n_nationkey\nand o_orderdate between to_date( '1995-01-01') and to_date( '1996-12-31') \nand p_type = 'ECONOMY ANODIZED NICKEL'\n) as all_nations\ngroup by\no_year\norder by\no_year;"

    snc.sql(q8_1).collect()
    snc.sql(q8_2).collect()

    val q9_1 = "select\nnation,\no_year,\nsum(amount) as sum_profit\nfrom (\nselect\nn_name as nation,\nyear ( o_orderdate) as o_year,\nl_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount\nfrom\npart,\nsupplier,\nlineitem,\npartsupp,\norders,\nnation\nwhere\ns_suppkey = l_suppkey\nand ps_suppkey = l_suppkey\nand ps_partkey = l_partkey\nand p_partkey = l_partkey\nand o_orderkey = l_orderkey\nand s_nationkey = n_nationkey\nand p_name like '%green%'\n) as profit\ngroup by\nnation,\no_year\norder by\nnation,\no_year desc;"
    val q9_2 = "select\nnation,\no_year,\nsum(amount) as sum_profit\nfrom (\nselect\nn_name as nation,\nyear ( o_orderdate) as o_year,\nl_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount\nfrom\npart,\nsupplier,\nlineitem,\npartsupp,\norders,\nnation\nwhere\ns_suppkey = l_suppkey\nand ps_suppkey = l_suppkey\nand ps_partkey = l_partkey\nand p_partkey = l_partkey\nand o_orderkey = l_orderkey\nand s_nationkey = n_nationkey\nand p_name like '%red%'\n) as profit\ngroup by\nnation,\no_year\norder by\nnation,\no_year desc;"

    snc.sql(q9_1).collect()
    snc.sql(q9_2).collect()

    val q10_1 = "select\nc_custkey,\nc_name,\nsum(l_extendedprice * (1 - l_discount)) as revenue,\nc_acctbal,\nn_name,\nc_address,\nc_phone,\nc_comment\nfrom\ncustomer,\norders,\nlineitem,\nnation\nwhere\nc_custkey = o_custkey\nand l_orderkey = o_orderkey\nand o_orderdate >= to_date( '1993-07-01')\nand o_orderdate < to_date( '1993-07-01') + interval 3 month\nand l_returnflag = 'R'\nand c_nationkey = n_nationkey\ngroup by\nc_custkey,\nc_name,\nc_acctbal,\nc_phone,\nn_name,\nc_address,\nc_comment\norder by\nrevenue desc;"
    val q10_2 = "select\nc_custkey,\nc_name,\nsum(l_extendedprice * (1 - l_discount)) as revenue,\nc_acctbal,\nn_name,\nc_address,\nc_phone,\nc_comment\nfrom\ncustomer,\norders,\nlineitem,\nnation\nwhere\nc_custkey = o_custkey\nand l_orderkey = o_orderkey\nand o_orderdate >= to_date( '1994-06-01')\nand o_orderdate < to_date( '1994-06-01') + interval 3 month\nand l_returnflag = 'R'\nand c_nationkey = n_nationkey\ngroup by\nc_custkey,\nc_name,\nc_acctbal,\nc_phone,\nn_name,\nc_address,\nc_comment\norder by\nrevenue desc;"

    snc.sql(q10_1).collect()
    snc.sql(q10_2).collect()


    val q11_1 = "select\nps_partkey,\nsum(ps_supplycost * ps_availqty) as value\nfrom\npartsupp,\nsupplier,\nnation\nwhere\nps_suppkey = s_suppkey\nand s_nationkey = n_nationkey\nand n_name = 'GERMANY'\ngroup by\nps_partkey having\nsum(ps_supplycost * ps_availqty) > (\nselect\nsum(ps_supplycost * ps_availqty) * 0.0001\nfrom\npartsupp,\nsupplier,\nnation\nwhere\nps_suppkey = s_suppkey\nand s_nationkey = n_nationkey\nand n_name = 'GERMANY'\n)\norder by\nvalue desc;"
    val q11_2 =  "select\nps_partkey,\nsum(ps_supplycost * ps_availqty) as value\nfrom\npartsupp,\nsupplier,\nnation\nwhere\nps_suppkey = s_suppkey\nand s_nationkey = n_nationkey\nand n_name = 'INDIA'\ngroup by\nps_partkey having\nsum(ps_supplycost * ps_availqty) > (\nselect\nsum(ps_supplycost * ps_availqty) * 0.0005\nfrom\npartsupp,\nsupplier,\nnation\nwhere\nps_suppkey = s_suppkey\nand s_nationkey = n_nationkey\nand n_name = 'INDIA'\n)\norder by\nvalue desc;"

    snc.sql(q11_1).collect()
    snc.sql(q11_2).collect()


    val q12_1 = "select\nl_shipmode,\nsum(case\nwhen o_orderpriority ='1-URGENT'\nor o_orderpriority ='2-HIGH'\nthen 1\nelse 0\nend) as high_line_count,\nsum(case\nwhen o_orderpriority <> '1-URGENT'\nand o_orderpriority <> '2-HIGH'\nthen 1\nelse 0\nend) as low_line_count\nfrom\norders,\nlineitem\nwhere\no_orderkey = l_orderkey\nand l_shipmode in ('AIR', 'RAIL')\nand l_commitdate < l_receiptdate\nand l_shipdate < l_commitdate\nand l_receiptdate >= to_date ('1994-01-01')\nand l_receiptdate < to_date ('1994-01-01') + interval 1 year\ngroup by\nl_shipmode\norder by\nl_shipmode;"
    val q12_2 = "select\nl_shipmode,\nsum(case\nwhen o_orderpriority ='1-URGENT'\nor o_orderpriority ='2-HIGH'\nthen 1\nelse 0\nend) as high_line_count,\nsum(case\nwhen o_orderpriority <> '1-URGENT'\nand o_orderpriority <> '2-HIGH'\nthen 1\nelse 0\nend) as low_line_count\nfrom\norders,\nlineitem\nwhere\no_orderkey = l_orderkey\nand l_shipmode in ('ROAD', 'SHIP')\nand l_commitdate < l_receiptdate\nand l_shipdate < l_commitdate\nand l_receiptdate >= to_date ('1995-01-01')\nand l_receiptdate < to_date ('1995-01-01') + interval 1 year\ngroup by\nl_shipmode\norder by\nl_shipmode;"


    snc.sql(q12_1).collect()
    snc.sql(q12_2).collect()

    val q13_1 = "select\nc_count, count(*) as custdist\nfrom (\nselect\nc_custkey,\ncount(o_orderkey) c_count \nfrom\ncustomer left outer join orders on\nc_custkey = o_custkey\nand o_comment not like '%special%packages%'\ngroup by\nc_custkey\n)as c_orders \ngroup by\nc_count\norder by\ncustdist desc,\nc_count desc"
    val q13_2 = "select\nc_count, count(*) as custdist\nfrom (\nselect\nc_custkey,\ncount(o_orderkey) c_count \nfrom\ncustomer left outer join orders on\nc_custkey = o_custkey\nand o_comment not like '%pending%accounts%'\ngroup by\nc_custkey\n)as c_orders \ngroup by\nc_count\norder by\ncustdist desc,\nc_count desc"


    snc.sql(q13_1).collect()
    snc.sql(q13_2).collect()

    val q14_1 = "select\n100.00 * sum(case\nwhen p_type like 'PROMO%'\nthen l_extendedprice*(1-l_discount)\nelse 0\nend) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue\nfrom\nlineitem,\npart\nwhere\nl_partkey = p_partkey\nand l_shipdate >= to_date('1993-07-01')\nand l_shipdate < to_date('1993-07-01') + interval 1 month;"
    val q14_2 = "select\n100.00 * sum(case\nwhen p_type like 'PROMO%'\nthen l_extendedprice*(1-l_discount)\nelse 0\nend) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue\nfrom\nlineitem,\npart\nwhere\nl_partkey = p_partkey\nand l_shipdate >= to_date('1994-11-01')\nand l_shipdate < to_date('1994-11-01') + interval 1 month;"


    snc.sql(q14_1).collect()
    snc.sql(q14_2).collect()

    val q15_view1 = "create view revenue1 (supplier_no, total_revenue) as\nselect\nl_suppkey,\nsum(l_extendedprice * (1 - l_discount))\nfrom\nlineitem\nwhere\nl_shipdate >= to_date ('1994-07-01')\nand l_shipdate < to_date ('1994-07-01') + interval 3 month\ngroup by\nl_suppkey;"
    val q15_1 = "select\ns_suppkey,\ns_name,\ns_address,\ns_phone,\ntotal_revenue\nfrom\nsupplier,\nrevenue1\nwhere\ns_suppkey = supplier_no\nand total_revenue = (\nselect\nmax(total_revenue)\nfrom\nrevenue1\n)\norder by\ns_suppkey;"

    val q15_view2 = "create view revenue2 (supplier_no, total_revenue) as\nselect\nl_suppkey,\nsum(l_extendedprice * (1 - l_discount))\nfrom\nlineitem\nwhere\nl_shipdate >= to_date ('1995-06-01')\nand l_shipdate < to_date ('1995-06-01') + interval 3 month\ngroup by\nl_suppkey;"

    val q15_2 = "select\ns_suppkey,\ns_name,\ns_address,\ns_phone,\ntotal_revenue\nfrom\nsupplier,\nrevenue2\nwhere\ns_suppkey = supplier_no\nand total_revenue = (\nselect\nmax(total_revenue)\nfrom\nrevenue2\n)\norder by\ns_suppkey;"

    snc.sql(q15_view1)
    snc.sql(q15_view2)
    snc.sql(q15_1).collect()
    snc.sql(q15_2).collect()
    snc.sql("drop view revenue1")
    snc.sql("drop view revenue2")


    val q16_1 = "select\np_brand,\np_type,\np_size,\ncount(distinct ps_suppkey) as supplier_cnt\nfrom\npartsupp,\npart\nwhere\np_partkey = ps_partkey\nand p_brand <> 'BRAND#13'\nand p_type not like 'STANDARD PLATED%'\nand p_size in (1, 7, 3, 21, 32, 41, 11, 6)\nand ps_suppkey not in (\nselect\ns_suppkey\nfrom\nsupplier\nwhere\ns_comment like '%Customer%Complaints%'\n)\ngroup by\np_brand,\np_type,\np_size\norder by\nsupplier_cnt desc,\np_brand,\np_type,\np_size;"
    val q16_2 = "select\np_brand,\np_type,\np_size,\ncount(distinct ps_suppkey) as supplier_cnt\nfrom\npartsupp,\npart\nwhere\np_partkey = ps_partkey\nand p_brand <> 'BRAND#24'\nand p_type not like 'LARGE POLISHED%'\nand p_size in (5, 9, 19, 13, 27, 17, 43, 49)\nand ps_suppkey not in (\nselect\ns_suppkey\nfrom\nsupplier\nwhere\ns_comment like '%Customer%Complaints%'\n)\ngroup by\np_brand,\np_type,\np_size\norder by\nsupplier_cnt desc,\np_brand,\np_type,\np_size;"

    snc.sql(q16_1).collect()
    snc.sql(q16_2).collect()


    val q17_1 = "select\nsum(l_extendedprice) / 7.0 as avg_yearly\nfrom\nlineitem,\npart\nwhere\np_partkey = l_partkey\nand p_brand = 'BRAND#14'\nand p_container = 'SM BOX'\nand l_quantity < (\nselect\n0.2 * avg(l_quantity)\nfrom\nlineitem\nwhere\nl_partkey = p_partkey\n);"
    val q17_2 = "select\nsum(l_extendedprice) / 7.0 as avg_yearly\nfrom\nlineitem,\npart\nwhere\np_partkey = l_partkey\nand p_brand = 'BRAND#33'\nand p_container = 'MED JAR'\nand l_quantity < (\nselect\n0.2 * avg(l_quantity)\nfrom\nlineitem\nwhere\nl_partkey = p_partkey\n);"

    snc.sql(q17_1).collect()
    snc.sql(q17_2).collect()

    val q18_1 = "select\nc_name,\nc_custkey,\no_orderkey,\no_orderdate,\no_totalprice,\nsum(l_quantity)\nfrom\ncustomer,\norders,\nlineitem\nwhere\no_orderkey in (\nselect\nl_orderkey\nfrom\nlineitem\ngroup by\nl_orderkey having\nsum(l_quantity) > 313\n)\nand c_custkey = o_custkey\nand o_orderkey = l_orderkey\ngroup by\nc_name,\nc_custkey,\no_orderkey,\no_orderdate,\no_totalprice\norder by\no_totalprice desc,\no_orderdate;"
    val q18_2 = "select\nc_name,\nc_custkey,\no_orderkey,\no_orderdate,\no_totalprice,\nsum(l_quantity)\nfrom\ncustomer,\norders,\nlineitem\nwhere\no_orderkey in (\nselect\nl_orderkey\nfrom\nlineitem\ngroup by\nl_orderkey having\nsum(l_quantity) > 315\n)\nand c_custkey = o_custkey\nand o_orderkey = l_orderkey\ngroup by\nc_name,\nc_custkey,\no_orderkey,\no_orderdate,\no_totalprice\norder by\no_totalprice desc,\no_orderdate;"


    snc.sql(q18_1).collect()
    snc.sql(q18_2).collect()

    val q19_1 = "select\nsum(l_extendedprice * (1 - l_discount) ) as revenue\nfrom\nlineitem,\npart\nwhere\n(\np_partkey = l_partkey\nand p_brand = 'MN#13'\nand p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\nand l_quantity >= 2 and l_quantity <= 2 + 10\nand p_size between 1 and 5\nand l_shipmode in ('AIR', 'AIR REG')\nand l_shipinstruct = 'DELIVER IN PERSON'\n)\nor\n(\np_partkey = l_partkey\nand p_brand = 'Brand#34'\nand p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\nand l_quantity >= 13 and l_quantity <= 13 + 10\nand p_size between 1 and 10\nand l_shipmode in ('AIR', 'AIR REG')\nand l_shipinstruct = 'DELIVER IN PERSON'\n)\nor\n(\np_partkey = l_partkey\nand p_brand = 'Brand#45'\nand p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\nand l_quantity >= 26 and l_quantity <= 26 + 10\nand p_size between 1 and 15\nand l_shipmode in ('AIR', 'AIR REG')\nand l_shipinstruct = 'DELIVER IN PERSON'\n);"
    val q19_2 = "select\nsum(l_extendedprice * (1 - l_discount) ) as revenue\nfrom\nlineitem,\npart\nwhere\n(\np_partkey = l_partkey\nand p_brand = 'MN#15'\nand p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\nand l_quantity >= 7 and l_quantity <= 7 + 10\nand p_size between 1 and 5\nand l_shipmode in ('AIR', 'AIR REG')\nand l_shipinstruct = 'DELIVER IN PERSON'\n)\nor\n(\np_partkey = l_partkey\nand p_brand = 'Brand#51'\nand p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\nand l_quantity >= 18 and l_quantity <= 18 + 10\nand p_size between 1 and 10\nand l_shipmode in ('AIR', 'AIR REG')\nand l_shipinstruct = 'DELIVER IN PERSON'\n)\nor\n(\np_partkey = l_partkey\nand p_brand = 'Brand#43'\nand p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\nand l_quantity >= 22 and l_quantity <= 22 + 10\nand p_size between 1 and 15\nand l_shipmode in ('AIR', 'AIR REG')\nand l_shipinstruct = 'DELIVER IN PERSON'\n);"

    snc.sql(q19_1).collect()
    snc.sql(q19_2).collect()


   //suspect

    val q20_1 = "select\ns_name,\ns_address\nfrom\nsupplier, nation\nwhere\ns_suppkey in (\nselect\nps_suppkey\nfrom\npartsupp\nwhere\nps_partkey in (\nselect\np_partkey\nfrom\npart\nwhere\np_name like 'forest%'\n)\nand ps_availqty > (\nselect\n0.5 * sum(l_quantity)\nfrom\nlineitem\nwhere\nl_partkey = ps_partkey\nand l_suppkey = ps_suppkey\nand l_shipdate >= to_date('1994-01-01')\nand l_shipdate < to_date('1994-01-01') + interval 1 year\n)\n)\nand s_nationkey = n_nationkey\nand n_name = 'CANADA'\norder by\ns_name"
    val q20_2 = "select\ns_name,\ns_address\nfrom\nsupplier, nation\nwhere\ns_suppkey in (\nselect\nps_suppkey\nfrom\npartsupp\nwhere\nps_partkey in (\nselect\np_partkey\nfrom\npart\nwhere\np_name like 'almond%'\n)\nand ps_availqty > (\nselect\n0.5 * sum(l_quantity)\nfrom\nlineitem\nwhere\nl_partkey = ps_partkey\nand l_suppkey = ps_suppkey\nand l_shipdate >= to_date('1996-01-01')\nand l_shipdate < to_date('1996-01-01') + interval 1 year\n)\n)\nand s_nationkey = n_nationkey\nand n_name = 'INDIA'\norder by\ns_name"

    snc.sql(q20_1).collect()
    snc.sql(q20_2).collect()

    val q21_1 = "select\ns_name,\ncount(*) as numwait\nfrom\nsupplier,\nlineitem l1,\norders,\nnation\nwhere\ns_suppkey = l1.l_suppkey\nand o_orderkey = l1.l_orderkey\nand o_orderstatus = 'F'\nand l1.l_receiptdate > l1.l_commitdate\nand exists (\nselect\n*\nfrom\nlineitem l2\nwhere\nl2.l_orderkey = l1.l_orderkey\nand l2.l_suppkey <> l1.l_suppkey\n)\nand not exists (\nselect\n*\nfrom\nlineitem l3\nwhere\nl3.l_orderkey = l1.l_orderkey\nand l3.l_suppkey <> l1.l_suppkey\nand l3.l_receiptdate > l3.l_commitdate\n)\nand s_nationkey = n_nationkey\nand n_name = 'IRAN'\ngroup by\ns_name\norder by\nnumwait desc,\ns_name;"
    val q21_2 = "select\ns_name,\ncount(*) as numwait\nfrom\nsupplier,\nlineitem l1,\norders,\nnation\nwhere\ns_suppkey = l1.l_suppkey\nand o_orderkey = l1.l_orderkey\nand o_orderstatus = 'F'\nand l1.l_receiptdate > l1.l_commitdate\nand exists (\nselect\n*\nfrom\nlineitem l2\nwhere\nl2.l_orderkey = l1.l_orderkey\nand l2.l_suppkey <> l1.l_suppkey\n)\nand not exists (\nselect\n*\nfrom\nlineitem l3\nwhere\nl3.l_orderkey = l1.l_orderkey\nand l3.l_suppkey <> l1.l_suppkey\nand l3.l_receiptdate > l3.l_commitdate\n)\nand s_nationkey = n_nationkey\nand n_name = 'GERMANY'\ngroup by\ns_name\norder by\nnumwait desc,\ns_name;"

    snc.sql(q21_1).collect()
    snc.sql(q21_2).collect()


    val q22_1 = "select\ncntrycode,\ncount(*) as numcust,\nsum(c_acctbal) as totacctbal\nfrom (\nselect\nsubstring(c_phone , 1 , 2) as cntrycode,\nc_acctbal\nfrom\ncustomer\nwhere\nsubstring(c_phone , 1 , 2) in\n('13','23','45','11','17','19','25')\nand c_acctbal > (\nselect\navg(c_acctbal)\nfrom\ncustomer\nwhere\nc_acctbal > 0.00\nand substring (c_phone , 1 , 2) in\n('13','23','45','11','17','19','25')\n)\nand not exists (\nselect\n*\nfrom\norders\nwhere\no_custkey = c_custkey\n)\n) as custsale\ngroup by\ncntrycode\norder by\ncntrycode;"
    val q22_2 = "select\ncntrycode,\ncount(*) as numcust,\nsum(c_acctbal) as totacctbal\nfrom (\nselect\nsubstring(c_phone , 1 , 2) as cntrycode,\nc_acctbal\nfrom\ncustomer\nwhere\nsubstring(c_phone , 1 , 2) in\n('12','28','47','18','33','20','30')\nand c_acctbal > (\nselect\navg(c_acctbal)\nfrom\ncustomer\nwhere\nc_acctbal > 0.00\nand substring (c_phone , 1 , 2) in\n('12','28','47','18','33','20','30')\n)\nand not exists (\nselect\n*\nfrom\norders\nwhere\no_custkey = c_custkey\n)\n) as custsale\ngroup by\ncntrycode\norder by\ncntrycode;"

    snc.sql(q22_1).collect()
    snc.sql(q22_2).collect()*/

  }


  def getSqlConnection: Connection =
    DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort2")

  def getNumCodeGenTrees(plan: SparkPlan): Int = {
    var numcodegenSubtrees = 0
    plan transform {
      case s: WholeStageCodegenExec =>
        numcodegenSubtrees += 1
        s
      case s => s
    }
    numcodegenSubtrees
  }

}
