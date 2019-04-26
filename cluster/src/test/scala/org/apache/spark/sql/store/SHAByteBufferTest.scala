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

    super.newSparkConf((conf: SparkConf) => {
      conf.set("spark.sql.codegen.maxFields", "110")
      conf
    })
  }

  override def afterAll(): Unit = {
    TestUtil.stopNetServer()
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
      s"id % $groupingDivisor ")
    insertDF.write.insertInto("test1")
    val rs = snc.sql("select col4, sum(col1) as summ1, sum(col2) as summ2 " +
      " from test1 group by col4")
    val results = rs.collect()
    assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    assertEquals(groupingDivisor, results.length)
    rs.foreach(row => {
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
    assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    assertEquals(groupingDivisor, results.length)
    rs.foreach(row => {
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
    assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    assertEquals(groupingDivisor, results.length)
    rs.foreach(row => {
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
    assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
    assertEquals(groupingDivisor1 * groupingDivisor2, results.length)
    rs.foreach(row => {
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
      assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
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
      assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
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
      assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
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
    assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
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
    assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
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
    assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
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
    assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
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
    assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
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
    assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
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
    assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
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
    assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
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
    assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
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
    assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
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
    assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
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
    assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
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
    expectedResult = mutable.Map(0 -> 10L,.7f -> 35L, "null" -> 60L)
    rs = snc.sql(q)
    assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
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
    expectedResult = mutable.Map(0 -> 10L,.7D -> 35L, "null" -> 60L)
    rs = snc.sql(q)
    assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
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
    assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
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
    assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
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
    assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
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
      assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
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
      assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))

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
      assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
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
      assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
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
    assertEquals(2, getNumCodeGenTrees(rs.queryExecution.executedPlan))
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
