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
package org.apache.spark.sql.store

import java.math.BigDecimal
import java.sql.{Connection, Date, DriverManager, SQLException, SQLType, Timestamp, Types}
import java.util.Random

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

    /**
     * Pls do not change the flag values of Property.TestDisableCodeGenFlag.name
     * and Property.UseOptimizedHashAggregateForSingleKey.name
     * They are meant to suppress CodegenFallback Plan so that optimized
     * byte buffer code path is tested & prevented from false passing.
     * If your test needs CodegenFallback, then override the newConf function
     * & clear the flag from the conf of the test locally.
     */
    System.setProperty("spark.testing", "true")
    super.newSparkConf((conf: SparkConf) => {
      conf.set("spark.sql.codegen.maxFields", "110")
      .set("spark.sql.codegen.fallback", "false")
      .set("snappydata.sql.useOptimizedHashAggregateForSingleKey", "true")
      .set(Property.TestDisableCodeGenFlag.name , "true")
      conf
    })
  }

  override def afterAll(): Unit = {
    TestUtil.stopNetServer()
    super.afterAll()
    System.clearProperty("spark.testing")
  }

  test("single key string group by column with aggregagte function using grouping key") {
    val snc1 = snc.newSession()
    snc1.setConf("snappydata.sql.useOptimizedHashAggregateForSingleKey", "true")
    snc1.sql("drop table if exists test1")
    snc1.sql("create table test1 (col1 int, col2 int, col3 int, col4 String) " +
      "using column ")
    val conn = getSqlConnection
    val st = conn.createStatement()
    st.execute("insert into test1 values (1,1,1,'asif1')")
    st.execute("insert into test1 values (2,2,2,'asif1')")
    val rs = snc1.sql("select col4, sum(length(col4)) as summ1 " +
      " from test1 group by col4")
    val results = rs.collect()
    assertEquals(1, results.size)
    assertEquals(10, results(0).getLong(1))
    assertEquals("asif1", results(0).getString(0))
    snc.dropTable("test1")
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
      for (j <- 0 until range) {
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

  test("BigDecimal bug SNAP-3047") {
    val rand = new Random(System.currentTimeMillis())

    def getPrice(): BigDecimal = new BigDecimal(java.lang.Double.toString(
      (rand.nextInt(10000) + 1) * .01))

    snc
    snc.sql("drop table if exists securities")
    snc.sql("create table securities(sec_id int not null, symbol varchar(10) not null, " +
      "price decimal (30, 20))")
    val conn = getSqlConnection
    val insertPs = conn.prepareStatement("insert into securities values (?, ?, ?)")
    for (i <- 0 until 5000) {
      insertPs.setInt(1, i)
      insertPs.setString(2, s"symbol_${i % 10}")
      insertPs.setBigDecimal(3, getPrice())
      insertPs.addBatch()
    }
    insertPs.executeBatch()

    val snc2 = snc.newSession()
    snc2.setConf("snappydata.sql.optimizedHashAggregate", "false")

    val query1 = "select cast(avg( distinct price) as decimal (30, 20)) as " +
      "avg_distinct_price from securities "

    var rs1 = snc.sql(query1).collect()(0)
    var rs2 = snc2.sql(query1).collect()(0)
    assertTrue(rs1.getDecimal(0).equals(rs2.getDecimal(0)))

    val query2 = "select count( distinct price) from securities "
    rs1 = snc.sql(query2).collect()(0)
    rs2 = snc2.sql(query2).collect()(0)
    assertEquals(rs1.getLong(0), rs2.getLong(0))
    snc.dropTable("securities")
  }

  test("SNAP-3077 test list of overflow BBMaps if capacity is reached") {
    snc
    snc.sql("drop table if exists test1")
    snc.sql("create table test1 (col1 int, col2 binary) using column ")
    val range: Byte = 100
    val byteArraySize = 512
    val numRowDuplication = 10
    val conn = getSqlConnection
    val insertPs = conn.prepareStatement("insert into test1 values (?,?)")
    for( i <- 1 to range) {
      val bytes = Array.ofDim[Byte](byteArraySize)
      bytes(i) = i.toByte
      for (j <- 0 until numRowDuplication) {
        insertPs.setInt(1, i)
        insertPs.setBytes(2, bytes)
        insertPs.addBatch()
      }
    }
    insertPs.executeBatch()
    val snc1 = snc.newSession()
    snc1.setConf("snappydata.sql.approxMaxCapacityOfBBMap", "4096")
    snc1.setConf("snappydata.sql.initialCapacityOfSHABBMap", "4")
    val rs = snc1.sql("select col2, sum(col1) from test1 group by col2").collect
    assertEquals(100, rs.length)
    rs.foreach(row => {
      val byteArr = row.getAs[Array[Byte]](0)
      val sum = row.getLong(1)
      val key = byteArr.find(_ > 0).get
      assertEquals(10 * key, sum)
    })
    snc.dropTable("test1")
  }

  test("SNAP-2567. Code size exceeds limit") {
    snc
    val numStrCols = 450
    val stringFieldsStr = (for (i <- 0 until numStrCols) yield {
      s"string_$i string"
    }).mkString(",")

    val numIntCols = 5

    val intFieldsStr = (for (i <- 0 until numIntCols) yield {
      s"int_$i int"
    }).mkString(",")

    snc.sql(s"create table test ($stringFieldsStr, $intFieldsStr) using column ")
    val prepStr = (for (i <- 0 until (numIntCols + numStrCols)) yield {
      "?"
    }).mkString(",")
    val conn = getSqlConnection
    val prepStmt = conn.prepareStatement(s"insert into test values ($prepStr)")
    for (i <- 0 until 100) {
      for (j <- 0 until numStrCols) {
        prepStmt.setString(j + 1, s"str_${i % 5}")
      }
      for (j <- 0 until numIntCols) {
        prepStmt.setInt(j + numStrCols + 1, i * j )
      }
      prepStmt.addBatch()
    }
    prepStmt.executeBatch()
    assertEquals(100, snc.sql("select count(*) from test").collect()(0).getLong(0))
    val groupByClause = (for (i <- 0 until numStrCols) yield {
      s"string_$i"
    }).mkString(",")
    val rs = snc.sql(s"select count(*), sum(int_0), sum(int_1)," +
      s" sum(int_2), sum(int_3), sum(int_4) from test group by $groupByClause")
    val rows = rs.collect()
  }

  test("test code splitting") {
    val snc1 = snc.newSession()
    snc1.setConf(Property.TestCodeSplitGroupSizeInSHA.name, "2")


    val fieldsStr = (for (i <- 0 until 5) yield {
      Array(s"string1_$i string", s"int_$i int", s"double_$i double", s"long_$i long",
        s"string2_$i string" ).mkString(",")
    }).mkString(",")

    snc1.sql(s"create table test ($fieldsStr) using column ")

    val prepStr = (for (i <- 0 until 25) yield {
      "?"
    }).mkString(",")

    val conn = getSqlConnection
    val prepStmt = conn.prepareStatement(s"insert into test values ($prepStr)")
    val distincts = scala.collection.mutable.Set[(String, Int, Double, Long, String)]()
    var j = 1
    for ( k <- 0 until 100) {
      for (i <- 0 until 5) {
        if (k % 10 == 0) {
          prepStmt.setNull(j, Types.VARCHAR)
        } else {
          prepStmt.setString(j, s"val_${k % 3}")
        }
        j = j + 1
        prepStmt.setInt(j, k % 3)

        j = j + 1
        prepStmt.setDouble(j, (k % 3) * .01d)

        j = j + 1
        prepStmt.setLong(j, k % 3)

        j = j + 1
        prepStmt.setString(j, s"val_${k % 3}")
        j = j + 1
        if (k % 10 == 0) {
          distincts.add((null, k % 3, (k % 3) * .01d, k % 3, s"val_${k % 3}"))
        } else {
          distincts.add((s"val_${k % 3}", k % 3, (k % 3) * .01d, k % 3, s"val_${k % 3}"))
        }
      }
      prepStmt.addBatch()
      j = 1
    }
    prepStmt.executeBatch()
    assertEquals(100, snc.sql("select count(*) from test").collect()(0).getLong(0))
    val groupBy = (for (i <- 0 until 5) yield {
      Array(s"string1_$i", s"int_$i", s"double_$i", s"long_$i", s"string2_$i" ).mkString(",")
    }).mkString(",")

    val rs = snc1.sql(s"select count(*) from test group by $groupBy")
    val rows = rs.collect()
    assertEquals(distincts.size, rows.length)
  }

  test("SNAP-3132") {
    snc
    snc.sql("drop table if exists test1")
    snc.sql("create table test1 (col1 int, col2 int, col3 Decimal(35,5), name string) using column")

    val conn = getSqlConnection
    val ps1 = conn.prepareStatement("insert into test1 values (?,?, ?, ?)")
    for (i <- 0 until 500) {
      ps1.setInt(1, i % 5)
      ps1.setInt(2, i % 10)
      val bd = new BigDecimal(17456567.576d * i)
      ps1.setBigDecimal(3, bd)
      ps1.setString(4, (i % 10).toString)
      ps1.addBatch
    }
    ps1.executeBatch
    snc.sql(s" select name, col3, sum(col2) from test1" +
      s" group by name, col3").collect
    snc.dropTable("test1")
  }

  ignore("SNAP-3077 test if default max capacity nearing Integer.MAX_VALUE is reached." +
    " Disabled due to heap requirements being more than 16G") {
    snc
    snc.sql("drop table if exists test1")
    snc.sql("create table test1 (col1 int, col2 binary) using column ")

    val conn = getSqlConnection
    val insertPs = conn.prepareStatement("insert into test1 values (?,?)")


    for (j <- 0 until 8) {
        insertPs.setInt(1, j)
        val bytes = Array.ofDim[Byte](Integer.MAX_VALUE/4 - 8)
        bytes(j) = 1
        insertPs.setBytes(2, bytes)
        insertPs.addBatch()
    }
    insertPs.executeBatch()
    val snc1 = snc.newSession()
    snc1.setConf("snappydata.sql.initialCapacityOfSHABBMap", "4")
    val rs = snc1.sql("select col2, sum(col1) from test1 group by col2").collect
    assertEquals(8, rs.length)
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
