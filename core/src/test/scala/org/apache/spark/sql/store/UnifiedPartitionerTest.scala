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

import java.math.BigDecimal
import java.sql.DriverManager

import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.cache.Region
import com.pivotal.gemfirexd.TestUtil
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionByExpressionResolver
import com.pivotal.gemfirexd.internal.iapi.types._
import io.snappydata.{Property, SnappyFunSuite}
import io.snappydata.core.{Data1, Data4, TestData2}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.Logging
import org.apache.spark.sql.{ColumnName, SnappyContext}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Murmur3Hash}
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, _}
import org.apache.spark.unsafe.types.UTF8String


/**
 * This test checks the validity of various functionality when we use Spark's
 * partitioner logic for underlying GemXD storage.
 */
class UnifiedPartitionerTest extends SnappyFunSuite
    with Logging
    with BeforeAndAfter
    with BeforeAndAfterAll {

  after {
    snc.dropTable(RowTableName2, ifExists = true)
    snc.dropTable(RowTableName1, ifExists = true)
    snc.dropTable(ColumnTableName2, ifExists = true)
    snc.dropTable(ColumnTableName1, ifExists = true)
    snc.dropTable("ColumnTable1Temp", ifExists = true)
  }

  val ColumnTableName1: String = "ColumnTable1"
  val ColumnTableName2: String = "ColumnTable2"
  val RowTableName1: String = "RowTable1"
  val RowTableName2: String = "RowTable2"

  val props = Map.empty[String, String]


  val options = "OPTIONS (PARTITION_BY 'col1')"

  val optionsWithURL = "OPTIONS (PARTITION_BY 'Col1', URL 'jdbc:snappydata:;')"

  val joinSuite = new SnappyJoinSuite

  private def pmod(a: Int, n: Int): Int = {
    // We should push this logic to store layer
    val r = a % n
    if (r < 0) r + n else r
  }

  private def createDate(year: Int, month: Int, date: Int): java.sql.Date = {
    // noinspection ScalaDeprecation
    new java.sql.Date(year, month, date)
  }

  private def createRow(value: Any, dt: DataType): Expression = {
    new Murmur3Hash(Seq(Literal.create(value, dt)))
  }

  private def createRow(values: Array[AnyRef],
      types: Array[DataType]): Expression = {
    new Murmur3Hash(values.zip(types).map(p => Literal.create(p._1, p._2)))
  }

  test("Test hash codes for all Sql types") {
    snc.sql(s"DROP TABLE IF EXISTS $ColumnTableName1")
    snc.sql(s"DROP TABLE IF EXISTS $ColumnTableName2")
    snc.sql(s"CREATE TABLE $ColumnTableName1(" +
        "OrderId INT, ItemId INT, ItemRef INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        "PERSISTENT 'ASYNC')")
    snc.sql(s"CREATE TABLE $ColumnTableName2(" +
        "OrderId INT, ItemId INT, ItemRef INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId,ItemId'," +
        "PERSISTENT 'ASYNC')")

    val reg1: Region[_, _] = Misc.getRegion("/APP/COLUMNTABLE1", true, true)
    val reg2: Region[_, _] = Misc.getRegion("/APP/COLUMNTABLE2", true, true)
    val rpr = reg1.getAttributes.getPartitionAttributes.getPartitionResolver
        .asInstanceOf[GfxdPartitionByExpressionResolver]
    val rpr2 = reg2.getAttributes.getPartitionAttributes.getPartitionResolver
        .asInstanceOf[GfxdPartitionByExpressionResolver]
    assert(rpr != null)
    assert(rpr2 != null)

    val numPartitions = ExternalStoreUtils.defaultTableBuckets.toInt

    // Check All Datatypes
    var row = createRow(200, IntegerType)
    var dvd: DataValueDescriptor = new SQLInteger(200)
    val dvds = new ArrayBuffer[(DataValueDescriptor, DataType)]()

    assert(rpr.getRoutingKeyForColumn(dvd) === pmod(
      row.eval().asInstanceOf[Int], numPartitions))

    row = createRow(200000, IntegerType)
    dvd = new SQLInteger(200000)
    dvds += (dvd -> IntegerType)
    assert(rpr.getRoutingKeyForColumn(dvd) === pmod(
      row.eval().asInstanceOf[Int], numPartitions))

    row = createRow(true, BooleanType)
    dvd = new SQLBoolean(true)
    dvds += (dvd -> BooleanType)
    assert(rpr.getRoutingKeyForColumn(dvd) === pmod(
      row.eval().asInstanceOf[Int], numPartitions))

    row = createRow(createDate(1, 1, 2011), DateType)
    dvd = new SQLDate(createDate(1, 1, 2011))
    dvds += (dvd -> DateType)
    assert(rpr.getRoutingKeyForColumn(dvd) === pmod(
      row.eval().asInstanceOf[Int], numPartitions))

    val ipaddr: Array[Byte] = Array(192.toByte, 168.toByte, 1.toByte, 9.toByte)
    row = createRow(ipaddr, BinaryType)
    dvd = new SQLBit(ipaddr)
    dvds += (dvd -> BinaryType)
    assert(rpr.getRoutingKeyForColumn(dvd) === pmod(
      row.eval().asInstanceOf[Int], numPartitions))

    row = createRow(10.5F, FloatType)
    dvd = new SQLReal(10.5F)
    dvds += (dvd -> FloatType)
    assert(rpr.getRoutingKeyForColumn(dvd) === pmod(
      row.eval().asInstanceOf[Int], numPartitions))

    row = createRow(479L, LongType)
    dvd = new SQLLongint(479L)
    dvds += (dvd -> LongType)
    assert(rpr.getRoutingKeyForColumn(dvd) === pmod(
      row.eval().asInstanceOf[Int], numPartitions))

    row = createRow(UTF8String.fromString("abcd"), StringType) // As
    dvd = new SQLVarchar("abcd")
    dvds += (dvd -> StringType)
    // catalyst converts String to UtfString
    assert(rpr.getRoutingKeyForColumn(dvd) === pmod(
      row.eval().asInstanceOf[Int], numPartitions))

    row = createRow(UTF8String.fromString("abcdx"), StringType) // As
    dvd = new SQLClob("abcdx")
    dvds += (dvd -> StringType)
    // catalyst converts String to UtfString
    assert(rpr.getRoutingKeyForColumn(dvd) === pmod(
      row.eval().asInstanceOf[Int], numPartitions))

    row = createRow(UTF8String.fromString("abcdef"), StringType) // As
    dvd = new SQLVarchar("abcdef")
    dvds += (dvd -> StringType)
    // catalyst converts String to UtfString
    assert(rpr.getRoutingKeyForColumn(dvd) === pmod(
      row.eval().asInstanceOf[Int], numPartitions))

    row = createRow(UTF8String.fromString("abcdefg"), StringType) // As
    dvd = new SQLClob("abcdefg")
    dvds += (dvd -> StringType)
    // catalyst converts String to UtfString
    assert(rpr.getRoutingKeyForColumn(dvd) === pmod(
      row.eval().asInstanceOf[Int], numPartitions))

    row = createRow(UTF8String.fromString("abcdefgh"), StringType) // As
    dvd = new SQLVarchar("abcdefgh")
    dvds += (dvd -> StringType)
    // catalyst converts String to UtfString
    assert(rpr.getRoutingKeyForColumn(dvd) === pmod(
      row.eval().asInstanceOf[Int], numPartitions))

    row = createRow(UTF8String.fromString("abcdefghx"), StringType) // As
    dvd = new SQLClob("abcdefghx")
    dvds += (dvd -> StringType)
    // catalyst converts String to UtfString
    assert(rpr.getRoutingKeyForColumn(dvd) === pmod(
      row.eval().asInstanceOf[Int], numPartitions))

    row = createRow(5, ShortType)
    dvd = new SQLSmallint(5)
    dvds += (dvd -> ShortType)
    assert(rpr.getRoutingKeyForColumn(dvd) === pmod(
      row.eval().asInstanceOf[Int], numPartitions))

    row = createRow(2, ByteType)
    dvd = new SQLTinyint(2)
    dvds += (dvd -> ByteType)
    assert(rpr.getRoutingKeyForColumn(dvd) === pmod(
      row.eval().asInstanceOf[Int], numPartitions))

    row = createRow(new BigDecimal(32000.05f), DecimalType.SYSTEM_DEFAULT)
    dvd = new SQLDecimal(new BigDecimal(32000.05f))
    dvds += (dvd -> DecimalType.SYSTEM_DEFAULT)
    assert(rpr.getRoutingKeyForColumn(dvd) === pmod(
      row.eval().asInstanceOf[Int], numPartitions))

    val r1 = new java.sql.Timestamp(System.currentTimeMillis())
    row = createRow(r1, TimestampType)
    dvd = new SQLTimestamp(r1)
    dvds += (dvd -> TimestampType)
    assert(rpr.getRoutingKeyForColumn(dvd) === pmod(
      row.eval().asInstanceOf[Int], numPartitions))

    // Test supplementary unicode chars
    // scalastyle:off
    var txt = "功能 絶\uD84C\uDFB4顯示廣告"
    // scalastyle:on
    row = createRow(UTF8String.fromString(txt), StringType)
    dvd = new SQLVarchar(txt)
    dvds += (dvd -> StringType)
    assert(rpr.getRoutingKeyForColumn(dvd) === pmod(
      row.eval().asInstanceOf[Int], numPartitions))
    // scalastyle:off
    txt = "功能 絶\uD84C\uDFB4顯示廣告示"
    // scalastyle:on
    row = createRow(UTF8String.fromString(txt), StringType)
    dvd = new SQLVarchar(txt)
    dvds += (dvd -> StringType)
    assert(rpr.getRoutingKeyForColumn(dvd) === pmod(
      row.eval().asInstanceOf[Int], numPartitions))
    // scalastyle:off
    txt = "功能 絶\uD84C\uDFB4顯示廣告\uD84C\uDFB4顯x"
    // scalastyle:on
    row = createRow(UTF8String.fromString(txt), StringType)
    dvd = new SQLVarchar(txt)
    dvds += (dvd -> StringType)
    assert(rpr.getRoutingKeyForColumn(dvd) === pmod(
      row.eval().asInstanceOf[Int], numPartitions))
    // scalastyle:off
    txt = "功能 絶\uD84C\uDFB4顯示廣告\uD84C\uDFB4顯xx"
    // scalastyle:on
    row = createRow(UTF8String.fromString(txt), StringType)
    dvd = new SQLVarchar(txt)
    dvds += (dvd -> StringType)
    assert(rpr.getRoutingKeyForColumn(dvd) === pmod(
      row.eval().asInstanceOf[Int], numPartitions))
    // scalastyle:off
    txt = "功能 絶\uD84C\uDFB4顯示廣告\uD84C\uDFB4顯xxx"
    // scalastyle:on
    row = createRow(UTF8String.fromString(txt), StringType)
    dvd = new SQLVarchar(txt)
    dvds += (dvd -> StringType)
    assert(rpr.getRoutingKeyForColumn(dvd) === pmod(
      row.eval().asInstanceOf[Int], numPartitions))
    // scalastyle:off
    txt = "功能 絶\uD84C\uDFB4顯示廣告\uD84C\uDFB4顯xxxx"
    // scalastyle:on
    row = createRow(UTF8String.fromString(txt), StringType)
    dvd = new SQLVarchar(txt)
    dvds += (dvd -> StringType)
    assert(rpr.getRoutingKeyForColumn(dvd) === pmod(
      row.eval().asInstanceOf[Int], numPartitions))

    // multiple column tests
    for (dvd1 <- dvds; dvd2 <- dvds) {
      row = createRow(Array(dvd1._1.getObject, dvd2._1.getObject),
        Array(dvd1._2, dvd2._2))
      assert(rpr2.getRoutingObjectFromDvdArray(Array(dvd1._1, dvd2._1)) ===
          pmod(row.eval().asInstanceOf[Int], numPartitions))
    }

    // Tests for external partitioner like Kafka partitioner

    val func = new StoreHashFunction

    dvd = new SQLDate(createDate(1, 1, 2011))
    assert(rpr.getRoutingKeyForColumn(dvd) === func.computeHash(
      createDate(1, 1, 2011), numPartitions))

    dvd = new SQLDate(createDate(1, 1, 2011))
    assert(rpr.getRoutingKeyForColumn(dvd) === func.computeHash(
      createDate(1, 1, 2011), numPartitions))

    dvd = new SQLClob("xxxxx")
    assert(rpr.getRoutingKeyForColumn(dvd) === func.computeHash(
      "xxxxx", numPartitions))

    dvd = new SQLBoolean(true)
    assert(rpr.getRoutingKeyForColumn(dvd) === func.computeHash(
      true, numPartitions))

    dvd = new SQLVarchar("xxxx")
    assert(rpr.getRoutingKeyForColumn(dvd) === func.computeHash(
      "xxxx", numPartitions))

    val timeStamp = new java.sql.Timestamp(System.currentTimeMillis())

    dvd = new SQLTimestamp(timeStamp)
    assert(rpr.getRoutingKeyForColumn(dvd) === func.computeHash(
      timeStamp, numPartitions))

    dvd = new SQLInteger(200000)
    assert(rpr.getRoutingKeyForColumn(dvd) === func.computeHash(
      200000, numPartitions))

    snc.sql(s"DROP TABLE IF EXISTS $ColumnTableName2")
    snc.sql(s"DROP TABLE IF EXISTS $ColumnTableName1")
  }

  test("Test PR for Int type column") {
    val snc = SnappyContext(sc)
    snc.sql(s"set ${Property.ColumnBatchSize.name}=30")
    snc.sql(s"set ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key}=1")
    snc.sql(s"CREATE TABLE $ColumnTableName1(OrderId INT ,ItemId INT, ItemRef INT) " +
        "USING column " +
        "options (" +
        "BUCKETS '8'," +
        "PARTITION_BY 'OrderId'," +
        "PERSISTENT 'ASYNCHRONOUS')")

    val rdd = sc.parallelize(
      (1 to 5).map(i => TestData2(i, i.toString, i)))
    val dataDF = snc.createDataFrame(rdd)

    val rep = dataDF.repartition(8, new ColumnName("key1"))

    rep.createOrReplaceTempView("ColumnTable1Temp")

    dataDF.write.insertInto(ColumnTableName1)

    val count = snc.sql(s"select * from $ColumnTableName1 P JOIN " +
        "ColumnTable1Temp R ON P.OrderId=R.key1")
    joinSuite.checkForShuffle(count.logicalPlan, snc, shuffleExpected = false)
    assert(count.count() === 5)
  }


  test("Test PR for String type column") {
    snc.sql(s"CREATE TABLE $ColumnTableName1(OrderId INT ,ItemRef String) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'ItemRef'," +
        "PERSISTENT 'ASYNCHRONOUS')")

    val rdd = sc.parallelize(
      (1 to 1000).map(i => Data1(i, i.toString)))

    val dataDF = snc.createDataFrame(rdd)

    val rep = dataDF.repartition(11, new ColumnName("sk"))

    rep.createOrReplaceTempView("ColumnTable1Temp")

    dataDF.write.insertInto(ColumnTableName1)
    val count = snc.sql(s"select * from $ColumnTableName1 P JOIN " +
        "ColumnTable1Temp R ON P.ItemRef=R.sk")
    assert(count.count() === 1000)
  }

  test("Test PR for String type column without repartition") {
    snc.sql(s"CREATE TABLE $ColumnTableName1(OrderId INT ,ItemRef String) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'ItemRef'," +
        "PERSISTENT 'ASYNCHRONOUS')")

    val rdd = sc.parallelize(
      (1 to 1000).map(i => Data1(i, i.toString)))

    val dataDF = snc.createDataFrame(rdd)

    dataDF.createOrReplaceTempView("ColumnTable1Temp")

    dataDF.write.insertInto(ColumnTableName1)
    val count = snc.sql(s"select * from $ColumnTableName1 P JOIN " +
        "ColumnTable1Temp R ON P.ItemRef=R.sk")
    /*
    val qe = new QueryExecution(snc, count.logicalPlan)
    println(qe.executedPlan)
    */
    assert(count.count() === 1000)
  }

  test("Test PR for String type column for row tables") {
    snc.sql(s"CREATE TABLE $ColumnTableName1(OrderId INT ,ItemRef String) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'ItemRef'," +
        "PERSISTENT 'ASYNCHRONOUS')")

    val rdd = sc.parallelize(
      (1 to 1000).map(i => Data1(i, i.toString)))

    val dataDF = snc.createDataFrame(rdd)

    val rep = dataDF.repartition(11, new ColumnName("sk"))

    rep.createOrReplaceTempView("ColumnTable1Temp")

    dataDF.write.insertInto(ColumnTableName1)
    val count = snc.sql(s"select * from $ColumnTableName1 P JOIN " +
        "ColumnTable1Temp R ON P.ItemRef=R.sk")
    assert(count.count() === 1000)
  }

  test("Test PR for String type column with collocation") {
    snc.sql(s"CREATE TABLE $ColumnTableName1(OrderId INT ,ItemRef String) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'ItemRef'," +
        "PERSISTENT 'ASYNCHRONOUS')")

    snc.sql(s"CREATE TABLE $ColumnTableName2(OrderId INT ,ItemRef String) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'ItemRef'," +
        "PERSISTENT 'ASYNCHRONOUS'," +
        "COLOCATE_WITH 'ColumnTable1')")

    val rdd = sc.parallelize(
      (1 to 1000).map(i => Data1(i, i.toString)))

    val dataDF = snc.createDataFrame(rdd)

    val rep = dataDF.repartition(11, new ColumnName("sk"))

    rep.createOrReplaceTempView("ColumnTable1Temp")


    dataDF.write.insertInto(ColumnTableName2)
    val count = snc.sql(s"select * from $ColumnTableName2 P JOIN " +
        "ColumnTable1Temp R ON P.ItemRef=R.sk")
    assert(count.count() === 1000)
  }

  test("Test Row PR for String type primary key") {

    snc.sql(s"CREATE TABLE $ColumnTableName1(OrderId INT NOT NULL PRIMARY KEY," +
        "ItemRef String) USING row " +
        "options (" +
        "PARTITION_BY 'PRIMARY KEY'," +
        "PERSISTENT 'ASYNCHRONOUS')")

    val rdd = sc.parallelize(
      (1 to 1000).map(i => Data1(i, i.toString)))

    val dataDF = snc.createDataFrame(rdd)

    val rep = dataDF.repartition(11, new ColumnName("pk"))

    rep.createOrReplaceTempView("ColumnTable1Temp")

    dataDF.write.insertInto(ColumnTableName1)

    val count = snc.sql(s"select * from $ColumnTableName1 P JOIN ColumnTable1Temp R " +
        "ON P.OrderId=R.pk")
    assert(count.count() === 1000)
  }

  test("Test Row PR for String type multiple column primary key") {

    snc.sql(s"CREATE TABLE $ColumnTableName1(ItemRef VARCHAR(100), " +
        s"rowid INT, OrderId INT, PRIMARY KEY (ItemRef, rowid)) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'ItemRef, rowid'," +
        "PERSISTENT 'ASYNCHRONOUS')")

    val rdd = sc.parallelize(
      (1 to 1000).map(i => Data4(i.toString, i * 100, i)))

    val dataDF = snc.createDataFrame(rdd)

    val rep = dataDF.repartition(11, new ColumnName("sk"), new ColumnName("pk1"))

    rep.createOrReplaceTempView("ColumnTable1Temp")

    dataDF.write.insertInto(ColumnTableName1)

    val count = snc.sql(s"select * from $ColumnTableName1 P JOIN ColumnTable1Temp R " +
        s"ON P.ItemRef=R.sk and P.rowid=R.pk1")
    assert(count.count() === 1000)
  }




  test("Test row PR with jdbc connection") {
    val serverHostPort = TestUtil.startNetServer()

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)

    snc.sql(s"CREATE TABLE $ColumnTableName1(OrderId INT NOT NULL " +
        s"PRIMARY KEY ," +
        s"ItemRef String) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'PRIMARY KEY'," +
        "PERSISTENT 'ASYNCHRONOUS')")

    val rdd = sc.parallelize(
      (1 to 1000).map(i => Data1(i, i.toString)))

    val dataDF = snc.createDataFrame(rdd)

    val rep = dataDF.repartition(11, new ColumnName("pk"))

    rep.createOrReplaceTempView("ColumnTable1Temp")

    val stmt = conn.createStatement()
    val rows = rdd.collect()
    try {

      rows.foreach(d =>
        stmt.executeUpdate(
          s"insert into $ColumnTableName1 values(${d.pk}, '${d.sk}')")
      )
    } finally {
      stmt.close()
      conn.close()
    }

    val count = snc.sql(s"select * from $ColumnTableName1 P JOIN " +
        s"ColumnTable1Temp R ON P.OrderId=R.pk")
    assert(count.count() === 1000)

    TestUtil.stopNetServer()
  }
}
