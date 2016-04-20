package org.apache.spark.sql.store

import java.math.{BigInteger, BigDecimal}
import java.sql.DriverManager
import java.util.Date

import com.gemstone.gemfire.cache.{PartitionResolver, RegionAttributes, Region, CacheFactory, Cache}
import com.pivotal.gemfirexd.TestUtil
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionByExpressionResolver
import com.pivotal.gemfirexd.internal.iapi.types._
import io.snappydata.SnappyFunSuite
import io.snappydata.core.{Data1, TestData2}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter}

import org.apache.spark.Logging
import org.apache.spark.sql.ColumnName
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, GenericMutableRow}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.unsafe.types.UTF8String

/**
 * This test checks the validity of various functionality when we use Spark's partitioner logic for underlying
 * GemXD storage.
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


  }

  val ColumnTableName1: String = "ColumnTable1"
  val ColumnTableName2: String = "ColumnTable2"
  val RowTableName1: String = "RowTable1"
  val RowTableName2: String = "RowTable2"

  val props = Map.empty[String, String]


  val options = "OPTIONS (PARTITION_BY 'col1')"

  val optionsWithURL = "OPTIONS (PARTITION_BY 'Col1', URL 'jdbc:snappydata:;')"


  test(" Test hash codes for all Sql types ") {
    snc.sql(s"CREATE TABLE $ColumnTableName1(OrderId INT ,ItemId INT, ItemRef INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        "PERSISTENT 'ASYNCHRONOUS')")

    val regtwo: Region[_, _] = Misc.getRegion("/APP/COLUMNTABLE1", true, true)
    val rattr: RegionAttributes[_, _] = regtwo.getAttributes
    val pr: PartitionResolver[_, _] = rattr.getPartitionAttributes.getPartitionResolver
    val rpr: GfxdPartitionByExpressionResolver = pr.asInstanceOf[GfxdPartitionByExpressionResolver]
    assert(rpr != null)

    def createRow(values: Any*): GenericInternalRow = new GenericInternalRow(values.toArray)

    // Check All Datatypes
    var row = createRow(200)
    var dvd: DataValueDescriptor = new SQLInteger(200)


    assert(rpr.getRoutingKeyForColumn(dvd) == row.hashCode)

    row = createRow(new BigInteger("200000"))
    dvd = new SQLInteger(200000)
    assert(rpr.getRoutingKeyForColumn(dvd) == row.hashCode)

    row = createRow(true)
    dvd = new SQLBoolean(true)
    assert(rpr.getRoutingKeyForColumn(dvd) == row.hashCode)

    row = createRow(new java.sql.Date(1, 1, 2011))
    dvd = new SQLDate(new java.sql.Date(1, 1, 2011))
    assert(rpr.getRoutingKeyForColumn(dvd) == row.hashCode)

    row = createRow(new java.util.Date(1, 1, 2011))
    dvd = new SQLDate(new java.sql.Date(1, 1, 2011))
    assert(rpr.getRoutingKeyForColumn(dvd) == row.hashCode)


    val ipaddr: Array[Byte] = Array(192.toByte, 168.toByte, 1.toByte, 9.toByte)
    row = createRow(ipaddr)
    dvd = new SQLBit(ipaddr)
    assert(rpr.getRoutingKeyForColumn(dvd) == row.hashCode)

    dvd = new SQLReal(10.5F)
    row = createRow(10.5F)
    assert(rpr.getRoutingKeyForColumn(dvd) == row.hashCode)

    dvd = new SQLLongint(479L)
    row = createRow(479L)
    assert(rpr.getRoutingKeyForColumn(dvd) == row.hashCode)

    dvd = new SQLVarchar("xxxx");
    row = createRow(UTF8String.fromString("xxxx")) // As catalyst converts String to UtfString
    assert(rpr.getRoutingKeyForColumn(dvd) == row.hashCode)

    dvd = new SQLClob("xxxxx")
    row = createRow(UTF8String.fromString("xxxxx")) // As catalyst converts String to UtfString
    assert(rpr.getRoutingKeyForColumn(dvd) == row.hashCode)

    dvd = new SQLTimestamp(new java.sql.Timestamp(System.currentTimeMillis()))
    row = createRow(new java.sql.Timestamp(System.currentTimeMillis()))
    assert(rpr.getRoutingKeyForColumn(dvd) == row.hashCode)

    dvd = new SQLSmallint(5)
    row = createRow( 5)
    assert(rpr.getRoutingKeyForColumn(dvd) == row.hashCode)

    dvd = new SQLTinyint(2)
    row = createRow(2)
    assert(rpr.getRoutingKeyForColumn(dvd) == row.hashCode)


    dvd = new SQLDecimal(new BigDecimal(32000.05f))
    row = createRow(new BigDecimal(32000.05f))
    assert(rpr.getRoutingKeyForColumn(dvd) == row.hashCode)


    // Tests for external partitioner like Kafka partitioner

    val func = new StoreHashFunction

    dvd = new SQLDate(new java.sql.Date(1, 1, 2011))
    assert(rpr.getRoutingKeyForColumn(dvd) == func.hashValue(new java.util.Date(1, 1, 2011)))

    dvd = new SQLDate(new java.sql.Date(1, 1, 2011))
    assert(rpr.getRoutingKeyForColumn(dvd) == func.hashValue(new java.sql.Date(1, 1, 2011)))

    dvd = new SQLClob("xxxxx")
    assert(rpr.getRoutingKeyForColumn(dvd) == func.hashValue("xxxxx"))

    dvd = new SQLBoolean(true)
    assert(rpr.getRoutingKeyForColumn(dvd) == func.hashValue(true))

    dvd = new SQLVarchar("xxxx");
    assert(rpr.getRoutingKeyForColumn(dvd) == func.hashValue("xxxx"))

    val timeStamp = new java.sql.Timestamp(System.currentTimeMillis())

    dvd = new SQLTimestamp(timeStamp)
    assert(rpr.getRoutingKeyForColumn(dvd) ==
        func.hashValue(timeStamp))

    dvd = new SQLInteger(200000)
    assert(rpr.getRoutingKeyForColumn(dvd) == func.hashValue(new BigInteger("200000")))


  }

  test("Test PR for Int type column") {
    snc.sql(s"CREATE TABLE $ColumnTableName1(OrderId INT ,ItemId INT, ItemRef INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        "PERSISTENT 'ASYNCHRONOUS')")

    val rdd = sc.parallelize(
      (1 to 5).map(i => TestData2(i, i.toString, i)))
    val dataDF = snc.createDataFrame(rdd)

    val rep = dataDF.repartition(11, new ColumnName("key1"))

    rep.registerTempTable("ColumnTable1Temp")

    dataDF.write.insertInto(ColumnTableName1)

    val count = snc.sql(s"select * from $ColumnTableName1 P JOIN ColumnTable1Temp R ON P.OrderId=R.key1")
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

    rep.registerTempTable("ColumnTable1Temp")


    dataDF.write.insertInto(ColumnTableName1)
    val count = snc.sql(s"select * from $ColumnTableName1 P JOIN ColumnTable1Temp R ON P.ItemRef=R.sk")
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

    dataDF.registerTempTable("ColumnTable1Temp")


    dataDF.write.insertInto(ColumnTableName1)
    val count = snc.sql(s"select * from $ColumnTableName1 P JOIN ColumnTable1Temp R ON P.ItemRef=R.sk")
    /*   val qe = new QueryExecution(snc, count.logicalPlan)
       println(qe.executedPlan)*/
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

    rep.registerTempTable("ColumnTable1Temp")


    dataDF.write.insertInto(ColumnTableName1)
    val count = snc.sql(s"select * from $ColumnTableName1 P JOIN ColumnTable1Temp R ON P.ItemRef=R.sk")
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

    rep.registerTempTable("ColumnTable1Temp")


    dataDF.write.insertInto(ColumnTableName2)
    val count = snc.sql(s"select * from $ColumnTableName2 P JOIN ColumnTable1Temp R ON P.ItemRef=R.sk")
    assert(count.count() === 1000)
  }

  test("Test Row PR for String type primary key") {

    snc.sql(s"CREATE TABLE $ColumnTableName1(OrderId INT NOT NULL PRIMARY KEY ,ItemRef String) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'PRIMARY KEY'," +
        "PERSISTENT 'ASYNCHRONOUS')")

    val rdd = sc.parallelize(
      (1 to 1000).map(i => Data1(i, i.toString)))

    val dataDF = snc.createDataFrame(rdd)

    val rep = dataDF.repartition(11, new ColumnName("pk"))

    rep.registerTempTable("ColumnTable1Temp")

    dataDF.write.insertInto(ColumnTableName1)

    val count = snc.sql(s"select * from $ColumnTableName1 P JOIN ColumnTable1Temp R " +
        s"ON P.OrderId=R.pk")
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

    rep.registerTempTable("ColumnTable1Temp")

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
