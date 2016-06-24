package org.apache.spark.sql.store

import java.math.{BigDecimal, BigInteger}
import java.sql.DriverManager

import com.gemstone.gemfire.cache.{PartitionResolver, Region, RegionAttributes}
import com.pivotal.gemfirexd.TestUtil
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionByExpressionResolver
import com.pivotal.gemfirexd.internal.iapi.types._
import io.snappydata.SnappyFunSuite
import io.snappydata.core.{Data1, Data4, TestData2}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.Logging
import org.apache.spark.sql.ColumnName
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.{Expression, Pmod, Murmur3Hash, Literal, AttributeReference, GenericInternalRow}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.plans.physical._


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

  private def pmod(a: Int, n: Int): Int = { //We should push this logic to store layer
  val r = a % n
    if (r < 0) r + n else a
  }


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
    val seed = 42

    val numPartitions = 11



    def createRow(value: Any, dt : DataType ): Expression = {
      new Murmur3Hash(Seq(Literal.create(value, dt)))
    }

    // Check All Datatypes
    var row = createRow(200, IntegerType)
    var dvd: DataValueDescriptor = new SQLInteger(200)


    assert(rpr.getRoutingKeyForColumn(dvd) == pmod(row.eval().asInstanceOf[Int], numPartitions))

    row = createRow(200000, IntegerType)
    dvd = new SQLInteger(200000)
    assert(rpr.getRoutingKeyForColumn(dvd) == pmod(row.eval().asInstanceOf[Int], numPartitions))

    row = createRow(true, BooleanType)
    dvd = new SQLBoolean(true)
    assert(rpr.getRoutingKeyForColumn(dvd) == pmod(row.eval().asInstanceOf[Int], numPartitions))

    row = createRow(new java.sql.Date(1, 1, 2011), DateType)
    dvd = new SQLDate(new java.sql.Date(1, 1, 2011))
    assert(rpr.getRoutingKeyForColumn(dvd) == pmod(row.eval().asInstanceOf[Int], numPartitions))


    /*val ipaddr: Array[Byte] = Array(192.toByte, 168.toByte, 1.toByte, 9.toByte)
    row = createRow(ipaddr, ArrayType(ByteType))
    dvd = new SQLBit(ipaddr)
    assert(rpr.getRoutingKeyForColumn(dvd) == row.eval())*/

    dvd = new SQLReal(10.5F)
    row = createRow(10.5F , FloatType)
    assert(rpr.getRoutingKeyForColumn(dvd) == pmod(row.eval().asInstanceOf[Int], numPartitions))

    dvd = new SQLLongint(479L)
    row = createRow(479L, LongType)
    assert(rpr.getRoutingKeyForColumn(dvd) == pmod(row.eval().asInstanceOf[Int], numPartitions))

    dvd = new SQLVarchar("xxxx");
    row = createRow(UTF8String.fromString("xxxx"), StringType) // As
    // catalyst converts String to UtfString
    assert(rpr.getRoutingKeyForColumn(dvd) == pmod(row.eval().asInstanceOf[Int], numPartitions))

    dvd = new SQLClob("xxxxx")
    row = createRow(UTF8String.fromString("xxxxx"), StringType) // As
    // catalyst converts String to UtfString
    assert(rpr.getRoutingKeyForColumn(dvd) == row.hashCode)

    dvd = new SQLSmallint(5)
    row = createRow(5, IntegerType)
    assert(rpr.getRoutingKeyForColumn(dvd) == row.hashCode)

    dvd = new SQLTinyint(2)
    row = createRow(2, IntegerType)
    assert(rpr.getRoutingKeyForColumn(dvd) == row.hashCode)


    dvd = new SQLDecimal(new BigDecimal(32000.05f))
    row = createRow(new BigDecimal(32000.05f), DecimalType.SYSTEM_DEFAULT)
    val hash = row.hashCode
    assert(rpr.getRoutingKeyForColumn(dvd) == hash )

    val r1 = new java.sql.Timestamp(System.currentTimeMillis())
    dvd = new SQLTimestamp(r1)
    row = createRow(r1, TimestampType)
    assert(rpr.getRoutingKeyForColumn(dvd) == row.hashCode)


    //Test supplementary unicode chars
    val txt = "功能 絶\uD84C\uDFB4顯示廣告"
    dvd = new SQLVarchar(txt);
    row = createRow(UTF8String.fromString(txt), StringType)
    assert(rpr.getRoutingKeyForColumn(dvd) == row.hashCode)




    // Tests for external partitioner like Kafka partitioner

    val func = new StoreHashFunction

    

    dvd = new SQLDate(new java.sql.Date(1, 1, 2011))
    assert(rpr.getRoutingKeyForColumn(dvd) == func.hashValue(new java.util.Date(1, 1, 2011), numPartitions))

    dvd = new SQLDate(new java.sql.Date(1, 1, 2011))
    assert(rpr.getRoutingKeyForColumn(dvd) == func.hashValue(new java.sql.Date(1, 1, 2011), numPartitions))

    dvd = new SQLClob("xxxxx")
    assert(rpr.getRoutingKeyForColumn(dvd) == func.hashValue("xxxxx", numPartitions))

    dvd = new SQLBoolean(true)
    assert(rpr.getRoutingKeyForColumn(dvd) == func.hashValue(true, numPartitions))

    dvd = new SQLVarchar("xxxx");
    assert(rpr.getRoutingKeyForColumn(dvd) == func.hashValue("xxxx", numPartitions))

    val timeStamp = new java.sql.Timestamp(System.currentTimeMillis())

    dvd = new SQLTimestamp(timeStamp)
    assert(rpr.getRoutingKeyForColumn(dvd) ==
        func.hashValue(timeStamp, numPartitions))

    dvd = new SQLInteger(200000)
    assert(rpr.getRoutingKeyForColumn(dvd) == func.hashValue(new BigInteger("200000"), numPartitions))


  }

  test("Test PR for Int type column") {
    snc.sql(s"CREATE TABLE $ColumnTableName1(OrderId INT ,ItemId INT, ItemRef INT) " +
        "USING column " +
        "options " +
        "(" +
        "BUCKETS '11',"+
        "PARTITION_BY 'OrderId'," +
        "PERSISTENT 'ASYNCHRONOUS')")

    val rdd = sc.parallelize(
      (1 to 5).map(i => TestData2(i, i.toString, i)))
    val dataDF = snc.createDataFrame(rdd);

    val rep = dataDF.repartition(11, new ColumnName("key1"))

    rep.registerTempTable("ColumnTable1Temp")

    dataDF.write.insertInto(ColumnTableName1)

    val count = snc.sql(s"select * from $ColumnTableName1 P JOIN ColumnTable1Temp R ON P.OrderId=R.key1")
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

  test("Test Row PR for String type multiple column primary key") {

    snc.sql(s"CREATE TABLE $ColumnTableName1(ItemRef VARCHAR(100), " +
        s"rowid INT, OrderId INT, PRIMARY KEY (ItemRef, rowid)) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'ItemRef, rowid'," +
        "PERSISTENT 'ASYNCHRONOUS')")

    val rdd = sc.parallelize(
      (1 to 1000).map(i => Data4(i.toString(), i*100, i)))

    val dataDF = snc.createDataFrame(rdd)

    val rep = dataDF.repartition(11, new ColumnName("sk"), new ColumnName("pk1"))

    rep.registerTempTable("ColumnTable1Temp")

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
