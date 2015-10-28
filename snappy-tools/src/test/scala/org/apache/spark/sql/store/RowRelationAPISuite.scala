package org.apache.spark.sql.store

import com.gemstone.gemfire.internal.cache.{DistributedRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.core.{TestData2, SnappySQLContext, TestData}
import org.scalatest.FunSuite

import org.apache.spark.Logging
import org.apache.spark.sql._

/**
 * Tests for ROW stores
 */
class RowRelationAPISuite extends FunSuite with Logging {

  private val sc = SnappySQLContext.sparkContext

  private val URL = "'jdbc:gemfirexd:;mcast-port=0;user=app;password=app'"

  private val driver = "'com.pivotal.gemfirexd.jdbc.EmbeddedDriver'"

  val props = Map(
    "url" -> "jdbc:gemfirexd:;mcast-port=0;user=app;password=app",
    "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver"
  )


 test("Create replicated row table with DataFrames") {

    val snc = org.apache.spark.sql.SnappyContext(sc)
    val rdd = sc.parallelize((1 to 1000).map(i => TestData(i, s"$i")))
    val dataDF = snc.createDataFrame(rdd)
    snc.sql("DROP TABLE IF EXISTS row_table1")
    snc.createExternalTable("row_table1", "row", dataDF.schema, props)
    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("row_table1")
    val countdf = snc.sql("select * from row_table1")
    val count = countdf.count()
    assert(count === 1000)
  }

  test("Test Partitioned row tables") {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    val rdd = sc.parallelize(
      (1 to 1000).map(i => TestData(i, i.toString)))

    val dataDF = snc.createDataFrame(rdd)
    snc.sql("DROP TABLE IF EXISTS row_table2")

    val df = snc.sql("CREATE TABLE row_table2(OrderId INT NOT NULL,ItemId INT)" +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        s"driver $driver," +
        s"URL $URL)")

    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("row_table2")
    val exp = snc.sql("select * from row_table2")

    val countdf = snc.sql("select * from row_table2")
    val count = countdf.count()
    assert(count === 1000)
  }

  test("Test PreserverPartition on  row tables") {
    val snc = org.apache.spark.sql.SnappyContext(sc)


    val rdd = sc.parallelize(1 to 1000, 113).map(i => TestData(i, i.toString))

    val k = 113
    val dataDF = snc.createDataFrame(rdd)
    snc.sql("DROP TABLE IF EXISTS row_table3")

    val df = snc.sql("CREATE TABLE row_table3(OrderId INT NOT NULL,ItemId INT) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        "preservepartitions 'true'," +
        s"driver $driver," +
        s"URL $URL)")

    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("row_table3")
    val countdf = snc.sql("select * from row_table3")
    val count = countdf.count()
    assert(count === 1000)
  }

  test("Test PR with Primary Key") {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    val df = snc.sql("CREATE TABLE ROW_TEST_TABLE1(OrderId INT NOT NULL PRIMARY KEY,ItemId INT) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'PRIMARY KEY'," +
        "REDUNDANCY '2'," +
        s"driver $driver," +
        s"URL $URL)")

    val region = Misc.getRegionForTable("APP.ROW_TEST_TABLE1", true).asInstanceOf[PartitionedRegion]

    val rCopy = region.getPartitionAttributes.getRedundantCopies
    assert(rCopy === 2)

  }

  test("Test PR with buckets") {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    val df = snc.sql("CREATE TABLE ROW_TEST_TABLE2(OrderId INT NOT NULL,ItemId INT) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        "BUCKETS '213'," +
        s"driver $driver," +
        s"URL $URL)")

    val region = Misc.getRegionForTable("APP.ROW_TEST_TABLE2", true).asInstanceOf[PartitionedRegion]

    val numPartitions = region.getTotalNumberOfBuckets
    assert(numPartitions === 213)

  }

  test("Test PR with REDUNDANCY") {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    val df = snc.sql("CREATE TABLE ROW_TEST_TABLE3(OrderId INT NOT NULL,ItemId INT) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        "BUCKETS '213'," +
        "REDUNDANCY '2'," +
        s"driver $driver," +
        s"URL $URL)")

    val region = Misc.getRegionForTable("APP.ROW_TEST_TABLE3", true).asInstanceOf[PartitionedRegion]
    snc.sql("insert into ROW_TEST_TABLE3 VALUES(1,11)")

    val rCopy = region.getPartitionAttributes.getRedundantCopies
    assert(rCopy === 2)

  }

  test("Test PR with RECOVERDELAY") {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    val df = snc.sql("CREATE TABLE ROW_TEST_TABLE4(OrderId INT NOT NULL PRIMARY KEY,ItemId INT) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        "BUCKETS '213'," +
        "RECOVERYDELAY '2'," +
        s"driver $driver," +
        s"URL $URL)")

    val region = Misc.getRegionForTable("APP.ROW_TEST_TABLE4", true).asInstanceOf[PartitionedRegion]

    val rDelay = region.getPartitionAttributes.getRecoveryDelay
    assert(rDelay === 2)

  }

  test("Test PR with MAXPART") {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    val df = snc.sql("CREATE TABLE ROW_TEST_TABLE5(OrderId INT NOT NULL PRIMARY KEY,ItemId INT) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        "MAXPARTSIZE '200'," +
        s"driver $driver," +
        s"URL $URL)")

    val region = Misc.getRegionForTable("APP.ROW_TEST_TABLE5", true).asInstanceOf[PartitionedRegion]

    val rDelay = region.getPartitionAttributes.getTotalMaxMemory
  }


  test("Test PR with EVICTION BY") {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    val df = snc.sql("CREATE TABLE ROW_TEST_TABLE6(OrderId INT NOT NULL PRIMARY KEY,ItemId INT) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        "EVICTION_BY 'LRUMEMSIZE 200'," +
        s"driver $driver," +
        s"URL $URL)")

    val region = Misc.getRegionForTable("APP.ROW_TEST_TABLE6", true).asInstanceOf[PartitionedRegion]
  }

  test("Test PR with PERSISTENT") {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    val df = snc.sql("CREATE TABLE ROW_TEST_TABLE7(OrderId INT NOT NULL PRIMARY KEY,ItemId INT) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'PRIMARY KEY'," +
        "PERSISTENT 'ASYNCHRONOUS'," +
        s"driver $driver," +
        s"URL $URL)")

    val region = Misc.getRegionForTable("APP.ROW_TEST_TABLE7", true).asInstanceOf[PartitionedRegion]
  }

  test("Test RR with PERSISTENT") {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    val df = snc.sql("CREATE TABLE ROW_TEST_TABLE8(OrderId INT NOT NULL PRIMARY KEY,ItemId INT) " +
        "USING row " +
        "options " +
        "(" +
        "PERSISTENT 'ASYNCHRONOUS'," +
        s"driver $driver," +
        s"URL $URL)")

    val region = Misc.getRegionForTable("APP.ROW_TEST_TABLE8", true).asInstanceOf[DistributedRegion]
    assert(region.getDiskStore != null)
  }


  test("Test PR with multiple columns") {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    val df = snc.sql("CREATE TABLE ROW_TEST_TABLE9(OrderId INT NOT NULL PRIMARY KEY,ItemId INT, ItemRef INT) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderID, ItemRef'," +
        "PERSISTENT 'ASYNCHRONOUS'," +
        s"driver $driver," +
        s"URL $URL)")


    val rdd = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i)))

    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("ROW_TEST_TABLE9")
    val countdf = snc.sql("select * from ROW_TEST_TABLE9")
    val count = countdf.count()
    assert(count === 1000)


  }
}
