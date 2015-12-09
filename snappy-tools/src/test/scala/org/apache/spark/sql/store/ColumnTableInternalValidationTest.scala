package org.apache.spark.sql.store

import com.gemstone.gemfire.internal.cache.{GemFireCacheImpl, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.{SnappyFunSuite}
import io.snappydata.core.FileCleaner
import org.apache.spark.Logging
import org.apache.spark.sql.execution.datasources.DDLException
import org.apache.spark.sql.{SQLConf, SaveMode}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import scala.util.Try

/**
 * Created by skumar on 23/11/15.
 */
class ColumnTableInternalValidationTest extends SnappyFunSuite
with Logging
with BeforeAndAfter {

  val tableName: String = "ColumnTable"
  val props = Map.empty[String, String]

  after {
    snc.dropExternalTable(tableName, true)
    snc.dropExternalTable("ColumnTable2", true)
    snc.dropExternalTable("COLUMNTABLE7", true)
  }

  test("test the shadow table with eviction options LRUCOUNT on compressed table") {
    intercept[DDLException] {
      val df = snc.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING)" +
          "USING column " +
          "options " +
          "(" +
          "PARTITION_BY 'Key1'," +
          "BUCKETS '213'," +
          "REDUNDANCY '2'," +
          "EVICTION_BY 'LRUCOUNT 20')")
    }
    println("Success")
  }

  test("test the shadow table with eviction options PARTITION BY PRIMARY KEY on compressed table") {
    intercept[DDLException] {
      val df = snc.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING)" +
          "USING column " +
          "options " +
          "(" +
          "PARTITION_BY 'PRIMARY KEY'," +
          "BUCKETS '213'," +
          "REDUNDANCY '2'," +
          "EVICTION_BY 'LRUCOUNT 20')")
    }
    println("Success")
  }

  test("test the shadow table with NOT NULL Column") {
    //snc.sql(s"DROP TABLE IF EXISTS $tableName")
    intercept[DDLException] {
      val df = snc.sql(s"CREATE TABLE $tableName(Key1 INT NOT NULL ,Value STRING) " +
          "USING column " +
          "options " +
          "(" +
          "BUCKETS '100')")
    }
    println("Success")
  }

  test("test the shadow table with primary key") {
    //snc.sql(s"DROP TABLE IF EXISTS $tableName")
    intercept[DDLException] {
      val df = snc.sql(s"CREATE TABLE $tableName(Key1 INT PRIMARY KEY ,Value STRING)" +
          "USING column " +
          "options " +
          "(" +
          "PARTITION_BY 'PRIMARY KEY'," +
          "BUCKETS '100')")
    }
    println("Success")
  }

  // TODO: Need to check insert individually is not working for column table asks for UpdatableRelation
  // withSQLConf doesn't work with sql, as in that case another sqlcontext is used.
  test("Test ShadowTable with 1 bucket") {
    snc.sql("DROP TABLE IF EXISTS COLUMNTABLE7")
    snc.sql("CREATE TABLE COLUMNTABLE7(Key1 INT ,Value INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Key1'," +
        "BUCKETS '1'," +
        "REDUNDANCY '2')")

    val region = Misc.getRegionForTable("APP.COLUMNTABLE7", true).asInstanceOf[PartitionedRegion]

    val shadowRegion = Misc.getRegionForTable("APP.COLUMNTABLE7_SHADOW_", true).asInstanceOf[PartitionedRegion]

    val data = Seq(Seq(1, 2), Seq(7, 8) , Seq(9, 2))//, Seq(4, 2), Seq(5, 6))

    val rdd = sc.parallelize(data, data.length).map(s => new MyTestData(s(0), s(1)))

    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("COLUMNTABLE7")
    //      snc.sql("insert into COLUMNTABLE3 VALUES(1,11)")
    //      snc.sql("insert into COLUMNTABLE3 VALUES(2,11)")
    //      snc.sql("insert into COLUMNTABLE3 VALUES(3,11)")
    //      snc.sql("insert into COLUMNTABLE3 VALUES(4,11)")
    //      snc.sql("insert into COLUMNTABLE3 VALUES(5,11)")

    val result = snc.sql("SELECT * FROM  COLUMNTABLE7")
    val r = result.collect
    assert(r.length == 3)

    val rCopy = region.getPartitionAttributes.getRedundantCopies
    assert(rCopy == 2)

    assert(GemFireCacheImpl.getColumnBatchSize == 3)

    assert(region.size == 0)
    assert(shadowRegion.size == 1)
    println("Success")
  }

  test("Test ShadowTable with 2 buckets") {
    snc.sql("DROP TABLE IF EXISTS COLUMNTABLE7")
    snc.sql("CREATE TABLE COLUMNTABLE7(Key1 INT ,Value INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Key1'," +
        "BUCKETS '2'," +
        "REDUNDANCY '2')")

    val region = Misc.getRegionForTable("APP.COLUMNTABLE7", true).asInstanceOf[PartitionedRegion]

    val shadowRegion = Misc.getRegionForTable("APP.COLUMNTABLE7_SHADOW_", true).asInstanceOf[PartitionedRegion]

    val data = Seq(Seq(1, 2), Seq(7, 8), Seq(9, 2), Seq(4, 2), Seq(5, 6))

    val rdd = sc.parallelize(data, data.length).map(s => new MyTestData(s(0), s(1)))

    val dataDF = snc.createDataFrame(rdd)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("COLUMNTABLE7")

    //      snc.sql("insert into COLUMNTABLE3 VALUES(1,11)")
    //      snc.sql("insert into COLUMNTABLE3 VALUES(2,11)")
    //      snc.sql("insert into COLUMNTABLE3 VALUES(3,11)")
    //      snc.sql("insert into COLUMNTABLE3 VALUES(4,11)")
    //      snc.sql("insert into COLUMNTABLE3 VALUES(5,11)")
    val result = snc.sql("SELECT * FROM  COLUMNTABLE7")
    val r = result.collect
    assert(r.length == 5)

    val rCopy = region.getPartitionAttributes.getRedundantCopies
    assert(rCopy == 2)

    //assert(GemFireCacheImpl.getColumnBatchSize == 2)
    // sometimes sizes may be different depending on how are the rows distributed
    if (GemFireCacheImpl.getColumnBatchSize == 3) {
      assert(region.size == 2)
      assert(shadowRegion.size > 0)
    }
    else {
      assert(region.size == 5)
      assert(shadowRegion.size == 0)
    }

    println("Success")
  }

  test("Test ShadowTable with 1 bucket, single insert") {
    snc.sql("DROP TABLE IF EXISTS COLUMNTABLE7")
    snc.dropExternalTable("COLUMNTABLE7", true)
    snc.sql("CREATE TABLE COLUMNTABLE7(Key1 INT ,Value INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Key1'," +
        "BUCKETS '1'," +
        "REDUNDANCY '2')")

    val region = Misc.getRegionForTable("APP.COLUMNTABLE7", true).asInstanceOf[PartitionedRegion]
    val shadowRegion = Misc.getRegionForTable("APP.COLUMNTABLE7_SHADOW_", true).asInstanceOf[PartitionedRegion]

    snc.sql("insert into COLUMNTABLE7 VALUES(1,11)")
    snc.sql("insert into COLUMNTABLE7 VALUES(2,11)")
    snc.sql("insert into COLUMNTABLE7 VALUES(3,11)")
    snc.sql("insert into COLUMNTABLE7 VALUES(4,11)")
    snc.sql("insert into COLUMNTABLE7 VALUES(5,11)")

    val result = snc.sql("SELECT * FROM  COLUMNTABLE7")
    val r = result.collect
    assert(r.length == 5)

    val rCopy = region.getPartitionAttributes.getRedundantCopies
    assert(rCopy == 2)

    //assert(GemFireCacheImpl.getColumnBatchSize == 2)
    // sometimes sizes may be different depending on how are the rows distributed
    if (GemFireCacheImpl.getColumnBatchSize == 3) {
      assert(region.size == 2)
      assert(shadowRegion.size == 1)
    }
    else {
      assert(region.size == 5)
      assert(shadowRegion.size == 0)
    }

    println("Success")
  }

  protected def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val (keys, values) = pairs.unzip
    val currentValues = keys.map(key => Try(snc.conf.getConfString(key)).toOption)
    (keys, values).zipped.foreach(snc.conf.setConfString)
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => snc.conf.setConfString(key, value)
        case (key, None) => snc.conf.unsetConf(key)
      }
    }
  }
}

case class MyTestData(Key1: Int, Value: Int)