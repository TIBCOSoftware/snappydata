package org.apache.spark.sql.store

import java.util.Properties

import com.pivotal.gemfirexd.internal.engine.fabricservice.FabricServiceImpl
import io.snappydata.{ServiceManager, SnappyFunSuite}
import io.snappydata.core.{Data, FileCleaner, TestData}
import org.apache.spark.Logging
import org.apache.spark.sql.SaveMode
import org.scalatest.{BeforeAndAfter}

/**
 * Created by skumar on 13/11/15.
 */
class ColumnTableBatchInsertTest extends SnappyFunSuite
with Logging
with BeforeAndAfter {

  val tableName: String = "ColumnTable"
  val props = Map.empty[String, String]

  after {
    snc.dropExternalTable(tableName, true)
    snc.dropExternalTable("ColumnTable2", true)
  }

  ignore("test the shadow table creation") {
    //snc.sql(s"DROP TABLE IF EXISTS $tableName")

    val df = snc.sql(s"CREATE TABLE $tableName(Col1 INT ,Col2 INT, Col3 INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Col1'," +
        "BUCKETS '1')")

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 0)

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
    val r2 = result.collect
    assert(r2.length == 5)
    println("Successful")
  }

  test("test the shadow table creation heavy insert") {
    // snc.sql(s"DROP TABLE IF EXISTS $tableName")

    val df = snc.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Key1'," +
        "BUCKETS '1')")

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 0)

    val rdd = sc.parallelize(
      (1 to 1000).map(i => TestData(i, i.toString)))

    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
    val r2 = result.collect
    assert(r2.length == 1000)
    println("Successful")
  }


  test("test the shadow table creation without partition by clause") {
    //snc.sql(s"DROP TABLE IF EXISTS $tableName")

    val df = snc.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING) " +
        "USING column " +
        "options " +
        "(" +
        "BUCKETS '100')")

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 0)

    val rdd = sc.parallelize(
      (1 to 19999).map(i => TestData(i, i.toString)))

    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
    val r2 = result.collect
    assert(r2.length == 19999)
    println("Successful")
  }

  test("test the shadow table with persistence") {
    //snc.sql(s"DROP TABLE IF EXISTS $tableName")

    val df = snc.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING)" +
        "USING column " +
        "options " +
        "(" +
        "PERSISTENT" +
        "BUCKETS '100')")

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 0)

    val rdd = sc.parallelize(
      (1 to 19999).map(i => TestData(i, i.toString)))

    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
    val r2 = result.collect
    assert(r2.length == 19999)
    println("Successful")
  }

  test("test the shadow table with eviction") {
    //snc.sql(s"DROP TABLE IF EXISTS $tableName")

    val df = snc.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING)" +
        "USING column " +
        "options " +
        "(" +
        "BUCKETS '100')")

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 0)

    val rdd = sc.parallelize(
      (1 to 19999).map(i => TestData(i, i.toString)))

    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
    val r2 = result.collect
    assert(r2.length == 19999)
    println("Successful")
  }

  test("test the shadow table with options on compressed table") {
    val df = snc.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING)" +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Key1'," +
        "BUCKETS '213'," +
        "REDUNDANCY '2')")

    val result = snc.sql("SELECT Key1 FROM " + tableName)
    val r = result.collect
    assert(r.length == 0)

    val rdd = sc.parallelize(
      (1 to 19999).map(i => TestData(i, i.toString)))

    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
    val r2 = result.collect
    assert(r2.length == 19999)
    println("Successful")
  }

  test("test the shadow table with eviction options on compressed table") {
    val df = snc.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING)" +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Key1'," +
        "BUCKETS '213'," +
        "REDUNDANCY '2'," +
        "EVICTION_BY 'LRUMEMSIZE 200')")

    val result = snc.sql("SELECT Value FROM " + tableName)
    val r = result.collect
    assert(r.length == 0)

    val rdd = sc.parallelize(
      (1 to 19999).map(i => TestData(i, i.toString)))

    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
    val r2 = result.collect
    assert(r2.length == 19999)
    println("Successful")
  }
}
