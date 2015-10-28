package io.snappydata.app

import org.apache.spark.Logging
import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.streaming.{Duration, Seconds}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * Created by Suranjan on 14/10/15.
 */
class ColumnTableTest extends FunSuite with Logging with BeforeAndAfter {

  private val sc = TestSQLContext.sparkContext

  val tableName : String = "ColumnTable"

  val props = Map(
    "url" -> "jdbc:gemfirexd:;mcast-port=33619;user=app;password=app;persist-dd=false",
    "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
    "poolImpl" -> "tomcat",
    "user" -> "app",
    "password" -> "app"
  )

  val snc = org.apache.spark.sql.SnappyContext(sc)

  after {
    snc.dropExternalTable(tableName, true)
  }

  test("Test the creation/dropping of table using Snappy API") {
    //shouldn't be able to create without schema
    intercept[AnalysisException] {
      snc.createExternalTable(tableName, "column", props)
    }

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createExternalTable(tableName, "column", dataDF.schema, props)
    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 0)
    println("Successful")
  }

  test("Test the creation of table using DataSource API") {

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 5)
    println("Successful")
  }

  test("Test the creation of table using Snappy API and then append/ignore/overwrite DF using DataSource API") {
    var data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    var rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    var dataDF = snc.createDataFrame(rdd)

    snc.createExternalTable(tableName, "column", dataDF.schema, props)

    intercept[AnalysisException] {
      dataDF.write.format("column").mode(SaveMode.ErrorIfExists).options(props).saveAsTable(tableName)
    }
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    var result = snc.sql("SELECT * FROM " + tableName)
    var r = result.collect
    assert(r.length == 5)

    // Ignore if table is present
    data = Seq(Seq(100, 200, 300), Seq(700, 800, 900), Seq(900, 200, 300), Seq(400, 200, 300), Seq(500, 600, 700), Seq(800, 900, 1000))
    rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    dataDF = snc.createDataFrame(rdd)
    dataDF.write.format("column").mode(SaveMode.Ignore).options(props).saveAsTable(tableName)
    result = snc.sql("SELECT * FROM " + tableName)
    r = result.collect
    assert(r.length == 5)

    // Append if table is present
    data = Seq(Seq(100, 200, 300), Seq(700, 800, 900), Seq(900, 200, 300), Seq(400, 200, 300), Seq(500, 600, 700), Seq(800, 900, 1000))
    rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    dataDF = snc.createDataFrame(rdd)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
    result = snc.sql("SELECT * FROM " + tableName)
    r = result.collect
    assert(r.length == 11)

    // Overwrite if table is present
    data = Seq(Seq(100, 200, 300), Seq(700, 800, 900), Seq(900, 200, 300), Seq(400, 200, 300), Seq(500, 600, 700), Seq(800, 900, 1000))
    rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    dataDF = snc.createDataFrame(rdd)
    dataDF.write.format("column").mode(SaveMode.Overwrite).options(props).saveAsTable(tableName)
    result = snc.sql("SELECT * FROM " + tableName)
    r = result.collect
    assert(r.length == 6)

    println("Successful")
  }

  val options =  "OPTIONS (url 'jdbc:gemfirexd:;mcast-port=33619;user=app;password=app;persist-dd=false' ," +
             "driver 'com.pivotal.gemfirexd.jdbc.EmbeddedDriver' ," +
             "poolImpl 'tomcat', " +
             "user 'app', " +
             "password 'app' ) "

  test("Test the creation/dropping of table using SQL") {

    snc.sql("CREATE TABLE " + tableName + " (Col1 INT, Col2 INT, Col3 INT) " + " USING column " +
        options
        )
    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 0)
    println("Successful")
  }

  test("Test the creation using SQL and insert a DF in append/overwrite/errorifexists mode") {

    snc.sql("CREATE TABLE " + tableName + " (Col1 INT, Col2 INT, Col3 INT) " + " USING column " +
        options )

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.format("column").mode(SaveMode.Ignore).options(props).saveAsTable(tableName)

    intercept[AnalysisException] {
      dataDF.write.format("column").mode(SaveMode.ErrorIfExists).options(props).saveAsTable(tableName)
    }

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 5)
    println("Successful")
  }

  test("Test the creation of table using SQL and SnappyContext ") {

    snc.sql("CREATE TABLE " + tableName + " (Col1 INT, Col2 INT, Col3 INT) " + " USING column " +
        options
    )
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    intercept[AnalysisException] {
      snc.createExternalTable(tableName, "column", dataDF.schema, props)
    }

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 5)
    println("Successful")
  }

  test("Test the creation of table using CREATE TABLE AS STATEMENT ") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createExternalTable(tableName, "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    val tableName2 = "CoulmnTable2"
    snc.sql("CREATE TABLE " + tableName2 + " USING column " +
        options + " AS (SELECT * FROM " + tableName + ")"
    )
    var result = snc.sql("SELECT * FROM " + tableName2)
    var r = result.collect
    assert(r.length == 5)

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName2)
    result = snc.sql("SELECT * FROM " + tableName2)
    r = result.collect
    assert(r.length == 10)

    snc.dropExternalTable(tableName2)
    println("Successful")
  }

  test("Test the truncate syntax SQL and SnappyContext") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createExternalTable(tableName, "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    snc.truncateExternalTable(tableName)

    var result = snc.sql("SELECT * FROM " + tableName)
    var r = result.collect
    assert(r.length == 0)

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
    snc.sql("TRUNCATE TABLE " + tableName)

    result = snc.sql("SELECT * FROM " + tableName)
    r = result.collect
    assert(r.length == 0)

    println("Successful")
  }

  test("Test the drop syntax SnappyContext and SQL ") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createExternalTable(tableName, "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    snc.dropExternalTable(tableName, true)

    intercept[AnalysisException] {
      snc.dropExternalTable(tableName, false)
    }

    intercept[AnalysisException] {
      snc.sql("DROP TABLE " + tableName)
    }

    snc.sql("DROP TABLE IF EXISTS " + tableName)

    println("Successful")
  }

  test("Test the drop syntax SQL and SnappyContext ") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createExternalTable(tableName, "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    snc.sql("DROP TABLE IF EXISTS " + tableName)

    intercept[AnalysisException] {
      snc.dropExternalTable(tableName, false)
    }

    intercept[AnalysisException] {
      snc.sql("DROP TABLE " + tableName)
    }

    snc.dropExternalTable(tableName, true)

    println("Successful")
  }
}