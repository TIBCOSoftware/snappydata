package io.snappydata.app

import org.apache.spark.Logging
import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.streaming.{Duration, Seconds}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * Created by Suranjan on 14/10/15.
 */
class ColumnTableTest extends FunSuite with Logging with BeforeAndAfter {

  private val testSparkContext = TestSQLContext.sparkContext

  private val tableName : String = "ColumnTable"

  val props = Map(
    "url" -> "jdbc:gemfirexd:;mcast-port=33619;user=app;password=app;persist-dd=false",
    "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
    "poolImpl" -> "tomcat",
    "user" -> "app",
    "password" -> "app"
  )

  val snc = org.apache.spark.sql.SnappyContext(testSparkContext)

  after {
    snc.dropExternalTable(tableName, true)
  }

  test("Test the creation/dropping of table using Snappy API") {
    //shouldn't be able to create without schema
    intercept[IllegalArgumentException] {
      snc.createExternalTable(tableName, "column", props)
    }

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = testSparkContext.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createExternalTable(tableName, "column", dataDF.schema, props)
    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 0)
    println("Successful")
  }

  test("Test the creation of table using DataSource API") {

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = testSparkContext.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 5)
    println("Successful")
  }

  test("Test the creation of table using Snappy API and then append/ignore/overwrite DF using DataSource API") {
    var data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    var rdd = testSparkContext.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
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
    rdd = testSparkContext.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    dataDF = snc.createDataFrame(rdd)
    dataDF.write.format("column").mode(SaveMode.Ignore).options(props).saveAsTable(tableName)
    result = snc.sql("SELECT * FROM " + tableName)
    r = result.collect
    assert(r.length == 5)

    // Append if table is present
    data = Seq(Seq(100, 200, 300), Seq(700, 800, 900), Seq(900, 200, 300), Seq(400, 200, 300), Seq(500, 600, 700), Seq(800, 900, 1000))
    rdd = testSparkContext.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    dataDF = snc.createDataFrame(rdd)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
    result = snc.sql("SELECT * FROM " + tableName)
    r = result.collect
    assert(r.length == 11)

    // Overwrite if table is present
    data = Seq(Seq(100, 200, 300), Seq(700, 800, 900), Seq(900, 200, 300), Seq(400, 200, 300), Seq(500, 600, 700), Seq(800, 900, 1000))
    rdd = testSparkContext.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
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
    val rdd = testSparkContext.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
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
    val rdd = testSparkContext.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
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
    val rdd = testSparkContext.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
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
    val rdd = testSparkContext.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
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
    val rdd = testSparkContext.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
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
    val rdd = testSparkContext.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
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



//  test("Test the creation of table") {
//    val props = Map(
//      "url" -> "jdbc:gemfirexd:;mcast-port=33619;user=app;password=app;persist-dd=false",
//      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
//      "poolImpl" -> "tomcat",
//      "user" -> "app",
//      "password" -> "app"
//    )
//
//    val create1 = "4869215,bbbbb"
//    val create2 = "4869215,aaaaa"
//
//    val snc = org.apache.spark.sql.SnappyContext(testSparkContext)
//
//    //val dataDF = snc.createDataFrame(Seq(create1,create2).map(Test.from))
//    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
//
//    //createDF.registerTempTable("test")
//    //val result = snc.sql("select * FROM test").collect()
//    //result === Array(Test.from(create1),Test.from(create2)).map(Row.fromTuple)
//
//
//    val rdd = testSparkContext.parallelize(data, 1).map(s => new Data(s(0), s(1), s(2)))
//    val dataDF = snc.createDataFrame(rdd)
//    //dataDF.write.format("jdbc").options(props).saveAsTable("Order20")
//    //dataDF.write.format("jdbc").mode(SaveMode.Append).options(props).saveAsTable("Order20")
//
//    snc.createtableName("tableName56", dataDF.schema, "jdbcColumnar", props)
//
//    dataDF.write.format("jdbcColumnar").mode(SaveMode.Append).options(props).saveAsTable("tableName56")
//
//
//    val result = snc.sql("SELECT * FROM tableName56")
//
//    val r = result.collect
//
//    r.foreach(println)
//    //snc.insert("tableName2",dataDF);
//    //val tr = snc.sql("TRUNCATE TABLE tableName56")
//    //
//    //snc.createtableName("Order20","jdbc" , props)
//    println("Successful")
//  }
//
//  test("Test the creation of table using sql") {
//    val props = Map(
//      "url" -> "jdbc:gemfirexd:;mcast-port=33619;user=app;password=app;persist-dd=false",
//      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
//      "poolImpl" -> "tomcat",
//      "user" -> "app",
//      "password" -> "app"
//    )
//
//    val snc = org.apache.spark.sql.SnappyContext(testSparkContext)
//    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
//    val rdd = testSparkContext.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
//    val dataDF = snc.createDataFrame(rdd)
//    //snc.createtableName("tableName56", dataDF.schema, "jdbcColumnar", props)
//    snc.createExternalTable("tableName56", "jdbcColumnar", dataDF.schema, props)
//    dataDF.write.format("jdbcColumnar").mode(SaveMode.Append).options(props).saveAsTable("tableName56")
//    val result = snc.sql("SELECT * FROM tableName56")
//    val r = result.collect
//
//    r.foreach(println)
//    println("Successful")
//  }

  /*
  test("Test DataFrame row count") {

    val snc = org.apache.spark.sql.SnappyContext(testSparkContext)

    val props = Map(
      "url" -> "jdbc:gemfirexd:;mcast-port=33619;user=app;password=app;persist-dd=false",
      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
      "poolImpl" -> "tomcat",
      "user" -> "app",
      "password" -> "app"
    )


    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = testSparkContext.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.registerAndInsertIntoExternalStore(dataDF,"table_name", dataDF.schema, props)
    val count = dataDF.count()
    assert(count === data.length)
  }
*/
  /*
  test("Test Stream row count") {

    val snc = org.apache.spark.sql.SnappyContext(testSparkContext)

    val props = Map(
      "url" -> "jdbc:gemfirexd:;mcast-port=33619;user=app;password=app;persist-dd=false",
      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
      "poolImpl" -> "tomcat",
      "user" -> "app",
      "password" -> "app"
    )
    val ssc = new StreamingContext(testSparkContext, batchDuration)
    val queue = new SynchronizedQueue[RDD[Data]]()
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))

    val queueStream = ssc.queueStream(queue, oneAtATime = true)
    queueStream.saveToExternalTable[Data]("table_stream", props)

    queueStream.foreachRDD(rdd => {

      val dataDF = snc.externalTable("table_stream")
      val count = dataDF.count()
      assert(count === data.length)
    })

    ssc.start()


    queue += ssc.sparkContext.makeRDD(data.map(s => new Data(s(0), s(1), s(2))))



    Thread.sleep(1000)
    logInfo("Stopping context")
    ssc.stop()
  }*/
/*
  test("TestExternal RowTable With Sql") {


    val snc = org.apache.spark.sql.SnappyContext(testSparkContext)

    val props = Map(
      "url" -> "jdbc:gemfirexd:;locators=10.112.204.54:10101;user=app;password=app",
      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
      "poolimpl" -> "tomcat",
      "user" -> "app",
      "password" -> "app"
    )

    val data = Seq(Seq(1, "1"), Seq(7, "8"))
    val rdd = testSparkContext.parallelize(data, data.length).map(s => new Data1(s(0).asInstanceOf[Int], s(1).asInstanceOf[String]))
    val dataDF = snc.createDataFrame(rdd)
    dataDF.registerTempTable("temp_table")

    snc.sql("DROP TABLE IF EXISTS Orders1234" )

    snc.registerAndInsertIntoExternalStore();

    val df = snc.sql("CREATE TABLE Orders1234 " +
      "USING jdbc " +
      "options " +
      "(" +
      "driver 'com.pivotal.gemfirexd.jdbc.EmbeddedDriver'," +
      "url 'jdbc:gemfirexd:;locators=10.112.204.54:10101;user=app;password=app'" +
      ") AS (select * from temp_table) ")

    println("test successful")

    /*  val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
      val rdd = testSparkContext.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))

      val dataDF = snc.createDataFrame(rdd)
      //snc.createExternalTable("table_name","org.apache.spark.sql.execution.row.JDBCUpdatableSource" , props)
      dataDF.write.format("jdbc").options(props).saveAsTable("Order20")

      val data1 = Seq(Seq(11, 2, 3), Seq(71, 8, 9), Seq(19, 2, 3), Seq(4, 2, 3), Seq(15, 6, 7))
      val rdd1 = testSparkContext.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))

      val dataDF2 = snc.createDataFrame(rdd1)
      dataDF.write.format("jdbc").mode(SaveMode.Append).options(props).saveAsTable("Order20")

     val count = dataDF.count() + dataDF2.count()
      assert(count === (data.length + data1.length))*/
  }*/

}