package io.snappydata.app

import org.apache.spark.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.{Duration, Seconds}
import org.scalatest.FunSuite

/**
 * Created by rishim on 27/8/15.
 */
class ExternalTablePersist extends FunSuite with Logging {

  private val testSparkContext = TestSQLContext.sparkContext

  // Batch duration
  def batchDuration: Duration = Seconds(1)

  test("Test the creation of table") {
    val props = Map(
      "url" -> "jdbc:gemfirexd:;mcast-port=33619;user=app;password=app;persist-dd=false",
      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
      "poolImpl" -> "tomcat",
      "user" -> "app",
      "password" -> "app"
    )

    val create1 = "4869215,bbbbb"
    val create2 = "4869215,aaaaa"

    val snc = org.apache.spark.sql.SnappyContext(testSparkContext)

    //val dataDF = snc.createDataFrame(Seq(create1,create2).map(Test.from))
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))


    //createDF.registerTempTable("test")
    //val result = snc.sql("select * FROM test").collect()
    //result === Array(Test.from(create1),Test.from(create2)).map(Row.fromTuple)


    val rdd = testSparkContext.parallelize(data, 1).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    //dataDF.write.format("jdbc").options(props).saveAsTable("Order20")
    //dataDF.write.format("jdbc").mode(SaveMode.Append).options(props).saveAsTable("Order20")

    snc.createColumnTable("ColumnTable56", dataDF.schema, "jdbcColumnar", props)

    dataDF.write.format("jdbcColumnar").mode(SaveMode.Append).options(props).saveAsTable("ColumnTable56")


    val result = snc.sql("SELECT * FROM ColumnTable56")

    val r = result.collect



    r.foreach(println)
    //snc.insert("ColumnTable2",dataDF);
    //val tr = snc.sql("TRUNCATE TABLE ColumnTable56")
    //
    //snc.createColumnTable("Order20","jdbc" , props)
    println("Successful")
  }

  case class Test(key:Int, value:String)

  object Test {
    def from(line:String):Test = {
      val f = line.split(",")
      Test(f(0).toInt,f(1))
    }
  }

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