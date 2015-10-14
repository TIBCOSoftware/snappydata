package io.snappydata.app

import org.apache.spark.Logging
import org.scalatest.FunSuite

/**
 * Created by skumar on 17/9/15
 */
class ColumnTablePersistSpec extends FunSuite with Logging {

  private val testSparkContext = TestSQLContext.sparkContext

  test(""" This test will create simple table""") {

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

    //snc.registerAndInsertIntoExternalStore("table_name", props)
  }
}