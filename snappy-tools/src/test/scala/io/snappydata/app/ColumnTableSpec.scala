package io.snappydata.app

import io.snappydata.{SnappyFunSuite, SnappyFunSuite}

/**
 * Created by skumar on 17/9/15
 */
class ColumnTableSpec extends SnappyFunSuite {

  test( """ This test will create simple table""") {
    /*
    val props = Map(
      "url" -> "jdbc:gemfirexd:;locators=10.112.204.54:10101;user=app;password=app",
      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
      "poolimpl" -> "tomcat",
      "user" -> "app",
      "password" -> "app"
    )
    */

    val data = Seq(Seq(1, "1"), Seq(7, "8"))
    val rdd = sc.parallelize(data, data.length).map(s =>
      new Data1(s(0).asInstanceOf[Int], s(1).asInstanceOf[String]))
    val dataDF = snc.createDataFrame(rdd)
    dataDF.registerTempTable("temp_table")

    // snc.registerAndInsertIntoExternalStore("table_name", props)
  }
}
