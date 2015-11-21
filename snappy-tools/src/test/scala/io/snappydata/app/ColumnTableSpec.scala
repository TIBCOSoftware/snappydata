package io.snappydata.app

import io.snappydata.core.{TestSqlContext, LocalSQLContext}

import org.apache.spark.sql.SnappyContext
import org.apache.spark.{SparkContext, Logging}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
 * Created by skumar on 17/9/15
 */
class ColumnTablePersistSpec extends FunSuite with Logging with BeforeAndAfterAll {

  var sc: SparkContext = null

  override def afterAll(): Unit = {
    SnappyContext.stop()
  }

  override def beforeAll(): Unit = {
    if (sc == null) {
      sc = TestSqlContext.newSparkContext
    }
  }

  test( """ This test will create simple table""") {

    val snc = org.apache.spark.sql.SnappyContext(sc)
    val props = Map(
      "url" -> "jdbc:gemfirexd:;locators=10.112.204.54:10101;user=app;password=app",
      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
      "poolimpl" -> "tomcat",
      "user" -> "app",
      "password" -> "app"
    )

    val data = Seq(Seq(1, "1"), Seq(7, "8"))
    val rdd = sc.parallelize(data, data.length).map(s => new Data1(s(0).asInstanceOf[Int], s(1).asInstanceOf[String]))
    val dataDF = snc.createDataFrame(rdd)
    dataDF.registerTempTable("temp_table")

    //snc.registerAndInsertIntoExternalStore("table_name", props)
  }
}
