package org.apache.spark.sql.store

import io.snappydata.core.SnappySQLContext
import io.snappydata.core.TestData
import org.scalatest.FunSuite

import org.apache.spark.Logging
import org.apache.spark.sql._

/**
 * Tests for ROW stores
 */
class RowRelationAPISuite extends FunSuite with Logging {

  private val testSparkContext = SnappySQLContext.sparkContext

  val props = Map(
    "url" -> "jdbc:gemfirexd:;mcast-port=33619;user=app;password=app;persist-dd=false",
    "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
    "poolImpl" -> "tomcat",
    "user" -> "app",
    "password" -> "app"
  )


  /*test("Create replicated row table with DataFrames") {

    import org.apache.spark.sql._
    val snc = org.apache.spark.sql.SnappyContext(sc)
    case class TestData(i:Int, j: String)

    val rdd = sc.parallelize((1 to 1000).map(i => TestData(i, s"$i")))
    val dataDF = snc.createDataFrame(rdd)
    snc.sql("DROP TABLE IF EXISTS row_table1")
    snc.createExternalTable("row_table1", "row", dataDF.schema, props)
    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("row_table1")
    val countdf = snc.sql("select * from row_table1")
    val count = countdf.count()
    assert(count === 1000)
  }*/

  test("Test Partitioned row tables") {
    val snc = org.apache.spark.sql.SnappyContext(testSparkContext)

    val rdd = testSparkContext.parallelize(
      (1 to 1000).map(i => TestData(i, i.toString)))

    val dataDF = snc.createDataFrame(rdd)
    snc.sql("DROP TABLE IF EXISTS row_table2")

    val df = snc.sql("CREATE TABLE row_table2(OrderId INT NOT NULL,ItemId INT) PARTITION BY COLUMN (OrderId) " +
        "USING row " +
        "options " +
        "(" +
         "partitionColumn 'OrderId'," +
        "driver 'com.pivotal.gemfirexd.jdbc.EmbeddedDriver'," +
        "URL 'jdbc:gemfirexd:;mcast-port=33620;user=app;password=app;persist-dd=false')")

    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("row_table2")
    val exp = snc.sql("select * from row_table2").explain(true)

    val countdf = snc.sql("select * from row_table2")
    val count = countdf.count()
    assert(count === 1000)
  }

  /*test("Test PreserverPartition on  row tables") {
    val snc = org.apache.spark.sql.SnappyContext(testSparkContext)


    val rdd = testSparkContext.parallelize(1 to 1000 ,113).map(i => TestData(i, i.toString))

  val k= 113
    val dataDF = snc.createDataFrame(rdd)
    snc.sql("DROP TABLE IF EXISTS row_table3")

    val df = snc.sql("CREATE TABLE row_table3(OrderId INT NOT NULL,ItemId INT) PARTITION BY COLUMN (OrderId) " +
      "USING row " +
      "options " +
      "(" +
      "partitionColumn 'OrderId'," +
      "preservepartitions 'true',"+
      "driver 'com.pivotal.gemfirexd.jdbc.EmbeddedDriver'," +
      "URL 'jdbc:gemfirexd:;mcast-port=33620;user=app;password=app;persist-dd=false')")

    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("row_table3")
    val countdf = snc.sql("select * from row_table3")
    val count = countdf.count()
    assert(count === 1000)
  }
*/

}
