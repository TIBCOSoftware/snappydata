package io.snappydata.dunit.externalstore

import io.snappydata.dunit.cluster.ClusterManagerTestBase

import org.apache.spark.sql.SaveMode

/**
 * Created by rishim on 6/11/15.
 */
class RowTableDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  def testTableCreation(): Unit = {
    startSparkJob()
  }

  def testCreateInsertAndDropOfTable(): Unit = {
    startSparkJob2()
  }

  private val tableName: String = "RowTable"

  val props = Map.empty[String, String]

  def startSparkJob(): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new RowData(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable(tableName, "row", dataDF.schema, props)
    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect()
    assert(r.length == 0)

    snc.dropTable(tableName, ifExists = true)
    logger.info("Successful")
  }

  def startSparkJob2(): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new RowData(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable(tableName, "row", dataDF.schema, props)

    dataDF.write.format("row").mode(SaveMode.Append)
        .options(props).saveAsTable(tableName)

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect()
    assert(r.length == 5)

    snc.dropTable(tableName, ifExists = true)
    logger.info("Successful")
  }
}

case class RowData(col1: Int, col2: Int, col3: Int)
