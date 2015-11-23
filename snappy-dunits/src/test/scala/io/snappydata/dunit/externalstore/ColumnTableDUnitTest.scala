package io.snappydata.dunit.externalstore

import io.snappydata.dunit.cluster.ClusterManagerTestBase
import io.snappydata.dunit.cluster.ClusterManagerTestUtils

import org.apache.spark.sql.SaveMode

/**
 * Created by skumar on 20/10/15.
 */
class ColumnTableDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  def testTableCreation(): Unit = {
    // Lead is started before other servers are started
    vm1.invoke(this.getClass, "startSnappyServer", startArgs)
    vm2.invoke(this.getClass, "startSnappyServer", startArgs)
    vm3.invoke(this.getClass, "startSnappyServer", startArgs)
    Thread.sleep(5000)
    // val fullStartArgs = startArgs :+ true.asInstanceOf[AnyRef]
    vm0.invoke(this.getClass, "startSnappyLead", startArgs)

    vm0.invoke(this.getClass, "startSparkJob")
    vm0.invoke(this.getClass, "stopSpark")
  }

  def testCreateInsertAndDropOfTable(): Unit = {
    // Lead is started before other servers are started.
    vm1.invoke(this.getClass, "startSnappyServer", startArgs)

    vm2.invoke(this.getClass, "startSnappyServer", startArgs)
    vm3.invoke(this.getClass, "startSnappyServer", startArgs)
    Thread.sleep(5000)
    // val fullStartArgs = startArgs :+ true.asInstanceOf[AnyRef]
    vm0.invoke(this.getClass, "startSnappyLead", startArgs)
    vm0.invoke(this.getClass, "startSparkJob2")
    vm0.invoke(this.getClass, "stopSpark")
  }
}

/**
 * Since this object derives from ClusterManagerTestUtils
 */
object ColumnTableDUnitTest extends ClusterManagerTestUtils {
  private val tableName: String = "ColumnTable"

  val props = Map.empty[String, String]

  def startSparkJob(): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createExternalTable(tableName, "column", dataDF.schema, props)
    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect()
    assert(r.length == 0)

    snc.dropExternalTable(tableName, ifExists = true)
    logger.info("Successful")
  }

  def startSparkJob2(): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createExternalTable(tableName, "column", dataDF.schema, props)

    dataDF.write.format("column").mode(SaveMode.Append)
        .options(props).saveAsTable(tableName)

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect()
    assert(r.length == 5)

    snc.dropExternalTable(tableName, ifExists = true)
    logger.info("Successful")
  }
}

case class Data(col1: Int, col2: Int, col3: Int)
