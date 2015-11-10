package io.snappydata.dunit.externalstore

import io.snappydata.dunit.cluster.{ClusterManagerTestUtils, ClusterManagerTestBase}

import org.apache.spark.sql.SaveMode

/**
 * Created by rishim on 6/11/15.
 */
class RowTableDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  def testTableCreation(): Unit = {
    // Lead is started before other servers are started
    vm1.invoke(this.getClass, "startSnappyServer", startArgs)
    vm2.invoke(this.getClass, "startSnappyServer", startArgs)
    vm3.invoke(this.getClass, "startSnappyServer", startArgs)
    Thread.sleep(5000)
    vm0.invoke(this.getClass, "startSnappyLead", startArgs)

    vm0.invoke(this.getClass, "startSparkJob")
  }

  def testCreateInsertAndDropOfTable(): Unit = {
    // Lead is started before other servers are started.
    vm1.invoke(this.getClass, "startSnappyServer", startArgs)

    vm2.invoke(this.getClass, "startSnappyServer", startArgs)
    vm3.invoke(this.getClass, "startSnappyServer", startArgs)
    Thread.sleep(5000)
    vm0.invoke(this.getClass, "startSnappyLead", startArgs)
    vm0.invoke(this.getClass, "startSparkJob2")
  }
}

/**
 * Since this object derives from ClusterManagerTestUtils
 */
object RowTableDUnitTest extends ClusterManagerTestUtils {
  private val tableName: String = "RowTable"

  val props = Map.empty[String, String]

  def startSparkJob(): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new RowData(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createExternalTable(tableName, "row", dataDF.schema, props)
    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect()
    assert(r.length == 0)

    snc.dropExternalTable(tableName, ifExists = true)
    println("Successful")
  }

  def startSparkJob2(): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new RowData(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createExternalTable(tableName, "row", dataDF.schema, props)

    dataDF.write.format("row").mode(SaveMode.Append)
        .options(props).saveAsTable(tableName)

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect()
    assert(r.length == 5)

    snc.dropExternalTable(tableName, ifExists = true)
    println("Successful")
  }
}

case class RowData(col1: Int, col2: Int, col3: Int)
