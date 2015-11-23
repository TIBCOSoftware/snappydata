package io.snappydata.dunit.externalstore

import io.snappydata.dunit.cluster.ClusterManagerTestBase
import io.snappydata.dunit.cluster.ClusterManagerTestUtils

import org.apache.spark.sql.SaveMode

import scala.util.Random

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


  def testCreateInsertAndDropOfTableProjectionQuery(): Unit = {
    // Lead is started before other servers are started.
    vm1.invoke(this.getClass, "startSnappyServer", startArgs)

    vm2.invoke(this.getClass, "startSnappyServer", startArgs)
    vm3.invoke(this.getClass, "startSnappyServer", startArgs)
    Thread.sleep(5000)
    // val fullStartArgs = startArgs :+ true.asInstanceOf[AnyRef]
    vm0.invoke(this.getClass, "startSnappyLead", startArgs)
    vm0.invoke(this.getClass, "startSparkJob3")
    vm0.invoke(this.getClass, "stopSpark")
  }

  def testCreateInsertAndDropOfTableWithPartition(): Unit = {
    // Lead is started before other servers are started.
    vm1.invoke(this.getClass, "startSnappyServer", startArgs)

    vm2.invoke(this.getClass, "startSnappyServer", startArgs)
    vm3.invoke(this.getClass, "startSnappyServer", startArgs)
    Thread.sleep(5000)
    // val fullStartArgs = startArgs :+ true.asInstanceOf[AnyRef]
    vm0.invoke(this.getClass, "startSnappyLead", startArgs)
    vm0.invoke(this.getClass, "startSparkJob4")
    vm0.invoke(this.getClass, "stopSpark")
  }

}

/**
 * Since this object derives from ClusterManagerTestUtils
 */
object ColumnTableDUnitTest extends ClusterManagerTestUtils {
  private val tableName: String = "ColumnTable"
  private val tableNameWithPartition: String = "ColumnTablePartition"

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

    var data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    1 to 1000 foreach { _ =>
      data = data :+ Seq.fill(3)(Random.nextInt)
    }

    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createExternalTable(tableName, "column", dataDF.schema, props)

    dataDF.write.format("column").mode(SaveMode.Append)
        .options(props).saveAsTable(tableName)


    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect()

    assert(r.length == 1005)

    snc.dropExternalTable(tableName, ifExists = true)
    logger.info("Successful")
  }

  def startSparkJob3(): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    var data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    1 to 1000 foreach { _ =>
      data = data :+ Seq.fill(3)(Random.nextInt)
    }

    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createExternalTable(tableName, "column", dataDF.schema, props)

    dataDF.write.format("column").mode(SaveMode.Append)
        .options(props).saveAsTable(tableName)


    val result = snc.sql("SELECT col2 FROM " + tableName)
    val r = result.collect()

    assert(r.length == 1005)

    snc.dropExternalTable(tableName, ifExists = true)
    logger.info("Successful")
  }

  def startSparkJob4(): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    snc.sql(s"CREATE TABLE $tableNameWithPartition(Key1 INT ,Value STRING, other1 STRING, other2 STRING )" +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Key1'," +
        "BUCKETS '2'," +
        "REDUNDANCY '2')")

    val data = Seq(Seq(1, 2, 3, 4), Seq(7, 8, 9, 4), Seq(9, 2, 3, 4), Seq(4, 2, 3, 4), Seq(5, 6, 7, 4))

    val rdd = sc.parallelize(data, data.length).map(s => new PartitionData(s(0),
      s(1).toString, s(2).toString, s(3).toString))
    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.format("column").mode(SaveMode.Append)
        .options(props).saveAsTable(tableNameWithPartition)


    var result = snc.sql("SELECT Value FROM " + tableNameWithPartition)
    var r = result.collect()
    assert(r.length == 5)

    result = snc.sql("SELECT other1 FROM " + tableNameWithPartition)
    r = result.collect()
    val colValues = Seq(3 ,9, 3, 3, 7)
    val resultValues = r map{ row =>
      row.getString(0).toInt
    }
    assert(resultValues.length == 5)
    colValues.foreach(v => assert(resultValues.contains(v)))
    resultValues.foreach(v => assert(colValues.contains(v)))

    snc.dropExternalTable(tableNameWithPartition, ifExists = true)
    logger.info("Successful")
  }
}

case class Data(col1: Int, col2: Int, col3: Int)

case class PartitionData(col1: Int, Value: String, other1: String, other2: String)
