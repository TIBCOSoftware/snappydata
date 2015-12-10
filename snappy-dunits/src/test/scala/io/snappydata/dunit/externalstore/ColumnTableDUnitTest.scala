package io.snappydata.dunit.externalstore

import com.gemstone.gemfire.internal.cache.PartitionedRegion
import com.pivotal.gemfirexd.internal.engine.Misc
import dunit.SerializableCallable
import io.snappydata.dunit.cluster.ClusterManagerTestBase

import org.apache.spark.sql.{SaveMode, SnappyContext}

/**
 * Some basic column table tests.
 *
 * Created by skumar on 20/10/15.
 */
class ColumnTableDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  def testTableCreation(): Unit = {
    startSparkJob()
  }

  def testCreateInsertAndDropOfTable(): Unit = {
    startSparkJob2()
  }

  def testSNAP205_InsertLocalBuckets(): Unit = {
    val snc = SnappyContext(sc)

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3),
      Seq(4, 2, 3), Seq(5, 6, 7), Seq(2, 8, 3), Seq(3, 9, 0))
    val rdd = sc.parallelize(data, data.length).map(
      s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    // Now column table with partition only can expect
    //local insertion. After Suranjan's change we can expect
    //cached batches to inserted locally if no partitioning is given.
    //TDOD : Merge and validate test after SNAP-105
    val p = Map[String,String]("PARTITION_BY"-> "col1")
    snc.createExternalTable(tableName, "column", dataDF.schema, p)

    // we don't expect any increase in put distribution stats
    val getPRMessageCount = new SerializableCallable[AnyRef] {
      override def call(): AnyRef = {
        Int.box(Misc.getRegion("/APP/COLUMNTABLE", true, false).
            asInstanceOf[PartitionedRegion].getPrStats.getPartitionMessagesSent)
      }
    }
    val counts = Array(vm0, vm1, vm2).map(_.invoke(getPRMessageCount))
    dataDF.write.mode(SaveMode.Append).saveAsTable(tableName)
    val newCounts = Array(vm0, vm1, vm2).map(_.invoke(getPRMessageCount))
    newCounts.zip(counts).foreach { case (c1, c2) =>
      assert(c1 == c2, s"newCount=$c1 count=$c2")
    }

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect()
    assert(r.length == 7)

    snc.dropExternalTable(tableName, ifExists = true)
    logger.info("Successful")
  }

  private val tableName: String = "ColumnTable"

  val props = Map.empty[String, String]

  def startSparkJob(): Unit = {
    val snc = SnappyContext(sc)

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
    val snc = SnappyContext(sc)

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
