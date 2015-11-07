package org.apache.spark.sql.store

import java.util.Properties

import com.pivotal.gemfirexd.internal.engine.Misc

import org.apache.spark.scheduler.cluster.SnappyCoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.local.LocalBackend
import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.collection.ExecutorLocalPartition
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcUtils, DriverRegistry}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.row.GemFireXDDialect
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import org.apache.spark.sql.sources.JdbcExtendedDialect
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.{Accumulator, AccumulatorParam, SparkEnv, TaskContext, Partition, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * This RDD is responsible for booting up GemFireXD store . It is needed for Spark's standalone cluster.
 * For Snappy cluster,Snappy non-embedded cluster we can ingnore it.
 */
class StoreInitRDD(@transient sc: SparkContext, url: String,
    val connProperties: Properties)
    (implicit param: Accumulator[Map[InternalDistributedMember, BlockManagerId]])
    extends RDD[InternalRow](sc, Nil) {

  val snc = SnappyContext(sc)
  val driver = DriverRegistry.getDriverClassName(url)
  val isLoner = snc.isLoner


  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    GemFireXDDialect.init()
    DriverRegistry.register(driver)
    JdbcDialects.get(url) match {
      case d: JdbcExtendedDialect =>
        val extraProps = d.extraCreateTableProperties(isLoner).propertyNames
        while (extraProps.hasMoreElements) {
          val p = extraProps.nextElement()
          if (connProperties.get(p) != null) {
            sys.error(s"Master specific property $p " +
                "shouldn't exist here in Executors")
          }
        }
    }
    val conn = JdbcUtils.createConnection(url, connProperties)
    conn.close()
    param += Map(Misc.getGemFireCache.getMyId -> SparkEnv.get.blockManager.blockManagerId)
    Iterator.empty
  }

  protected def getSparkPartitions: Array[Partition] = {
    //TODO : Find a cleaner way of starting all executors.
    val partitions = new Array[Partition](100)
    for (p <- 0 until 100) {
      partitions(p) = new ExecutorLocalPartition(p, null)
    }
    partitions
  }

  override def getPartitions: Array[Partition] = {
    sc.schedulerBackend match {
      case lb: SnappyCoarseGrainedSchedulerBackend => getPeerPartitions
      case _ => getSparkPartitions
    }
  }

  def getPeerPartitions(): Array[Partition] = {
    val numberedPeers = org.apache.spark.sql.collection.Utils.getAllExecutorsMemoryStatus(sc).
        keySet.zipWithIndex

    if (numberedPeers.nonEmpty) {
      numberedPeers.map {
        case (bid, idx) => createPartition(idx, bid)
      }.toArray[Partition]
    } else {
      Array.empty[Partition]
    }
  }

  def createPartition(index: Int,
      blockId: BlockManagerId): ExecutorLocalPartition =
    new ExecutorLocalPartition(index, blockId)


}
