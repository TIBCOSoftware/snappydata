package org.apache.spark.sql.store

import java.util.Properties

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.pivotal.gemfirexd.internal.engine.Misc
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.collection.{ExecutorLocalPartition, Utils}
import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.row.GemFireXDDialect
import org.apache.spark.sql.sources.JdbcExtendedDialect
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.{Accumulator, Partition, SparkEnv, TaskContext}

/**
  * This RDD is responsible for booting up GemFireXD store . It is needed for Spark's
  * standalone cluster.
  * For Snappy cluster,Snappy non-embedded cluster we can ingnore it.
  */
class StoreInitRDD(@transient sqlContext: SQLContext, url: String,
    val connProperties: Properties)
    (implicit param: Accumulator[Map[InternalDistributedMember, BlockManagerId]])
    extends RDD[InternalRow](sqlContext.sparkContext, Nil) {

  val driver = DriverRegistry.getDriverClassName(url)
  val isLoner = Utils.isLoner(sqlContext.sparkContext)

  GemFireCacheImpl.setColumnBatchSize(sqlContext.conf.columnBatchSize)

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    GemFireXDDialect.init()
    DriverRegistry.register(driver)
    JdbcDialects.get(url) match {
      case d: JdbcExtendedDialect =>
        val extraProps = d.extraDriverProperties(isLoner).propertyNames
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

  override def getPartitions: Array[Partition] = {
    getPeerPartitions
  }

  def getPeerPartitions(): Array[Partition] = {
    val numberedPeers = org.apache.spark.sql.collection.Utils.getAllExecutorsMemoryStatus(sqlContext.sparkContext).
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
