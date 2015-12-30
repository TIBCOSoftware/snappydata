package org.apache.spark.sql.store

import java.util.Properties

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.pivotal.gemfirexd.internal.engine.Misc

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.collection.{ExecutorLocalPartition, Utils}
import org.apache.spark.sql.columnar.ConnectionProperties
import org.apache.spark.sql.columntable.StoreCallbacksImpl
import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.row.GemFireXDDialect
import org.apache.spark.sql.sources.JdbcExtendedDialect
import org.apache.spark.sql.store.impl.JDBCSourceAsColumnarStore
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.{Accumulator, Partition, SparkEnv, TaskContext}

/**
  * This RDD is responsible for booting up GemFireXD store . It is needed for Spark's
  * standalone cluster.
  * For Snappy cluster,Snappy non-embedded cluster we can ingnore it.
  */


class StoreInitRDD(@transient sqlContext: SQLContext,
    table: String,
    userSchema: Option[StructType],
    partitions:Int,
    connProperties:ConnectionProperties
    )
    extends RDD[(InternalDistributedMember, BlockManagerId)](sqlContext.sparkContext, Nil) {


  val driver = DriverRegistry.getDriverClassName(connProperties.url)
  val isLoner = Utils.isLoner(sqlContext.sparkContext)
  val userCompression = sqlContext.conf.useCompression
  val columnBatchSize = sqlContext.conf.columnBatchSize
  GemFireCacheImpl.setColumnBatchSize(columnBatchSize)

  override def compute(split: Partition, context: TaskContext): Iterator[(InternalDistributedMember, BlockManagerId)] = {
    GemFireXDDialect.init()
    DriverRegistry.register(driver)

    //TODO:Suranjan Hackish as we have to register this store at each executor, for storing the cachedbatch
    // We are creating JDBCSourceAsColumnarStore without blockMap as storing at each executor
    // doesn't require blockMap
    userSchema match {
      case Some(schema) =>
        val store = new JDBCSourceAsColumnarStore(connProperties,partitions)
        StoreCallbacksImpl.registerExternalStoreAndSchema(sqlContext, table.toUpperCase,
          schema, store, columnBatchSize, userCompression)
      case None =>
    }

    JdbcDialects.get(connProperties.url) match {
      case d: JdbcExtendedDialect =>
        val extraProps = d.extraDriverProperties(isLoner).propertyNames
        while (extraProps.hasMoreElements) {
          val p = extraProps.nextElement()
          if (connProperties.connProps.get(p) != null) {
            sys.error(s"Master specific property $p " +
                "shouldn't exist here in Executors")
          }
        }
    }
    val conn = JdbcUtils.createConnection(connProperties.url, connProperties.connProps)
    conn.close()
    GemFireCacheImpl.setColumnBatchSize(columnBatchSize)
    Seq((Misc.getGemFireCache.getMyId -> SparkEnv.get.blockManager.blockManagerId)).iterator

  }

  override def getPartitions: Array[Partition] = {
    getPeerPartitions
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    Seq(split.asInstanceOf[ExecutorLocalPartition].hostExecutorId)

  def getPeerPartitions: Array[Partition] = {
    val numberedPeers = org.apache.spark.sql.collection.Utils.
        getAllExecutorsMemoryStatus(sqlContext.sparkContext).keySet.zipWithIndex

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
