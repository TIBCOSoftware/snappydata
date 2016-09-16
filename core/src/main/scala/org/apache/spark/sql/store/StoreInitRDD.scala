/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.sql.store

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.{GemFireCacheImpl, LocalRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.Constant

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.collection.{ExecutorLocalPartition, Utils}
import org.apache.spark.sql.execution.columnar.impl.{JDBCSourceAsColumnarStore, StoreCallbacksImpl}
import org.apache.spark.sql.sources.ConnectionProperties
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.{Partition, SparkEnv, TaskContext}

/**
 * This RDD registers a store for each table being created.
 */
class StoreInitRDD(@transient private val sqlContext: SQLContext,
    table: String,
    userSchema: Option[StructType],
    partitions: Int,
    connProperties: ConnectionProperties)
    extends RDD[(InternalDistributedMember, BlockManagerId)](
      sqlContext.sparkContext, Nil) {

  val isLoner = Utils.isLoner(sqlContext.sparkContext)
  val userCompression = sqlContext.conf.useCompression
  val columnBatchSize = sqlContext.conf.columnBatchSize
  GemFireCacheImpl.setColumnBatchSizes(columnBatchSize,
    Constant.COLUMN_MIN_BATCH_SIZE)

  override def compute(split: Partition,
      context: TaskContext): Iterator[(InternalDistributedMember,
      BlockManagerId)] = {

    // TODO:Suranjan Hackish as we have to register this store at each
    // executor, for storing the CachedBatch we are creating
    // JDBCSourceAsColumnarStore without blockMap as storing at each executor
    // doesn't require blockMap
    userSchema match {
      case Some(schema) =>
        val store = new JDBCSourceAsColumnarStore(connProperties, partitions)
        StoreCallbacksImpl.registerExternalStoreAndSchema(sqlContext, table,
          schema, store, columnBatchSize, userCompression)
      case None => // row table case
        val region = Misc.getRegionForTable(table, false)
        if (region != null &&
            region.getAttributes.getDataPolicy.withReplication()) {
          region.asInstanceOf[LocalRegion].setIsUsedForUserReplicatedTable(true)
        }
    }

    GemFireCacheImpl.setColumnBatchSizes(columnBatchSize,
      Constant.COLUMN_MIN_BATCH_SIZE)
    Seq(Misc.getGemFireCache.getMyId ->
        SparkEnv.get.blockManager.blockManagerId).iterator
  }

  override def getPartitions: Array[Partition] = {
    getPeerPartitions
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    Seq(split.asInstanceOf[ExecutorLocalPartition].hostExecutorId)

  def getPeerPartitions: Array[Partition] = {
    val numberedPeers = org.apache.spark.sql.collection.Utils.
        getAllExecutorsMemoryStatus(sqlContext.sparkContext)
        .keySet.toList.zipWithIndex

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
