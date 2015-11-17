package org.apache.spark.memory

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.storage.{BlockStatus, BlockId}

/**
 * Created by shirishd on 17/11/15.
 */
private[spark] class SnappyStaticMemoryManager(override val conf: SparkConf,
    override val maxExecutionMemory: Long,
    override val maxStorageMemory: Long) extends StaticMemoryManager(
  conf: SparkConf, maxExecutionMemory, maxStorageMemory) {

  def this(conf: SparkConf) {
    this(
      conf,
      StaticMemoryManager.getMaxExecutionMemory(conf),
      StaticMemoryManager.getMaxStorageMemory(conf))
  }

  override def acquireExecutionMemory(
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Long = synchronized {
    if (SnappyMemoryUtils.isCriticalUp || SnappyMemoryUtils.isEvictionUp) {
      0
    } else {
      super.acquireExecutionMemory(numBytes, evictedBlocks)
    }
  }

  override private[spark] def acquireStorageMemory(
      blockId: BlockId,
      numBytesToAcquire: Long,
      numBytesToFree: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean = synchronized {
    if (SnappyMemoryUtils.isCriticalUp || SnappyMemoryUtils.isEvictionUp) {
      false
    } else {
      super.acquireStorageMemory(blockId, numBytesToAcquire, numBytesToFree, evictedBlocks)
    }
  }
}
