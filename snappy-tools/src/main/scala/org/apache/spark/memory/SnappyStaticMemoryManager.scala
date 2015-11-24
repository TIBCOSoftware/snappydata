package org.apache.spark.memory

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.storage.{BlockStatus, BlockId}

/**
 * Created by shirishd on 17/11/15.
 */
private[spark] class SnappyStaticMemoryManager(
    override val conf: SparkConf,
    override val maxExecutionMemory: Long,
    override val maxStorageMemory: Long,
    numCores: Int)
    extends StaticMemoryManager(conf, maxExecutionMemory,
      maxStorageMemory, numCores) {

  def this(conf: SparkConf, numCores: Int) {
    this(conf,
      StaticMemoryManager.getMaxExecutionMemory(conf),
      StaticMemoryManager.getMaxStorageMemory(conf),
      numCores)
  }

  override def doAcquireExecutionMemory(
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Long = synchronized {
    if (SnappyMemoryUtils.isCriticalUp || SnappyMemoryUtils.isEvictionUp) {
      0
    } else {
      super.doAcquireExecutionMemory(numBytes, evictedBlocks)
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
