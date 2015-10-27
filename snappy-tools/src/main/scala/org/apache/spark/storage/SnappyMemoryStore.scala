package org.apache.spark.storage

import scala.collection.mutable.ArrayBuffer

import com.pivotal.gemfirexd.internal.engine.store.GemFireStore


/**
 * Created by shirishd on 9/10/15.
 */
private[spark] class SnappyMemoryStore(blockManager: BlockManager, maxMemory: Long)
    extends MemoryStore(blockManager, maxMemory) {

  override def freeMemory: Long = {
    if(SnappyMemoryStore.isEvictionUp()) {
      logInfo(s"Snappy-store EVICTION UP event detected")
      0
    } else {
      super.freeMemory
    }
  }

  override def ensureFreeSpace(
      blockIdToAdd: BlockId,
      space: Long): ResultWithDroppedBlocks = {

    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]

    if (SnappyMemoryStore.isCriticalUp()) {
      logInfo(s"Will not store $blockIdToAdd as CRITICAL UP event is detected")
      return ResultWithDroppedBlocks(success = false, droppedBlocks)
    }
    super.ensureFreeSpace(blockIdToAdd, space)
  }
}

object SnappyMemoryStore {

  def isCriticalUp(): Boolean = {
    GemFireStore.getBootedInstance != null && GemFireStore.getBootedInstance.thresholdListener.isCritical
  }

  def isEvictionUp(): Boolean = {
    GemFireStore.getBootedInstance != null && GemFireStore.getBootedInstance.thresholdListener.isEviction
  }

}


