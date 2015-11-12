package org.apache.spark.storage

import scala.collection.mutable.ArrayBuffer


/**
 * Created by shirishd on 9/10/15.
 */
private[spark] class SnappyMemoryStore(blockManager: BlockManager, maxMemory: Long)
    extends MemoryStore(blockManager, maxMemory) {

  override def freeMemory: Long = {
    if(SnappyMemoryUtils.isEvictionUp) {
      logInfo(s"Snappy-store EVICTION UP event detected")
      0
    } else {
      super.freeMemory
    }
  }

  override def ensureFreeSpace(
      blockIdToAdd: BlockId,
      space: Long): ResultWithDroppedBlocks = {

    if (SnappyMemoryUtils.isCriticalUp) {
      logInfo(s"Will not store $blockIdToAdd as CRITICAL UP event is detected")
      val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
      return ResultWithDroppedBlocks(success = false, droppedBlocks)
    }
    super.ensureFreeSpace(blockIdToAdd, space)
  }
}



