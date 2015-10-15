package org.apache.spark.storage

import scala.collection.mutable.ArrayBuffer

import com.pivotal.gemfirexd.internal.engine.store.GemFireStore


/**
 * Created by shirishd on 9/10/15.
 */
private[spark] class SnappyMemoryStore(blockManager: BlockManager, maxMemory: Long)
    extends MemoryStore(blockManager, maxMemory) {

  override def ensureFreeSpace(
      blockIdToAdd: BlockId,
      space: Long): ResultWithDroppedBlocks = {

    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]

    println(s"GemFireStore.getBootedInstance = " + GemFireStore.getBootedInstance)

    if (SnappyMemoryStore.isCriticalUp()) {
      println(s"Will not store $blockIdToAdd as CRITICAL UP event is received")
      logInfo(s"Will not store $blockIdToAdd as CRITICAL UP event is received")
      return ResultWithDroppedBlocks(success = false, droppedBlocks)
    }
    super.ensureFreeSpace(blockIdToAdd, space)
  }

}

object SnappyMemoryStore {

  def isCriticalUp(): Boolean = {
    GemFireStore.getBootedInstance != null && GemFireStore.getBootedInstance.thresholdListener.isCritical
  }

}


