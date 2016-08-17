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
package org.apache.spark.memory

import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.pivotal.gemfirexd.internal.engine.Misc
import org.apache.spark.SparkConf
import scala.collection.mutable

import org.apache.spark.storage.{BlockStatus, BlockId}

/**
  * When there is request for execution or storage memory, critical up and eviction up
  * events are checked. If they are set, try to free the memory cached by Spark rdds
  * by calling memoryStore.evictBlocksToFreeSpace. If enough memory cannot be freed,
  * return the call and let Spark take a corrective action.
  * In such cases Spark either fails the task or move the current RDDs data to disk.
  * If the critical and eviction events are not set, it asks the UnifiedMemoryManager
  * to allocate the space.
  * @param conf
  * @param maxHeapMemory
  * @param numCores
  */
class SnappyUnifiedMemoryManager private[memory](
    conf: SparkConf,
    override val maxHeapMemory: Long,
    numCores: Int) extends UnifiedMemoryManager(conf,
  maxHeapMemory,
  (maxHeapMemory * conf.getDouble("spark.memory.storageFraction", 0.5)).toLong,
  numCores) {

  def this(conf: SparkConf, numCores: Int) = {
    this( conf,
      SnappyUnifiedMemoryManager.getMaxMemory(conf),
      numCores)
  }
  private val listener = Misc.getMemStoreBooting.thresholdListener()

  /**
   * if freeMemory has been invoked from acquireStorageMemory, return false if
   * evictBlocksToFreeSpace fails and evictionUp is still true
   * if freeMemory() has been invoked from acquireExecutionMemory then
   * only fail if criticalUp is still true after evictBlocksToFreeSpace
   */
  private def freeMemory(blockId: Option[BlockId], numBytes: Long,
    memoryMode: MemoryMode, storageMemory: Boolean): Boolean = {
    val freeMemory = Runtime.getRuntime.freeMemory()
    if (freeMemory < numBytes) {
      val storageMemoryPool = memoryMode match {
        case MemoryMode.ON_HEAP =>
          onHeapStorageMemoryPool
        case MemoryMode.OFF_HEAP =>
          offHeapStorageMemoryPool
      }
      val requestedSpace = numBytes - freeMemory
      val spaceFreed = storageMemoryPool.memoryStore.evictBlocksToFreeSpace(blockId,
        requestedSpace, memoryMode)
      if (storageMemory) {
        (spaceFreed >= requestedSpace) || !listener.isEviction
      } else {
        !listener.isCritical
      }
    }
    false
  }

  override private[memory] def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long = synchronized {
    assert(numBytes >= 0)
    if (listener.isEviction || listener.isCritical) {
      val evictedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
      // If free memory not available, let Spark
      // take a corrective action
      if (!freeMemory(None, numBytes, memoryMode, false)) {
        return 0
      }
    }
    super.acquireExecutionMemory(numBytes, taskAttemptId, memoryMode)
  }

  override def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    if (listener.isEviction || listener.isCritical) {
      // If free memory not available, let Spark
      // take a corrective action
      if (!freeMemory(Some(blockId), numBytes, memoryMode, true)){
        return false
      }
    }
    super.acquireStorageMemory(blockId, numBytes, memoryMode)
  }
}

private object SnappyUnifiedMemoryManager {

  private val RESERVED_SYSTEM_MEMORY_BYTES = 300 * 1024 * 1024

  /**
   * Return the total amount of memory shared between execution and storage, in bytes.
   * This is a direct copy from UnifiedMemorymanager with an extra check for evit fraction
   */
  private def getMaxMemory(conf: SparkConf): Long = {
    val systemMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)
    val reservedMemory = conf.getLong("spark.testing.reservedMemory",
      if (conf.contains("spark.testing")) 0 else RESERVED_SYSTEM_MEMORY_BYTES)
    val minSystemMemory = (reservedMemory * 1.5).ceil.toLong
    if (systemMemory < minSystemMemory) {
      throw new IllegalArgumentException(s"System memory $systemMemory must " +
        s"be at least $minSystemMemory. Please increase heap size using the --driver-memory " +
        s"option or spark.driver.memory in Spark configuration.")
    }
    // SPARK-12759 Check executor memory to fail fast if memory is insufficient
    if (conf.contains("spark.executor.memory")) {
      val executorMemory = conf.getSizeAsBytes("spark.executor.memory")
      if (executorMemory < minSystemMemory) {
        throw new IllegalArgumentException(s"Executor memory $executorMemory must be at least " +
          s"$minSystemMemory. Please increase executor memory using the " +
          s"--executor-memory option or spark.executor.memory in Spark configuration.")
      }
    }
    val usableMemory = systemMemory - reservedMemory
    // GemFireXD is required to be already booted before constructing
    // snappy memory manager
    var evictFraction: Double = GemFireCacheImpl.getExisting.getResourceManager
      .getEvictionHeapPercentage/100
    if (evictFraction <= 0.0) {
      evictFraction = 0.75
    }
    val memoryFraction = conf.getDouble("spark.memory.fraction", evictFraction)
    (usableMemory * memoryFraction).toLong
  }

}
