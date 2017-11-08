/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import java.nio.ByteBuffer

import com.gemstone.gemfire.internal.snappy.UMMMemoryTracker
import com.gemstone.gemfire.internal.snappy.memory.MemoryManagerStats
import io.snappydata.collection.ObjectLongHashMap

import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.storage.BlockId


class DefaultMemoryManager extends StoreUnifiedManager with Logging {

  private val memoryForObject: ObjectLongHashMap[(String, MemoryMode)]
  = ObjectLongHashMap.withExpectedSize[(String, MemoryMode)](16)


  private val managerId = "DefaultMemoryManager"

  override def acquireStorageMemoryForObject(objectName: String,
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode,
      buffer: UMMMemoryTracker,
      shouldEvict: Boolean): Boolean = synchronized {
    logDebug(s"Acquiring DefaultManager memory for $objectName $numBytes")
    val env = SparkEnv.get
    if (env ne null) {
      val success = env.memoryManager.acquireStorageMemory(blockId, numBytes, memoryMode)
      memoryForObject.addTo(objectName -> memoryMode, numBytes)
      success
    } else {
      true
    }
  }

  // This is not be called in connector mode. This api is called from
  // region.clear hence only applicable for local mode
  override def dropStorageMemoryForObject(
      objectName: String,
      memoryMode: MemoryMode,
      ignoreNumBytes: Long): Long = synchronized {
    val env = SparkEnv.get
    if (env ne null) {
      val key = objectName -> memoryMode
      val bytesToBeFreed = memoryForObject.getLong(key)
      val numBytes = Math.max(0, bytesToBeFreed - ignoreNumBytes)
      logDebug(s"Dropping $managerId memory for $objectName =" +
          s" $numBytes (registered=$bytesToBeFreed)")
      if (numBytes > 0) {
        env.memoryManager.releaseStorageMemory(numBytes, memoryMode)
        memoryForObject.removeAsLong(key)
      }
    }
    0L
  }

  override def releaseStorageMemoryForObject(
      objectName: String,
      numBytes: Long,
      memoryMode: MemoryMode): Unit = synchronized {
    logDebug(s"Releasing DefaultManager memory for $objectName $numBytes")
    val env = SparkEnv.get
    if (env ne null) {
      env.memoryManager.releaseStorageMemory(numBytes, memoryMode)
      val key = objectName -> memoryMode
      if (memoryForObject.containsKey(key)) {
        if (memoryForObject.addTo(key, -numBytes) == numBytes) {
          memoryForObject.removeAsLong(key)
        }
      }
    }
  }

  override def getStoragePoolMemoryUsed(memoryMode: MemoryMode): Long = 0L

  override def getStoragePoolSize(memoryMode: MemoryMode): Long = 0L

  override def getExecutionPoolUsedMemory(memoryMode: MemoryMode): Long = 0L

  override def getExecutionPoolSize(memoryMode: MemoryMode): Long = 0L

  override def getOffHeapMemory(objectName: String): Long = 0L

  override def hasOffHeap: Boolean = false

  override def logStats(): Unit = logInfo("No stats for NoOpSnappyMemoryManager")

  override def changeOffHeapOwnerToStorage(buffer: ByteBuffer,
      allowNonAllocator: Boolean): Unit = {}

  override def shouldStopRecovery(): Boolean = false

  override def initMemoryStats(stats: MemoryManagerStats): Unit = {}

  override def close(): Unit = {}

  /**
    * Clears the internal map
    */
  override def clear(): Unit = {}
}
