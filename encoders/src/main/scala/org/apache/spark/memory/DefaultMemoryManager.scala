/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS  IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.memory

import java.nio.ByteBuffer

import com.gemstone.gemfire.internal.snappy.UMMMemoryTracker
import com.gemstone.gemfire.internal.snappy.memory.MemoryManagerStats
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap

import org.apache.spark.storage.BlockId
import org.apache.spark.{Logging, SparkEnv}


class DefaultMemoryManager extends StoreUnifiedManager with Logging {

  private val memoryForObject: ObjectLongHashMap[MemoryOwner] =
    new ObjectLongHashMap[MemoryOwner](16)

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
      env.memoryManager.synchronized {
        val success = env.memoryManager.acquireStorageMemory(blockId, numBytes, memoryMode)
        memoryForObject.addToValue(new MemoryOwner(objectName, memoryMode), numBytes)
        success
      }
    } else {
      true
    }
  }

  // This is not be called in connector mode. This api is called from
  // region.clear hence only applicable for local mode
  override def dropStorageMemoryForObject(
      objectName: String,
      memoryMode: MemoryMode,
      ignoreNumBytes: Long): Long = {
    val env = SparkEnv.get
    if (env ne null) {
      env.memoryManager.synchronized {
        val key = new MemoryOwner(objectName, memoryMode)
        val bytesToBeFreed = memoryForObject.get(key)
        val numBytes = Math.max(0, bytesToBeFreed - ignoreNumBytes)
        logDebug(s"Dropping $managerId memory for $objectName =" +
            s" $numBytes (registered=$bytesToBeFreed)")
        if (numBytes > 0) {
          env.memoryManager.releaseStorageMemory(numBytes, memoryMode)
          memoryForObject.removeKey(key)
        }
      }
    }
    0L
  }

  override def releaseStorageMemoryForObject(
      objectName: String,
      numBytes: Long,
      memoryMode: MemoryMode): Unit = {
    logDebug(s"Releasing DefaultManager memory for $objectName $numBytes")
    val env = SparkEnv.get
    if (env ne null) {
      env.memoryManager.synchronized {
        env.memoryManager.releaseStorageMemory(numBytes, memoryMode)
        val key = new MemoryOwner(objectName, memoryMode)
        if (memoryForObject.containsKey(key)) {
          if (memoryForObject.addToValue(key, -numBytes) <= 0) {
            memoryForObject.removeKey(key)
          }
        }
      }
    }
  }

  override def getStoragePoolMemoryUsed(memoryMode: MemoryMode): Long = 0L

  override def getStoragePoolSize(memoryMode: MemoryMode): Long = 0L

  override def getExecutionPoolUsedMemory(memoryMode: MemoryMode): Long = 0L

  override def getExecutionPoolSize(memoryMode: MemoryMode): Long = 0L

  override def getOffHeapMemory(objectName: String): Long = {
    val env = SparkEnv.get
    if (env ne null) {
      env.memoryManager.synchronized {
        memoryForObject.get(MemoryOwner(objectName, offHeap = true))
      }
    }
    0L
  }

  override def hasOffHeap: Boolean = {
    val env = SparkEnv.get
    if (env ne null) {
      env.memoryManager.tungstenMemoryMode eq MemoryMode.OFF_HEAP
    }
    false
  }


  override def logStats(): Unit = logInfo("No stats for NoOpSnappyMemoryManager")

  override def changeOffHeapOwnerToStorage(buffer: ByteBuffer,
      allowNonAllocator: Boolean): Unit = {}

  override def shouldStopRecovery(): Boolean = false

  override def bootManager: Boolean = false

  override def initMemoryStats(stats: MemoryManagerStats): Unit = {}

  override def close(): Unit = {}

  override def clear(): Unit = {
    memoryForObject.clear()
  }

  /**
    * Initializes the memoryManager
    */
  override def init(): Unit = {}
}

case class MemoryOwner(owner: String, offHeap: Boolean) {

  def this(owner: String, mode: MemoryMode) = {
    this(owner, mode eq MemoryMode.OFF_HEAP)
  }

  override def hashCode(): Int = {
    val h = owner.hashCode
    if (offHeap) -h else h
  }

  override def toString: String = owner + (if (offHeap) "(OFFHEAP)" else "(HEAP)")
}
