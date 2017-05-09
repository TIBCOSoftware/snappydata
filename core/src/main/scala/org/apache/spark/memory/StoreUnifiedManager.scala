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

import java.nio.ByteBuffer

import scala.collection.mutable

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.gemstone.gemfire.internal.snappy.UMMMemoryTracker

import org.apache.spark.storage.{BlockId, TestBlockId}
import org.apache.spark.{Logging, SparkEnv}


trait StoreUnifiedManager {

  def acquireStorageMemoryForObject(
      objectName : String,
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode,
      buffer: UMMMemoryTracker,
      shouldEvict: Boolean): Boolean

  def dropStorageMemoryForObject(objectName : String, memoryMode: MemoryMode,
                                 ignoreNumBytes : Long): Long

  def releaseStorageMemoryForObject(objectName : String, numBytes: Long,
                                    memoryMode: MemoryMode): Unit

  def getStoragePoolMemoryUsed(memoryMode: MemoryMode): Long
  def getStoragePoolSize(memoryMode: MemoryMode): Long
  def getExecutionPoolUsedMemory(memoryMode: MemoryMode): Long
  def getExecutionPoolSize(memoryMode: MemoryMode): Long
  def getOffHeapMemory(objectName: String): Long

  /**
   * Change the off-heap owner to mark it being used for storage.
   * Passing the owner as null allows moving ByteBuffers not allocated
   * by [[com.gemstone.gemfire.internal.cache.store.BufferAllocator]]s
   * to be also changed and freshly accounted.
   */
  def changeOffHeapOwnerToStorage(buffer: ByteBuffer,
      allowNonAllocator: Boolean): Unit
}

/**
  * This class will store all the memory usage for GemFireXD boot up time when SparkEnv
  * is not initialised.
  * This class will not actually allocate any memory.
  * It is just a temp account holder till SnappyUnifiedManager is started.
  */
class TempMemoryManager extends StoreUnifiedManager with Logging{

  val memoryForObject = new mutable.HashMap[String, Long]()

  override def acquireStorageMemoryForObject(objectName: String,
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode,
      buffer: UMMMemoryTracker,
      shouldEvict: Boolean): Boolean = synchronized {
    logDebug(s"Acquiring mem [TEMP] for $objectName $numBytes")
    if (!memoryForObject.contains(objectName)) {
      memoryForObject(objectName) = 0L
    }
    memoryForObject(objectName) += numBytes
    true
  }

  override def dropStorageMemoryForObject(
      objectName: String,
      memoryMode: MemoryMode,
      ignoreNumBytes : Long): Long = synchronized {
    memoryForObject.remove(objectName).getOrElse(0L)
  }

  override def releaseStorageMemoryForObject(
      objectName: String,
      numBytes: Long,
      memoryMode: MemoryMode): Unit = synchronized {
    memoryForObject(objectName) -= numBytes
  }

  override def getStoragePoolMemoryUsed(memoryMode: MemoryMode): Long = 0L

  override def getStoragePoolSize(memoryMode: MemoryMode): Long = 0L

  override def getExecutionPoolUsedMemory(memoryMode: MemoryMode): Long = 0L

  override def getExecutionPoolSize(memoryMode: MemoryMode): Long = 0L

  override def getOffHeapMemory(objectName: String): Long = 0L

  override def changeOffHeapOwnerToStorage(buffer: ByteBuffer,
      allowNonAllocator: Boolean): Unit = {}
}


class NoOpSnappyMemoryManager extends StoreUnifiedManager with Logging {

  override def acquireStorageMemoryForObject(objectName: String,
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode,
      buffer: UMMMemoryTracker,
      shouldEvict: Boolean): Boolean = {
    logDebug(s"Acquiring mem [NO-OP] for $objectName $numBytes")
    true
  }

  override def dropStorageMemoryForObject(
      objectName: String,
      memoryMode: MemoryMode,
      ignoreNumBytes : Long): Long = 0L

  override def releaseStorageMemoryForObject(
      objectName: String,
      numBytes: Long,
      memoryMode: MemoryMode): Unit = {}

  override def getStoragePoolMemoryUsed(memoryMode: MemoryMode): Long = 0L

  override def getStoragePoolSize(memoryMode: MemoryMode): Long = 0L

  override def getExecutionPoolUsedMemory(memoryMode: MemoryMode): Long = 0L

  override def getExecutionPoolSize(memoryMode: MemoryMode): Long = 0L

  override def getOffHeapMemory(objectName: String): Long = 0L

  override def changeOffHeapOwnerToStorage(buffer: ByteBuffer,
      allowNonAllocator: Boolean): Unit = {}
}

object MemoryManagerCallback extends Logging {

  val storageBlockId = TestBlockId("SNAPPY_STORAGE_BLOCK_ID")

  val tempMemoryManager = new TempMemoryManager
  @volatile private var snappyUnifiedManager: StoreUnifiedManager = _
  private val noOpMemoryManager : StoreUnifiedManager = new NoOpSnappyMemoryManager

  def resetMemoryManager(): Unit = synchronized {
    tempMemoryManager.memoryForObject.clear()
    snappyUnifiedManager = null // For local mode testing
  }

  private final val isCluster = {
    try {
      org.apache.spark.util.Utils.classForName(
        "org.apache.spark.memory.SnappyUnifiedMemoryManager")
      // Class is loaded means we are running in SnappyCluster mode.

      true
    } catch {
      case _: ClassNotFoundException =>
        logWarning("MemoryManagerCallback couldn't be INITIALIZED." +
            "SnappyUnifiedMemoryManager won't be used.")
        false
    }
  }

  def memoryManager: StoreUnifiedManager = {
    val manager = snappyUnifiedManager
    if ((manager ne null) && isCluster) manager
    else getMemoryManager
  }

  private def getMemoryManager: StoreUnifiedManager = synchronized {
    // First check if SnappyUnifiedManager is set. If yes no need to look further.
    if (isCluster && (snappyUnifiedManager ne null)) {
      return snappyUnifiedManager
    }
    if (!isCluster) {
      return noOpMemoryManager
    }

    if (GemFireCacheImpl.getInstance ne null) {
      val env = SparkEnv.get
      if (env ne null) {
        env.memoryManager match {
          case unifiedManager: StoreUnifiedManager =>
            snappyUnifiedManager = unifiedManager
            unifiedManager
          case _ =>
            // For testing purpose if we want to disable SnappyUnifiedManager
            noOpMemoryManager
        }
      } else {
        tempMemoryManager
      }
    } else {
      // Till GemXD Boot Time
      tempMemoryManager
    }
  }
}


