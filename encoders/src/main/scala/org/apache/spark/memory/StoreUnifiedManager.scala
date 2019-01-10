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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.memory

import java.nio.{ByteBuffer, ByteOrder}

import com.gemstone.gemfire.SystemFailure
import com.gemstone.gemfire.internal.cache.LocalRegion
import com.gemstone.gemfire.internal.shared.BufferAllocator
import com.gemstone.gemfire.internal.shared.unsafe.FreeMemory
import com.gemstone.gemfire.internal.snappy.UMMMemoryTracker
import com.gemstone.gemfire.internal.snappy.memory.MemoryManagerStats
import org.slf4j.LoggerFactory

import org.apache.spark.sql.collection.SharedUtils
import org.apache.spark.storage.{BlockId, TestBlockId}
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkConf, SparkEnv}

/**
  * Base trait for different memory manager used by SnappyData in different modes
  */
trait StoreUnifiedManager {

  def acquireStorageMemoryForObject(
      objectName: String,
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode,
      buffer: UMMMemoryTracker,
      shouldEvict: Boolean): Boolean

  def dropStorageMemoryForObject(objectName: String, memoryMode: MemoryMode,
      ignoreNumBytes: Long): Long

  def releaseStorageMemoryForObject(objectName: String, numBytes: Long,
      memoryMode: MemoryMode): Unit

  def getStoragePoolMemoryUsed(memoryMode: MemoryMode): Long

  def getStoragePoolSize(memoryMode: MemoryMode): Long

  def getExecutionPoolUsedMemory(memoryMode: MemoryMode): Long

  def getExecutionPoolSize(memoryMode: MemoryMode): Long

  def getOffHeapMemory(objectName: String): Long

  def hasOffHeap: Boolean

  def logStats(): Unit

  def shouldStopRecovery(): Boolean

  def bootManager: Boolean

  def initMemoryStats(stats: MemoryManagerStats): Unit

  /**
    * Change the off-heap owner to mark it being used for storage.
    * Passing the owner as null allows moving ByteBuffers not allocated
    * by [[BufferAllocator]]s to be also changed and freshly accounted.
    */
  def changeOffHeapOwnerToStorage(buffer: ByteBuffer,
      allowNonAllocator: Boolean): Unit

  /**
    * Clears the internal map
    */
  def clear()

  /**
    * Closes the memory manager.
    */
  def close()

  /**
    * Initializes the memoryManager
    */
  def init()
}



object MemoryManagerCallback extends Logging {

  val storageBlockId = TestBlockId("SNAPPY_STORAGE_BLOCK_ID")
  val cachedDFBlockId = TestBlockId("SNAPPY_CACHED_DF_BLOCK_ID")

  val ummClass = "org.apache.spark.memory.SnappyUnifiedMemoryManager"

  // This memory manager will be used while GemXD is booting up and SparkEnv is not ready.
  private[memory] lazy val bootMemoryManager = {
    try {
      val conf = new SparkConf()
      Utils.classForName(ummClass)
          .getConstructor(classOf[SparkConf], classOf[Int], classOf[Boolean])
          .newInstance(conf, Int.box(1), Boolean.box(true)).asInstanceOf[StoreUnifiedManager]
      // We don't need execution memory during GemXD boot. Hence passing num core as 1
    } catch {
      case _: ClassNotFoundException =>
        logWarning("MemoryManagerCallback couldn't be INITIALIZED. " +
            "SnappyUnifiedMemoryManager won't be used.")
        new DefaultMemoryManager
    }
  }

  @volatile private var snappyUnifiedManager: StoreUnifiedManager = _

  private lazy val defaultManager: StoreUnifiedManager = new DefaultMemoryManager

  // Is called from cache.close() && session.close() & umm.close
  def resetMemoryManager(): Unit = synchronized {
    bootMemoryManager.clear()
    snappyUnifiedManager = null
  }


  private final val isCluster = {
    try {
      org.apache.spark.util.Utils.classForName(ummClass)
      // Class is loaded means we are running in SnappyCluster mode.
      true
    } catch {
      case _: ClassNotFoundException =>
        logWarning("MemoryManagerCallback couldn't be INITIALIZED. " +
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
      return defaultManager
    }

    val env = SparkEnv.get
    if (env ne null) {
      env.memoryManager match {
        case unifiedManager: StoreUnifiedManager =>
          snappyUnifiedManager = unifiedManager
          unifiedManager
        case _ =>
          // if SnappyUnifiedManager is disabled or for local mode
          defaultManager
      }
    } else { // Spark.env will be null only with gemxd boot time
      bootMemoryManager
    }
  }

  /** allocate buffer of given size and return a little-endian buffer */
  def allocateExecutionMemory(size: Int, owner: String,
      allocator: BufferAllocator): ByteBuffer = {
    /* (doesn't work properly for some reason: fails with LME frequently)
    if (allocator.isManagedDirect) {
      val context = TaskContext.get()
      if (context ne null) {
        val memoryManager = context.taskMemoryManager()
        val totalSize = UnsafeHolder.getAllocationSize(size) +
            DirectBufferAllocator.DIRECT_OBJECT_OVERHEAD
        val consumer = new DefaultMemoryConsumer(memoryManager, MemoryMode.OFF_HEAP)
        var granted, totalGranted = 0L
        do {
          granted = consumer.acquireMemory(totalSize - totalGranted)
          totalGranted += granted
        } while (granted > 0 && totalGranted < totalSize)
        if (totalGranted < totalSize) {
          consumer.freeMemory(consumer.getUsed)
          memoryManager.showMemoryUsage()
          throw DirectBufferAllocator.instance().lowMemoryException(
            "EXECUTION:" + owner, totalSize)
        }
        return allocator.allocateCustom(totalSize, new FreeMemory.Factory {
          override def newFreeMemory(address: Long, size: Int): ExecutionFreeMemory =
            new ExecutionFreeMemory(consumer, address)
        }).order(ByteOrder.LITTLE_ENDIAN)
      }
    } else
    */
    if (!allocator.isDirect && !SharedUtils.acquireStorageMemory(
      owner, size, buffer = null, offHeap = false, shouldEvict = true)) {
      throw LocalRegion.lowMemoryException(null, size)
    }
    allocator.allocate(size, owner).order(ByteOrder.LITTLE_ENDIAN)
  }

  /** release and accounting for byte buffer allocated by [[allocateExecutionMemory]] */
  def releaseExecutionMemory(buffer: ByteBuffer, owner: String): Unit = {
    if (buffer.hasArray) {
      SharedUtils.releaseStorageMemory(owner, buffer.capacity(), offHeap = false)
    }
  }
}

final class DefaultMemoryConsumer(taskMemoryManager: TaskMemoryManager,
    mode: MemoryMode = MemoryMode.ON_HEAP)
    extends MemoryConsumer(taskMemoryManager, taskMemoryManager.pageSizeBytes(), mode) {

  def this(taskMemoryManager: TaskMemoryManager, offHeap: Boolean) =
    this(taskMemoryManager, if (offHeap) MemoryMode.OFF_HEAP else MemoryMode.ON_HEAP)

  override def spill(size: Long, trigger: MemoryConsumer): Long = 0L

  override def getUsed: Long = this.used
}

final class ExecutionFreeMemory(consumer: DefaultMemoryConsumer,
    address: Long) extends FreeMemory(address) {

  override protected def objectName(): String = BufferAllocator.EXECUTION

  override def run() {
    val address = tryFree()
    if (address != 0) {
      Platform.freeMemory(address)
      releaseExecutionMemory()
    }
  }

  def releaseExecutionMemory(): Unit = {
    try {
      // release from execution pool
      consumer.freeMemory(consumer.getUsed)
    } catch {
      case t: Throwable => // ignore exceptions
        SystemFailure.checkFailure()
        try {
          val logger = LoggerFactory.getLogger(getClass)
          logger.error("ExecutionFreeMemory unexpected exception", t)
        } catch {
          case _: Throwable => // ignore if even logging failed
        }
    }
  }
}
