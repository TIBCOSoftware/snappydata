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

import java.util.concurrent.atomic.AtomicInteger

import com.gemstone.gemfire.internal.cache.LocalRegion

import scala.collection.mutable
import com.gemstone.gemfire.internal.snappy.UMMMemoryTracker
import org.apache.spark.storage.{BlockId, TestBlockId}
import org.apache.spark.{Logging, SparkConf}

/**
  * When there is request for execution or storage memory, critical up and eviction up
  * events are checked. If they are set, try to free the memory cached by Spark rdds
  * by calling memoryStore.evictBlocksToFreeSpace. If enough memory cannot be freed,
  * return the call and let Spark take a corrective action.
  * In such cases Spark either fails the task or move the current RDDs data to disk.
  * If the critical and eviction events are not set, it asks the UnifiedMemoryManager
  * to allocate the space.
  *
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
  numCores) with StoreUnifiedManager {

  val onHeapStorageRegionSize = onHeapStorageMemoryPool.poolSize

  val maxStorageSize = (maxHeapMemory * 0.75).toLong

  val maxExecutionSize = (maxHeapMemory * 0.75).toLong

  private[memory] val memoryForObject = new mutable.HashMap[String, Long]()

  val threadsWaitingForStorage = new AtomicInteger()

  val SPARK_CACHE = "_SPARK_CACHE_"

  private[memory] val evictor = new SnappyStorageEvictor

  def this(conf: SparkConf, numCores: Int) = {
    this(conf,
      SnappyUnifiedMemoryManager.getMaxMemory(conf),
      numCores)

    MemoryManagerCallback.tempMemoryManager.memoryForObject.map { entry =>
      val blockId = TestBlockId(s"SNAPPY_STORAGE_BLOCK_ID_${entry._1}")
      acquireStorageMemoryForObject(entry._1, blockId, entry._2, MemoryMode.ON_HEAP, null, false)
    }
  }

  override def getStoragePoolSize : Long = onHeapStorageMemoryPool.poolSize

  override def getStoragePoolMemoryUsed(): Long = onHeapStorageMemoryPool.memoryUsed

  override def getExecutionPoolUsedMemory: Long = onHeapExecutionMemoryPool.memoryUsed

  override def getExecutionPoolSize : Long = onHeapExecutionMemoryPool.poolSize


  /**
    * This method is copied from Spark. In addition to evicting data from spark block manager,
    * this will also evict data from SnappyStore.
    *
    * Try to acquire up to `numBytes` of execution memory for the current task and return the
    * number of bytes obtained, or 0 if none can be allocated.
    *
    * This call may block until there is enough free memory in some situations, to make sure each
    * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
    * active tasks) before it is forced to spill. This can happen if the number of tasks increase
    * but an older task had a lot of memory already.
    */
  override private[memory] def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long = synchronized {
    assertInvariants()
    assert(numBytes >= 0)
    val (executionPool, storagePool, storageRegionSize, maxMemory) = memoryMode match {
      case MemoryMode.ON_HEAP => (
          onHeapExecutionMemoryPool,
          onHeapStorageMemoryPool,
          onHeapStorageRegionSize,
          maxHeapMemory)
      case MemoryMode.OFF_HEAP => (
          offHeapExecutionMemoryPool,
          offHeapStorageMemoryPool,
          offHeapStorageMemory,
          maxOffHeapMemory)
    }

    /**
      * Grow the execution pool by evicting cached blocks, thereby shrinking the storage pool.
      *
      * When acquiring memory for a task, the execution pool may need to make multiple
      * attempts. Each attempt must be able to evict storage in case another task jumps in
      * and caches a large block between the attempts. This is called once per attempt.
      */
    def maybeGrowExecutionPool(extraMemoryNeeded: Long): Unit = {
      if (extraMemoryNeeded > 0) {

        if (SnappyMemoryUtils.isCriticalUp(getStoragePoolMemoryUsed + getExecutionPoolUsedMemory)){
          logWarning(s"CRTICAL_UP event raised due to critical heap memory usage. " +
            s"No memory allocated to thread ${Thread.currentThread()}")
          return
        }

        // There is not enough free memory in the execution pool, so try to reclaim memory from
        // storage. We can reclaim any free memory from the storage pool. If the storage pool
        // has grown to become larger than `storageRegionSize`, we can evict blocks and reclaim
        // the memory that storage has borrowed from execution.
        val memoryReclaimableFromStorage = math.max(
          storagePool.memoryFree,
          storagePool.poolSize - storageRegionSize)
        if (memoryReclaimableFromStorage > 0) {
          // Only reclaim as much space as is necessary and available:
          val spaceToReclaim = storagePool.freeSpaceToShrinkPool(
            math.min(extraMemoryNeeded, memoryReclaimableFromStorage))

          val bytesEvictedFromStore = if (spaceToReclaim < extraMemoryNeeded) {
            val moreBytesRequired = extraMemoryNeeded - spaceToReclaim
            evictor.evictRegionData(math.min(moreBytesRequired, memoryReclaimableFromStorage))
          } else {
            0L
          }
          if(storagePool.poolSize - (spaceToReclaim + bytesEvictedFromStore)
            >= storagePool.memoryUsed){
            // Some eviction might have increased the storage memory used which will
            // case some requirement failing
            // while decreasing pool size.
            storagePool.decrementPoolSize(spaceToReclaim + bytesEvictedFromStore)
            executionPool.incrementPoolSize(spaceToReclaim + bytesEvictedFromStore)
          }

        }
      }
    }

    /**
      * The size the execution pool would have after evicting storage memory.
      *
      * The execution memory pool divides this quantity among the active tasks evenly to cap
      * the execution memory allocation for each task. It is important to keep this greater
      * than the execution pool size, which doesn't take into account potential memory that
      * could be freed by evicting storage. Otherwise we may hit SPARK-12155.
      *
      * Additionally, this quantity should be kept below `maxMemory` to arbitrate fairness
      * in execution memory allocation across tasks, Otherwise, a task may occupy more than
      * its fair share of execution memory, mistakenly thinking that other tasks can acquire
      * the portion of storage memory that cannot be evicted.
      */
    def computeMaxExecutionPoolSize(): Long = {
      maxMemory - math.min(storagePool.memoryUsed, storageRegionSize)
    }

    executionPool.acquireMemory(
      numBytes, taskAttemptId, maybeGrowExecutionPool, computeMaxExecutionPoolSize)
  }


  override def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = {
    acquireStorageMemoryForObject(SPARK_CACHE, blockId, numBytes, memoryMode, null,
      shouldEvict = true)
  }

  private def askStoragePool(objectName: String,
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode,
      shouldEvict: Boolean): Boolean = {
    println(s"Acquiring [SNAP] memory for $objectName $numBytes $shouldEvict ")

    threadsWaitingForStorage.incrementAndGet()
    synchronized {
      if (!shouldEvict) {
        SnappyUnifiedMemoryManager.
          invokeListenersOnPositiveMemoryIncreaseDueToEviction(objectName, numBytes)
      }
      if (!memoryForObject.contains(objectName)) {
        memoryForObject(objectName) = 0L
      }
      assertInvariants()
      assert(numBytes >= 0)
      val (executionPool, storagePool, maxMemory) = memoryMode match {
        case MemoryMode.ON_HEAP => (
            onHeapExecutionMemoryPool,
            onHeapStorageMemoryPool,
            maxOnHeapStorageMemory)
        case MemoryMode.OFF_HEAP => (
            offHeapExecutionMemoryPool,
            offHeapStorageMemoryPool,
            maxOffHeapMemory)
      }


      if (numBytes > maxMemory) {
        // Fail fast if the block simply won't fit
        logWarning(s"Will not store $blockId for $objectName as " +
          s"the required space ($numBytes bytes) exceeds our " +
            s"memory limit ($maxMemory bytes)")
        return false
      }
      if (numBytes > storagePool.memoryFree) {
        // There is not enough free memory in the storage pool, so try to borrow free memory from
        // the execution pool.
        val memoryBorrowedFromExecution = Math.min(executionPool.memoryFree, numBytes)
        val actualBorrowedMemory =
          if (storagePool.poolSize + memoryBorrowedFromExecution > maxStorageSize) {
            maxStorageSize - storagePool.poolSize
          } else {
            memoryBorrowedFromExecution
          }
        executionPool.decrementPoolSize(actualBorrowedMemory)
        storagePool.incrementPoolSize(actualBorrowedMemory)
      }
      // First let spark try to free some memory
      val enoughMemory = storagePool.acquireMemory(blockId, numBytes)
      if (!enoughMemory) {

        if (SnappyMemoryUtils.isCriticalUp(getStoragePoolMemoryUsed + getExecutionPoolUsedMemory)) {
          logWarning(s"CRTICAL_UP event raised due to critical heap memory usage. " +
            s"No memory allocated to thread ${Thread.currentThread()}")
          return false
        }

        if (shouldEvict) {
          // Sufficient memory could not be freed. Time to evict from SnappyData store.
          val requiredBytes = numBytes - storagePool.memoryFree
          // Evict data a little more than required based on waiting tasks
          evictor.evictRegionData(requiredBytes * threadsWaitingForStorage.get())
        }

        threadsWaitingForStorage.decrementAndGet()
        val couldEvictSomeData = storagePool.acquireMemory(blockId, numBytes)
        if (!couldEvictSomeData) {
          logWarning(s"Could not allocate memory for $blockId of " +
            s"$objectName. Memory pool size " + storagePool.memoryUsed)
        } else {
          memoryForObject(objectName) += numBytes
          logDebug(s"Allocated memory for $blockId of " +
            s"$objectName. Memory pool size " + storagePool.memoryUsed)
        }
        couldEvictSomeData
      } else {
        threadsWaitingForStorage.decrementAndGet()
        memoryForObject(objectName) += numBytes
        enoughMemory
      }
    }
  }



  override def acquireStorageMemoryForObject(objectName: String,
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode,
      buffer: UMMMemoryTracker,
      shouldEvict: Boolean): Boolean = {
    logDebug(s"Acquiring [SNAP] memory for $objectName $numBytes ")
    if (buffer ne null) {
      if (buffer.freeMemory() > numBytes) {
        buffer.incMemoryUsed(numBytes)
        true
      } else {
        val predictedMemory = numBytes * buffer.getTotalOperationsExpected
        buffer.incAllocatedMemory(predictedMemory)
        val success = askStoragePool(objectName, blockId, predictedMemory, memoryMode, shouldEvict)
        buffer.setFirstAllocationObject(objectName)
        buffer.incMemoryUsed(numBytes)
        success
      }
    } else {
      askStoragePool(objectName, blockId, numBytes, memoryMode, shouldEvict)
    }
  }

  override def releaseStorageMemoryForObject(objectName: String,
                                             numBytes: Long,
                                             memoryMode: MemoryMode): Unit = synchronized {
    logDebug(s"releasing [SNAP] memory for $objectName $numBytes")
    println(s"releasing [SNAP] memory for $objectName $numBytes")
    memoryForObject.get(objectName) match {
      case Some(x) => {
        memoryForObject(objectName) -= numBytes
        super.releaseStorageMemory(numBytes, memoryMode)
      }
      case None => // Do nothing
    }
  }

  override def releaseStorageMemory(numBytes: Long, memoryMode: MemoryMode): Unit = synchronized {
    memoryForObject(SPARK_CACHE) -= numBytes
    super.releaseStorageMemory(numBytes, memoryMode)
  }

  override def dropStorageMemoryForObject(name: String,
                                          memoryMode: MemoryMode,
                                          ignoreNumBytes: Long): Long =
    synchronized {
      logDebug(s"Dropping memory for $name")
      val (executionPool, storagePool, maxMemory) = memoryMode match {
        case MemoryMode.ON_HEAP => (
          onHeapExecutionMemoryPool,
          onHeapStorageMemoryPool,
          maxOnHeapStorageMemory)
        case MemoryMode.OFF_HEAP => (
          offHeapExecutionMemoryPool,
          offHeapStorageMemoryPool,
          maxOffHeapMemory)
      }

      val bytesToBeFreed = memoryForObject.getOrElse(name, 0L)
      val numBytes = Math.max(0, bytesToBeFreed - ignoreNumBytes)

      if (numBytes > 0) {
        super.releaseStorageMemory(numBytes, memoryMode)
        memoryForObject.remove(name)
      }
      bytesToBeFreed
    }

  // Test Hook. Not to be used anywhere else
  private[memory] def dropAllObjects(memoryMode: MemoryMode): Unit = synchronized {
    memoryForObject.values.foreach(bytes => super.releaseStorageMemory(bytes, memoryMode))
    memoryForObject.clear()
  }


}

private object SnappyUnifiedMemoryManager extends Logging{

  // Reserving 500MB data for internal tables
  private val RESERVED_SYSTEM_MEMORY_BYTES = 500 * 1024 * 1024

  val bufferMemory = new AtomicInteger()
  val nonBufferMemory = new AtomicInteger()

  val testCallbacks = scala.collection.mutable.ArrayBuffer.empty[MemoryEventListener]

  def addMemoryEventListener(listener: MemoryEventListener): Unit = {
    testCallbacks += listener
  }

  def clearMemoryEventListener(): Unit = {
    testCallbacks.clear()
  }

  private def invokeListenersOnPositiveMemoryIncreaseDueToEviction(objectName: String,
                                                                   bytes: Long): Unit = {
    testCallbacks.map(l => l.onPositiveMemoryIncreaseDueToEviction(objectName, bytes))
  }


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
    val memoryFraction = conf.getDouble("spark.memory.fraction", 0.90)
    val totalMemory = (usableMemory * memoryFraction).toLong
    logInfo(s"Total memory allocated for execution and storage pool is $totalMemory")
    totalMemory
  }
}

// Test listeners. Should not be used in production code.
class MemoryEventListener{
  def onStorageMemoryAcquireSuccess(objectName : String, bytes : Long) : Unit = {}
  def onStorageMemoryAcquireFailure(objectName : String, bytes : Long) : Unit = {}
  def onPositiveMemoryIncreaseDueToEviction(objectName : String, bytes : Long) : Unit = {}
  def onExecutionMemoryAcquireSuccess(taskAttemptId : Long, bytes : Long) : Unit = {}
  def onExecutionMemoryAcquireFailure(taskAttemptId : Long, bytes : Long) : Unit = {}
}
