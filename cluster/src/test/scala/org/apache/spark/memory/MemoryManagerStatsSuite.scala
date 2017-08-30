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

import io.snappydata.test.dunit.DistributedTestBase.InitializeRun

import org.apache.spark.SparkEnv
import org.apache.spark.sql.{SnappySession, SparkSession}


class MemoryManagerStatsSuite extends MemoryFunSuite {

  InitializeRun.setUp()

  test("Test heap stats") {
    val offHeap = false
    val sparkSession = createSparkSession(1, 0.5, 2000)
    new SnappySession(sparkSession.sparkContext)

    val memoryManager = SparkEnv.get.memoryManager
        .asInstanceOf[SnappyUnifiedMemoryManager]
    val stats = memoryManager.wrapperStats
    assert(stats.getMaxStorageSize(offHeap) == 1800)
    assert(stats.getStoragePoolSize(offHeap) >= 1000)
    val blockId = MemoryManagerCallback.storageBlockId
    assert(!SparkEnv.get.memoryManager.acquireStorageMemory(blockId, 1000L, MemoryMode.ON_HEAP))
    // Some other heap allocation from Snappy layer might have failed
    assert(stats.getNumFailedStorageRequest(offHeap) >= 1)
    assert(stats.getExecutionPoolSize(offHeap) == (2000 - stats.getStoragePoolSize(offHeap)))
    memoryManager.dropAllObjects(MemoryMode.ON_HEAP)
    assert(stats.getStorageMemoryUsed(offHeap) == 0)


    val taskAttemptId = 0L
    // artificially acquire memory
    val numBytes =
      SparkEnv.get.memoryManager.acquireExecutionMemory(1000L, taskAttemptId, MemoryMode.ON_HEAP)
    assert(stats.getStoragePoolSize(offHeap) == 1000)
    // Only can evict till original storage fraction
    assert(stats.getExecutionPoolSize(offHeap) == numBytes)
  }

  test("Test off-heap stats") {
    val offHeap = true
    val sparkSession = SparkSession
        .builder
        .appName(getClass.getName)
        .master("local[*]")
        .config(io.snappydata.Property.ColumnBatchSize.name, 500)
        .config("spark.memory.fraction", 1)
        .config("spark.memory.storageFraction", 0.5)
        .config("spark.testing.memory", 2000)
        .config("spark.testing.reservedMemory", "0")
        .config("snappydata.store.critical-heap-percentage", "90")
        .config("spark.testing.maxStorageFraction", "0.9")
        .config("spark.memory.manager", "org.apache.spark.memory.SnappyUnifiedMemoryManager")
        .config("spark.storage.unrollMemoryThreshold", 500)
        .config("snappydata.store.memory-size", 200000)
        .getOrCreate

    new SnappySession(sparkSession.sparkContext)

    val memoryManager = SparkEnv.get.memoryManager
        .asInstanceOf[SnappyUnifiedMemoryManager]
    val stats = memoryManager.wrapperStats
    assert(stats.getMaxStorageSize(offHeap) == 190000) // 95%
    assert(stats.getStoragePoolSize(offHeap) >= 100000)
    val blockId = MemoryManagerCallback.storageBlockId
    assert(!SparkEnv.get.memoryManager.acquireStorageMemory(blockId, 1000L, MemoryMode.ON_HEAP))
    assert(stats.getNumFailedStorageRequest(offHeap) == 1)
    assert(stats.getExecutionPoolSize(offHeap) == (200000 - stats.getStoragePoolSize(offHeap)))
    memoryManager.dropAllObjects(MemoryMode.OFF_HEAP)
    assert(stats.getStorageMemoryUsed(offHeap) == 0)


    val taskAttemptId = 0L
    // artificially acquire memory
    val numBytes =
      SparkEnv.get.memoryManager.acquireExecutionMemory(100000L,
        taskAttemptId, MemoryMode.OFF_HEAP)
    // Only can evict till original storage fraction
    assert(stats.getStoragePoolSize(offHeap) == 100000)

    assert(stats.getExecutionPoolSize(offHeap) == numBytes)
  }
}
