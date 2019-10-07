/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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


import com.gemstone.gemfire.internal.cache.LocalRegion
import io.snappydata.test.dunit.DistributedTestBase.InitializeRun

import org.apache.spark.SparkEnv
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SnappySession}
import org.apache.spark.storage.TestBlockId

case class Data1(col1: Int, col2: Int, col3: Int)

class SnappyStorageEvictorSuite extends MemoryFunSuite {

  InitializeRun.setUp()

  private val struct = new StructType()
    .add(StructField("col1", IntegerType))
    .add(StructField("col2", IntegerType))
    .add(StructField("col3", IntegerType))

  val options = Map("PARTITION_BY" -> "col1",
    "EVICTION_BY" -> "LRUHEAPPERCENT",
    "PERSISTENCE" -> "none")
  val coptions = Map("PARTITION_BY" -> "col1",
    "BUCKETS" -> "1", "EVICTION_BY" -> "LRUHEAPPERCENT")
  val cwoptions = Map("EVICTION_BY" -> "LRUHEAPPERCENT")
  val roptions = Map("EVICTION_BY" -> "LRUHEAPPERCENT")

  val memoryMode = MemoryMode.ON_HEAP

  test("Test UnRollMemory") {
    val sparkSession = createSparkSession(1, 0)
    new SnappySession(sparkSession.sparkContext) // initialize SnappyData components
    val memoryManager = SparkEnv.get.memoryManager
        .asInstanceOf[SnappyUnifiedMemoryManager]
    memoryManager.dropAllObjects(memoryMode)
    assert(memoryManager.storageMemoryUsed == 0)
    val blockId = TestBlockId(s"SNAPPY_STORAGE_BLOCK_ID_test")
    memoryManager.acquireUnrollMemory(blockId, 500, memoryMode)

    assert(memoryManager.storageMemoryUsed == 500)
    val key = new MemoryOwner("_SPARK_CACHE_", memoryMode)
    assert(memoryManager.memoryForObject.get(key) == 500)
    memoryManager.releaseUnrollMemory(500, memoryMode)

    assert(memoryManager.getStoragePoolMemoryUsed(MemoryMode.OFF_HEAP) +
        memoryManager.getStoragePoolMemoryUsed(MemoryMode.ON_HEAP) == 0)
    assert(memoryManager.memoryForObject.get(key) == 0)
  }


  test("Test storage when storage can borrow from execution memory") {
    val sparkSession = createSparkSession(1, 0)
    val snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    snSession.createTable("t1", "row", struct, options)
    SparkEnv.get.memoryManager.asInstanceOf[SnappyUnifiedMemoryManager].dropAllObjects(memoryMode)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)
    val row = Row(1, 1, 1)
    snSession.insert("t1", row)
    val afterInsertSize = SparkEnv.get.memoryManager.storageMemoryUsed
    assert( afterInsertSize > 0) // borrowed from execution memory
    snSession.delete("t1", "col1=1")
    val afterDeleteSize = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afterDeleteSize < afterInsertSize)
    snSession.dropTable("t1")
  }

  test("Test storage when storage can not borrow from execution memory") {
    val sparkSession = createSparkSession(1, 0.5, sparkMemory = 1500000L)
    val snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    snSession.createTable("t1", "row", struct, options)
    var memoryIncreaseDuetoEviction = 0L

    val memoryEventListener = new MemoryEventListener {
      override def onPositiveMemoryIncreaseDueToEviction(objectName: String, bytes: Long): Unit = {
        memoryIncreaseDuetoEviction += bytes
      }
    }
    SnappyUnifiedMemoryManager.addMemoryEventListener(memoryEventListener)

    val snappyMemoryManager = SparkEnv.get.memoryManager.asInstanceOf[SnappyUnifiedMemoryManager]

    snappyMemoryManager.dropAllObjects(memoryMode)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)
    val taskAttemptId = 0L
    // artificially acquire memory
    SparkEnv.get.memoryManager.acquireExecutionMemory(500L, taskAttemptId, memoryMode)
    assert(SparkEnv.get.memoryManager.executionMemoryUsed == 500)

    import scala.util.control.Breaks._

    var numRows = 0
    try {
      breakable {
        for (i <- 1 to 20) {
          val rows = (1 to 1000).map(j => Row(i * 1000 + j, i, j))
          snSession.insert("t1", rows: _*)
          numRows += 1000
        }
        fail("Should not have reached here due to LowMemory")
      }
    } catch {
      case _: Exception =>
        assert(memoryIncreaseDuetoEviction > 0)
        assert(snappyMemoryManager.wrapperStats.getNumFailedEvictionRequest(false) > 1)
    }
    snappyMemoryManager.dropAllObjects(memoryMode)
    SparkEnv.get.memoryManager.releaseExecutionMemory(500L, taskAttemptId, memoryMode)
    val count = snSession.sql("select * from t1").count()
    assert(count >= numRows)
    snSession.dropTable("t1")
  }

  test("Test eviction when storage memory has borrowed some memory from execution") {
    val sparkSession = createSparkSession(1, 0.5, 1500000L)
    val snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    snSession.createTable("t1", "row", struct, options)
    SparkEnv.get.memoryManager.asInstanceOf[SnappyUnifiedMemoryManager].dropAllObjects(memoryMode)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)

    (1 to 6).map(i => {
      val row = Row(i, i, i)
      snSession.insert("t1", row)
    })
    assert(SparkEnv.get.memoryManager.storageMemoryUsed > 500L)
    // based on 32 bytes value and 88 bytes entry overhead
    val count = snSession.sql("select * from t1").count()
    assert(count == 6)

    // @TODO Uncomment this assertion up once we set per
    // region entry overhead and put a check before eviction
    // assert(SparkEnv.get.memoryManager.storageMemoryUsed == 500L)
    val otherExecutorThread = new Thread(new Runnable {
      def run() {
        // This should not hang as we are dropping the table after
        // this thread is executed.
        SparkEnv.get.memoryManager.acquireExecutionMemory(750L, 1L, memoryMode)
      }
    })
    otherExecutorThread.start()
    snSession.dropTable("t1")

    if (otherExecutorThread.isAlive) {
      otherExecutorThread.wait(2000)
    }

  }
}
