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
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SnappySession}

case class Data1(col1: Int, col2: Int, col3: Int)

class SnappyStorageEvictorSuite extends MemoryFunSuite {

  InitializeRun.setUp()

  val struct = (new StructType())
      .add(StructField("col1", IntegerType, true))
      .add(StructField("col2", IntegerType, true))
      .add(StructField("col3", IntegerType, true))

  val options = Map("PARTITION_BY" -> "col1", "EVICTION_BY" -> "LRUHEAPPERCENT", "OVERFLOW" -> "true")
  val coptions = Map("PARTITION_BY" -> "col1", "BUCKETS" -> "1", "EVICTION_BY" -> "LRUHEAPPERCENT", "OVERFLOW" -> "true")
  val cwoptions = Map("EVICTION_BY" -> "LRUHEAPPERCENT", "OVERFLOW" -> "true")
  val roptions = Map("EVICTION_BY" -> "LRUHEAPPERCENT", "OVERFLOW" -> "true")

  test("Test storage when storage can borrow from execution memory") {
    val sparkSession = createSparkSession(1, 0)
    val snSession = new SnappySession(sparkSession.sparkContext)
    snSession.createTable("t1", "row", struct, options)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)
    val row = Row(1, 1, 1)
    snSession.insert("t1", row)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed > 0) // borrowed from execution memory
    snSession.delete("t1", "col1=1")
    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)
    snSession.dropTable("t1")
  }

  test("Test storage when storage can not borrow from execution memory") {
    val sparkSession = createSparkSession(1, 0.5)
    val snSession = new SnappySession(sparkSession.sparkContext)
    snSession.createTable("t1", "row", struct, options)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)
    val memoryMode = MemoryMode.ON_HEAP
    val taskAttemptId = 0L
    //artificially acquire memory
    SparkEnv.get.memoryManager.acquireExecutionMemory(500L, taskAttemptId, memoryMode)
    assert(SparkEnv.get.memoryManager.executionMemoryUsed == 500)

    (1 to 20).map(i => {
      val row = Row(i, i, i)
      snSession.insert("t1", row)
    })

    val count = snSession.sql("select * from t1").count()
    assert(count == 20)
    snSession.dropTable("t1")
  }

  test("Test eviction when storage memory has borrowed some memory from execution") {
    val sparkSession = createSparkSession(1, 0.5, 10000)
    val snSession = new SnappySession(sparkSession.sparkContext)
    snSession.createTable("t1", "row", struct, options)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)

    (1 to 60).map(i => {
      val row = Row(i, i, i)
      snSession.insert("t1", row)
    })
    assert(SparkEnv.get.memoryManager.storageMemoryUsed > 5000L)//based on 32 bytes value and 64 bytes entry overhead

    val memoryMode = MemoryMode.ON_HEAP
    val taskAttemptId = 0L
    //artificially acquire memory
    SparkEnv.get.memoryManager.acquireExecutionMemory(5000L, taskAttemptId, memoryMode)
    assert(SparkEnv.get.memoryManager.executionMemoryUsed == 5000L)
    val count = snSession.sql("select * from t1").count()
    assert(count == 60)

    //@TODO Uncomment this assertion up once we set per region entry overhead and put a check before eviction
    //assert(SparkEnv.get.memoryManager.storageMemoryUsed == 500L)
    val otherExecutorThread = new Thread(new Runnable {
      def run() {
        //This should not hang as we are dropping the table after this thread is executed.
        SparkEnv.get.memoryManager.acquireExecutionMemory(5000L, 1L, memoryMode)
      }
    })
    otherExecutorThread.start()
    snSession.dropTable("t1")

    if (otherExecutorThread.isAlive) {
      otherExecutorThread.wait(2000)
    }

    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0L)
  }
}
