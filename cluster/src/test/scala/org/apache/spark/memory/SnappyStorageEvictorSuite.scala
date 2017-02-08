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
import org.apache.spark.sql.{Row, SnappyContext, SnappySession, SparkSession}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

case class Data1(col1: Int, col2: Int, col3: Int)

class SnappyStorageEvictorSuite extends FunSuite with BeforeAndAfter with BeforeAndAfterAll {

  InitializeRun.setUp()

  after{
    if(SnappyContext.globalSparkContext != null){
      SnappyContext.globalSparkContext.stop()
    }
  }

  val struct = (new StructType())
    .add(StructField("col1", IntegerType, true))
    .add(StructField("col2", IntegerType, true))
    .add(StructField("col3", IntegerType, true))

  val options = Map("PARTITION_BY" -> "col1", "EVICTION_BY" -> "LRUHEAPPERCENT", "OVERFLOW" -> "true")
  val roptions = Map("EVICTION_BY" -> "LRUHEAPPERCENT", "OVERFLOW" -> "true")

  private def createSparkSession(memoryFraction : Double, storageFraction: Double) : SparkSession = {
    SparkSession
      .builder
      .appName(getClass.getName)
      .master("local[*]")
        .config(io.snappydata.Property.CachedBatchSize.name, "4")
      .config("spark.memory.fraction", memoryFraction)
      .config("spark.memory.storageFraction", storageFraction)
      .config("spark.testing.memory", "100000")
      .config("spark.testing.reservedMemory", "0")
      .config("spark.memory.manager", "org.apache.spark.memory.SnappyUnifiedMemoryManager")
      .getOrCreate
  }

  test("Test storage for replicated tables"){
    val sparkSession = createSparkSession(1,0)
    val snSession = new SnappySession(sparkSession.sparkContext)
    println(SparkEnv.get.memoryManager.storageMemoryUsed)
    /*snSession.createTable("t1", "row", struct, roptions)
    println(SparkEnv.get.memoryManager)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)
    val row = Row(1, 1, 1)
    snSession.insert("t1", row)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed > 0)// borrowed from execution memory
    snSession.delete("t1", "col1=1")
    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)
    snSession.dropTable("t1")*/
  }

  ignore("Test storage when storage can borrow from execution memory"){
    val sparkSession = createSparkSession(1,0)
    val snSession = new SnappySession(sparkSession.sparkContext)
    snSession.createTable("t1", "row", struct, options)
    println(SparkEnv.get.memoryManager)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)
    val row = Row(1, 1, 1)
    snSession.insert("t1", row)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed > 0)// borrowed from execution memory
    snSession.delete("t1", "col1=1")
    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)
    snSession.dropTable("t1")
  }

  ignore("Test storage when storage can not borrow from execution memory") {
    val sparkSession = createSparkSession(1, 0.5)
    val snSession = new SnappySession(sparkSession.sparkContext)
    println(SparkEnv.get.memoryManager)
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

  ignore("Test eviction when storage memory has borrowed some memory from execution") {
    val sparkSession = createSparkSession(1, 0.5)
    val snSession = new SnappySession(sparkSession.sparkContext)
    snSession.createTable("t1", "row", struct, options)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)

    (1 to 20).map(i => {
      val row = Row(i, i, i)
      snSession.insert("t1", row)
    })
    assert(SparkEnv.get.memoryManager.storageMemoryUsed > 500L)

    println("Storage memory " + SparkEnv.get.memoryManager.storageMemoryUsed)
    val memoryMode = MemoryMode.ON_HEAP
    val taskAttemptId = 0L
    //artificially acquire memory
    SparkEnv.get.memoryManager.acquireExecutionMemory(500L, taskAttemptId, memoryMode)
    assert(SparkEnv.get.memoryManager.executionMemoryUsed == 500)
    val count = snSession.sql("select * from t1").count()
    assert(count == 20)

    //@TODO Uncomment this assertion up once we set per region entry overhead and put a check before eviction
    //assert(SparkEnv.get.memoryManager.storageMemoryUsed == 500L)
    val otherExecutorThread = new Thread(new Runnable {
      def run() {
        //This should not hang as we are dropping the table after this thread is executed.
        SparkEnv.get.memoryManager.acquireExecutionMemory(500L, 1L, memoryMode)
      }
    })
    otherExecutorThread.start()
    snSession.dropTable("t1")

    if(otherExecutorThread.isAlive){
      otherExecutorThread.wait(2000)
    }

    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0L)
  }
}
