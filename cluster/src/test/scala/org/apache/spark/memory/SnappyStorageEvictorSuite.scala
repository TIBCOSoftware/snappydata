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


import java.io.PrintWriter

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import io.snappydata.core.Data
import io.snappydata.test.dunit.DistributedTestBase.InitializeRun

import org.apache.spark.SparkEnv
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SnappyContext, SnappySession, SparkSession}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

case class Data1(col1: Int, col2: Int, col3: Int)

class SnappyStorageEvictorSuite extends FunSuite with BeforeAndAfter with BeforeAndAfterAll {

  InitializeRun.setUp()

  after {
    if (SnappyContext.globalSparkContext != null) {
      SnappyContext.globalSparkContext.stop()
    }
  }

  val struct = (new StructType())
      .add(StructField("col1", IntegerType, true))
      .add(StructField("col2", IntegerType, true))
      .add(StructField("col3", IntegerType, true))

  val options = Map("PARTITION_BY" -> "col1", "EVICTION_BY" -> "LRUHEAPPERCENT", "OVERFLOW" -> "true")
  val coptions = Map("PARTITION_BY" -> "col1", "BUCKETS" -> "1", "EVICTION_BY" -> "LRUHEAPPERCENT", "OVERFLOW" -> "true")
  val cwoptions = Map("EVICTION_BY" -> "LRUHEAPPERCENT", "OVERFLOW" -> "true")
  val roptions = Map("EVICTION_BY" -> "LRUHEAPPERCENT", "OVERFLOW" -> "true")

  private def createSparkSession(memoryFraction: Double,
      storageFraction: Double,
      sparkMemory: Long = 1000,
      cachedBatchSize : Int = 5): SparkSession = {
    SparkSession
        .builder
        .appName(getClass.getName)
        .master("local[*]")
        .config(io.snappydata.Property.CachedBatchSize.name, cachedBatchSize)
        .config("spark.memory.fraction", memoryFraction)
        .config("spark.memory.storageFraction", storageFraction)
        .config("spark.testing.memory", sparkMemory)
        .config("spark.testing.reservedMemory", "0")
        .config("spark.memory.manager", "org.apache.spark.memory.SnappyUnifiedMemoryManager")
        .getOrCreate
  }

  test("Test column table row ageing") {
    val sparkSession = createSparkSession(1, 0, 2000000L)
    val snSession = new SnappySession(sparkSession.sparkContext)
    val options = Map("PARTITION_BY" -> "col1",
      "BUCKETS" -> "1",
      "EVICTION_BY" -> "LRUHEAPPERCENT",
      "OVERFLOW" -> "true"
    )
    val beforeTableSize = SparkEnv.get.memoryManager.storageMemoryUsed
    snSession.createTable("t1", "column", struct, options)
    val afterTableSize = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afterTableSize > beforeTableSize)

    val row = Row(100000000, 10000000, 10000000)
    (1 to 10).map(i => snSession.insert("t1", row))
    val afterInsertSize = SparkEnv.get.memoryManager.storageMemoryUsed
    snSession.dropTable("t1")
    val afterDropSize = SparkEnv.get.memoryManager.storageMemoryUsed
    // For less number of rows in table the below assertion might
    // fail as some of hive table store dropped table entries.
    assert(afterDropSize < afterInsertSize)
  }

  val memoryMode = MemoryMode.ON_HEAP

  test("Test column table with eviction") {
    val sparkSession = createSparkSession(1, 0, 10000L)
    val snSession = new SnappySession(sparkSession.sparkContext)
    val options = Map("PARTITION_BY" -> "col1",
      "BUCKETS" -> "1",
      "EVICTION_BY" -> "LRUHEAPPERCENT",
      "OVERFLOW" -> "true"
    )
    snSession.createTable("t1", "column", struct, options)
    SparkEnv.get.memoryManager.asInstanceOf[SnappyUnifiedMemoryManager].dropAllObjects(memoryMode)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)
    val row = Row(100000000, 10000000, 10000000)
    val taskAttemptId = 0L
    //artificially acquire memory
    SparkEnv.get.memoryManager.acquireExecutionMemory(5000L, taskAttemptId, memoryMode)
    //136 *10. 136 is the row size + memory overhead
    (1 to 70).map(i => snSession.insert("t1", row))

    val afterEvictionMemory = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(snSession.sql("select * from t1").collect().length == 70)
    val afterFaultInMemory = SparkEnv.get.memoryManager.storageMemoryUsed
    //@TODO enable this assertion after Hemant's change is in where column table cached batches would be faulted in
    //assert(afterFaultInMemory > afterEvictionMemory)
    snSession.dropTable("t1")
    val afterTableDrop = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afterTableDrop < afterFaultInMemory)
  }

  test("Reboot GemFireXD") {
    assert(GemFireCacheImpl.getInstance == null)
    var sparkSession = createSparkSession(1, 0, 100000L)
    var snSession = new SnappySession(sparkSession.sparkContext)
    val options = "OPTIONS (BUCKETS '1', PARTITION_BY 'Col1', PERSISTENT 'SYNCHRONOUS')"
    SnappyUnifiedMemoryManager.debug = true
    snSession.sql("CREATE TABLE t1 (Col1 INT, Col2 INT, Col3 INT) " + " USING row " +
        options
    )

    val row = Row(100000000, 10000000, 10000000)
    (1 to 5).map(i => snSession.insert("t1", row))

    val beforeRebootMemory = SparkEnv.get.memoryManager.storageMemoryUsed
    SnappyContext.globalSparkContext.stop()
    println("Gemxd shutdown")
    Thread.sleep(2000)
    assert(SparkEnv.get == null)
    sparkSession = createSparkSession(1, 0, 100000L)
    SnappyUnifiedMemoryManager.debug = true
    snSession = new SnappySession(sparkSession.sparkContext)

    snSession.sql("select * from t1").collect().foreach(println)
    val afterRebootMemory = SparkEnv.get.memoryManager.storageMemoryUsed
    println(s"beforeRebootMemory $beforeRebootMemory afterRebootMemory $afterRebootMemory")
    assert(beforeRebootMemory == afterRebootMemory)
    snSession.dropTable("t1")
  }


  test("Test row table with eviction") {
    val sparkSession = createSparkSession(1, 0, 10000L)
    val snSession = new SnappySession(sparkSession.sparkContext)
    val options = Map("PARTITION_BY" -> "col1",
      "BUCKETS" -> "1",
      "EVICTION_BY" -> "LRUHEAPPERCENT",
      "OVERFLOW" -> "true"
    )
    val struct = (new StructType())
        .add(StructField("col1", IntegerType, true))
        .add(StructField("col2", IntegerType, true))
        .add(StructField("col3", IntegerType, true))
        .add(StructField("col4", IntegerType, true))
        .add(StructField("col5", IntegerType, true))
        .add(StructField("col6", IntegerType, true))
        .add(StructField("col7", IntegerType, true))
        .add(StructField("col8", IntegerType, true))
        .add(StructField("col9", IntegerType, true))
        .add(StructField("col10", IntegerType, true))
        .add(StructField("col11", IntegerType, true))
        .add(StructField("col12", IntegerType, true))
        .add(StructField("col13", IntegerType, true))
        .add(StructField("col14", IntegerType, true))
        .add(StructField("col15", IntegerType, true))
        .add(StructField("col16", IntegerType, true))
        .add(StructField("col17", IntegerType, true))
        .add(StructField("col18", IntegerType, true))
        .add(StructField("col19", IntegerType, true))
        .add(StructField("col20", IntegerType, true))

    snSession.createTable("t1", "row", struct, options)
    val region = GemFireCacheImpl.getInstance().getRegion("/APP/T1")
    SparkEnv.get.memoryManager.asInstanceOf[SnappyUnifiedMemoryManager].dropAllObjects(memoryMode)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)
    assert(SparkEnv.get.memoryManager.executionMemoryUsed == 0)
    val row = Row(1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1)
    val taskAttemptId = 0L
    //artificially acquire memory
    SparkEnv.get.memoryManager.acquireExecutionMemory(5000L, taskAttemptId, memoryMode)
    //136 *10. 136 is the row size + memory overhead
    (1 to 35).map(i => snSession.insert("t1", row))

    val afterEvictionMemory = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(snSession.sql("select * from t1").collect().length == 35)
    val afterFaultInMemory = SparkEnv.get.memoryManager.storageMemoryUsed
    //assert(afterFaultInMemory > afterEvictionMemory)
    snSession.dropTable("t1")
    val afterTableDrop = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afterTableDrop < afterFaultInMemory)
  }


  /*test("Test storage for column tables with one row") {
    val sparkSession = createSparkSession(1, 0, 10000L)
    val snSession = new SnappySession(sparkSession.sparkContext)
    snSession.createTable("t1", "column", struct, options)

    //SparkEnv.get.memoryManager.asInstanceOf[SnappyUnifiedMemoryManager].memoryForObject.foreach(println)

    println(SparkEnv.get.memoryManager.asInstanceOf[SnappyUnifiedMemoryManager].memoryForObject.values.sum)
    println(SparkEnv.get.memoryManager.storageMemoryUsed)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed > 0)
    val memoryMode = MemoryMode.ON_HEAP
    val taskAttemptId = 0L
    //artificially acquire memory
    SparkEnv.get.memoryManager.acquireExecutionMemory(5000L, taskAttemptId, memoryMode)
    assert(SparkEnv.get.memoryManager.executionMemoryUsed == 5000L)
    val row = Row(100000000, 10000000, 10000000)
    snSession.insert("t1", row)
    println(SparkEnv.get.memoryManager.storageMemoryUsed)
    snSession.insert("t1", row)
    println(SparkEnv.get.memoryManager.storageMemoryUsed)
    snSession.insert("t1", row)
    snSession.insert("t1", row)
    snSession.insert("t1", row)
    println(SparkEnv.get.memoryManager.storageMemoryUsed)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed > 0) // borrowed from execution memory
    snSession.dropTable("t1")

    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)

    }

  test("Test storage for replicated tables") {
    val sparkSession = createSparkSession(1, 0)
    val snSession = new SnappySession(sparkSession.sparkContext)
    snSession.createTable("t1", "row", struct, roptions)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)
    val row = Row(1, 1, 1)
    snSession.insert("t1", row)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed > 0) // borrowed from execution memory
    snSession.delete("t1", "col1=1")
    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)
    snSession.dropTable("t1")
  }

  test("Test storage for replicated tables with drop tables") {
    val sparkSession = createSparkSession(1, 0)
    val snSession = new SnappySession(sparkSession.sparkContext)
    snSession.createTable("t1", "row", struct, roptions)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)
    val row = Row(1, 1, 1)
    snSession.insert("t1", row)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed > 0) // borrowed from execution memory
    snSession.dropTable("t1")
    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)
  }

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

  test("Test storage for column tables") {
    val sparkSession = createSparkSession(1, 0, 10000)
    val snSession = new SnappySession(sparkSession.sparkContext)
    snSession.createTable("t1", "column", struct, coptions)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)

    (1 to 100).map(i => {
      val row = Row(i, i, i)
      snSession.insert("t1", row)
    })
    assert(SparkEnv.get.memoryManager.storageMemoryUsed > 0 &&
        SparkEnv.get.memoryManager.storageMemoryUsed <= 10000)
    val count = snSession.sql("select * from t1").count()
    assert(count == 100)
    snSession.dropTable("t1")
    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)
  }

  test("Test storage for column tables with df inserts") {
    val sparkSession = createSparkSession(1, 0, 10000)
    val snSession = new SnappySession(sparkSession.sparkContext)
    snSession.createTable("t1", "column", struct, cwoptions)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)

    val data =(1 to 100).toSeq

    val rdd = sparkSession.sparkContext.parallelize(data, data.length)
        .map(s => Data1(s, s+1, s+2))
    val dataDF = snSession.createDataFrame(rdd)

    dataDF.write.insertInto("t1")
    println(SparkEnv.get.memoryManager.storageMemoryUsed)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed > 0 &&
        SparkEnv.get.memoryManager.storageMemoryUsed <= 10000)
    val count = snSession.sql("select * from t1").count()
    assert(count == 100)
    snSession.dropTable("t1")
    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)
  }*/

}
