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

import java.sql.DriverManager

import com.gemstone.gemfire.internal.cache.LocalRegion
import com.pivotal.gemfirexd.TestUtil
import io.snappydata.test.dunit.DistributedTestBase.InitializeRun
import org.apache.spark.SparkEnv
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SnappyContext, SnappySession}


class SnappyLocalIndexAccountingSuite extends MemoryFunSuite {

  InitializeRun.setUp()

  val struct = (new StructType())
      .add(StructField("col1", IntegerType, true))
      .add(StructField("col2", IntegerType, true))
      .add(StructField("col3", IntegerType, true))

  val options = Map("PARTITION_BY" -> "col1", "EVICTION_BY" ->
      "LRUHEAPPERCENT", "OVERFLOW" -> "true")
  val coptions = Map("PARTITION_BY" -> "col1", "BUCKETS" -> "1",
    "EVICTION_BY" -> "LRUHEAPPERCENT", "OVERFLOW" -> "true")
  val cwoptions = Map("BUCKETS" -> "1", "EVICTION_BY" -> "LRUHEAPPERCENT",
    "OVERFLOW" -> "true")
  val roptions = Map("EVICTION_BY" -> "LRUHEAPPERCENT", "OVERFLOW" -> "true")
  val poptions = Map("PARTITION_BY" -> "col1", "BUCKETS" -> "1", "PERSISTENT" -> "SYNCHRONOUS")
  val memoryMode = MemoryMode.ON_HEAP

  test("Test CreateIndex on row partitioned table") {
    val sparkSession = createSparkSession(1, 0, 2000000L)
    val snSession = new SnappySession(sparkSession.sparkContext)
    val serverHostPort = TestUtil.startNetServer()
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    val options = Map("PARTITION_BY" -> "col1",
      "BUCKETS" -> "1"
    )
    val beforeTableSize = SparkEnv.get.memoryManager.storageMemoryUsed
    snSession.createTable("t1", "row", struct, options)
    val afterTableSize = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afterTableSize > beforeTableSize)

    val row = Row(100000000, 10000000, 10000000)
    (1 to 10).map(i => snSession.insert("t1", Row(i, i, i)))
    val afterInsertSize = SparkEnv.get.memoryManager.storageMemoryUsed
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val stmt = conn.createStatement()
    stmt.execute("create index t1_index1 on t1 (col1)")
    val afterIndexCreationSize = SparkEnv.get.memoryManager.storageMemoryUsed
    println(s"afterIndexCreationSize $afterIndexCreationSize")
    assert(afterIndexCreationSize > afterInsertSize)
    stmt.execute("drop index t1_index1")
    val afterIndexDropSize = SparkEnv.get.memoryManager.storageMemoryUsed
    println(s"afterIndexDropSize $afterIndexDropSize")
    snSession.dropTable("t1")
    TestUtil.stopNetServer()
  }

  ignore("Test CreateIndex on row persistent partitioned table") {
    val sparkSession = createSparkSession(1, 0, 2000000L)
    val snSession = new SnappySession(sparkSession.sparkContext)
    val serverHostPort = TestUtil.startNetServer()
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    val options = Map("PARTITION_BY" -> "col1",
      "BUCKETS" -> "1",
      "PERSISTENT" -> "SYNCHRONOUS"
    )
    val beforeTableSize = SparkEnv.get.memoryManager.storageMemoryUsed
    snSession.createTable("t1", "row", struct, options)
    val afterTableSize = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afterTableSize > beforeTableSize)

    val row = Row(100000000, 10000000, 10000000)
    (1 to 10).map(i => snSession.insert("t1", Row(i, i, i)))
    val afterInsertSize = SparkEnv.get.memoryManager.storageMemoryUsed

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val stmt = conn.createStatement()
    stmt.execute("create index t1_index1 on t1 (col1)")
    val afterIndexCreationSize = SparkEnv.get.memoryManager.storageMemoryUsed
    println(s"afterIndexCreationSize $afterIndexCreationSize")
    assert(afterIndexCreationSize > afterInsertSize)
    snSession.dropTable("t1")
  }

  ignore("Test CreateIndex on row replicated table") {
    val sparkSession = createSparkSession(1, 0, 2000000L)
    val snSession = new SnappySession(sparkSession.sparkContext)
    val serverHostPort = TestUtil.startNetServer()
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    val options = Map.empty[String, String]
    val beforeTableSize = SparkEnv.get.memoryManager.storageMemoryUsed
    snSession.createTable("t1", "row", struct, options)
    val afterTableSize = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afterTableSize > beforeTableSize)

    val row = Row(100000000, 10000000, 10000000)
    (1 to 10).map(i => snSession.insert("t1", Row(i, i, i)))
    val afterInsertSize = SparkEnv.get.memoryManager.storageMemoryUsed
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val stmt = conn.createStatement()
    stmt.execute("create index t1_index1 on t1 (col1)")
    val afterIndexCreationSize = SparkEnv.get.memoryManager.storageMemoryUsed
    println(s"afterIndexCreationSize $afterIndexCreationSize")
    assert(afterIndexCreationSize > afterInsertSize)
    snSession.dropTable("t1")
  }

  ignore("Test CreateIndex on row persistent replicated table") {
    val sparkSession = createSparkSession(1, 0, 2000000L)
    val snSession = new SnappySession(sparkSession.sparkContext)
    val serverHostPort = TestUtil.startNetServer()
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    val options = Map("PERSISTENT" -> "SYNCHRONOUS")
    val beforeTableSize = SparkEnv.get.memoryManager.storageMemoryUsed
    snSession.createTable("t1", "row", struct, options)
    val afterTableSize = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afterTableSize > beforeTableSize)

    val row = Row(100000000, 10000000, 10000000)
    (1 to 10).map(i => snSession.insert("t1", Row(i, i, i)))
    val afterInsertSize = SparkEnv.get.memoryManager.storageMemoryUsed

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val stmt = conn.createStatement()
    stmt.execute("create index t1_index1 on t1 (col1)")
    val afterIndexCreationSize = SparkEnv.get.memoryManager.storageMemoryUsed
    println(s"afterIndexCreationSize $afterIndexCreationSize")
    assert(afterIndexCreationSize > afterInsertSize)
    snSession.dropTable("t1")
  }
}
