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

import java.sql.DriverManager

import com.gemstone.gemfire.internal.cache.LocalRegion
import com.pivotal.gemfirexd.TestUtil
import io.snappydata.SnappyTableStatsProviderService
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


  val memoryMode = MemoryMode.ON_HEAP

  test("Test Drop index releases memory"){
    val sparkSession = createSparkSession(1, 0, 2000000L)
    val snSession = new SnappySession(sparkSession.sparkContext)
    val serverHostPort = TestUtil.startNetServer()
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1

    val options = "OPTIONS (PARTITION_BY 'col1', " +
      "BUCKETS '1')"
    snSession.sql("CREATE TABLE t1 (Col1 INT, Col2 INT, Col3 INT, col4 INT, col5 INT" +
      ") " + " USING row " +
      options
    )
    (1 to 20).map(i => snSession.insert("t1", Row(i, i, i, i, i)))
    SparkEnv.get.memoryManager.
      asInstanceOf[SnappyUnifiedMemoryManager].dropAllObjects(memoryMode)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val stmt = conn.createStatement()
    stmt.execute("create index t1_index1 on t1 (col1)")
    val afterCreateIndex = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afterCreateIndex > 0)
    stmt.execute("drop index t1_index1")
    val afterDropIndex = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afterDropIndex <   afterCreateIndex)
  }

  test("Test Put Overhead on row partitioned table") {
    val sparkSession = createSparkSession(1, 0, 2000000L)
    val snSession = new SnappySession(sparkSession.sparkContext)
    val serverHostPort = TestUtil.startNetServer()
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1

    val options = "OPTIONS (PARTITION_BY 'col1', " +
      "BUCKETS '1')"
    snSession.sql("CREATE TABLE t1 (Col1 INT, Col2 INT, Col3 INT, col4 INT, col5 INT" +
      ") " + " USING row " +
      options
    )
    (1 to 10).map(i => snSession.insert("t1", Row(i, i, i, i, i)))
    SparkEnv.get.memoryManager.asInstanceOf[SnappyUnifiedMemoryManager].dropAllObjects(memoryMode)
    val afterInsertSize_WithoutIndex = SparkEnv.get.memoryManager.storageMemoryUsed
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val stmt = conn.createStatement()
    stmt.execute("create index t1_index1 on t1 (col1)")
    SparkEnv.get.memoryManager.asInstanceOf[SnappyUnifiedMemoryManager].dropAllObjects(memoryMode)
    (1 to 10).map(i => snSession.insert("t1", Row(i, i, i, i, i)))
    val afterIndexCreationSize = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afterIndexCreationSize > afterInsertSize_WithoutIndex)
    stmt.execute("drop index t1_index1")
    snSession.dropTable("t1")
  }



  test("Test CreateIndex before insert") {
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


    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val stmt = conn.createStatement()
    stmt.execute("create index t1_index1 on t1 (col1)")

    SparkEnv.get.memoryManager.asInstanceOf[SnappyUnifiedMemoryManager].dropAllObjects(memoryMode)
    (1 to 10).map(i => snSession.insert("t1", Row(i, i, i)))
    val afterPutWithoutIndex = SparkEnv.get.memoryManager.storageMemoryUsed
    SparkEnv.get.memoryManager.asInstanceOf[SnappyUnifiedMemoryManager].dropAllObjects(memoryMode)
    SnappyTableStatsProviderService.getService.getAggregatedStatsOnDemand
    (1 to 10).map(i => snSession.insert("t1", Row(i, i, i)))
    val afterPutWitIndex = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afterPutWitIndex > afterPutWithoutIndex)
    snSession.dropTable("t1")
  }


  test("Test CreateIndex on row persistent partitioned table") {
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
    assert(afterIndexCreationSize > afterInsertSize)
    snSession.dropTable("t1")
  }

  test("Test CreateIndex on row replicated table") {
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
    assert(afterIndexCreationSize > afterInsertSize)
    snSession.dropTable("t1")
  }

  test("Test CreateIndex on row persistent replicated table") {
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
    assert(afterIndexCreationSize > afterInsertSize)
    snSession.dropTable("t1")
  }

  test("Test Index recovery on row partitioned table") {
    var sparkSession = createSparkSession(1, 0, 2000000L)
    var snSession = new SnappySession(sparkSession.sparkContext)
    val serverHostPort = TestUtil.startNetServer()
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    val options = Map("PARTITION_BY" -> "col1",
      "BUCKETS" -> "1",
      "PERSISTENT" -> "SYNCHRONOUS"
    )
    snSession.createTable("t1", "row", struct, options)
    (1 to 10).map(i => snSession.insert("t1", Row(i, i, i)))
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val stmt = conn.createStatement()
    stmt.execute("create index t1_index1 on t1 (col1)")
    val afterIndex = SparkEnv.get.memoryManager.storageMemoryUsed
    SnappyContext.globalSparkContext.stop()
    sparkSession = createSparkSession(1, 0, 2000000L)
    snSession = new SnappySession(sparkSession.sparkContext)
    val afterRecoverySize = SparkEnv.get.memoryManager.storageMemoryUsed
    assertApproximate(afterIndex, afterRecoverySize, 20)
    snSession.dropTable("t1")
  }

  test("Test Index recovery on row replicated table") {
    var sparkSession = createSparkSession(1, 0, 2000000L)
    var snSession = new SnappySession(sparkSession.sparkContext)
    val serverHostPort = TestUtil.startNetServer()
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    val options = Map("PERSISTENT" -> "SYNCHRONOUS")
    snSession.createTable("t1", "row", struct, options)
    (1 to 10).map(i => snSession.insert("t1", Row(i, i, i)))
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val stmt = conn.createStatement()
    stmt.execute("create index t1_index1 on t1 (col1)")
    val afterIndex = SparkEnv.get.memoryManager.storageMemoryUsed
    SnappyContext.globalSparkContext.stop()
    sparkSession = createSparkSession(1, 0, 2000000L)
    snSession = new SnappySession(sparkSession.sparkContext)
    val afterRecoverySize = SparkEnv.get.memoryManager.storageMemoryUsed
    assertApproximate(afterIndex, afterRecoverySize, 20)
    snSession.dropTable("t1")
  }

  test("Test Index recovery on row partitioned table with overflow") {
    var sparkSession = createSparkSession(1, 0, 2000000L)
    var snSession = new SnappySession(sparkSession.sparkContext)
    val serverHostPort = TestUtil.startNetServer()
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1

    val options = "OPTIONS (BUCKETS '1', " +
      "PARTITION_BY 'Col1', " +
      "PERSISTENCE 'none', " +
      "EVICTION_BY 'LRUCOUNT 30')"
    snSession.sql("CREATE TABLE t1 (Col1 INT, Col2 INT, Col3 INT, col4 INT, col5 INT" +
      ") " + " USING row " +
      options
    )
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val stmt = conn.createStatement()
    stmt.execute("create index t1_index1 on t1 (col1, col2)")
    SparkEnv.get.memoryManager.asInstanceOf[SnappyUnifiedMemoryManager].dropAllObjects(memoryMode)
    (1 to 30).map(i => snSession.insert("t1", Row(i, i, i, i, i)))
    val afterThreeEntries = SparkEnv.get.memoryManager.storageMemoryUsed
    val avgEntrySize = afterThreeEntries /3
    SparkEnv.get.memoryManager.asInstanceOf[SnappyUnifiedMemoryManager].dropAllObjects(memoryMode)
    (31 to 60).map(i => snSession.insert("t1", Row(i, i, i, i, i)))
    val withOverflow = SparkEnv.get.memoryManager.storageMemoryUsed
    val avgEntrySizeWithOverflow = withOverflow /3
    assert(avgEntrySizeWithOverflow > avgEntrySize)
    snSession.dropTable("t1")
  }

  test("Test Index recovery on row replicated table with overflow") {
    var sparkSession = createSparkSession(1, 0, 2000000L)
    var snSession = new SnappySession(sparkSession.sparkContext)
    val serverHostPort = TestUtil.startNetServer()
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1

    val options = "OPTIONS (EVICTION_BY 'LRUCOUNT 3', " +
      "PERSISTENCE 'NONE')"
    snSession.sql("CREATE TABLE t1 (Col1 INT, Col2 INT, Col3 INT, col4 INT, col5 INT" +
      ") " + " USING row " +
      options
    )
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val stmt = conn.createStatement()
    stmt.execute("create index t1_index1 on t1 (col1, col2)")
    SparkEnv.get.memoryManager.asInstanceOf[SnappyUnifiedMemoryManager].dropAllObjects(memoryMode)
    (1 to 3).map(i => snSession.insert("t1", Row(i, i, i, i, i)))
    val afterThirtyEntries = SparkEnv.get.memoryManager.storageMemoryUsed
    val avgEntrySize = afterThirtyEntries /3
    SparkEnv.get.memoryManager.asInstanceOf[SnappyUnifiedMemoryManager].dropAllObjects(memoryMode)
    (4 to 6).map(i => snSession.insert("t1", Row(i, i, i, i, i)))
    val withOverflow = SparkEnv.get.memoryManager.storageMemoryUsed
    val avgEntrySizeWithOverflow = withOverflow /3
    assert(avgEntrySizeWithOverflow > avgEntrySize)
    assert(snSession.sql("select * from t1").collect().length == 6)
    snSession.dropTable("t1")
  }
}
