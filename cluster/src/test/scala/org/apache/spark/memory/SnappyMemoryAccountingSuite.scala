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

import java.nio.charset.StandardCharsets
import java.sql.SQLException
import java.util.Properties

import scala.actors.Futures._

import com.gemstone.gemfire.cache.LowMemoryException
import com.gemstone.gemfire.internal.cache.{GemFireCacheImpl, LocalRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.externalstore.Data
import io.snappydata.test.dunit.DistributedTestBase.InitializeRun

import org.apache.spark.sql.catalyst.expressions.{SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{CachedDataFrame, Row, SnappyContext, SnappySession}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{SparkEnv, TaskContextImpl}


class SnappyMemoryAccountingSuite extends MemoryFunSuite {

  InitializeRun.setUp()


  val struct = (new StructType())
      .add(StructField("col1", IntegerType, true))
      .add(StructField("col2", IntegerType, true))
      .add(StructField("col3", IntegerType, true))

  val options = Map("PARTITION_BY" -> "col1", "EVICTION_BY" ->
    "LRUHEAPPERCENT")
  val coptions = Map("PARTITION_BY" -> "col1", "BUCKETS" -> "1",
    "EVICTION_BY" -> "LRUHEAPPERCENT")
  val cwoptions = Map("BUCKETS" -> "1", "EVICTION_BY" -> "LRUHEAPPERCENT")
  val roptions = Map("EVICTION_BY" -> "LRUHEAPPERCENT",
    "PERSISTENCE" -> "NONE")
  val poptions = Map("PARTITION_BY" -> "col1", "BUCKETS" -> "1", "PERSISTENCE" -> "SYNCHRONOUS")
  val memoryMode = MemoryMode.ON_HEAP

  test("Test drop table accounting for column partitioned table") {
    val sparkSession = createSparkSession(1, 0, 2000000L)
    val snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    val options = Map("PARTITION_BY" -> "col1",
      "BUCKETS" -> "1",
      "EVICTION_BY" -> "LRUHEAPPERCENT"
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

  test("Test drop table accounting for replicated table") {
    val sparkSession = createSparkSession(1, 0, 2000000L)
    val snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    val options = Map.empty[String, String]

    val beforeTableSize = SparkEnv.get.memoryManager.storageMemoryUsed
    snSession.createTable("t1", "row", struct, options)
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

  test("Test truncate table accounting for replicated table") {
    val sparkSession = createSparkSession(1, 0, 2000000L)
    val snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    val options = Map.empty[String, String]

    val beforeTableSize = SparkEnv.get.memoryManager.storageMemoryUsed
    snSession.createTable("t1", "row", struct, options)
    val afterTableSize = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afterTableSize > beforeTableSize)

    val row = Row(100000000, 10000000, 10000000)
    (1 to 10).map(i => snSession.insert("t1", row))
    val afterInsertSize = SparkEnv.get.memoryManager.storageMemoryUsed
    snSession.truncateTable("t1")
    val afterTruncateSize = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afterTruncateSize < afterInsertSize)
  }

  test("Test truncate table accounting for PR table") {
    val sparkSession = createSparkSession(1, 0, 2000000L)
    val snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    val options = Map("PARTITION_BY" -> "col1",
      "BUCKETS" -> "1",
      "EVICTION_BY" -> "LRUHEAPPERCENT"
    )

    val beforeTableSize = SparkEnv.get.memoryManager.storageMemoryUsed
    snSession.createTable("t1", "row", struct, options)
    val afterTableSize = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afterTableSize > beforeTableSize)

    val row = Row(100000000, 10000000, 10000000)
    (1 to 10).map(i => snSession.insert("t1", row))
    val afterInsertSize = SparkEnv.get.memoryManager.storageMemoryUsed
    snSession.truncateTable("t1")
    val afterTruncateSize = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afterTruncateSize < afterInsertSize)
  }

  test("Test delete all accounting for replicated table") {
    val sparkSession = createSparkSession(1, 0, 2000000L)
    val snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    val options = Map.empty[String, String]

    val beforeTableSize = SparkEnv.get.memoryManager.storageMemoryUsed
    snSession.createTable("t1", "row", struct, options)
    val afterTableSize = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afterTableSize > beforeTableSize)

    val row = Row(100000000, 10000000, 10000000)
    (1 to 10).map(i => snSession.insert("t1", row))
    val afterInsertSize = SparkEnv.get.memoryManager.storageMemoryUsed
    snSession.sql("delete from t1")
    val afetrDeleteSize = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afetrDeleteSize < afterInsertSize)
  }

  test("Test delete all accounting for PR table") {
    val sparkSession = createSparkSession(1, 0, 2000000L)
    val snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    val options = Map("PARTITION_BY" -> "col1",
      "BUCKETS" -> "1",
      "EVICTION_BY" -> "LRUHEAPPERCENT"
    )

    val beforeTableSize = SparkEnv.get.memoryManager.storageMemoryUsed
    snSession.createTable("t1", "row", struct, options)
    val afterTableSize = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afterTableSize > beforeTableSize)

    val row = Row(100000000, 10000000, 10000000)
    (1 to 10).map(i => snSession.insert("t1", row))
    val afterInsertSize = SparkEnv.get.memoryManager.storageMemoryUsed
    snSession.sql("delete from t1")
    val afetrDeleteSize = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afetrDeleteSize < afterInsertSize)
  }

  test("Test drop table accounting for row partitioned table") {
    val sparkSession = createSparkSession(1, 0, 2000000L)
    val snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    val options = Map("PARTITION_BY" -> "col1",
      "BUCKETS" -> "1",
      "EVICTION_BY" -> "LRUHEAPPERCENT"
    )

    val beforeTableSize = SparkEnv.get.memoryManager.storageMemoryUsed
    snSession.createTable("t1", "row", struct, options)
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


  test("Test accounting for column table with eviction") {
    val sparkSession = createSparkSession(1, 0)
    val snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    val options = Map("PARTITION_BY" -> "col1",
      "BUCKETS" -> "1",
      "EVICTION_BY" -> "LRUHEAPPERCENT"
    )
    snSession.createTable("t1", "column", struct, options)
    SparkEnv.get.memoryManager.asInstanceOf[SnappyUnifiedMemoryManager].dropAllObjects(memoryMode)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed == 0)
    val taskAttemptId = 0L
    // artificially acquire memory
    SparkEnv.get.memoryManager.acquireExecutionMemory(5000L, taskAttemptId, memoryMode)

    var totalEvictedBytes = 0L

    val memoryEventListener = new MemoryEventListener {
      override def onEviction(objectName: String, evictedBytes: Long): Unit = {
        totalEvictedBytes += evictedBytes
      }
    }
    SnappyUnifiedMemoryManager.addMemoryEventListener(memoryEventListener)

    // 208 *10. 208 is the row size + memory overhead

    var rows = 0
    try {
      for (i <- 1 to 100) {
        val row = Row(100000000, 10000000, 10000000)
        logInfo(s"RowCount1 = $rows")
        snSession.insert("t1", row)
        rows += 1
        logInfo(s"RowCount2 = $rows")
      }
    } catch {
      case sqle: SQLException if sqle.getSQLState == "XCL54" =>
        logInfo(s"RowCount3 in exception = $rows")
        assert(totalEvictedBytes > 0)
    }
    SparkEnv.get.memoryManager.
        asInstanceOf[SnappyUnifiedMemoryManager].dropAllObjects(memoryMode)
    val count = snSession.sql("select * from t1").count()
    assert(count >= rows)
    snSession.dropTable("t1")
  }

  test("Test accounting for recovery of row partitioned tables with lru count & no persistent") {
    var sparkSession = createSparkSession(1, 0)
    var snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    val options = "OPTIONS (BUCKETS '1', " +
        "PARTITION_BY 'Col1', " +
        "PERSISTENCE 'none', " +
        "EVICTION_BY 'LRUCOUNT 3')"
    snSession.sql("CREATE TABLE t1 (Col1 INT, Col2 INT, Col3 INT) " + " USING row " +
        options
    )
    val beforeInsertMem = SparkEnv.get.memoryManager.storageMemoryUsed

    val row = Row(100000000, 10000000, 10000000)
    (1 to 5).map(i => snSession.insert("t1", row))

    SnappyContext.globalSparkContext.stop()
    assert(SparkEnv.get == null)
    sparkSession = createSparkSession(1, 0)
    snSession = new SnappySession(sparkSession.sparkContext)

    assert(snSession.sql("select * from t1").collect().length == 0)
    val afterRebootMemory = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(beforeInsertMem == afterRebootMemory) // 4 bytes for hashmap. Need to check
    snSession.dropTable("t1")
  }

  test("Test accounting for recovery of row partitioned tables with lru count & persistent") {
    assert(GemFireCacheImpl.getInstance == null)
    var sparkSession = createSparkSession(1, 0)
    var snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    val options = "OPTIONS (BUCKETS '1', " +
        "PARTITION_BY 'Col1', " +
        "PERSISTENCE 'SYNCHRONOUS', " +
        "EVICTION_BY 'LRUCOUNT 3', " +
        "OVERFLOW 'true')"
    snSession.sql("CREATE TABLE t1 (Col1 INT, Col2 INT, Col3 INT) " + " USING row " +
        options
    )

    val row = Row(100000000, 10000000, 10000000)
    (1 to 5).map(i => snSession.insert("t1", row))
    val beforeRebootMemory = SparkEnv.get.memoryManager.storageMemoryUsed
    SnappyContext.globalSparkContext.stop()

    assert(SparkEnv.get == null)
    sparkSession = createSparkSession(1, 0)
    snSession = new SnappySession(sparkSession.sparkContext)

    assert(snSession.sql("select * from t1").collect().length == 5)

    val afterRebootMemory = SparkEnv.get.memoryManager.storageMemoryUsed
    // Due to a design flaw in recovery we always recover one more value than the LRU limit.
    assertApproximate(beforeRebootMemory, afterRebootMemory)
    snSession.dropTable("t1")
  }

  test("Test accounting for recovery of row replicate tables with lru count & no persistent") {

    var sparkSession = createSparkSession(1, 0)
    var snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    val options = "OPTIONS (EVICTION_BY 'LRUCOUNT 3', OVERFLOW 'true', PERSISTENCE 'none')"
    snSession.sql("CREATE TABLE t1 (Col1 INT, Col2 INT, Col3 INT) USING row " +
        options)
    val beforeInsertMem = SparkEnv.get.memoryManager.storageMemoryUsed

    val row = Row(100000000, 10000000, 10000000)
    (1 to 5).map(i => snSession.insert("t1", row))

    SnappyContext.globalSparkContext.stop()
    assert(SparkEnv.get == null)
    sparkSession = createSparkSession(1, 0)
    snSession = new SnappySession(sparkSession.sparkContext)

    assert(snSession.sql("select * from t1").collect().length == 0)
    val afterRebootMemory = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(beforeInsertMem == afterRebootMemory) // 4 bytes for hashmap. Need to check
    snSession.dropTable("t1")
  }

  test("Test accounting for recovery of row replicate tables with lru count & persistent") {
    assert(GemFireCacheImpl.getInstance == null)
    var sparkSession = createSparkSession(1, 0)
    var snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    val options = "OPTIONS (EVICTION_BY 'LRUCOUNT 3', PERSISTENCE 'SYNCHRONOUS')"
    snSession.sql("CREATE TABLE t1 (Col1 INT, Col2 INT, Col3 INT) " + " USING row " +
        options
    )

    val row = Row(100000000, 10000000, 10000000)
    (1 to 5).map(i => snSession.insert("t1", row))

    val beforeRebootMemory = SparkEnv.get.memoryManager.storageMemoryUsed
    SnappyContext.globalSparkContext.stop()
    assert(SparkEnv.get == null)
    sparkSession = createSparkSession(1, 0)
    snSession = new SnappySession(sparkSession.sparkContext)

    assert(snSession.sql("select * from t1").collect().length == 5)
    val afterRebootMemory = SparkEnv.get.memoryManager.storageMemoryUsed
    // Due to a design flaw in recovery we always recover one more value than the LRU limit.
    assertApproximate(beforeRebootMemory, afterRebootMemory)
    snSession.dropTable("t1")
  }


  test("Test Recovery column partitioned table") {
    var sparkSession = createSparkSession(1, 0, 100000000L)

    var snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    val options = "OPTIONS (BUCKETS '1', PARTITION_BY 'Col1', PERSISTENCE 'SYNCHRONOUS')"
    snSession.sql("CREATE TABLE t1 (Col1 INT, Col2 INT, Col3 INT) " + " USING column " +
        options
    )

    (1 to 10).map(i => snSession.insert("t1", Row(i, 10000000, 10000000)))

    val beforeRebootMemory = SparkEnv.get.memoryManager.storageMemoryUsed
    SnappyContext.globalSparkContext.stop()
    assert(SparkEnv.get == null)
    sparkSession = createSparkSession(1, 0, 1000000L)
    snSession = new SnappySession(sparkSession.sparkContext)

    assert(snSession.sql("select * from t1").collect().length == 10)
    val afterRebootMemory = SparkEnv.get.memoryManager.storageMemoryUsed
    assertApproximate(beforeRebootMemory, afterRebootMemory, 4)
    snSession.dropTable("t1")
  }


  test("Test accounting of eviction for row partitioned table with lru heap percent") {
    val sparkSession = createSparkSession(1, 0)
    val snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    val options = Map("PARTITION_BY" -> "col1",
      "PERSISTENCE" -> "none",
      "BUCKETS" -> "1",
      "EVICTION_BY" -> "LRUHEAPPERCENT"
    )
    snSession.createTable("t1", "row", struct, options)
    SparkEnv.get.memoryManager.asInstanceOf[SnappyUnifiedMemoryManager].dropAllObjects(memoryMode)

    val taskAttemptId = 0L
    // artificially acquire memory
    SparkEnv.get.memoryManager.acquireExecutionMemory(4000L, taskAttemptId, memoryMode)
    var memoryIncreaseDuetoEviction = 0L
    val memoryEventListener = new MemoryEventListener {
      override def onPositiveMemoryIncreaseDueToEviction(objectName: String, bytes: Long): Unit = {
        memoryIncreaseDuetoEviction += bytes
      }
    }
    SnappyUnifiedMemoryManager.addMemoryEventListener(memoryEventListener)

    // 208 *10. 208 is the row size + memory overhead
    import scala.util.control.Breaks._

    var rows = 0
    try {
      breakable {
        for (i <- 1 to 20) {
          val row = Row(100000000, 10000000, 10000000)
          snSession.insert("t1", row)
          rows += 1
        }
      }
    } catch {
      case e: Exception => {
        assert(memoryIncreaseDuetoEviction > 0)
      }
    }
    SparkEnv.get.memoryManager.
        asInstanceOf[SnappyUnifiedMemoryManager].dropAllObjects(memoryMode)
    val count = snSession.sql("select * from t1").count()
    assert(count == rows)
    snSession.dropTable("t1")
  }

  test("Test accounting of delete for row partitioned tables") {
    val sparkSession = createSparkSession(1, 0)
    val snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    snSession.createTable("t1", "row", struct, poptions)
    val afterCreateTable = SparkEnv.get.memoryManager.storageMemoryUsed
    val region = GemFireCacheImpl.getExisting.getRegion("/APP/T1").asInstanceOf[LocalRegion]
    val row = Row(1, 1, 1)
    snSession.insert("t1", row)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed > 0) // borrowed from execution memory
    snSession.delete("t1", "col1=1")
    // we need to wait for atleast OLD_ENTRIES_CLEANER_TIME_INTERVAL
    ClusterManagerTestBase.waitForCriterion(
      (SparkEnv.get.memoryManager.storageMemoryUsed == afterCreateTable),
      s"The memory after delete is not same even after waiting for oldEntryRemoval",
      4 * Misc.getGemFireCache.getOldEntryRemovalPeriod, 500, true)
    snSession.dropTable("t1")
  }


  test("Test Spark Cache") {
    val sparkSession = createSparkSession(1, 0)
    val snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    SparkEnv.get.memoryManager.
        asInstanceOf[SnappyUnifiedMemoryManager].dropAllObjects(memoryMode)
    val beforeCache = SparkEnv.get.memoryManager.storageMemoryUsed
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sparkSession.sparkContext.parallelize(data, 2).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snSession.createDataFrame(rdd)
    dataDF.cache()
    dataDF.count
    val afterCache = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afterCache > beforeCache)
  }

  test("Test accounting of delete for replicated tables") {
    val sparkSession = createSparkSession(1, 0, sparkMemory = 1200000L)
    val snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    snSession.createTable("t1", "row", struct, Map.empty[String, String])
    val afterCreateTable = SparkEnv.get.memoryManager.storageMemoryUsed
    val row = Row(1, 1, 1)
    snSession.insert("t1", row)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed > 0) // borrowed from execution memory
    snSession.delete("t1", "col1=1")
    // we need to wait for atleast OLD_ENTRIES_CLEANER_TIME_INTERVAL
    ClusterManagerTestBase.waitForCriterion(
      (SparkEnv.get.memoryManager.storageMemoryUsed == afterCreateTable),
      s"The memory after delete is not same even after waiting for oldEntryRemoval",
      4 * Misc.getGemFireCache.getOldEntryRemovalPeriod, 500, true)
    // assert(afterDelete == afterCreateTable)
    snSession.dropTable("t1")
  }

  test("Test accounting of update for replicated tables") {
    val sparkSession = createSparkSession(1, 0, 1000000L)
    val snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    val struct = (new StructType())
        .add(StructField("col1", IntegerType, true))
        .add(StructField("col2", IntegerType, true))
        .add(StructField("col3", StringType, true))

    snSession.createTable("t1", "row", struct, Map.empty[String, String])
    val row = Row(1, 1, "1")
    snSession.insert("t1", row)
    val afterInsert = SparkEnv.get.memoryManager.storageMemoryUsed
    snSession.update("t1", "COL1=1", Row("XXXXXXXXXX"), "COL3")
    val afterUpdate = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afterUpdate > afterInsert)
    snSession.dropTable("t1")
  }

  test("Test accounting of update for row partitioned tables") {
    val sparkSession = createSparkSession(1, 0, 1000000L)
    val snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    val struct = (new StructType())
        .add(StructField("col1", IntegerType, true))
        .add(StructField("col2", IntegerType, true))
        .add(StructField("col3", StringType, true))

    snSession.createTable("t1", "row", struct, roptions)
    val row = Row(1, 1, "1")
    snSession.insert("t1", row)
    val afterInsert = SparkEnv.get.memoryManager.storageMemoryUsed
    snSession.update("t1", "COL1=1", Row("XXXXXXXXXX"), "COL3")
    val afterUpdate = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afterUpdate > afterInsert)
    snSession.dropTable("t1")
  }

  test("Test accounting of drop table for replicated tables") {
    val sparkSession = createSparkSession(1, 0, sparkMemory = 1200000L)
    val snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    val beforeCreateTable = SparkEnv.get.memoryManager.storageMemoryUsed
    snSession.createTable("t1", "row", struct, roptions)
    val row = Row(1, 1, 1)
    snSession.insert("t1", row)
    snSession.dropTable("t1")
    val afterDropTable = SparkEnv.get.memoryManager.storageMemoryUsed
    // Approximate because drop table adds entry in system table which causes memory to grow a bit
    assertApproximate(afterDropTable, beforeCreateTable, error = 10)
  }

  test("Test storage for column tables with df inserts") {
    val sparkSession = createSparkSession(1, 0)
    val snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    snSession.createTable("t1", "column", struct, cwoptions)
    val afterCreate = SparkEnv.get.memoryManager.storageMemoryUsed
    val data = (1 to 10).toSeq

    val rdd = sparkSession.sparkContext.parallelize(data, 2)
        .map(s => Data1(s, s + 1, s + 2))
    val dataDF = snSession.createDataFrame(rdd)
    dataDF.write.insertInto("t1")
    assert(SparkEnv.get.memoryManager.storageMemoryUsed > afterCreate)
    val count = snSession.sql("select * from t1").count()
    assert(count == 10)
    snSession.dropTable("t1")

  }

  test("Concurrent query mem-check"){
    val sparkSession = createSparkSession(1, 0, 1000000)
    val snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 120 * 100

    val options = "OPTIONS (BUCKETS '8', " +
      "PARTITION_BY 'Col1')"

    snSession.sql("CREATE TABLE t1 (Col1 INT, Col2 INT, Col3 INT) " + " USING row " +
      options
    )
    val rowCount = 100

    def runQueries(i : Int): Unit = {
      for (_ <- 0 until rowCount) {
        snSession.insert("t1", Row(1, 1, 1))
      }
    }

    val tasks = for (i <- 1 to 5) yield future {
      runQueries(i)
    }

    // wait a lot
    awaitAll(20000000L, tasks: _*)

    // Rough estimation of 120 bytes per row
    assert(SparkEnv.get.memoryManager.storageMemoryUsed >= 120 * 100 * 5 )
    val count = snSession.sql("select * from t1").count()
    assert(count == 500)
    snSession.dropTable("t1")
  }

  test("CachedDataFrame accounting") {
    val sparkSession = createSparkSession(1, 1)
    // create SnappySession to boot GemFireCache which is required for SnappyUMM
    new SnappySession(sparkSession.sparkContext)
    val fieldTypes: Array[DataType] = Array(LongType, StringType, BinaryType)
    val converter = UnsafeProjection.create(fieldTypes)

    val row = new SpecificInternalRow(fieldTypes)
    row.setLong(0, 0)
    row.update(1, UTF8String.fromString("Hello"))
    row.update(2, "World".getBytes(StandardCharsets.UTF_8))

    val unsafeRow: UnsafeRow = converter.apply(row)

    SparkEnv.get.memoryManager
          .acquireStorageMemory(MemoryManagerCallback.storageBlockId, 300, memoryMode)

    val taskMemoryManager =
      new TaskMemoryManager(sparkSession.sparkContext.env.memoryManager, 0L)
    val taskContext =
      new TaskContextImpl(0, 0, taskAttemptId = 1, 0, taskMemoryManager, new Properties, null)
    try {
      CachedDataFrame(taskContext, Seq(unsafeRow).iterator)
      assert(false , "Should not have obtained memory")
    } catch {
      case lme : LowMemoryException => // Success
    }
  }

  // @TODO Place holder for column partitioned tables. Enable them after Sumedh's changes

  // Enable test after Sumedh's checkin
  ignore("Test accounting of delete for column partitioned tables") {
    val sparkSession = createSparkSession(1, 0)
    val snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    snSession.createTable("t1", "column", struct, poptions)
    val afterCreateTable = SparkEnv.get.memoryManager.storageMemoryUsed
    val row = Row(1, 1, 1)
    snSession.insert("t1", row)
    assert(SparkEnv.get.memoryManager.storageMemoryUsed > 0) // borrowed from execution memory
    snSession.delete("t1", "col1=1")
    val afterDelete = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afterDelete == afterCreateTable)
    snSession.dropTable("t1")
  }

  ignore("Test accounting of update for column partitioned tables") {
    val sparkSession = createSparkSession(1, 0, 1000000L)
    val snSession = new SnappySession(sparkSession.sparkContext)
    LocalRegion.MAX_VALUE_BEFORE_ACQUIRE = 1
    val struct = (new StructType())
        .add(StructField("col1", IntegerType, true))
        .add(StructField("col2", IntegerType, true))
        .add(StructField("col3", StringType, true))

    snSession.createTable("t1", "column", struct, roptions)
    val row = Row(1, 1, "1")
    snSession.insert("t1", row)
    val afterInsert = SparkEnv.get.memoryManager.storageMemoryUsed
    snSession.update("t1", "COL1=1", Row("XXXXXXXXXX"), "COL3")
    val afterUpdate = SparkEnv.get.memoryManager.storageMemoryUsed
    assert(afterUpdate > afterInsert)
    snSession.dropTable("t1")
  }
}
