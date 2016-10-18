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

package io.snappydata.cluster

import scala.collection.JavaConverters._

import com.gemstone.gemfire.internal.cache.{DistributedRegion, LocalRegion, PartitionedRegion}
import com.gemstone.gemfire.management.internal.SystemManagementService
import com.gemstone.gemfire.management.{ManagementService, RegionMXBean}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.ui.SnappyRegionStatsCollectorResult
import com.pivotal.gemfirexd.tools.sizer.GemFireXDInstrumentation
import io.snappydata.SnappyTableStatsProviderService
import io.snappydata.test.dunit.SerializableRunnable

import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation
import org.apache.spark.sql.{SaveMode, SnappyContext}

class SnappyTableStatsProviderDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  override def beforeClass(): Unit = {
    ClusterManagerTestBase.stopSpark()
    bootProps.setProperty("eviction-heap-percentage", "20")
    bootProps.setProperty("spark.sql.inMemoryColumnarStorage.batchSize", "500")
    super.beforeClass()
  }


  override def afterClass(): Unit = {
    super.afterClass()
    // force restart with default properties in subsequent tests
    ClusterManagerTestBase.stopSpark()
  }

  def testVerifyTableStats(): Unit = {
    val snc = SnappyContext(sc).newSession()
    var table = "TEST.TEST_TABLE"

    createTable(snc, table, "row")
    SnappyTableStatsProviderDUnitTest.verifyResults(snc, table, "R")
    snc.dropTable(table)


    createTable(snc, table, "row", Map("PARTITION_BY" -> "col1"))
    SnappyTableStatsProviderDUnitTest.verifyResults(snc, table, "P")
    snc.dropTable(table)


    createTable(snc, table, "row", Map("PARTITION_BY" -> "col1", "PERSISTENT" -> "sync"))
    SnappyTableStatsProviderDUnitTest.verifyResults(snc, table, "P")
    snc.dropTable(table)

    createTable(snc, table, "column")
    SnappyTableStatsProviderDUnitTest.verifyResults(snc, table)
    snc.dropTable(table)


    createTable(snc, table, "column", Map("BUCKETS" -> "2", "PARTITION_BY" -> "col1"))
    SnappyTableStatsProviderDUnitTest.verifyResults(snc, table)
    snc.dropTable(table)

    createTable(snc, table, "column", Map("PARTITION_BY" -> "col1", "PERSISTENT" -> "sync"))
    SnappyTableStatsProviderDUnitTest.verifyResults(snc, table)
    snc.dropTable(table)

    createTable(snc, table, "column", Map("BUCKETS" -> "2",
      "PARTITION_BY" -> "col1", "PERSISTENT" -> "sync"))
    SnappyTableStatsProviderDUnitTest.verifyResults(snc, table)
    snc.dropTable(table)
  }

  def testVerifyTableStatsEvictionAndHA(): Unit = {
    val props = bootProps
    val port = locatorPort
    val expectedRowCount = 1888622

    val snc = SnappyContext(sc).newSession()
    val table = "TEST.TEST_TABLE"

    val airlineDataFrame = snc.read.load(getClass.getResource("/2015.parquet").getPath)
    snc.createTable(table, "column", airlineDataFrame.schema, Map.empty[String, String])
    airlineDataFrame.write.format("column").mode(SaveMode.Append).saveAsTable(table)

    SnappyTableStatsProviderDUnitTest.verifyResults(snc, table, "C", expectedRowCount)

    snc.dropTable(table)

    snc.createTable(table, "column", airlineDataFrame.schema, Map("PERSISTENT" -> "SYNC"))
    airlineDataFrame.write.format("column").mode(SaveMode.Append).saveAsTable(table)

    SnappyTableStatsProviderDUnitTest.verifyResults(snc, table, "C", expectedRowCount)
    vm1.invoke(classOf[ClusterManagerTestBase], "stopAny")

    vm1.invoke(new SerializableRunnable() {
      override def run(): Unit = {
        ClusterManagerTestBase.startSnappyServer(port, props)
      }
    })

    SnappyTableStatsProviderDUnitTest.verifyResults(snc, table, "C", expectedRowCount)

    snc.dropTable(table, true)
  }


  def createTable(snc: SnappyContext, tableName: String,
      tableType: String, props: Map[String, String] = Map.empty): Unit = {
    val data = for (i <- 1 to 700) yield (Seq(i, (i + 1), (i + 2)))
    val rdd = snc.sparkContext.parallelize(data.toSeq, data.length).map(s =>
      new io.snappydata.externalstore.Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(tableName, tableType, dataDF.schema, props)
    dataDF.write.format(tableType).mode(SaveMode.Append).saveAsTable(tableName)
  }
}


object SnappyTableStatsProviderDUnitTest {

  def getPartitionedRegionStats(tableName: String, isColumnTable: Boolean):
  SnappyRegionStatsCollectorResult = {
    val region = Misc.getRegionForTable(tableName, true).asInstanceOf[PartitionedRegion]
    var result = new SnappyRegionStatsCollectorResult(tableName,
      region.asInstanceOf[LocalRegion].getDataPolicy)
    if (isColumnTable) {
      result.setColumnTable(true)
      val cachedBatchTableName = ColumnFormatRelation.cachedBatchTableName(tableName)
      result = getDetailsForPR(cachedBatchTableName, true, result)
    }
    getDetailsForPR(tableName, false, result)
  }

  def getDetailsForPR(table: String, isCachedBatchTable: Boolean,
      stats: SnappyRegionStatsCollectorResult): SnappyRegionStatsCollectorResult = {
    val region = Misc.getRegionForTable(table, true).asInstanceOf[PartitionedRegion]
    val managementService = ManagementService.getManagementService(Misc.getGemFireCache).
        asInstanceOf[SystemManagementService]
    val regionBean: RegionMXBean = managementService.getLocalRegionMBean(region.getFullPath)
    val totalSize = region.getDataStore.getAllLocalBucketRegions.asScala.
        foldLeft(0L)(_ + _.getTotalBytes)
    stats.setTotalSize(stats.getTotalSize + totalSize)
    stats.setSizeInMemory(stats.getSizeInMemory + regionBean.getEntrySize)
    val size = if (isCachedBatchTable) regionBean.getRowsInCachedBatches
    else regionBean.getEntryCount
    stats.setRowCount(stats.getRowCount + size)
    stats
  }

  def getReplicatedRegionStats(tableName: String): SnappyRegionStatsCollectorResult = {
    val region = Misc.getRegionForTable(tableName, true).asInstanceOf[DistributedRegion]
    val result = new SnappyRegionStatsCollectorResult(tableName, region.getDataPolicy)
    val managementService =
      ManagementService.getManagementService(Misc.getGemFireCache)
          .asInstanceOf[SystemManagementService]
    val totalSize = region.getBestLocalIterator(true).asScala.
        foldLeft(0L)(_ + GemFireXDInstrumentation.getInstance.sizeof(_))
    val regionBean = managementService.getLocalRegionMBean(region.getFullPath)
    result.setSizeInMemory(totalSize)
    result.setColumnTable(false)
    result.setRowCount(regionBean.getEntryCount)
    result
  }

  def getExpectedResult(snc: SnappyContext, tableName: String,
      isReplicatedTable: Boolean = false, isColumnTable: Boolean = false):
  SnappyRegionStatsCollectorResult = {
    def aggregateResults(left: SnappyRegionStatsCollectorResult,
        right: SnappyRegionStatsCollectorResult):
    SnappyRegionStatsCollectorResult = {
      left.getCombinedStats(right)
    }

    val expected = Utils.mapExecutors[SnappyRegionStatsCollectorResult](snc, () => {
      val result = if (isReplicatedTable) getReplicatedRegionStats(tableName)
      else getPartitionedRegionStats(tableName, isColumnTable)
      Iterator[SnappyRegionStatsCollectorResult](result)
    }).collect()

    expected.reduce(aggregateResults(_, _))

  }


  def verifyResults(snc: SnappyContext, table: String,
      tableType: String = "C", expectedRowCount: Int = 700): Unit = {
    val isColumnTable = if (tableType.equals("C")) true else false
    val isReplicatedTable = if (tableType.equals("R")) true else false
    def expected = SnappyTableStatsProviderDUnitTest.getExpectedResult(snc, table,
      isReplicatedTable, isColumnTable)
    def actual = SnappyTableStatsProviderService.
        getAggregatedTableStatsOnDemand(snc.sparkContext).get(table).get


    assert(actual.getRegionName == expected.getRegionName)
    assert(actual.getDataPolicy == expected.getDataPolicy)
    assert(actual.isColumnTable == expected.isColumnTable)

    ClusterManagerTestBase.waitForCriterion(actual.getSizeInMemory == expected.getSizeInMemory
        && actual.getSizeInMemory == expected.getSizeInMemory
        && actual.getRowCount == expected.getRowCount,
      s"Expected Size ${expected.getSizeInMemory} Actual size ${actual.getSizeInMemory} \n" +
      s"Expected Total Size ${expected.getTotalSize} Actual Total size  ${actual.getTotalSize} \n" +
      s"Expected Count ${expected.getRowCount} Actual Count  ${actual.getRowCount} \n",
      10000, 1000, true)
  }
}
