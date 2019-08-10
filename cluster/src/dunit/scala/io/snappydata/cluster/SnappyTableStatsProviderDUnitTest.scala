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

package io.snappydata.cluster

import java.util.Properties

import scala.collection.JavaConverters._

import com.gemstone.gemfire.internal.cache.{CachedDeserializableFactory, DiskEntry, DistributedRegion, PartitionedRegion, RegionEntry}
import com.gemstone.gemfire.management.ManagementService
import com.gemstone.gemfire.management.internal.SystemManagementService
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.ui.SnappyRegionStats
import com.pivotal.gemfirexd.tools.sizer.GemFireXDInstrumentation
import io.snappydata.test.dunit.SerializableRunnable
import io.snappydata.{SnappyEmbeddedTableStatsProviderService, SnappyTableStatsProviderService}

import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation
import org.apache.spark.sql.{SaveMode, SnappyContext}

class SnappyTableStatsProviderDUnitTest(s: String) extends ClusterManagerTestBase(s)
{

  val table = "test.test_table"

  override def afterClass(): Unit = {
    ClusterManagerTestBase.stopSpark()
    super.afterClass()
  }
  def nodeShutDown(): Unit = {
    ClusterManagerTestBase.stopSpark()
    vm2.invoke(classOf[ClusterManagerTestBase], "stopAny")
    vm1.invoke(classOf[ClusterManagerTestBase], "stopAny")
    vm0.invoke(classOf[ClusterManagerTestBase], "stopAny")
  }

  def newContext(): SnappyContext = {
    val snc = SnappyContext(sc).newSession()
    io.snappydata.Property.ColumnBatchSize.set(snc.sessionState.conf, "5120")
    snc
  }

  def testVerifyTableStats(): Unit = {
    val snc = newContext()

    createTable(snc, table, "row")
    SnappyTableStatsProviderDUnitTest.verifyResults(snc, table, "R")
    snc.dropTable(table)

    createTable(snc, table, "row", Map("PERSISTENCE" -> "none"))
    SnappyTableStatsProviderDUnitTest.verifyResults(snc, table, "R")
    snc.dropTable(table)


    createTable(snc, table, "row", Map("PARTITION_BY" -> "col1"))
    SnappyTableStatsProviderDUnitTest.verifyResults(snc, table, "P")
    snc.dropTable(table)


    createTable(snc, table, "row", Map("PARTITION_BY" -> "col1", "PERSISTENCE" -> "sync"))
    SnappyTableStatsProviderDUnitTest.verifyResults(snc, table, "P")
    snc.dropTable(table)

    createTable(snc, table, "column")
    SnappyTableStatsProviderDUnitTest.verifyResults(snc, table)
    snc.dropTable(table)


    createTable(snc, table, "column", Map("BUCKETS" -> "2", "PARTITION_BY" -> "col1"))
    SnappyTableStatsProviderDUnitTest.verifyResults(snc, table)
    snc.dropTable(table)

    createTable(snc, table, "column", Map("PARTITION_BY" -> "col1", "PERSISTENCE" -> "sync"))
    SnappyTableStatsProviderDUnitTest.verifyResults(snc, table)
    snc.dropTable(table)

    createTable(snc, table, "column", Map("BUCKETS" -> "2",
      "PARTITION_BY" -> "col1", "PERSISTENT" -> "sync"))
    SnappyTableStatsProviderDUnitTest.verifyResults(snc, table)
    snc.dropTable(table)
  }

  def testVerifyTableStatsEvictionAndHA(): Unit = {
    val props = bootProps
    val port = ClusterManagerTestBase.locPort

    val restartServer = new SerializableRunnable() {
      override def run(): Unit = ClusterManagerTestBase.startSnappyServer(port, props)
    }

    val snc = newContext()

    createTable(snc, table, "column", Map("BUCKETS" -> "6"
      , "PERSISTENT" -> "sync"))

    SnappyTableStatsProviderDUnitTest.verifyResults(snc, table)

    vm1.invoke(classOf[ClusterManagerTestBase], "stopAny")
    vm1.invoke(restartServer)

    SnappyTableStatsProviderDUnitTest.verifyResults(snc, table)

    snc.dropTable(table)
  }

  def testHeapEvictionHA(): Unit = {

    var props = bootProps.clone().asInstanceOf[java.util.Properties]
    val port = ClusterManagerTestBase.locPort

    props.setProperty("eviction-heap-percentage", "20")

    def restartServer(props: Properties): SerializableRunnable = new SerializableRunnable() {
      override def run(): Unit = ClusterManagerTestBase.startSnappyServer(port, props)
    }

    val snc = newContext()
    val expectedRowCount = 1888622

    vm1.invoke(classOf[ClusterManagerTestBase], "stopAny")
    vm2.invoke(classOf[ClusterManagerTestBase], "stopAny")

    vm1.invoke(restartServer(props))
    vm2.invoke(restartServer(props))

    val airlineDataFrame = snc.read.load(getClass.getResource("/2015.parquet").getPath)
    snc.createTable(table, "column", airlineDataFrame.schema, Map("PERSISTENT" -> "async"))
    airlineDataFrame.write.format("column").mode(SaveMode.Append).saveAsTable(table)
    SnappyTableStatsProviderDUnitTest.verifyResults(snc, table, "C", expectedRowCount)

    vm1.invoke(classOf[ClusterManagerTestBase], "stopAny")
    vm2.invoke(classOf[ClusterManagerTestBase], "stopAny")

    props = bootProps

    vm1.invoke(restartServer(props))
    vm2.invoke(restartServer(props))

    SnappyTableStatsProviderDUnitTest.verifyResults(snc, table, "C", expectedRowCount)
    snc.dropTable(table, true)
  }


  def createTable(snc: SnappyContext, tableName: String,
      tableType: String, props: Map[String, String] = Map.empty): Unit = {
    val data = for (i <- 1 to 7000) yield (Seq(i, (i + 1), (i + 2)))
    val rdd = snc.sparkContext.parallelize(data.toSeq, 8).map(s =>
      new io.snappydata.externalstore.Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(tableName, tableType, dataDF.schema, props)
    dataDF.write.format(tableType).mode(SaveMode.Append).saveAsTable(tableName)
  }
}


object SnappyTableStatsProviderDUnitTest {

  def getPartitionedRegionStats(tableName: String, isColumnTable: Boolean):
  SnappyRegionStats = {
    var result = new SnappyRegionStats(tableName)
    if (isColumnTable) {
      result.setColumnTable(true)
      val columnBatchTableName = ColumnFormatRelation.columnBatchTableName(tableName)
      result = getDetailsForPR(columnBatchTableName, true, result)
    }
    getDetailsForPR(tableName, false, result)
  }

  def getDetailsForPR(table: String, isColumnBatchTable: Boolean,
      stats: SnappyRegionStats): SnappyRegionStats = {
    val region = Misc.getRegionForTable(table.toUpperCase, true).asInstanceOf[PartitionedRegion]
    val managementService = ManagementService.getManagementService(Misc.getGemFireCache).
        asInstanceOf[SystemManagementService]
    val regionBean = managementService.getLocalRegionMBean(region.getFullPath)
    val sizer = GemFireXDInstrumentation.getInstance()
    var entryOverhead = 0L
    var entryCount = 0L
    val (memSize, totalSize) = region.getDataStore.getAllLocalBucketRegions.asScala
        .foldLeft(0L -> 0L) { case ((msize, tsize), br) =>
          val overhead = br.estimateMemoryOverhead(sizer)
          if (entryOverhead == 0) {
            val iter = br.entries.regionEntries().iterator()
            if (iter.hasNext) {
              val re = iter.next()
              entryOverhead = sizer.sizeof(re) + (re match {
                case de: DiskEntry => sizer.sizeof(de.getDiskId)
                case _ => 0
              })
            }
          }
          entryCount += br.entryCount()
          (msize + br.getSizeInMemory + overhead, tsize + br.getTotalBytes + overhead)
        }
    stats.setReplicatedTable(false)
    stats.setBucketCount(region.getTotalNumberOfBuckets)
    val size = if (isColumnBatchTable) regionBean.getRowsInColumnBatches
    else regionBean.getEntryCount
    stats.setRowCount(stats.getRowCount + size)
    entryOverhead *= entryCount
    stats.setSizeInMemory(stats.getSizeInMemory + memSize + entryOverhead)
    stats.setTotalSize(stats.getTotalSize + totalSize + entryOverhead)
    stats.setSizeSpillToDisk(stats.getTotalSize - stats.getSizeInMemory)
    stats
  }

  def getReplicatedRegionStats(tableName: String): SnappyRegionStats = {
    val region = Misc.getRegionForTable(tableName.toUpperCase, true)
        .asInstanceOf[DistributedRegion]
    val result = new SnappyRegionStats(tableName)
    val managementService =
      ManagementService.getManagementService(Misc.getGemFireCache)
          .asInstanceOf[SystemManagementService]
    val sizer = GemFireXDInstrumentation.getInstance()

    def getReplicatedEntrySize(re: RegionEntry): Long = {
      var size = 0L
      val key = re.getRawKey
      if (key ne null) {
        size = CachedDeserializableFactory.calcMemSize(key)
      }
      size + CachedDeserializableFactory.calcMemSize(re._getValue())
    }

    var totalSize = region.estimateMemoryOverhead(sizer) +
        region.getBestLocalIterator(true).asScala
            .foldLeft(0L)(_ + getReplicatedEntrySize(_))
    val regionBean = managementService.getLocalRegionMBean(region.getFullPath)
    result.setReplicatedTable(true)
    result.setColumnTable(false)
    result.setBucketCount(1)
    result.setRowCount(regionBean.getEntryCount)
    val overhead = region.getBestLocalIterator(true).next() match {
      case de: DiskEntry => sizer.sizeof(de) + sizer.sizeof(de.getDiskId)
      case re => sizer.sizeof(re)
    }
    totalSize += overhead * result.getRowCount
    result.setSizeInMemory(totalSize)
    result.setTotalSize(totalSize)
    result.setSizeSpillToDisk(0)
    result
  }

  def getExpectedResult(snc: SnappyContext, tableName: String,
      isReplicatedTable: Boolean = false, isColumnTable: Boolean = false):
  SnappyRegionStats = {
    def aggregateResults(left: SnappyRegionStats,
        right: SnappyRegionStats):
    SnappyRegionStats = {
      left.getCombinedStats(right)
    }

    val expected = Utils.mapExecutors[RegionStat](snc.sparkContext, () => {
      val result = if (isReplicatedTable) getReplicatedRegionStats(tableName)
      else getPartitionedRegionStats(tableName, isColumnTable)
      Iterator[RegionStat](convertToSerializableForm(result))
    })

    expected.map(getRegionStat).reduce(aggregateResults)

  }

  def convertToSerializableForm(stat: SnappyRegionStats): RegionStat = {
    RegionStat(stat.getTableName, stat.getTotalSize, stat.getSizeInMemory,
      stat.getRowCount, stat.isColumnTable, stat.isReplicatedTable, stat.getBucketCount)
  }

  def getRegionStat(stat: RegionStat): SnappyRegionStats = {
    new SnappyRegionStats(stat.tableName, stat.totalSize,
      stat.memSize, stat.rowCount, stat.isColumnType, stat.isReplicated, stat.bucketCount)
  }


  def verifyResults(snc: SnappyContext, table: String,
      tableType: String = "C", expectedRowCount: Int = 7000): Unit = {
    SnappyEmbeddedTableStatsProviderService.publishColumnTableRowCountStats()
    val isColumnTable = tableType.equals("C")
    val isReplicatedTable = tableType.equals("R")
    def expected = SnappyTableStatsProviderDUnitTest.getExpectedResult(snc, table,
      isReplicatedTable, isColumnTable)
    def actual = SnappyTableStatsProviderService.getService.
        getAggregatedStatsOnDemand._1(table.toUpperCase)

    assert(actual.getTableName.toLowerCase == expected.getTableName)
    assert(actual.isColumnTable == expected.isColumnTable,
      s"Actual=${actual.isColumnTable} expected=${expected.isColumnTable} for $table")
    
    ClusterManagerTestBase.waitForCriterion(actual.getSizeInMemory == expected.getSizeInMemory
        && actual.getSizeInMemory == expected.getSizeInMemory
        && actual.getRowCount == expected.getRowCount,
      s"Expected Size ${expected.getSizeInMemory} Size ${actual.getSizeInMemory} \n" +
          s"Expected Total Size ${expected.getTotalSize} Total Size ${actual.getTotalSize} \n" +
          s"Expected Count ${expected.getRowCount} Count  ${actual.getRowCount} \n",
      20000, 1000, true)
  }
}

case class RegionStat(tableName: String, totalSize: Long,
    memSize: Long, rowCount: Long, isColumnType: Boolean,
    isReplicated: Boolean, bucketCount: Int)
