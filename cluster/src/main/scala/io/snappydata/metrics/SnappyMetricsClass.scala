/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package io.snappydata.metrics

import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.ui.{ClusterStatistics, MemberStatistics, SnappyExternalTableStats, SnappyRegionStats}
import io.snappydata.SnappyTableStatsProviderService
import org.apache.spark.SparkContext
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.groupon.metrics._

import scala.collection.mutable

object SnappyMetricsClass {

  val oldSizeArr = Array.fill[Int](22)(0)

  def init(sc: SparkContext): Unit = {

    // initialize metric system with cluster id as metrics namespace
    val clusterUuid = Misc.getMemStore.getMetadataCmdRgn.get("__ClusterID__")
    UserMetricsSystem.initialize(sc, clusterUuid)

    val timeInterval = 1000

    // concurrently executing threads to get stats from StatsProviderServices
    val runnable = new Runnable {
      override def run(): Unit = {
        while (true) {
          try {
            Thread.sleep(timeInterval)

            // get cluster stats and publish into metrics system
            setMetricsForClusterStatDetails()

            // get table stats and publish into metrics system
            val tableBuff = SnappyTableStatsProviderService.getService.getAllTableStatsFromService
            setMetricsForTableStatDetails(tableBuff)

            // get external table stats and publish into metrics system
            val externalTableBuff =
              SnappyTableStatsProviderService.getService.getAllExternalTableStatsFromService
            setMetricsForExternalTableStatDetails(externalTableBuff)

            // get member stats and publish into metrics system
            val memberBuff = SnappyTableStatsProviderService.getService.getMembersStatsFromService
            setMetricsForMemberStatDetails(memberBuff)
          }
          catch {
            case e: InterruptedException => e.printStackTrace()
          }
        }
      }
    }
    val thread: Thread = new Thread(runnable)
    thread.start()
  }

  def createGauge(metricName: String, metricValue: AnyVal): Unit = {
    lazy val tempGauge: SparkGauge = UserMetricsSystem.gauge(metricName)
    tempGauge.set(metricValue)
  }

  def createHistogram(metricName: String, metricValue: Long): Unit = {
    lazy val tempHistogram: SparkHistogram = UserMetricsSystem.histogram(metricName)
    tempHistogram.update(metricValue)
  }

  def setMetricsForClusterStatDetails(): Unit = {
    val csInstance = ClusterStatistics.getInstance()
    createGauge("ClusterMetrics.coresInfo.totalCores", csInstance.getTotalCPUCores)
    updateHistogram("ClusterMetrics.timeLine", 0,
      csInstance.getUsageTrends(ClusterStatistics.TREND_TIMELINE).toList)
    updateHistogram("ClusterMetrics.cpuUsageTrend", 1,
      csInstance.getUsageTrends(ClusterStatistics.TREND_CPU_USAGE).toList)
    updateHistogram("ClusterMetrics.jvmUsageTrend", 2,
      csInstance.getUsageTrends(ClusterStatistics.TREND_JVM_HEAP_USAGE).toList)
    updateHistogram("ClusterMetrics.heapUsageTrend", 3,
      csInstance.getUsageTrends(ClusterStatistics.TREND_HEAP_USAGE).toList)
    updateHistogram("ClusterMetrics.heapStorageUsageTrend", 4,
      csInstance.getUsageTrends(ClusterStatistics.TREND_HEAP_STORAGE_USAGE).toList)
    updateHistogram("ClusterMetrics.heapExecutionUsageTrend", 5,
      csInstance.getUsageTrends(ClusterStatistics.TREND_HEAP_EXECUTION_USAGE).toList)
    updateHistogram("ClusterMetrics.offHeapUsageTrend", 6,
      csInstance.getUsageTrends(ClusterStatistics.TREND_OFFHEAP_USAGE).toList)
    updateHistogram("ClusterMetrics.offHeapStorageUsageTrend", 7,
      csInstance.getUsageTrends(ClusterStatistics.TREND_OFFHEAP_STORAGE_USAGE).toList)
    updateHistogram("ClusterMetrics.offHeapExecutionUsageTrend", 8,
      csInstance.getUsageTrends(ClusterStatistics.TREND_OFFHEAP_EXECUTION_USAGE).toList)
    updateHistogram("ClusterMetrics.aggrMemoryUsageTrend", 9,
      csInstance.getUsageTrends(ClusterStatistics.TREND_AGGR_MEMORY_USAGE).toList)
    updateHistogram("ClusterMetrics.diskStoreDiskSpaceTrend", 10,
      csInstance.getUsageTrends(ClusterStatistics.TREND_DISKSTORE_DISKSPACE_USAGE).toList)
  }

  def updateHistogram(metricName: String, index: Int, newList: List[AnyRef]) {
    if (oldSizeArr(index) < newList.size) {
      for (i <- oldSizeArr(index) until newList.size) {
        createHistogram(metricName, newList(i).asInstanceOf[Number].longValue())
      }
    }
    oldSizeArr(index) = newList.size
  }

  def setMetricsForTableStatDetails(tableBuff: Map[String, SnappyRegionStats]): Unit = {
    createGauge(s"TableMetrics.embeddedTablesCount", tableBuff.size)

    var columnTablesCount, rowTablesCount = 0
    for (elem <- tableBuff.values) {
      if (elem.isColumnTable) {
        columnTablesCount = columnTablesCount + 1
      } else {
        rowTablesCount = rowTablesCount + 1
      }
    }

    createGauge(s"TableMetrics.columnTablesCount", columnTablesCount)
    createGauge(s"TableMetrics.rowTablesCount", rowTablesCount)

    for ((k, v) <- tableBuff) {
      SnappyTableMetrics.convertStatsToMetrics(k, v)
    }
  }

  def setMetricsForExternalTableStatDetails(externalTableBuff:
                                            Map[String, SnappyExternalTableStats]): Unit = {
    var externalTablesCount = 0
    externalTableBuff.values.foreach(v => {
      if (v.getTableType == "EXTERNAL") {
        externalTablesCount = externalTablesCount + 1
      }
    })
    createGauge(s"TableMetrics.externalTablesCount", externalTablesCount)

    for ((k, v) <- externalTableBuff) {
      SnappyTableMetrics.convertExternalTableStatstoMetrics(k, v)
    }
  }

  def setMetricsForMemberStatDetails(membersBuff: mutable.Map[String, MemberStatistics]): Unit = {

    var leadCount, locatorCount, dataServerCount, connectorCount, totalMembersCount = 0

    for (elem <- membersBuff.values) {
      if (elem.isLead) {
        leadCount += 1
      } else if (elem.isLocator) {
        locatorCount += 1
      } else if (elem.isDataServer) {
        dataServerCount += 1
      } else {
        connectorCount += 1
      }
    }

    totalMembersCount = leadCount + locatorCount + dataServerCount + connectorCount

    createGauge(s"MemberMetrics.totalMembersCount", totalMembersCount)
    createGauge(s"MemberMetrics.leadCount", leadCount)
    createGauge(s"MemberMetrics.locatorCount", locatorCount)
    createGauge(s"MemberMetrics.dataServerCount", dataServerCount)
    createGauge(s"MemberMetrics.connectorCount", connectorCount)

    for ((k, v) <- membersBuff) {
      SnappyMemberMetrics.convertStatsToMetrics(k, v)
    }
  }
}