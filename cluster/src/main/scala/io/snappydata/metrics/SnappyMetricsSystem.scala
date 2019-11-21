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
import com.pivotal.gemfirexd.internal.engine.ui._
import io.snappydata.{Constant, SnappyTableStatsProviderService}

import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import org.apache.spark.SparkContext
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.groupon.metrics._

import scala.collection.mutable

object SnappyMetricsSystem {

  val oldSizeMap = collection.mutable.Map.empty[String, Int]
  
  val MAX_SIZE = 180

  def init(sc: SparkContext): Unit = {

    GemFireXDUtils.waitForNodeInitialization()
    // initialize metric system with cluster id as metrics namespace
    val clusterUuid = Misc.getMemStore.getMetadataCmdRgn.get(Constant.CLUSTER_ID)
    UserMetricsSystem.initialize(sc, clusterUuid)

    val timeInterval = 5000

    // concurrently executing threads to get stats from StatsProviderServices
    val runnable = new Runnable {
      override def run(): Unit = {
        while (true) {
          try {
            Thread.sleep(timeInterval)

            // store every member diskStore ID to metadataCmdRgn
            putMembersDiskStoreIdInRegion()

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

  def putMembersDiskStoreIdInRegion(): Unit = {
    val membersBuff = SnappyTableStatsProviderService.getService.getMembersStatsFromService
    val sep = System.getProperty("file.separator")
    val region = Misc.getMemStore.getMetadataCmdRgn
    for ((k, v) <- membersBuff) {
      val shortDirName = v.getUserDir.substring(
        v.getUserDir.lastIndexOf(sep) + 1)
      region.put(Constant.MEMBER_ID_PREFIX + shortDirName + k + "__", v.getDiskStoreUUID.toString)
    }
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
    createGauge("ClusterMetrics.totalCores", csInstance.getTotalCPUCores)
    updateHistogram("ClusterMetrics.cpuUsageTrend",
      csInstance.getUsageTrends(ClusterStatistics.TREND_CPU_USAGE).toList)
    updateHistogram("ClusterMetrics.jvmUsageTrend",
      csInstance.getUsageTrends(ClusterStatistics.TREND_JVM_HEAP_USAGE).toList)
    updateHistogram("ClusterMetrics.heapUsageTrend",
      csInstance.getUsageTrends(ClusterStatistics.TREND_HEAP_USAGE).toList)
    updateHistogram("ClusterMetrics.heapStorageUsageTrend",
      csInstance.getUsageTrends(ClusterStatistics.TREND_HEAP_STORAGE_USAGE).toList)
    updateHistogram("ClusterMetrics.heapExecutionUsageTrend",
      csInstance.getUsageTrends(ClusterStatistics.TREND_HEAP_EXECUTION_USAGE).toList)
    updateHistogram("ClusterMetrics.offHeapUsageTrend",
      csInstance.getUsageTrends(ClusterStatistics.TREND_OFFHEAP_USAGE).toList)
    updateHistogram("ClusterMetrics.offHeapStorageUsageTrend",
      csInstance.getUsageTrends(ClusterStatistics.TREND_OFFHEAP_STORAGE_USAGE).toList)
    updateHistogram("ClusterMetrics.offHeapExecutionUsageTrend",
      csInstance.getUsageTrends(ClusterStatistics.TREND_OFFHEAP_EXECUTION_USAGE).toList)
    updateHistogram("ClusterMetrics.aggrMemoryUsageTrend",
      csInstance.getUsageTrends(ClusterStatistics.TREND_AGGR_MEMORY_USAGE).toList)
    updateHistogram("ClusterMetrics.diskStoreDiskSpaceTrend",
      csInstance.getUsageTrends(ClusterStatistics.TREND_DISKSTORE_DISKSPACE_USAGE).toList)
  }

  def updateHistogram(metricName: String, newList: List[AnyRef]) {
    if (!oldSizeMap.contains(metricName)) {
      oldSizeMap.put(metricName, 0)
    }
    if ((oldSizeMap(metricName) < newList.size) && (newList.size != MAX_SIZE)) {
      for (i <- oldSizeMap(metricName) until newList.size) {
        if (!metricName.contains("cpuUsage")) {
          val convertedMBValue = (newList(i).asInstanceOf[Double] * 1024).asInstanceOf[Long]
          createHistogram(metricName, convertedMBValue)
          createGauge(metricName.replace("Trend", ""), newList(i).asInstanceOf[AnyVal])
        } else {
          createHistogram(metricName, newList(i).asInstanceOf[Number].longValue())
          createGauge(metricName.replace("Trend", ""), newList(i).asInstanceOf[AnyVal])
        }
      }
    } else {
      if (oldSizeMap(metricName) == MAX_SIZE - 1) {
        if (!metricName.contains("cpuUsage")) {
          val convertedMBVal = (newList(MAX_SIZE - 1).asInstanceOf[Double] * 1024).asInstanceOf[Long]
          createHistogram(metricName, convertedMBVal)
          createGauge(metricName.replace("Trend", ""), newList(MAX_SIZE - 1).asInstanceOf[AnyVal])
        } else {
          createHistogram(metricName, newList(MAX_SIZE - 1).asInstanceOf[Number].longValue())
          createGauge(metricName.replace("Trend", ""), newList(MAX_SIZE - 1).asInstanceOf[AnyVal])
        }
      }
    }
    oldSizeMap.update(metricName, newList.size)
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

  def setMetricsForMemberStatDetails(membersBuff: mutable.Map[String, MemberStatistics]) {

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
