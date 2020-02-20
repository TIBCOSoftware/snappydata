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

import com.gemstone.gemfire.CancelException
import com.gemstone.gemfire.i18n.LogWriterI18n
import com.gemstone.gemfire.internal.SystemTimer
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.ui._
import io.snappydata.SnappyTableStatsProviderService
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import org.apache.spark.SparkContext
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.groupon.metrics._
import io.snappydata.Constant._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SnappyMetricsSystem {

  val oldSizeMap = collection.mutable.Map.empty[String, Int]
  val MAX_SIZE = 180

  var doRun: Boolean = false
  var running: Boolean = false

  var tables = ArrayBuffer.empty[String]
  var externaltables = ArrayBuffer.empty[String]

  private val logger: LogWriterI18n = Misc.getGemFireCache.getLoggerI18n

  def init(sc: SparkContext): Unit = {
    GemFireXDUtils.waitForNodeInitialization()
    // initialize metric system with cluster id as metrics namespace
    val clusterUuidObj = Misc.getMemStore.getMetadataCmdRgn.get(CLUSTER_ID)
    val clusterUuid: String = if (clusterUuidObj != null) {
      clusterUuidObj.asInstanceOf[String]
    } else null
    UserMetricsSystem.initialize(sc, clusterUuid)

    if (!doRun) {
      val delay = sc.getConf.getLong(PROPERTY_PREFIX +
          "calcTableSizeInterval", DEFAULT_CALC_TABLE_SIZE_SERVICE_INTERVAL)
      doRun = true
      Misc.getGemFireCache.getCCPTimer.schedule(
        new SystemTimer.SystemTimerTask {

          override def run2(): Unit = {
            try {
              if (doRun) {
                convertStatsToMetrics()
              }
            } catch {
              case _: CancelException => // ignore
              case e: Exception => if (e.getMessage != null && !e.getMessage.contains(
                "com.gemstone.gemfire.cache.CacheClosedException")) {
                logger.warning(e)
              } else {
                logger.error(e)
              }
            }
          }

          override def getLoggerI18n: LogWriterI18n = {
            logger
          }
        },
        delay, delay)
    }
  }

  def stop(): Unit = {
    doRun = false
    // wait for it to end for sometime
    if (running) wait(10000)
  }

  def convertStatsToMetrics(): Unit = {
    try {
      val cache = Misc.getGemFireCacheNoThrow
      if (doRun && cache != null) {
        running = true
        try {
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
        } finally {
          running = false
        }
      }
    } catch {
      case _: CancelException => // ignore
      case e: Exception =>
        val msg = if (e.getMessage ne null) e.getMessage else e.toString
        if (!msg.contains("com.gemstone.gemfire.cache.CacheClosedException")) {
          logger.warning(e)
        } else {
          logger.error(e)
        }
    }
  }

  def putMembersDiskStoreIdInRegion(): Unit = {
    try {
      val cache = Misc.getGemFireCacheNoThrow
      if (cache != null) {
        val membersBuff = SnappyTableStatsProviderService.getService.getMembersStatsFromService
        val sep = System.getProperty("file.separator")
        val region = Misc.getMemStore.getMetadataCmdRgn
        for ((k, v) <- membersBuff) {
          val shortDirName = v.getUserDir.substring(
            v.getUserDir.lastIndexOf(sep) + 1)
          val key = MEMBER_ID_PREFIX + shortDirName + k + "__"
          if (!region.containsKey(key)) {
            // in future, DiskStoreUUID may get replaced by user provided name.
            region.put(key, v.getDiskStoreUUID.toString)
          }
        }
      }
    } catch {
      case _: CancelException => // ignore
      case e: Exception =>
        val msg = if (e.getMessage ne null) e.getMessage else e.toString
        if (!msg.contains("com.gemstone.gemfire.cache.CacheClosedException")) {
          logger.warning(e)
        } else {
          logger.error(e)
        }
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

  def remove(metricName: String): Unit = {
    UserMetricsSystem.gauge(metricName).remove()
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
          val convertedMBVal = (newList(MAX_SIZE - 1).
              asInstanceOf[Double] * 1024).asInstanceOf[Long]
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

  def removeTableMetricsIfTableDeleted(stringToStats: Map[String, SnappyRegionStats]): Unit = {
    for (t <- tables) {
      if (!stringToStats.contains(t)) {
        SnappyTableMetrics.removeTableMetrics(t)
      }
    }
    tables = tables.filter(d => stringToStats.contains(d))
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
    if (tableBuff.nonEmpty) {
      for ((k, v) <- tableBuff) {
        SnappyTableMetrics.convertStatsToMetrics(k, v)
        if(!tables.contains(k)) {
          tables += k
        }
      }
    }
    removeTableMetricsIfTableDeleted(tableBuff)
  }

  def removeExternalTableMetricsIfTableDeleted(stringToStats:
    Map[String, SnappyExternalTableStats]): Unit = {
    for (t <- externaltables) {
      if (!stringToStats.exists(x => x._1 == t)) {
        SnappyTableMetrics.removeExternalTableMetrics(t)
      }
    }
    externaltables = externaltables.filter(d => stringToStats.contains(d))
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
      if(!externaltables.contains(k)) {
        externaltables += k
      }
    }
    removeExternalTableMetricsIfTableDeleted(externalTableBuff)
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
