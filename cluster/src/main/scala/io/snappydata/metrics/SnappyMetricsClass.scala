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

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.ui.{MemberStatistics, SnappyExternalTableStats, SnappyRegionStats}
import io.snappydata.SnappyTableStatsProviderService
import org.apache.spark.SparkContext
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.groupon.metrics._
import org.apache.spark.status.api.v1.MemberDetails.getMemberSummary
import org.apache.spark.status.api.v1.{ClusterDetails, ClusterSummary, MemberDetails, MemberSummary}

import scala.collection.mutable

object SnappyMetricsClass {
  def init(sc: SparkContext): Unit = {
    val startTime = Calendar.getInstance().getTime

    UserMetricsSystem.initialize(sc, "")
    val timeInterval = 3000
    val runnable = new Runnable {
      override def run(): Unit = {
        while (true) {
          try {
            Thread.sleep(timeInterval)
            val clusterBuff = ClusterDetails.getClusterDetailsInfo
            setMetricsForClusterStatDetails(clusterBuff)
            val tableBuff = SnappyTableStatsProviderService.getService.getAllTableStatsFromService
            setMetricsForTableStatDetails(tableBuff)
            val externalTableBuff =
              SnappyTableStatsProviderService.getService.getAllExternalTableStatsFromService
            setMetricsForExternalTableStatDetails(externalTableBuff)
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

  def createHistogram(metricName: String, metricValue: Int): Unit = {
    lazy val tempHistogram: SparkHistogram = UserMetricsSystem.histogram(metricName)
    tempHistogram.update(metricValue)
  }

  def setMetricsForClusterStatDetails(clusterBuff: Seq[ClusterSummary]): Unit = {
      for (c <- clusterBuff) {
        for ((k, v) <- c.clusterInfo) {
          SnappyClusterMetrics.convertStatsToMetrics(k, v)
        }
      }
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

    var startTime: Long = 0
    for ((k, v) <- membersBuff) {
      startTime = SnappyMemberMetrics.convertStatsToMetrics(k, v)
    }
    // var totalTime = (System.currentTimeMillis() - startTime)
    // Misc.getCacheLogWriter.info("Total time " + totalTime)
    // val DATE_FORMAT = "dd/MM/yyyy HH:mm:ss.SSS z"
    // val sdf = new SimpleDateFormat(DATE_FORMAT, Locale.US)
    // Misc.getCacheLogWriter.info("Date=== " + sdf.format(totalTime))
  }
}