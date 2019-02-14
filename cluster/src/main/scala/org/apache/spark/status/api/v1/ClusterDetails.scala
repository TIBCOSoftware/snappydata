/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.status.api.v1


import java.text.SimpleDateFormat
import java.util.Locale

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import com.pivotal.gemfirexd.internal.engine.ui.ClusterStatistics

import org.apache.spark.sql.SnappyContext

object ClusterDetails {
  def getClusterDetailsInfo: Seq[ClusterSummary] = {
    val clusterBuff: ListBuffer[ClusterSummary] = ListBuffer.empty[ClusterSummary]

    val csInstance = ClusterStatistics.getInstance()

    val coresInfo = mutable.HashMap.empty[String, Int]
    coresInfo += ("totalCores" -> csInstance.getTotalCPUCores)

    val sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS", Locale.US)
    val sparkContextStartTime = SnappyContext.globalSparkContext.startTime
    val clusterStartDateTime = sdf.format(sparkContextStartTime)
    val clusterUpTime = System.currentTimeMillis() - sparkContextStartTime

    val clusterInfo = mutable.HashMap.empty[String, Any]
    clusterInfo += ("coresInfo" -> coresInfo);
    clusterInfo += ("clusterStartDateTime" -> clusterStartDateTime);
    clusterInfo += ("clusterUpTime" -> clusterUpTime);
    clusterInfo += ("timeLine" ->
        csInstance.getUsageTrends(ClusterStatistics.TREND_TIMELINE));
    clusterInfo += ("cpuUsageTrend" ->
        csInstance.getUsageTrends(ClusterStatistics.TREND_CPU_USAGE));
    clusterInfo += ("jvmUsageTrend" ->
        csInstance.getUsageTrends(ClusterStatistics.TREND_JVM_HEAP_USAGE));
    clusterInfo += ("heapUsageTrend" ->
        csInstance.getUsageTrends(ClusterStatistics.TREND_HEAP_USAGE));
    clusterInfo += ("heapStorageUsageTrend" ->
        csInstance.getUsageTrends(ClusterStatistics.TREND_HEAP_STORAGE_USAGE));
    clusterInfo += ("heapExecutionUsageTrend" ->
        csInstance.getUsageTrends(ClusterStatistics.TREND_HEAP_EXECUTION_USAGE));
    clusterInfo += ("offHeapUsageTrend" ->
        csInstance.getUsageTrends(ClusterStatistics.TREND_OFFHEAP_USAGE));
    clusterInfo += ("offHeapStorageUsageTrend" ->
        csInstance.getUsageTrends(ClusterStatistics.TREND_OFFHEAP_STORAGE_USAGE));
    clusterInfo += ("offHeapExecutionUsageTrend" ->
        csInstance.getUsageTrends(ClusterStatistics.TREND_OFFHEAP_EXECUTION_USAGE));
    clusterInfo += ("aggrMemoryUsageTrend" ->
        csInstance.getUsageTrends(ClusterStatistics.TREND_AGGR_MEMORY_USAGE));
    clusterInfo += ("diskStoreDiskSpaceTrend" ->
        csInstance.getUsageTrends(ClusterStatistics.TREND_DISKSTORE_DISKSPACE_USAGE));

    val membersInfo = MemberDetails.getAllMembersInfo
    val tablesInfo = TableDetails.getAllTablesInfo
    val extTablesInfo = TableDetails.getAllExternalTablesInfo

    clusterBuff += new ClusterSummary(clusterInfo, membersInfo, tablesInfo, extTablesInfo)
    clusterBuff.toList
  }
}
