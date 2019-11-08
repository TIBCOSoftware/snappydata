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

import io.snappydata.metrics.SnappyMetricsClass.{createGauge, updateHistogram}
import com.pivotal.gemfirexd.internal.engine.ui.MemberStatistics
import java.util

object SnappyMemberMetrics {

  def convertStatsToMetrics(member: String, memberDetails: MemberStatistics,
                            memberEntries: util.Map[String, String]) {
    val shortDirName = memberDetails.getUserDir.substring(
      memberDetails.getUserDir.lastIndexOf(System.getProperty("file.separator")) + 1)

    val memberUuid = memberEntries.get("__" + shortDirName + member + "__").toString

    val pId = memberDetails.getProcessId

    val nameOrId = {
      if (memberDetails.getName.isEmpty
        || memberDetails.getName.equalsIgnoreCase("NA")) {
        memberDetails.getId
      } else {
        memberDetails.getName
      }
    }

    val memberType = {
      if (memberDetails.isLead || memberDetails.isLeadActive) {
        "LEAD"
      } else if (memberDetails.isLocator) {
        "LOCATOR"
      } else if (memberDetails.isDataServer) {
        "DATA SERVER"
      } else {
        "CONNECTOR"
      }
    }

    val namespace = s"MemberMetrics.$shortDirName-$memberUuid"

    createGauge(s"$namespace.memberId", memberDetails.getId.asInstanceOf[AnyVal])
    createGauge(s"$namespace.nameOrId", nameOrId.asInstanceOf[AnyVal])
    createGauge(s"$namespace.host", memberDetails.getHost.asInstanceOf[AnyVal])
    createGauge(s"$namespace.shortDirName", shortDirName.asInstanceOf[AnyVal])
    createGauge(s"$namespace.fullDirName", memberDetails.getUserDir.asInstanceOf[AnyVal])
    createGauge(s"$namespace.logFile", memberDetails.getLogFile.asInstanceOf[AnyVal])
    createGauge(s"$namespace.processId", memberDetails.getProcessId.asInstanceOf[AnyVal])
    createGauge(s"$namespace.diskStoreUUID", memberDetails.getDiskStoreUUID.asInstanceOf[AnyVal])
    createGauge(s"$namespace.diskStoreName", memberDetails.getDiskStoreName.asInstanceOf[AnyVal])
    createGauge(s"$namespace.status", memberDetails.getStatus.asInstanceOf[AnyVal])
    createGauge(s"$namespace.memberType", memberType.asInstanceOf[AnyVal])
    createGauge(s"$namespace.isLocator", memberDetails.isLocator)
    createGauge(s"$namespace.isDataServer", memberDetails.isDataServer)
    createGauge(s"$namespace.isLead", memberDetails.isLead)
    createGauge(s"$namespace.isActiveLead", memberDetails.isLeadActive)
    createGauge(s"$namespace.cores", memberDetails.getCores)
    createGauge(s"$namespace.cpuActive", memberDetails.getCpuActive)
    createGauge(s"$namespace.clients", memberDetails.getClientsCount)
    createGauge(s"$namespace.jvmHeapMax", memberDetails.getJvmMaxMemory)
    createGauge(s"$namespace.jvmHeapUsed", memberDetails.getJvmUsedMemory)
    createGauge(s"$namespace.jvmHeapTotal", memberDetails.getJvmTotalMemory)
    createGauge(s"$namespace.jvmHeapFree", memberDetails.getJvmFreeMemory)
    createGauge(s"$namespace.heapStoragePoolUsed", memberDetails.getHeapStoragePoolUsed)
    createGauge(s"$namespace.heapStoragePoolSize", memberDetails.getHeapStoragePoolSize)
    createGauge(s"$namespace.heapExecutionPoolUsed", memberDetails.getHeapExecutionPoolUsed)
    createGauge(s"$namespace.heapExecutionPoolSize", memberDetails.getHeapExecutionPoolSize)
    createGauge(s"$namespace.heapMemorySize", memberDetails.getHeapMemorySize)
    createGauge(s"$namespace.heapMemoryUsed", memberDetails.getHeapMemoryUsed)
    createGauge(s"$namespace.offHeapStoragePoolUsed", memberDetails.getOffHeapStoragePoolUsed)
    createGauge(s"$namespace.offHeapStoragePoolSize", memberDetails.getOffHeapStoragePoolSize)
    createGauge(s"$namespace.offHeapExecutionPoolUsed", memberDetails.getOffHeapExecutionPoolUsed)
    createGauge(s"$namespace.offHeapExecutionPoolSize", memberDetails.getOffHeapExecutionPoolSize)
    createGauge(s"$namespace.offHeapMemorySize", memberDetails.getOffHeapMemorySize)
    createGauge(s"$namespace.offHeapMemoryUsed", memberDetails.getOffHeapMemoryUsed)
    createGauge(s"$namespace.diskStoreDiskSpace", memberDetails.getDiskStoreDiskSpace)
    updateHistogram(s"$namespace.timeLineTrend",
      memberDetails.getUsageTrends(MemberStatistics.TREND_TIMELINE).toList)
    updateHistogram(s"$namespace.cpuUsageTrend",
      memberDetails.getUsageTrends(MemberStatistics.TREND_CPU_USAGE).toList)
    updateHistogram(s"$namespace.jvmUsageTrend",
      memberDetails.getUsageTrends(MemberStatistics.TREND_JVM_HEAP_USAGE).toList)
    updateHistogram(s"$namespace.heapUsageTrend",
      memberDetails.getUsageTrends(MemberStatistics.TREND_HEAP_USAGE).toList)
    updateHistogram(s"$namespace.heapStorageUsageTrend",
      memberDetails.getUsageTrends(MemberStatistics.TREND_HEAP_STORAGE_USAGE).toList)
    updateHistogram(s"$namespace.heapExecutionUsageTrend",
      memberDetails.getUsageTrends(MemberStatistics.TREND_HEAP_EXECUTION_USAGE).toList)
    updateHistogram(s"$namespace.offHeapUsageTrend",
      memberDetails.getUsageTrends(MemberStatistics.TREND_OFFHEAP_USAGE).toList)
    updateHistogram(s"$namespace.offHeapStorageUsageTrend",
      memberDetails.getUsageTrends(MemberStatistics.TREND_OFFHEAP_STORAGE_USAGE).toList)
    updateHistogram(s"$namespace.offHeapExecutionUsageTrend",
      memberDetails.getUsageTrends(MemberStatistics.TREND_OFFHEAP_EXECUTION_USAGE).toList)
    updateHistogram(s"$namespace.aggrMemoryUsageTrend",
      memberDetails.getUsageTrends(MemberStatistics.TREND_AGGR_MEMORY_USAGE).toList)
    updateHistogram(s"$namespace.diskStoreDiskSpaceTrend",
      memberDetails.getUsageTrends(MemberStatistics.TREND_DISKSTORE_DISKSPACE_USAGE).toList)
  }
}