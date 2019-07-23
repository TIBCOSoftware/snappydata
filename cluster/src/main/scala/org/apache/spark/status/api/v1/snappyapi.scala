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

import java.util.UUID

import scala.collection.mutable



class ClusterSummary private[spark](
    val clusterInfo: mutable.HashMap[String, Any],
    val membersInfo: Seq[MemberSummary],
    val tablesInfo: Seq[TableSummary],
    val externalTablesInfo: Seq[ExternalTableSummary]
)

class MemberSummary private[spark](
    val id: String,
    val name: String,
    val host: String,
    val userDir: String,
    val userDirFullPath: String,
    val logFile: String,
    val processId: String,
    val diskStoreUUID: UUID,
    val diskStoreName: String,
    val status: String,
    val memberType: String,
    val isLocator: Boolean,
    val isDataServer: Boolean,
    val isLead: Boolean,
    val isActiveLead: Boolean,
    val cores: Int,
    val cpuActive: Int,
    val clients: Long,
    val maxMemory: Long,
    val usedMemory: Long,
    val totalMemory: Long,
    val freeMemory: Long,
    val heapStoragePoolUsed: Long,
    val heapStoragePoolSize: Long,
    val heapExecutionPoolUsed: Long,
    val heapExecutionPoolSize: Long,
    val heapMemorySize: Long,
    val heapMemoryUsed: Long,
    val offHeapStoragePoolUsed: Long,
    val offHeapStoragePoolSize: Long,
    val offHeapExecutionPoolUsed: Long,
    val offHeapExecutionPoolSize: Long,
    val offHeapMemorySize: Long,
    val offHeapMemoryUsed: Long,
    val diskStoreDiskSpace: Long,
    val timeLine: Array[Object],
    val cpuUsageTrend: Array[Object],
    val jvmUsageTrend: Array[Object],
    val heapUsageTrend: Array[Object],
    val heapStorageUsageTrend: Array[Object],
    val heapExecutionUsageTrend: Array[Object],
    val offHeapUsageTrend: Array[Object],
    val offHeapStorageUsageTrend: Array[Object],
    val offHeapExecutionUsageTrend: Array[Object],
    val aggrMemoryUsageTrend: Array[Object],
    val diskStoreDiskSpaceTrend: Array[Object]
)

class TableSummary private[spark](
    val tableName: String,
    val storageModel: String,
    val distributionType: String,
    val isColumnTable: Boolean,
    val isReplicatedTable: Boolean,
    val rowCount: Long,
    val sizeInMemory: Long,
    val sizeSpillToDisk: Long,
    val totalSize: Long,
    val bucketCount: Int,
    val redundancy: Int,
    val redundancyImpaired: Boolean
)

class ExternalTableSummary private[spark](
    val tableFQName: String,
    val provider: String,
    val source: String
)
