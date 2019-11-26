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

import com.pivotal.gemfirexd.internal.engine.ui.{SnappyExternalTableStats, SnappyRegionStats}
import io.snappydata.metrics.SnappyMetricsSystem.createGauge

object SnappyTableMetrics {

  def convertStatsToMetrics(table: String, tableStats: SnappyRegionStats) {
    
    val namespace = s"TableMetrics.$table"
    createGauge(s"$namespace.tableName", tableStats.getTableName.asInstanceOf[AnyVal])
    createGauge(s"$namespace.isColumnTable", tableStats.isColumnTable)
    createGauge(s"$namespace.rowCount", tableStats.getRowCount)
    createGauge(s"$namespace.sizeInMemory", tableStats.getSizeInMemory)
    createGauge(s"$namespace.sizeSpillToDisk", tableStats.getSizeSpillToDisk)
    createGauge(s"$namespace.totalSize", tableStats.getTotalSize)
    createGauge(s"$namespace.isReplicatedTable", tableStats.isReplicatedTable)
    createGauge(s"$namespace.bucketCount", tableStats.getBucketCount)
    createGauge(s"$namespace.redundancy", tableStats.getRedundancy)
    createGauge(s"$namespace.isRedundancyImpaired", tableStats.isRedundancyImpaired)
    createGauge(s"$namespace.isAnyBucketLost", tableStats.isAnyBucketLost)

  }

  def convertExternalTableStatstoMetrics(table: String,
                                        externalTableStats: SnappyExternalTableStats): Unit = {

    val namespace = s"ExternalTableMetrics.$table"
    createGauge(s"$namespace.tableName",
      externalTableStats.getTableFullyQualifiedName.asInstanceOf[AnyVal])
    createGauge(s"$namespace.provider", externalTableStats.getProvider.asInstanceOf[AnyVal])
    createGauge(s"$namespace.dataSourcePath",
      externalTableStats.getDataSourcePath.asInstanceOf[AnyVal])
    createGauge(s"$namespace.tableType", externalTableStats.getTableType.asInstanceOf[AnyVal])
  }
}
