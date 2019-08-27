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
package org.apache.spark.status.api.v1

import io.snappydata.SnappyTableStatsProviderService

object TableDetails {

  def getAllTablesInfo: Seq[TableSummary] = {

    val tableBuff = SnappyTableStatsProviderService.getService.getAllTableStatsFromService

    tableBuff.mapValues(table => {
      val storageModel = {
        if (table.isColumnTable) {
          "COLUMN"
        } else {
          "ROW"
        }
      }

      val distributionType = {
        if (table.isReplicatedTable){
          "REPLICATE"
        } else {
          "PARTITION"
        }
      }

      new TableSummary(table.getTableName, storageModel, distributionType,
        table.isColumnTable, table.isReplicatedTable, table.getRowCount, table.getSizeInMemory,
        table.getSizeSpillToDisk, table.getTotalSize, table.getBucketCount,
        table.getRedundancy, table.isRedundancyImpaired, table.isAnyBucketLost)
    }).values.toList

  }

  def getAllExternalTablesInfo: Seq[ExternalTableSummary] = {

    val externalTableBuff =
      SnappyTableStatsProviderService.getService.getAllExternalTableStatsFromService

    externalTableBuff.mapValues(table => {
      new ExternalTableSummary(table.getTableFullyQualifiedName, table.getProvider,
        table.getDataSourcePath)
    }).values.toList
  }
}
