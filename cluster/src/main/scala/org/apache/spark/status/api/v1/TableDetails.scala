/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import scala.collection.mutable.ListBuffer

import io.snappydata.SnappyTableStatsProviderService

object TableDetails {

  def getAllTablesInfo: Seq[TableSummary] = {
    val tablesBuff: ListBuffer[TableSummary] = ListBuffer.empty[TableSummary]
    val (tableBuff, indexBuff, externalTableBuff) =
      SnappyTableStatsProviderService.getService.getAggregatedStatsOnDemand

    tableBuff.foreach(tb => {
      val table = tb._2

      val storageModel = {
        if (table.isColumnTable) {
          "COLOUMN"
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

      tablesBuff += new TableSummary(table.getTableName, storageModel, distributionType,
        table.isColumnTable, table.isReplicatedTable, table.getRowCount, table.getSizeInMemory,
        table.getTotalSize, table.getBucketCount)
    })

    tablesBuff.toList
  }

  def getAllExternalTablesInfo: Seq[ExternalTableSummary] = {
    val extTables: ListBuffer[ExternalTableSummary] = ListBuffer.empty[ExternalTableSummary]
    val (tableBuff, indexBuff, externalTableBuff) =
      SnappyTableStatsProviderService.getService.getAggregatedStatsOnDemand

    externalTableBuff.foreach(tb => {
      val table = tb._2

      extTables += new ExternalTableSummary(table.getTableName, table.getProvider,
        table.getDataSourcePath)
    })

    extTables.toList
  }
}
