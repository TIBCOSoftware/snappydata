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

import scala.collection.mutable

import io.snappydata.SnappyTableStatsProviderService

import org.apache.spark.sql.types.StructType

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

  def getAllGlobalTempViewsInfo: Seq[GlobalTemporaryViewSummary] = {

    val gblTempViewBuff =
      SnappyTableStatsProviderService.getService.getAllGlobalTempViewStatsFromService

    gblTempViewBuff.mapValues(view => {
      val colCount = view.getSchema.asInstanceOf[StructType].size
      val schemaFields = view.getSchema.asInstanceOf[StructType].fields
      val schemaStringBuilder = new StringBuilder
      schemaFields.foreach(field => {
        schemaStringBuilder.append("(" + field.name + ":" + field.dataType + ", " +
            "nullable=" + { if (field.nullable) "Yes" else "No" } + ")\n")
      })

      val columnsInfo = mutable.HashMap.empty[String, Any]
      columnsInfo += ("numColumns" -> colCount);
      columnsInfo += ("fieldsString" -> schemaStringBuilder.toString());

      new GlobalTemporaryViewSummary(view.getFullyQualifiedName, view.getTableName,
        view.getTableType, columnsInfo)
    }).values.toList
  }
}
