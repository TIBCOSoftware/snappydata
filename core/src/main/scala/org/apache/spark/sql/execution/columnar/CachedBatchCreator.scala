/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql.execution.columnar

import java.util.UUID

import com.gemstone.gemfire.internal.cache.ExternalTableMetaData
import com.pivotal.gemfirexd.internal.engine.access.heap.MemHeapScanController
import com.pivotal.gemfirexd.internal.engine.store.AbstractCompactExecRow
import com.pivotal.gemfirexd.internal.iapi.store.access.ScanController

import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow
import org.apache.spark.sql.execution.CompactExecRowToMutableRow
import org.apache.spark.sql.execution.columnar.impl.{ColumnFormatRelation, IndexColumnFormatRelation}
import org.apache.spark.sql.types.StructType

final class CachedBatchCreator(
    val tableName: String, // internal column table name
    val userTableName: String, // user given table name (row buffer)
    override val schema: StructType,
    val externalStore: ExternalStore,
    val dependents: Seq[ExternalTableMetaData],
    val columnBatchSize: Int,
    val useCompression: Boolean) extends CompactExecRowToMutableRow {

  def createAndStoreBatch(sc: ScanController, row: AbstractCompactExecRow,
      batchID: UUID, bucketID: Int): java.util.HashSet[AnyRef] = {

    def columnBuilders = schema.map {
      attribute =>
        val columnType = ColumnType(attribute.dataType)
        val initialBufferSize = columnType.defaultSize * columnBatchSize
        ColumnBuilder(attribute.dataType, initialBufferSize,
          attribute.name, useCompression)
    }.toArray

    val connectedExternalStore = externalStore.getConnectedExternalStore(tableName,
      onExecutor = true)

    var success: Boolean = false
    try {

      val indexStatements = dependents.map(ColumnFormatRelation.getIndexUpdateStruct(_,
        connectedExternalStore))

      def cachedBatchAggregate(batch: CachedBatch): Unit = {
        connectedExternalStore.withDependentAction { conn =>
          indexStatements.foreach { case (_, ps) => ps.executeBatch() }
        }.storeCachedBatch(tableName, batch,
          bucketID, Option(batchID))
      }

      // adding one variable so that only one cached batch is created
      val holder = new CachedBatchHolder(columnBuilders, 0, Integer.MAX_VALUE, schema,
        cachedBatchAggregate,
        indexStatements)

      val memHeapScanController = sc.asInstanceOf[MemHeapScanController]
      memHeapScanController.setAddRegionAndKey()
      val keySet = new java.util.HashSet[AnyRef]
      val mutableRow = new SpecificMutableRow(dataTypes)
      try {
        while (memHeapScanController.fetchNext(row)) {
          holder.appendRow(createInternalRow(row, mutableRow))
          keySet.add(row.getAllRegionAndKeyInfo.first().getKey)
        }
        holder.forceEndOfBatch()
        keySet
      } finally {
        sc.close()
        success = true
      }
    } finally {
      connectedExternalStore.commitAndClose(success)
    }
  }
}
