/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

package org.apache.spark.sql.execution.columnar.impl

import java.nio.ByteBuffer
import java.sql.Blob

import com.gemstone.gemfire.internal.cache.{BucketRegion, EntryEventImpl, RegionEntry}
import com.pivotal.gemfirexd.internal.engine.store.RowEncoder.PreProcessRow
import com.pivotal.gemfirexd.internal.engine.store.{GemFireContainer, RegionKey, RowEncoder}
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow
import com.pivotal.gemfirexd.internal.iapi.types.{DataValueDescriptor, SQLBlob, SQLInteger, SQLLongint}
import com.pivotal.gemfirexd.internal.impl.sql.execute.ValueRow
import io.snappydata.thrift.common.BufferedBlob
import io.snappydata.thrift.internal.ClientBlob

import org.apache.spark.sql.execution.columnar.encoding.ColumnDeleteDelta

/**
 * A [[RowEncoder]] implementation for [[ColumnFormatValue]] and child classes.
 */
final class ColumnFormatEncoder extends RowEncoder {

  override def toRow(entry: RegionEntry, value: AnyRef,
      container: GemFireContainer): ExecRow = {
    val batchKey = entry.getRawKey.asInstanceOf[ColumnFormatKey]
    val batchValue = value.asInstanceOf[ColumnFormatValue]
    // layout the same way as declared in ColumnFormatRelation
    val row = new ValueRow(5)
    row.setColumn(1, new SQLLongint(batchKey.uuid))
    row.setColumn(2, new SQLInteger(batchKey.partitionId))
    row.setColumn(3, new SQLInteger(batchKey.columnIndex))
    // set value reference which will be released after thrift write
    row.setColumn(4, new SQLBlob(new ClientBlob(batchValue)))
    row
  }

  private def getUUID(row: Array[DataValueDescriptor]): Long = {
    val uuid = row(0).getLong
    assert(BucketRegion.isValidUUID(uuid), s"Invalid batch UUID in ${row.mkString(" ; ")}")
    uuid
  }

  override def fromRow(row: Array[DataValueDescriptor],
      container: GemFireContainer): java.util.Map.Entry[RegionKey, AnyRef] = {
    val batchKey = new ColumnFormatKey(uuid = getUUID(row),
      partitionId = row(1).getInt, columnIndex = row(2).getInt)
    // transfer buffer from BufferedBlob as is, or copy for others
    val columnBuffer = row(3).getObject match {
      case blob: BufferedBlob => blob.getAsLastChunk.chunk
      case blob: Blob => ByteBuffer.wrap(blob.getBytes(1, blob.length().toInt))
    }
    columnBuffer.rewind()
    // set the buffer into ColumnFormatValue, ColumnDelta or ColumnDeleteDelta
    val batchValue = batchKey.columnIndex match {
      case index if index >= ColumnFormatEntry.STATROW_COL_INDEX =>
        new ColumnFormatValue(columnBuffer)
      case ColumnFormatEntry.DELETE_MASK_COL_INDEX => new ColumnDeleteDelta(columnBuffer)
      case _ => new ColumnDelta(columnBuffer)
    }
    new java.util.AbstractMap.SimpleEntry[RegionKey, AnyRef](batchKey, batchValue)
  }

  override def fromRowToKey(key: Array[DataValueDescriptor],
      container: GemFireContainer): RegionKey =
    new ColumnFormatKey(uuid = getUUID(key),
      partitionId = key(1).getInt, columnIndex = key(2).getInt)

  override def getPreProcessorForRows(
      container: GemFireContainer): PreProcessRow = new PreProcessRow {

    private var regionUUID: Long = BucketRegion.INVALID_UUID

    override def preProcess(
        row: Array[DataValueDescriptor]): Array[DataValueDescriptor] = {
      // check invalid UUID (from smart connector)
      if (BucketRegion.isValidUUID(row(0).getLong)) row
      else {
        if (!BucketRegion.isValidUUID(regionUUID)) {
          regionUUID = container.newUUIDForRegionKey()
        }
        row(0).setValue(regionUUID)
        row
      }
    }
  }

  override def afterColumnStorePuts(bucket: BucketRegion,
      events: Array[EntryEventImpl]): Unit = {
    // delete entire batch if all rows are marked deleted
    events.foreach(event => event.getKey match {
      case deleteKey: ColumnFormatKey
        if deleteKey.columnIndex == ColumnFormatEntry.DELETE_MASK_COL_INDEX =>

        val deleteDelta = event.getNewValue.asInstanceOf[ColumnFormatValue]
        if (deleteDelta eq null) return

        val region = bucket.getPartitionedRegion
        val deleteBuffer = deleteDelta.getBufferRetain
        val deleteBatch = try {
          if (!deleteBuffer.hasRemaining) return
          ColumnDelta.checkBatchDeleted(deleteBuffer)
        } finally {
          deleteDelta.release()
        }
        if (deleteBatch) {
          ColumnDelta.deleteBatch(deleteKey, region,
            region.getUserAttribute.asInstanceOf[GemFireContainer].getQualifiedTableName,
            forUpdate = true)
        }
      case _ =>
    })
  }
}
