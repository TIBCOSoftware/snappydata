/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

import java.nio.{ByteBuffer, ByteOrder}
import java.sql.Blob
import java.util.function.Function

import com.gemstone.gemfire.SystemFailure
import com.gemstone.gemfire.internal.cache.{BucketRegion, EntryEventImpl, PartitionedRegion, TXStateProxy}
import com.gemstone.gemfire.internal.shared.FetchRequest
import com.pivotal.gemfirexd.internal.engine.store.RowEncoder.PreProcessRow
import com.pivotal.gemfirexd.internal.engine.store.{GemFireContainer, RegionKey, RowEncoder}
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow
import com.pivotal.gemfirexd.internal.iapi.types.{DataValueDescriptor, SQLBlob, SQLInteger, SQLLongint}
import com.pivotal.gemfirexd.internal.impl.sql.execute.ValueRow
import io.snappydata.thrift.common.BufferedBlob
import io.snappydata.thrift.internal.ClientBlob
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet

import org.apache.spark.Logging
import org.apache.spark.sql.execution.columnar.encoding.ColumnDeleteDelta
import org.apache.spark.sql.store.CompressionCodecId
import org.apache.spark.sql.types.StructType

/**
 * A [[RowEncoder]] implementation for [[ColumnFormatValue]] and child classes.
 */
final class ColumnFormatEncoder extends RowEncoder with Logging {

  override def toRow(rawKey: Object, value: AnyRef, container: GemFireContainer): ExecRow = {
    val batchKey = rawKey.asInstanceOf[ColumnFormatKey]
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
    if (!BucketRegion.isValidUUID(uuid)) {
      throw new IllegalStateException(s"Invalid batch UUID in ${row.mkString(" ; ")}")
    }
    uuid
  }

  override def fromRow(row: Array[DataValueDescriptor],
      container: GemFireContainer): java.util.Map.Entry[RegionKey, AnyRef] = {
    val batchKey = new ColumnFormatKey(uuid = getUUID(row),
      partitionId = row(1).getInt, columnIndex = row(2).getInt)
    // transfer buffer from BufferedBlob as is, or copy for others
    var columnBuffer = row(3).getObject match {
      case blob: BufferedBlob =>
        // the chunk can never be a ByteBufferReference in this case and
        // the internal buffer will now be owned by ColumnFormatValue
        val chunk = blob.getAsLastChunk
        assert(!chunk.isSetChunkReference)
        chunk.chunk
      case blob: Blob => ByteBuffer.wrap(blob.getBytes(1, blob.length().toInt))
    }
    columnBuffer.rewind()
    columnBuffer = columnBuffer.order(ByteOrder.LITTLE_ENDIAN)
    val codec = -columnBuffer.getInt(0)
    val isCompressed = CompressionCodecId.isCompressed(codec)
    val codecId = if (isCompressed) codec
    else CompressionCodecId.fromName(container.getRegion.getColumnCompressionCodec).id
    // set the buffer into ColumnFormatValue, ColumnDelta or ColumnDeleteDelta
    val batchValue = batchKey.columnIndex match {
      case index if index > ColumnFormatEntry.STATROW_COL_INDEX =>
        new ColumnFormatValue(columnBuffer, codecId, isCompressed)
      case ColumnFormatEntry.DELETE_MASK_COL_INDEX =>
        new ColumnDeleteDelta(columnBuffer, codecId, isCompressed)
      case _ => new ColumnDelta(columnBuffer, codecId, isCompressed)
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
          // generate the key using the row buffer table else it will be inconsistent
          // with those generated by rollover and by direct embedded puts
          regionUUID = container.getRegion.asInstanceOf[PartitionedRegion].getColocatedWithRegion
              .newUUID(!BucketRegion.ALLOW_COLUMN_STORE_UUID_OVERWRITE_ON_OVERFLOW)
        }
        row(0).setValue(regionUUID)
        row
      }
    }
  }

  override def afterColumnStorePuts(bucket: BucketRegion,
      events: Array[EntryEventImpl]): Unit = {
    if (!bucket.getBucketAdvisor.isPrimary) return

    val keysToCompact = new ObjectOpenHashSet[ColumnFormatKey]()
    val keysToDelete = new ObjectOpenHashSet[ColumnFormatKey]()
    val pr = bucket.getPartitionedRegion
    val schema = pr.getUserAttribute.asInstanceOf[GemFireContainer]
        .fetchHiveMetaData(false).schema.asInstanceOf[StructType]
    // 1) Delete entire batch if all rows are marked deleted
    // 2) Compact the batch if either the deletes or updates exceed the limit
    //    (check the condition in ColumnCompactor.isCompactionRequired)
    events.foreach(event => event.getKey match {
      case key: ColumnFormatKey
        if key.columnIndex <= ColumnFormatEntry.DELETE_MASK_COL_INDEX =>

        var delta = event.getNewValue.asInstanceOf[ColumnFormatValue]
        if (delta ne null) {
          delta = delta.getValueRetain(FetchRequest.DECOMPRESS)
          if (delta ne null) {
            try {
              val deltaBuffer = delta.getBuffer
              if (deltaBuffer.hasRemaining) {
                val statsKey = key.toStatsRowKey
                if (key.columnIndex == ColumnFormatEntry.DELETE_MASK_COL_INDEX) {
                  ColumnCompactor.batchDeleteOrCompact(deltaBuffer) match {
                    case Some(true) => keysToDelete.add(statsKey)
                    case Some(false) => keysToCompact.add(statsKey)
                    case _ =>
                  }
                } else if (!keysToCompact.contains(statsKey)) {
                  // check for compaction; read number of base rows and delta values
                  val nullBitmaskBytes = deltaBuffer.getInt(4 /* skip typeId */)
                  val index = 8 + nullBitmaskBytes
                  val numBaseRows = deltaBuffer.getInt(index)
                  val numDeltas = deltaBuffer.getInt(index + 4)
                  if (ColumnCompactor.isCompactionRequired(numDeltas, numBaseRows)) {
                    keysToCompact.add(statsKey)
                  }
                }
              }
            } finally {
              delta.release()
            }
          }
        }
      case _ =>
    })
    // perform the required actions
    if (!keysToDelete.isEmpty) {
      if (!keysToCompact.isEmpty) keysToCompact.removeAll(keysToDelete)
      val numColumns = schema.length
      val iter = keysToDelete.iterator()
      while (iter.hasNext) {
        ColumnDelta.deleteBatch(iter.next(), pr, numColumns)
      }
    }
    if (!keysToCompact.isEmpty) {
      // register to compact with the transaction at the end because the table scan
      // might pick up the newly compacted batch too in addition to the previous one;
      // the pre-commit actions with results in the transactions works both for local
      // node as well as remote node compactions
      ColumnCompactor.getValidTransaction(expectedRolloverDisabled = true) match {
        case Some(tx) =>
          val iter = keysToCompact.iterator()
          while (iter.hasNext) {
            val key = iter.next()
            tx.getProxy.addBeforeCommitAction(keysToCompact, new Function[TXStateProxy, AnyRef] {
              override def apply(proxy: TXStateProxy): AnyRef = {
                try {
                  val success = ColumnCompactor.compact(key, bucket)
                  CompactionResult(key, bucket.getId, success)
                } catch {
                  case t: Throwable if !SystemFailure.isJVMFailureError(t) =>
                    logError("Unexpected failure in ColumnCompactor", t)
                    throw t
                }
              }
            })
          }
        case _ =>
      }
    }
  }
}
