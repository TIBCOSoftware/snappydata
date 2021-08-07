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
package org.apache.spark.sql.execution.columnar

import java.nio.ByteBuffer
import java.util.function.BiFunction

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

import com.gemstone.gemfire.cache.EntryDestroyedException
import com.gemstone.gemfire.internal.cache.{BucketRegion, LocalRegion, NonLocalRegionEntry, PartitionedRegion, RegionEntry, TXManagerImpl, TXStateInterface, Token}
import com.gemstone.gemfire.internal.shared.FetchRequest
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer

import org.apache.spark.TaskContext
import org.apache.spark.sql.execution.columnar.encoding.{ColumnDecoder, ColumnDeleteDecoder, ColumnEncoding, UpdatedColumnDecoder, UpdatedColumnDecoderBase}
import org.apache.spark.sql.execution.columnar.impl._
import org.apache.spark.sql.execution.row.PRValuesIterator
import org.apache.spark.sql.types.StructField

object ColumnBatchIterator {

  def apply(region: LocalRegion,
      bucketIds: java.util.Set[Integer], projection: Array[Int],
      fullScan: Boolean, context: TaskContext): ColumnBatchIterator = {
    new ColumnBatchIterator(region, batch = null, bucketIds, projection, fullScan, context)
  }

  def apply(batch: ColumnBatch): ColumnBatchIterator = {
    new ColumnBatchIterator(region = null, batch, bucketIds = null,
      projection = null, fullScan = false, context = null)
  }
}

class ColumnBatchIterator(region: LocalRegion, val batch: ColumnBatch,
    bucketIds: java.util.Set[Integer], projection: Array[Int],
    fullScan: Boolean, context: TaskContext)
    extends PRValuesIterator[ByteBuffer](container = null, region, bucketIds, context,
      TXManagerImpl.getCurrentTXState) {

  if (region ne null) {
    assert(!region.getEnableOffHeapMemory,
      s"Unexpected buffer iterator call for off-heap $region")
  }

  if (taskContext ne null) {
    taskContext.addTaskCompletionListener(_ => close())
  }

  protected[sql] final var currentVal: ByteBuffer = _
  protected[this] final var currentDeltaStats: ByteBuffer = _
  protected[this] final var currentKeyPartitionId: Int = _
  protected[this] final var currentKeyUUID: Long = _
  protected[this] final var batchProcessed = false
  protected[this] final var currentColumns: ArrayBuffer[ColumnFormatValue] = _

  override protected def createIterator(container: GemFireContainer, region: LocalRegion,
      tx: TXStateInterface): PRIterator = if (region ne null) {
    val txState = if (tx ne null) tx.getLocalTXState else null
    val createIterator = new BiFunction[BucketRegion, java.lang.Long,
        java.util.Iterator[RegionEntry]] {
      override def apply(br: BucketRegion,
          numEntries: java.lang.Long): java.util.Iterator[RegionEntry] = {
        new ColumnFormatIterator(br, projection, fullScan, txState)
      }
    }
    val createRemoteIterator = new BiFunction[java.lang.Integer, PRIterator,
        java.util.Iterator[RegionEntry]] {
      override def apply(bucketId: Integer,
          iter: PRIterator): java.util.Iterator[RegionEntry] = {
        new RemoteEntriesIterator(bucketId, projection, iter.getPartitionedRegion, tx)
      }
    }
    val pr = region.asInstanceOf[PartitionedRegion]
    new pr.PRLocalScanIterator(bucketIds, txState, createIterator, createRemoteIterator,
      false, true, true)
  } else null

  final def getCurrentBatchId: Long = currentKeyUUID

  final def getCurrentBucketId: Int = currentKeyPartitionId

  private[execution] final def getCurrentStatsColumn: ColumnFormatValue = currentColumns(0)

  private[sql] final def getColumnBuffer(columnPosition: Int,
      throwIfMissing: Boolean): ByteBuffer = {
    val value = itr.getBucketEntriesIterator.asInstanceOf[ClusteredColumnIterator]
        .getColumnValue(columnPosition)
    if (value ne null) {
      val columnValue = value.asInstanceOf[ColumnFormatValue].getValueRetain(
        FetchRequest.DECOMPRESS)
      val buffer = columnValue.getBuffer
      if (buffer.remaining() > 0) {
        currentColumns += columnValue
        return buffer
      } else columnValue.release()
    }
    if (throwIfMissing) {
      // empty buffer indicates value removed from region
      throw new EntryDestroyedException(s"Iteration on column=$columnPosition " +
          s"partition=$currentKeyPartitionId batchUUID=$currentKeyUUID " +
          "failed due to missing value")
    } else null
  }

  final def getColumnLob(columnIndex: Int): ByteBuffer = {
    if (region ne null) {
      getColumnBuffer(columnIndex + 1, throwIfMissing = true)
    } else {
      batch.buffers(columnIndex)
    }
  }

  final def getCurrentDeltaStats: ByteBuffer = currentDeltaStats

  final def getUpdatedColumnDecoder(decoder: ColumnDecoder, field: StructField,
      columnIndex: Int): UpdatedColumnDecoderBase = {
    val deltaPosition = ColumnDelta.deltaColumnIndex(columnIndex, 0)
    val delta1 = getColumnBuffer(deltaPosition, throwIfMissing = false)
    val delta2 = getColumnBuffer(deltaPosition - 1, throwIfMissing = false)
    if ((delta1 ne null) || (delta2 ne null)) {
      UpdatedColumnDecoder(decoder, field, delta1, delta2)
    } else null
  }

  final def getDeletedColumnDecoder: ColumnDeleteDecoder = {
    if (region eq null) null
    else getColumnBuffer(ColumnFormatEntry.DELETE_MASK_COL_INDEX,
      throwIfMissing = false) match {
      case null => null
      case deleteBuffer => new ColumnDeleteDecoder(deleteBuffer)
    }
  }

  final def getDeletedRowCount: Int = {
    if (region eq null) 0
    else {
      val delete = getColumnBuffer(ColumnFormatEntry.DELETE_MASK_COL_INDEX,
        throwIfMissing = false)
      if (delete eq null) 0
      else {
        val allocator = ColumnEncoding.getAllocator(delete)
        ColumnEncoding.readInt(allocator.baseObject(delete),
          allocator.baseOffset(delete) + delete.position() + 8)
      }
    }
  }

  private final def releaseColumns(): Int = {
    val previousColumns = currentColumns
    if ((previousColumns ne null) && previousColumns.nonEmpty) {
      currentColumns = null
      val len = previousColumns.length
      var i = 0
      while (i < len) {
        previousColumns(i).release()
        i += 1
      }
      len
    } else 0
  }

  override protected[sql] final def moveNext(): Unit = {
    if (region ne null) {
      // release previous set of values
      currentColumns = new ArrayBuffer[ColumnFormatValue](math.max(1, releaseColumns()))
      currentVal = null
      currentDeltaStats = null
      while (itr.hasNext) {
        val re = itr.next().asInstanceOf[RegionEntry]
        // the underlying ClusteredColumnIterator allows fetching entire projected
        // columns of a column batch as a single entity (SNAP-2102)
        val bucketRegion = itr.getHostedBucketRegion
        if ((bucketRegion ne null) || re.isInstanceOf[NonLocalRegionEntry]) {
          if ((re ne null) && !re.isDestroyedOrRemoved) {
            // re could be NonLocalRegionEntry in case of snapshot isolation
            // in some cases, old value could be TOMBSTONE and not a ColumnFormatValue
            val key = re.getRawKey.asInstanceOf[ColumnFormatKey]
            val v = re.getValue(bucketRegion)
            if ((v ne null) && !v.isInstanceOf[Token]) {
              val columnValue = v.asInstanceOf[ColumnFormatValue].getValueRetain(
                FetchRequest.DECOMPRESS)
              val buffer = columnValue.getBuffer
              // empty buffer indicates value removed from region
              if (buffer.remaining() > 0) {
                currentKeyPartitionId = key.partitionId
                currentKeyUUID = key.uuid
                currentVal = buffer
                currentColumns += columnValue
                // check for update/delete stats row
                currentDeltaStats = getColumnBuffer(ColumnFormatEntry.DELTA_STATROW_COL_INDEX,
                  throwIfMissing = false)
                return
              } else columnValue.release()
            }
          }
        }
      }
      itr.close()
      hasNextValue = false
    } else if (!batchProcessed) {
      currentVal = ByteBuffer.wrap(batch.statsData)
      batchProcessed = true
    } else {
      hasNextValue = false
    }
  }

  def close(): Unit = {
    if (itr ne null) {
      itr.close()
    }
    releaseColumns()
  }
}
