/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

import java.nio.{ByteBuffer, ByteOrder}
import java.sql.{Connection, ResultSet, Statement}
import java.util.function.BiFunction

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.util.control.NonFatal

import com.gemstone.gemfire.cache.EntryDestroyedException
import com.gemstone.gemfire.internal.cache.{BucketRegion, GemFireCacheImpl, LocalRegion, NonLocalRegionEntry, PartitionedRegion, RegionEntry, TXStateInterface}
import com.gemstone.gemfire.internal.shared.{BufferAllocator, FetchRequest}
import com.koloboke.function.IntObjPredicate
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection
import io.snappydata.collection.IntObjectHashMap
import io.snappydata.thrift.common.BufferedBlob

import org.apache.spark.memory.MemoryManagerCallback.releaseExecutionMemory
import org.apache.spark.sql.execution.columnar.encoding.{ColumnDecoder, ColumnDeleteDecoder, ColumnEncoding, UpdatedColumnDecoder, UpdatedColumnDecoderBase}
import org.apache.spark.sql.execution.columnar.impl._
import org.apache.spark.sql.execution.row.PRValuesIterator
import org.apache.spark.sql.store.CompressionUtils
import org.apache.spark.sql.types.StructField
import org.apache.spark.{Logging, TaskContext, TaskContextImpl, TaskKilledException}

case class ColumnBatch(numRows: Int, buffers: Array[ByteBuffer],
    statsData: Array[Byte], deltaIndexes: Array[Int])

abstract class ResultSetIterator[A](conn: Connection,
    stmt: Statement, rs: ResultSet, context: TaskContext,
    closeConnectionOnResultsClose: Boolean = true)
    extends Iterator[A] with Logging {

  protected[this] final var doMove = true

  protected[this] final var hasNextValue: Boolean = rs ne null

  protected[this] final val taskContext = context.asInstanceOf[TaskContextImpl]

  if (taskContext ne null) {
    val partitionId = taskContext.partitionId
    taskContext.addTaskCompletionListener { _ =>
      logDebug(s"closed connection for task from listener $partitionId")
      close()
    }
  }

  override final def hasNext: Boolean = {
    if (doMove && hasNextValue) {
      doMove = false
      hasNextValue = false
      // check for task killed before moving to next element
      if ((taskContext ne null) && taskContext.isInterrupted()) {
        throw new TaskKilledException
      }
      hasNextValue = moveNext()
      hasNextValue
    } else {
      hasNextValue
    }
  }

  protected def moveNext(): Boolean = rs.next()

  override final def next(): A = {
    if (!doMove || hasNext) {
      doMove = true
      getCurrentValue
    } else null.asInstanceOf[A]
  }

  protected def getCurrentValue: A

  def close() {
    // if (!hasNextValue) return
      try {
        if (rs ne null) {
          // GfxdConnectionWrapper.restoreContextStack(stmt, rs)
          // rs.lightWeightClose()
          rs.close()
        }
      } catch {
        case NonFatal(e) => logWarning("Exception closing resultSet", e)
      }
      try {
        if (stmt ne null) {
          stmt.getConnection match {
            case embedConn: EmbedConnection =>
              val lcc = embedConn.getLanguageConnection
              if (lcc ne null) {
                lcc.clearExecuteLocally()
              }
            case _ =>
          }
          stmt.close()
        }
      } catch {
        case NonFatal(e) => logWarning("Exception closing statement", e)
      }
      hasNextValue = false

  }
}

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

final class ColumnBatchIterator(region: LocalRegion, val batch: ColumnBatch,
    bucketIds: java.util.Set[Integer], projection: Array[Int],
    fullScan: Boolean, context: TaskContext)
    extends PRValuesIterator[ByteBuffer](container = null, region, bucketIds, context) {

  if (region ne null) {
    assert(!region.getEnableOffHeapMemory,
      s"Unexpected buffer iterator call for off-heap $region")
  }

  if (taskContext ne null) {
    taskContext.addTaskCompletionListener(_ => close())
  }

  protected[sql] var currentVal: ByteBuffer = _
  private var currentDeltaStats: ByteBuffer = _
  private var currentKeyPartitionId: Int = _
  private var currentKeyUUID: Long = _
  private var batchProcessed = false
  private var currentColumns = new ArrayBuffer[ColumnFormatValue]()

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

  def getCurrentBatchId: Long = currentKeyUUID

  def getCurrentBucketId: Int = currentKeyPartitionId

  private[execution] def getCurrentStatsColumn: ColumnFormatValue = currentColumns(0)

  private[sql] def getColumnBuffer(columnPosition: Int, throwIfMissing: Boolean): ByteBuffer = {
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

  def getColumnLob(columnIndex: Int): ByteBuffer = {
    if (region ne null) {
      getColumnBuffer(columnIndex + 1, throwIfMissing = true)
    } else {
      batch.buffers(columnIndex)
    }
  }

  def getCurrentDeltaStats: ByteBuffer = currentDeltaStats

  def getUpdatedColumnDecoder(decoder: ColumnDecoder, field: StructField,
      columnIndex: Int): UpdatedColumnDecoderBase = {
    if (currentDeltaStats eq null) null
    else {
      val deltaPosition = ColumnDelta.deltaColumnIndex(columnIndex, 0)
      val delta1 = getColumnBuffer(deltaPosition, throwIfMissing = false)
      val delta2 = getColumnBuffer(deltaPosition - 1, throwIfMissing = false)
      if ((delta1 ne null) || (delta2 ne null)) {
        UpdatedColumnDecoder(decoder, field, delta1, delta2)
      } else null
    }
  }

  def getDeletedColumnDecoder: ColumnDeleteDecoder = {
    if (region eq null) null
    else getColumnBuffer(ColumnFormatEntry.DELETE_MASK_COL_INDEX,
      throwIfMissing = false) match {
      case null => null
      case deleteBuffer => new ColumnDeleteDecoder(deleteBuffer)
    }
  }

  def getDeletedRowCount: Int = {
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

  private def releaseColumns(): Int = {
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

  override protected[sql] def moveNext(): Unit = {
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
          if (!re.isDestroyedOrRemoved) {
            // re could be NonLocalRegionEntry in case of snapshot isolation
            // in some cases, old value could be TOMBSTONE and not a ColumnFormatValue
            val key = re.getRawKey.asInstanceOf[ColumnFormatKey]
            val v = re.getValue(bucketRegion)
            if (v ne null) {
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

final class ColumnBatchIteratorOnRS(conn: Connection,
    projection: Array[Int], stmt: Statement, rs: ResultSet,
    context: TaskContext, partitionId: Int)
    extends ResultSetIterator[ByteBuffer](conn, stmt, rs, context) {
  private var currentUUID: Long = _
  // upto three deltas for each column and a deleted mask
  private val totalColumns = (projection.length * (ColumnDelta.MAX_DEPTH + 1)) + 1
  private val allocator = GemFireCacheImpl.getCurrentBufferAllocator
  private var colBuffers: IntObjectHashMap[ByteBuffer] = _
  private var currentStats: ByteBuffer = _
  private var currentDeltaStats: ByteBuffer = _
  private var rsHasNext: Boolean = rs.next()

  def getCurrentBatchId: Long = currentUUID

  def getCurrentBucketId: Int = partitionId

  private def decompress(buffer: ByteBuffer): ByteBuffer = {
    if ((buffer ne null) && buffer.remaining() > 0) {
      val result = CompressionUtils.codecDecompressIfRequired(
        buffer.order(ByteOrder.LITTLE_ENDIAN), allocator)
      if (result ne buffer) {
        BufferAllocator.releaseBuffer(buffer)
        // decompressed buffer will be ordered by LITTLE_ENDIAN while non-decompressed
        // is returned with BIG_ENDIAN in order to distinguish the two cases
        result
      } else result.order(ByteOrder.BIG_ENDIAN)
    } else null // indicates missing value
  }

  private def getBufferFromBlob(blob: java.sql.Blob): ByteBuffer = {
    val buffer = decompress(blob match {
      case blob: BufferedBlob =>
        // the chunk can never be a ByteBufferReference in this case and
        // the internal buffer will now be owned by ColumnFormatValue
        val chunk = blob.getAsLastChunk
        assert(!chunk.isSetChunkReference)
        chunk.chunk
      case _ => ByteBuffer.wrap(blob.getBytes(1, blob.length().asInstanceOf[Int]))
    })
    blob.free()
    buffer
  }

  def getColumnLob(columnIndex: Int): ByteBuffer = {
    val buffer = colBuffers.get(columnIndex + 1)
    if (buffer ne null) buffer
    else {
      // empty buffer indicates value removed from region
      throw new EntryDestroyedException(s"Iteration on column=${columnIndex + 1} " +
          s"bucket=$partitionId uuid=$currentUUID failed due to missing value")
    }
  }

  def getCurrentDeltaStats: ByteBuffer = currentDeltaStats

  def getUpdatedColumnDecoder(decoder: ColumnDecoder, field: StructField,
      columnIndex: Int): UpdatedColumnDecoderBase = {
    if (currentDeltaStats eq null) return null
    val buffers = colBuffers
    val deltaPosition = ColumnDelta.deltaColumnIndex(columnIndex, 0)
    val delta1 = buffers.get(deltaPosition)
    val delta2 = buffers.get(deltaPosition - 1)
    if ((delta1 ne null) || (delta2 ne null)) {
      UpdatedColumnDecoder(decoder, field, delta1, delta2)
    } else null
  }

  def getDeletedColumnDecoder: ColumnDeleteDecoder = {
    colBuffers.get(ColumnFormatEntry.DELETE_MASK_COL_INDEX) match {
      case null => null
      case deleteBuffer => new ColumnDeleteDecoder(deleteBuffer)
    }
  }

  def getDeletedRowCount: Int = {
    val delete = colBuffers.get(ColumnFormatEntry.DELETE_MASK_COL_INDEX)
    if (delete eq null) 0
    else {
      val allocator = ColumnEncoding.getAllocator(delete)
      ColumnEncoding.readInt(allocator.baseObject(delete),
        allocator.baseOffset(delete) + delete.position() + 8)
    }
  }

  private def releaseColumns(): Unit = {
    val buffers = colBuffers
    // not null check in case constructor itself fails due to low memory
    if ((buffers ne null) && buffers.size() > 0) {
      buffers.forEachWhile(new IntObjPredicate[ByteBuffer] {
        override def test(col: Int, buffer: ByteBuffer): Boolean = {
          // release previous set of buffers immediately
          if (buffer ne null) {
            // release from accounting if decompressed buffer
            if (!BufferAllocator.releaseBuffer(buffer) &&
                (buffer.order() eq ByteOrder.LITTLE_ENDIAN)) {
              releaseExecutionMemory(buffer, CompressionUtils.DECOMPRESSION_OWNER)
            }
          }
          true
        }
      })
      colBuffers = null
    }
  }

  private def readColumnData(): Unit = {
    val columnIndex = rs.getInt(3)
    val columnBlob = rs.getBlob(4)
    val columnBuffer = getBufferFromBlob(columnBlob)
    if (columnBuffer ne null) {
      // put all the read buffers in "colBuffers" to free on next() or close()
      colBuffers.justPut(columnIndex, columnBuffer)
      columnIndex match {
        case ColumnFormatEntry.STATROW_COL_INDEX => currentStats = columnBuffer
        case ColumnFormatEntry.DELTA_STATROW_COL_INDEX => currentDeltaStats = columnBuffer
        case _ =>
      }
    }
  }

  override protected def moveNext(): Boolean = {
    currentStats = null
    currentDeltaStats = null
    releaseColumns()
    if (rsHasNext) {
      currentUUID = rs.getLong(1)
      // create a new map instead of clearing old one to help young gen GC
      colBuffers = IntObjectHashMap.withExpectedSize[ByteBuffer](totalColumns + 1)
      // keep reading next till its still part of current column batch; if UUID changes
      // then next call to "moveNext" will read from incremented cursor position
      // else all rows may have been read which is indicated by "rsHasNext"
      do {
        readColumnData()
        rsHasNext = rs.next()
      } while (rsHasNext && rs.getLong(1) == currentUUID)
      true
    } else false
  }

  override protected def getCurrentValue: ByteBuffer = currentStats

  override def close(): Unit = {
    releaseColumns()
    super.close()
  }
}
