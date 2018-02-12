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
package org.apache.spark.sql.execution.columnar

import java.nio.{ByteBuffer, ByteOrder}
import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.util.function.BiFunction

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.util.control.NonFatal

import com.gemstone.gemfire.cache.EntryDestroyedException
import com.gemstone.gemfire.internal.cache.{BucketRegion, LocalRegion, NonLocalRegionEntry, PartitionedRegion, RegionEntry, TXStateInterface}
import com.gemstone.gemfire.internal.shared.unsafe.UnsafeHolder
import com.koloboke.function.IntObjPredicate
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection
import io.snappydata.collection.IntObjectHashMap
import io.snappydata.thrift.common.BufferedBlob

import org.apache.spark.sql.execution.columnar.encoding.{ColumnDecoder, ColumnDeleteDecoder, ColumnEncoding, UpdatedColumnDecoder, UpdatedColumnDecoderBase}
import org.apache.spark.sql.execution.columnar.impl.{ClusteredColumnIterator, ColumnDelta, ColumnFormatEntry, ColumnFormatIterator, ColumnFormatKey, ColumnFormatValue, RemoteEntriesIterator}
import org.apache.spark.sql.execution.row.PRValuesIterator
import org.apache.spark.sql.store.CompressionUtils
import org.apache.spark.sql.types.StructField
import org.apache.spark.{Logging, TaskContext}

case class ColumnBatch(numRows: Int, buffers: Array[ByteBuffer],
    statsData: Array[Byte], deltaIndexes: Array[Int])

abstract class ResultSetIterator[A](conn: Connection,
    stmt: Statement, rs: ResultSet, context: TaskContext)
    extends Iterator[A] with Logging {

  protected[this] final var doMove = true

  protected[this] final var hasNextValue: Boolean = rs ne null

  if (context ne null) {
    val partitionId = context.partitionId()
    context.addTaskCompletionListener { _ =>
      logDebug(s"closed connection for task from listener $partitionId")
      close()
    }
  }

  override final def hasNext: Boolean = {
    var success = false
    try {
      if (doMove && hasNextValue) {
        success = rs.next()
        doMove = false
        success
      } else {
        success = hasNextValue
        success
      }
    } finally {
      if (!success) {
        hasNextValue = false
      }
    }
  }

  override final def next(): A = {
    if (doMove) {
      hasNext
      doMove = true
      if (!hasNextValue) return null.asInstanceOf[A]
    }
    val result = getCurrentValue
    doMove = true
    result
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
    extends PRValuesIterator[ByteBuffer](container = null, region, bucketIds) {

  if (region ne null) {
    assert(!region.getEnableOffHeapMemory,
      s"Unexpected buffer iterator call for off-heap $region")
  }

  if (context ne null) {
    context.addTaskCompletionListener(_ => releaseColumns())
  }

  protected var currentVal: ByteBuffer = _
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

  private def getColumnBuffer(columnPosition: Int, throwIfMissing: Boolean): ByteBuffer = {
    val value = itr.getBucketEntriesIterator.asInstanceOf[ClusteredColumnIterator]
        .getColumnValue(columnPosition)
    if (value ne null) {
      val columnValue = value.asInstanceOf[ColumnFormatValue].getValueRetain(
        decompress = true, compress = false)
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

  def hasUpdatedColumns: Boolean = currentDeltaStats ne null

  def getUpdatedColumnDecoder(decoder: ColumnDecoder, field: StructField,
      columnIndex: Int): UpdatedColumnDecoderBase = {
    if (currentDeltaStats eq null) null
    else {
      // TODO: SW: check for actual delta stats to see if there are updates
      val deltaPosition = ColumnDelta.deltaColumnIndex(columnIndex, 0)
      val delta1 = getColumnBuffer(deltaPosition, throwIfMissing = false)
      val delta2 = getColumnBuffer(deltaPosition - 1, throwIfMissing = false)
      if ((delta1 ne null) || (delta2 ne null)) {
        UpdatedColumnDecoder(decoder, field, delta1, delta2)
      } else null
    }
  }

  def getDeletedColumnDecoder: ColumnDeleteDecoder = {
    if (currentDeltaStats eq null) null
    else getColumnBuffer(ColumnFormatEntry.DELETE_MASK_COL_INDEX,
      throwIfMissing = false) match {
      case null => null
      case deleteBuffer => new ColumnDeleteDecoder(deleteBuffer)
    }
  }

  def getDeletedRowCount: Int = {
    if (currentDeltaStats eq null) 0
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

  override protected def moveNext(): Unit = {
    if (region ne null) {
      // release previous set of values
      val numColumns = releaseColumns()
      if (numColumns > 0) {
        currentColumns = new ArrayBuffer[ColumnFormatValue](numColumns)
      }
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
                decompress = true, compress = false)
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
}

final class ColumnBatchIteratorOnRS(conn: Connection,
    requiredColumns: Array[String],
    stmt: Statement, rs: ResultSet,
    context: TaskContext,
    partitionId: Int,
    fetchColQuery: String)
    extends ResultSetIterator[ByteBuffer](conn, stmt, rs, context) {
  private var currentUUID: Long = _
  // upto three deltas for each column and a deleted mask
  private val totalColumns = (requiredColumns.length * (ColumnDelta.MAX_DEPTH + 1)) + 1
  private var colBuffers: IntObjectHashMap[ByteBuffer] =
    IntObjectHashMap.withExpectedSize[ByteBuffer](totalColumns + 1)
  private var hasUpdates: Boolean = _
  private val ps: PreparedStatement = conn.prepareStatement(fetchColQuery)

  def getCurrentBatchId: Long = currentUUID

  def getCurrentBucketId: Int = partitionId

  private def decompress(buffer: ByteBuffer): ByteBuffer = {
    val allocator = ColumnEncoding.getAllocator(buffer)
    val result = CompressionUtils.codecDecompressIfRequired(
      buffer.order(ByteOrder.LITTLE_ENDIAN), allocator)
    if (result ne buffer) {
      UnsafeHolder.releaseIfDirectBuffer(buffer)
    }
    result
  }

  private def fillBuffers(): Unit = {
    colBuffers match {
      case buffers if buffers.size() > 1 => // already filled in
      case buffers =>
        hasUpdates = false
        ps.setLong(1, currentUUID)
        val colIter = ps.executeQuery()
        while (colIter.next()) {
          val colBlob = colIter.getBlob(4)
          val position = colIter.getInt(3)
          val colBuffer = colBlob match {
            case blob: BufferedBlob =>
              // the chunk can never be a ByteBufferReference in this case and
              // the internal buffer will now be owned by ColumnFormatValue
              val chunk = blob.getAsLastChunk
              assert(!chunk.isSetChunkReference)
              chunk.chunk
            case blob => ByteBuffer.wrap(blob.getBytes(
              1, blob.length().asInstanceOf[Int]))
          }
          colBlob.free()
          buffers.justPut(position, decompress(colBuffer))
          // check if this an update delta
          if (position < ColumnFormatEntry.DELETE_MASK_COL_INDEX && !hasUpdates) {
            hasUpdates = true
          }
        }
    }
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

  def hasUpdatedColumns: Boolean = hasUpdates

  def getUpdatedColumnDecoder(decoder: ColumnDecoder, field: StructField,
      columnIndex: Int): UpdatedColumnDecoderBase = {
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
    fillBuffers()
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
          UnsafeHolder.releaseIfDirectBuffer(buffer)
          true
        }
      })
    }
  }

  override protected def getCurrentValue: ByteBuffer = {
    currentUUID = rs.getLong(1)
    releaseColumns()
    // create a new map instead of clearing old one to help young gen GC
    colBuffers = IntObjectHashMap.withExpectedSize[ByteBuffer](totalColumns + 1)
    val statsBlob = rs.getBlob(4)
    val statsBuffer = decompress(statsBlob match {
      case blob: BufferedBlob =>
        // the chunk can never be a ByteBufferReference in this case and
        // the internal buffer will now be owned by ColumnFormatValue
        val chunk = blob.getAsLastChunk
        assert(!chunk.isSetChunkReference)
        chunk.chunk
      case blob => ByteBuffer.wrap(blob.getBytes(
        1, blob.length().asInstanceOf[Int]))
    })
    statsBlob.free()
    // put the stats buffer to free on next() or close()
    colBuffers.justPut(ColumnFormatEntry.STATROW_COL_INDEX, statsBuffer)
    statsBuffer
  }

  override def close(): Unit = {
    releaseColumns()
    super.close()
  }
}
