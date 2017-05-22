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

import java.nio.ByteBuffer
import java.sql.{Blob, Connection, PreparedStatement, ResultSet, Statement}

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.util.control.NonFatal

import com.gemstone.gemfire.cache.EntryDestroyedException
import com.gemstone.gemfire.internal.cache.{BucketRegion, LocalRegion, NonLocalRegionEntry, RegionEntry}
import com.gemstone.gemfire.internal.shared.unsafe.UnsafeHolder
import io.snappydata.thrift.common.BufferedBlob
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap

import org.apache.spark.sql.execution.columnar.impl.{ColumnFormatEntry, ColumnFormatKey, ColumnFormatValue}
import org.apache.spark.sql.execution.row.PRValuesIterator
import org.apache.spark.{Logging, TaskContext}

case class ColumnBatch(numRows: Int, buffers: Array[ByteBuffer],
    statsData: Array[Byte])

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
        stmt.close()
      }
    } catch {
      case NonFatal(e) => logWarning("Exception closing statement", e)
    }
    try {
      conn.commit()
      conn.close()
      logDebug("closed connection for task " + context.partitionId())
    } catch {
      case NonFatal(e) => logWarning("Exception closing connection", e)
    }
    hasNextValue = false
  }
}

object ColumnBatchIterator {

  def apply(region: LocalRegion,
      bucketIds: java.util.Set[Integer],
      context: TaskContext): ColumnBatchIterator = {
    new ColumnBatchIterator(region, batch = null, bucketIds, context)
  }

  def apply(batch: ColumnBatch): ColumnBatchIterator = {
    new ColumnBatchIterator(region = null, batch, bucketIds = null,
      context = null)
  }

  val STATROW_COL_INDEX: Int = -1
}

final class ColumnBatchIterator(region: LocalRegion, val batch: ColumnBatch,
    bucketIds: java.util.Set[Integer], context: TaskContext)
    extends PRValuesIterator[ByteBuffer](container = null, region, bucketIds) {

  if (region ne null) {
    assert(!region.getEnableOffHeapMemory,
      s"Unexpected buffer iterator call for off-heap $region")
  } else {
    // skip the serialization headers in the ByteBuffers
    batch.buffers.foreach(buffer => buffer.position(buffer.position() +
        ColumnFormatEntry.VALUE_HEADER_SIZE))
  }

  if (context ne null) {
    context.addTaskCompletionListener(_ => releaseColumns())
  }

  protected var currentVal: ByteBuffer = _
  private var currentKeyPartitionId: Int = _
  private var currentKeyUUID: String = _
  private var currentBucketRegion: BucketRegion = _
  private var batchProcessed = false
  private var currentColumns = new ArrayBuffer[ColumnFormatValue]()

  def getColumnLob(bufferPosition: Int): ByteBuffer = {
    if (region ne null) {
      val key = new ColumnFormatKey(currentKeyPartitionId, bufferPosition,
        currentKeyUUID)
      val value = if (currentBucketRegion != null) currentBucketRegion.get(key)
      else region.get(key)
      if (value ne null) {
        val columnValue = value.asInstanceOf[ColumnFormatValue]
        val buffer = columnValue.getBufferRetain
        if (buffer.remaining() > 0) {
          currentColumns += columnValue
          return buffer
        }
      }
      // empty buffer indicates value removed from region
      throw new EntryDestroyedException(s"Iteration on column=$bufferPosition " +
          s"partition=$currentKeyPartitionId key=$key failed due to missing value")
    } else {
      batch.buffers(bufferPosition - 1)
    }
  }

  def releaseColumns(): Int = {
    val previousColumns = currentColumns
    if ((previousColumns ne null) && previousColumns.nonEmpty) {
      currentColumns = null
      previousColumns.foreach(_.release())
      previousColumns.length
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
        currentBucketRegion = itr.getHostedBucketRegion
        // get the stat row region entries only. region entries for individual
        // columns will be fetched on demand
        if ((currentBucketRegion ne null) ||
            re.isInstanceOf[NonLocalRegionEntry]) {
          val key = re.getRawKey.asInstanceOf[ColumnFormatKey]
          if (key.columnIndex == ColumnBatchIterator.STATROW_COL_INDEX) {
            // if currentBucketRegion is null then its the case of
            // NonLocalRegionEntry where RegionEntryContext arg is not required
            val v = re.getValue(currentBucketRegion)
            if (v ne null) {
              val columnValue = v.asInstanceOf[ColumnFormatValue]
              val buffer = columnValue.getBufferRetain
              // empty buffer indicates value removed from region
              if (buffer.remaining() > 0) {
                currentKeyPartitionId = key.partitionId
                currentKeyUUID = key.uuid
                currentVal = buffer
                currentColumns += columnValue
                return
              }
            }
          }
        }
      }
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
    fetchColQuery: String)
    extends ResultSetIterator[Array[Byte]](conn, stmt, rs, context) {
  private var currentUUID: String = _
  private val ps: PreparedStatement = conn.prepareStatement(fetchColQuery)
  private var colBuffers: Option[Int2ObjectOpenHashMap[(ByteBuffer, Blob)]] = None

  def getColumnLob(bufferPosition: Int): ByteBuffer = {
    colBuffers match {
      case Some(map) => map.get(bufferPosition)._1
      case None =>
        for (i <- requiredColumns.indices) {
          ps.setString(i + 1, currentUUID)
        }
        val colIter = ps.executeQuery()
        val bufferMap = new Int2ObjectOpenHashMap[(ByteBuffer, Blob)]()
        var index = 1
        while (colIter.next()) {
          val colBlob = colIter.getBlob(1)
          val colBuffer = colBlob match {
            case blob: BufferedBlob => blob.getAsLastChunk.chunk
            case blob => ByteBuffer.wrap(blob.getBytes(
              1, blob.length().asInstanceOf[Int]))
          }
          bufferMap.put(index, (colBuffer, colBlob))
          index = index + 1
        }
        colBuffers = Some(bufferMap)
        bufferMap.get(bufferPosition)._1
    }
  }

  override protected def getCurrentValue: Array[Byte] = {
    currentUUID = rs.getString(2)
    colBuffers match {
      case Some(buffers) =>
        val values = buffers.values().iterator()
        while (values.hasNext) {
          val (buffer, blob) = values.next()
          blob.free()
          // release previous set of buffers immediately
          UnsafeHolder.releaseIfDirectBuffer(buffer)
        }
      case _ =>
    }
    colBuffers = None
    val statsData = rs.getBlob(1)
    val statsBytes = statsData.getBytes(1, statsData.length().asInstanceOf[Int])
    statsData.free()
    statsBytes
  }

  override def close(): Unit = {
    colBuffers match {
      case Some(buffers) =>
        val values = buffers.values().iterator()
        while (values.hasNext) {
          val (buffer, blob) = values.next()
          try {
            blob.free()
          } catch {
            case NonFatal(e) => logWarning("Exception clearing Blob", e)
          }
          // release last set of buffers immediately
          UnsafeHolder.releaseIfDirectBuffer(buffer)
        }
      case _ =>
    }
    super.close()
  }
}
