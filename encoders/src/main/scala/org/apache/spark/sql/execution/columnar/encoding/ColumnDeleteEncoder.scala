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

package org.apache.spark.sql.execution.columnar.encoding

import java.nio.{ByteBuffer, ByteOrder}

import com.gemstone.gemfire.cache.{EntryEvent, Region}
import com.gemstone.gemfire.internal.cache.delta.Delta
import com.gemstone.gemfire.internal.cache.versions.{VersionSource, VersionTag}
import com.gemstone.gemfire.internal.cache.{DiskEntry, EntryEventImpl, GemFireCacheImpl}
import com.gemstone.gemfire.internal.shared.{BufferAllocator, FetchRequest}
import com.pivotal.gemfirexd.internal.engine.GfxdSerializable

import org.apache.spark.sql.execution.columnar.impl.ColumnFormatValue
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.unsafe.Platform

/**
 * Currently just stores the deleted positions assuming sorted by position at plan level.
 * This can be optimized to use a more efficient storage when number of positions is large
 * large like a boolean bitset, or use a more comprehensive compression scheme like
 * PFOR (https://github.com/lemire/JavaFastPFOR).
 */
final class ColumnDeleteEncoder extends ColumnEncoder {

  override def typeId: Int = -1

  override def supports(dataType: DataType): Boolean = dataType eq IntegerType

  override def nullCount: Int = 0

  override def isNullable: Boolean = false

  override protected[sql] def getNumNullWords: Int = 0

  override protected[sql] def initializeNulls(initSize: Int): Int =
    throw new UnsupportedOperationException(s"initializeNulls for $toString")

  override protected[sql] def writeNulls(columnBytes: AnyRef, cursor: Long, numWords: Int): Long =
    throw new UnsupportedOperationException(s"writeNulls for $toString")

  override def writeIsNull(ordinal: Int): Unit =
    throw new UnsupportedOperationException(s"decoderBeforeFinish for $toString")

  /**
   * The deleted position array is maintained as a raw ByteBuffer with native ByteOrder so
   * that ByteBuffer.get/put and Unsafe access via Platform class are equivalent.
   */
  private var deletedPositions: ByteBuffer = _

  private[this] def bufferOwner: String = "DELETE_ENCODER"

  private[this] def allocatePositions(size: Int): Unit = {
    deletedPositions = allocator.allocate(size, bufferOwner).order(ByteOrder.nativeOrder())
    allocator.clearPostAllocate(deletedPositions, 0)
  }

  def initialize(initSize: Int): Int = {
    allocator = GemFireCacheImpl.getCurrentBufferAllocator
    allocatePositions(initSize << 2)
    // cursor indicates index into deletedPositions array
    0
  }

  override def initialize(dataType: DataType, nullable: Boolean, initSize: Int,
      withHeader: Boolean, allocator: BufferAllocator,
      minBufferSize: Int = -1): Long = initialize(initSize)

  private[this] def baseSetPosition(index: Int, value: Int): Unit = {
    deletedPositions.putInt(index << 2, value)
  }

  def writeInt(position: Int, value: Int): Int = {
    val limit = deletedPositions.limit()
    if (position >= (limit >> 2)) {
      val expandBy = position >> 1
      // expand will ensure that ByteOrder is same
      deletedPositions = allocator.expand(deletedPositions, expandBy << 1, bufferOwner)
      assert(deletedPositions.order() == ByteOrder.nativeOrder())
      allocator.clearPostAllocate(deletedPositions, limit)
    }
    baseSetPosition(position, value)
    position + 1
  }

  private def createFinalBuffer(numPositions: Int, numBaseRows: Int): ByteBuffer = {
    val allocator = storageAllocator
    // add a header of 4 bytes for future use (e.g. format change)
    val bufferSize = (numPositions << 2) + 12
    val buffer = allocator.allocateForStorage(ColumnEncoding.checkBufferSize(bufferSize))
    val bufferBytes = allocator.baseObject(buffer)
    var bufferCursor = allocator.baseOffset(buffer)

    // header for future use
    ColumnEncoding.writeInt(bufferBytes, bufferCursor, 0)
    bufferCursor += 4
    // write the number of base rows to compact the buffer if required
    ColumnEncoding.writeInt(bufferBytes, bufferCursor, numBaseRows)
    bufferCursor += 4
    // number of deletes
    ColumnEncoding.writeInt(bufferBytes, bufferCursor, numPositions)
    bufferCursor += 4
    if (ColumnEncoding.littleEndian) {
      // bulk copy if platform endian-ness matches the final format
      Platform.copyMemory(this.allocator.baseObject(deletedPositions),
        this.allocator.baseOffset(deletedPositions), bufferBytes, bufferCursor, numPositions << 2)
    } else {
      deletedPositions.rewind()
      var index = 0
      while (index < numPositions) {
        ColumnEncoding.writeInt(bufferBytes, bufferCursor, deletedPositions.getInt())
        bufferCursor += 4
        index += 1
      }
    }
    this.allocator.release(deletedPositions)
    deletedPositions = null
    buffer
  }

  def merge(newValue: ByteBuffer, existingValue: ByteBuffer): ByteBuffer = {
    var position = 0

    val allocator1 = ColumnEncoding.getAllocator(newValue)
    allocator = allocator1
    allocatePositions(16 << 2)
    val columnBytes1 = allocator1.baseObject(newValue)
    var cursor1 = allocator1.baseOffset(newValue) + newValue.position()
    val endOffset1 = cursor1 + newValue.remaining()
    // skip four byte header
    cursor1 += 4
    val numBaseRows = ColumnEncoding.readInt(columnBytes1, cursor1)
    // skip number of elements
    cursor1 += 8
    var position1 = ColumnEncoding.readInt(columnBytes1, cursor1)

    val allocator2 = ColumnEncoding.getAllocator(existingValue)
    val columnBytes2 = allocator2.baseObject(existingValue)
    var cursor2 = allocator2.baseOffset(existingValue) + existingValue.position()
    val endOffset2 = cursor2 + existingValue.remaining()
    // skip 12 byte header (4 byte + number of base rows + number of elements)
    cursor2 += 12
    var position2 = ColumnEncoding.readInt(columnBytes2, cursor2)

    // Simple two-way merge of deleted positions with duplicate elimination.
    var doProcess = cursor1 < endOffset1 && cursor2 < endOffset2
    while (doProcess) {
      if (position1 > position2) {
        // consume position2 and move
        position = writeInt(position, position2)
        cursor2 += 4
        if (cursor2 < endOffset2) {
          position2 = ColumnEncoding.readInt(columnBytes2, cursor2)
        } else {
          doProcess = false
        }
      } else {
        // consume position1 and move
        position = writeInt(position, position1)
        if (position1 == position2) {
          // move position2 without consuming
          cursor2 += 4
          if (cursor2 < endOffset2) {
            position2 = ColumnEncoding.readInt(columnBytes2, cursor2)
          } else {
            doProcess = false
          }
        }
        cursor1 += 4
        if (cursor1 < endOffset1) {
          position1 = ColumnEncoding.readInt(columnBytes1, cursor1)
        } else {
          doProcess = false
        }
      }
    }
    // consume any remaining (slight inefficiency of reading first positions again
    //   but doing that for code clarity)
    while (cursor1 < endOffset1) {
      position = writeInt(position, ColumnEncoding.readInt(columnBytes1, cursor1))
      cursor1 += 4
    }
    while (cursor2 < endOffset2) {
      position = writeInt(position, ColumnEncoding.readInt(columnBytes2, cursor2))
      cursor2 += 4
    }

    createFinalBuffer(position, numBaseRows)
  }

  override def finish(cursor: Long): ByteBuffer = {
    throw new UnsupportedOperationException(
      "ColumnDeleteEncoder.finish(cursor) not expected to be called")
  }

  def finish(numPositions: Int, numBaseRows: Int): ByteBuffer = {
    createFinalBuffer(numPositions, numBaseRows)
  }
}

/** Simple delta that merges the deleted positions */
final class ColumnDeleteDelta extends ColumnFormatValue with Delta {

  def this(buffer: ByteBuffer, codecId: Int, isCompressed: Boolean,
      changeOwnerToStorage: Boolean = true) = {
    this()
    setBuffer(buffer, codecId, isCompressed, changeOwnerToStorage)
  }

  override protected def copy(buffer: ByteBuffer, isCompressed: Boolean,
      changeOwnerToStorage: Boolean): ColumnDeleteDelta = {
    new ColumnDeleteDelta(buffer, compressionCodecId, isCompressed, changeOwnerToStorage)
  }

  override def apply(putEvent: EntryEvent[_, _]): AnyRef = {
    val event = putEvent.asInstanceOf[EntryEventImpl]
    apply(event.getRegion, event.getKey, event.getOldValueAsOffHeapDeserializedOrRaw,
      event.getTransactionId == null)
  }

  override def apply(region: Region[_, _], key: AnyRef, oldValue: AnyRef,
      prepareForOffHeap: Boolean): AnyRef = {
    if (oldValue eq null) synchronized {
      // first delta, so put as is
      val result = new ColumnFormatValue(columnBuffer, compressionCodecId, isCompressed)
      // buffer has been transferred and should be removed from delta
      // which would no longer be usable after this point
      columnBuffer = DiskEntry.Helper.NULL_BUFFER
      decompressionState = -1
      result
    } else {
      // merge with existing delete list
      val encoder = new ColumnDeleteEncoder
      val oldColumnValue = oldValue.asInstanceOf[ColumnFormatValue].getValueRetain(
        FetchRequest.DECOMPRESS)
      val existingBuffer = oldColumnValue.getBuffer
      val newValue = getValueRetain(FetchRequest.DECOMPRESS)
      try {
        new ColumnFormatValue(encoder.merge(newValue.getBuffer, existingBuffer),
          oldColumnValue.compressionCodecId, isCompressed = false)
      } finally {
        oldColumnValue.release()
        newValue.release()
        // release own buffer too and delta should be unusable now
        release()
      }
    }
  }

  /** first delta update for a column will be put as is into the region */
  override def allowCreate(): Boolean = true

  override def merge(region: Region[_, _], toMerge: Delta): Delta =
    throw new UnsupportedOperationException("Unexpected call to ColumnDeleteDelta.merge")

  override def cloneDelta(): Delta =
    throw new UnsupportedOperationException("Unexpected call to ColumnDeleteDelta.cloneDelta")

  override def setVersionTag(versionTag: VersionTag[_ <: VersionSource[_]]): Unit =
    throw new UnsupportedOperationException("Unexpected call to ColumnDeleteDelta.setVersionTag")

  override def getVersionTag: VersionTag[_ <: VersionSource[_]] =
    throw new UnsupportedOperationException("Unexpected call to ColumnDeleteDelta.getVersionTag")

  override def getGfxdID: Byte = GfxdSerializable.COLUMN_DELETE_DELTA

  override protected def className: String = "ColumnDeleteDelta"
}
