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
package org.apache.spark.sql.execution.columnar.encoding

import java.nio.ByteBuffer

import com.gemstone.gemfire.internal.shared.BufferAllocator

import org.apache.spark.sql.types.{BooleanType, DataType, StructField}

trait BooleanBitSetEncoding extends ColumnEncoding {

  override final def typeId: Int = 4

  override final def supports(dataType: DataType): Boolean =
    dataType == BooleanType
}

final class BooleanBitSetDecoder(columnBytes: AnyRef, startCursor: Long,
    field: StructField, initDelta: (AnyRef, Long) => Long = ColumnEncoding.identityLong)
    extends BooleanBitSetDecoderBase(columnBytes, startCursor, field,
      initDelta) with NotNullDecoder

final class BooleanBitSetDecoderNullable(columnBytes: AnyRef, startCursor: Long,
    field: StructField, initDelta: (AnyRef, Long) => Long = ColumnEncoding.identityLong)
    extends BooleanBitSetDecoderBase(columnBytes, startCursor, field,
      initDelta) with NullableDecoder

final class BooleanBitSetEncoder
    extends NotNullEncoder with BooleanBitSetEncoderBase

final class BooleanBitSetEncoderNullable
    extends NullableEncoder with BooleanBitSetEncoderBase

abstract class BooleanBitSetDecoderBase(columnBytes: AnyRef, startCursor: Long,
    field: StructField, initDelta: (AnyRef, Long) => Long)
    extends ColumnDecoder(columnBytes, startCursor, field,
      initDelta) with BooleanBitSetEncoding {

  override protected[sql] def initializeCursor(columnBytes: AnyRef, cursor: Long,
      dataType: DataType): Long = cursor

  override final def readBoolean(columnBytes: AnyRef, nonNullPosition: Int): Boolean = {
    BitSet.isSet(columnBytes, baseCursor, nonNullPosition)
  }
}

trait BooleanBitSetEncoderBase
    extends ColumnEncoder with BooleanBitSetEncoding {

  /**
   * This stores the actual cursor. The cursor returned by writeBoolean
   * is actually the current index into the currentWord for best performance.
   */
  private var byteCursor: Long = _
  private var dataOffset: Long = _
  private var currentWord: Long = _

  override def initSizeInBytes(dataType: DataType,
      initSize: Long, defSize: Int): Long = dataType match {
    // round to nearest 64 since each bit mask is a long word
    case BooleanType => ((initSize + 63) >>> 6) << 3
    case _ => throw new UnsupportedOperationException(
      s"Unexpected BooleanBitSet encoding for $dataType")
  }

  override def initialize(dataType: DataType, nullable: Boolean, initSize: Int,
      withHeader: Boolean, allocator: BufferAllocator, minBufferSize: Int = -1): Long = {
    byteCursor = super.initialize(dataType, nullable, initSize,
      withHeader, allocator, minBufferSize)
    dataOffset = byteCursor - columnBeginPosition
    currentWord = 0L
    // returns the OR mask to use for currentWord
    1L
  }

  override def offset(cursor: Long): Long = byteCursor - columnBeginPosition

  override def writeInternals(columnBytes: AnyRef, cursor: Long): Long = {
    byteCursor = cursor
    dataOffset = cursor - columnBeginPosition
    // nothing extra to be written but clear the currentWord
    currentWord = 0L
    // return the initial OR mask as expected by writeBoolean
    1L
  }

  private def writeCurrentWord(): Long = {
    var cursor = byteCursor
    if (cursor + 8 > columnEndPosition) {
      cursor = expand(cursor, 8)
    }
    val currentWord = this.currentWord
    ColumnEncoding.writeLong(columnBytes, cursor, currentWord)
    cursor += 8
    // update the statistics
    if (currentWord != 0L) {
      // there are true values
      updateLongStats(1L)
    }
    if (currentWord != -1L) {
      // there are false values
      updateLongStats(0L)
    }
    cursor
  }

  override def sizeInBytes(cursor: Long): Long = byteCursor - columnBeginPosition + 8

  override final def writeBoolean(mask: Long, value: Boolean): Long = {
    if (value) {
      currentWord |= mask
    }
    if (mask != ColumnEncoding.MAX_BITMASK) {
      mask << 1
    } else {
      byteCursor = writeCurrentWord()
      currentWord = 0L
      1L
    }
  }

  override def flushWithoutFinish(mask: Long): Long = {
    if (mask != 1L) {
      // one more word required
      byteCursor = writeCurrentWord()
    }
    byteCursor
  }

  override abstract def finish(mask: Long): ByteBuffer = {
    super.finish(flushWithoutFinish(mask))
  }

  override def encodedSize(mask: Long, dataBeginPosition: Long): Long = {
    super.encodedSize(byteCursor, columnBeginPosition + dataOffset)
  }
}