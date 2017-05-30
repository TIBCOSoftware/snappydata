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
package org.apache.spark.sql.execution.columnar.encoding

import java.nio.ByteBuffer

import com.gemstone.gemfire.internal.shared.BufferAllocator

import org.apache.spark.sql.types.{BooleanType, DataType, StructField}

trait BooleanBitSetEncoding extends ColumnEncoding {

  override final def typeId: Int = 4

  override final def supports(dataType: DataType): Boolean =
    dataType == BooleanType
}

final class BooleanBitSetDecoder
    extends BooleanBitSetDecoderBase with NotNullDecoder

final class BooleanBitSetDecoderNullable
    extends BooleanBitSetDecoderBase with NullableDecoder

final class BooleanBitSetEncoder
    extends NotNullEncoder with BooleanBitSetEncoderBase

final class BooleanBitSetEncoderNullable
    extends NullableEncoder with BooleanBitSetEncoderBase

abstract class BooleanBitSetDecoderBase
    extends ColumnDecoder with BooleanBitSetEncoding {

  private[this] var byteCursor = 0L
  private[this] var currentWord = 0L

  override protected def initializeCursor(columnBytes: AnyRef, cursor: Long,
      field: StructField): Long = {
    byteCursor = cursor
    // return current bit index as the cursor so that is used and
    // incremented in the next call; the byte position will happen once
    // every 64 calls so that can be a member variable;
    // return max to force reading word in first nextBoolean call
    ColumnEncoding.BITS_PER_LONG
  }

  override final def nextBoolean(columnBytes: AnyRef, cursor: Long): Long = {
    var currentBitIndex = cursor
    currentBitIndex += 1
    if (currentBitIndex < ColumnEncoding.BITS_PER_LONG) currentBitIndex
    else {
      currentWord = ColumnEncoding.readLong(columnBytes, byteCursor)
      byteCursor += 8
      0L
    }
  }

  override final def readBoolean(columnBytes: AnyRef, cursor: Long): Boolean =
    ((currentWord >> cursor) & 1) != 0
}

trait BooleanBitSetEncoderBase
    extends ColumnEncoder with BooleanBitSetEncoding {

  /**
   * This stores the actual cursor. The cursor returned by writeBoolean
   * is actually the current index into the currentWord for best performance.
   */
  private var byteCursor: Long = _
  private var currentWord: Long = _

  override def initSizeInBytes(dataType: DataType,
      initSize: Long, defSize: Int): Long = dataType match {
    // round to nearest 64 since each bit mask is a long word
    case BooleanType => ((initSize + 63) >>> 6) << 3
    case _ => throw new UnsupportedOperationException(
      s"Unexpected BooleanBitSet encoding for $dataType")
  }

  override def initialize(field: StructField, initSize: Int,
      withHeader: Boolean, allocator: BufferAllocator): Long = {
    byteCursor = super.initialize(field, initSize, withHeader, allocator)
    currentWord = 0L
    // returns the index into currentWord
    0L
  }

  private def writeCurrentWord(): Long = {
    var cursor = byteCursor
    if (cursor + 8 > columnEndPosition) {
      cursor = expand(cursor, 8)
    }
    val currentWord = this.currentWord
    ColumnEncoding.writeLong(columnBytes, cursor, currentWord)
    // update the statistics
    if (currentWord != 0L) {
      // there are true's
      updateLongStats(1L)
    }
    if (currentWord != -1L) {
      // there are false's
      updateLongStats(0L)
    }
    cursor
  }

  override final def writeBoolean(ordinal: Long, value: Boolean): Long = {
    if (ordinal < ColumnEncoding.BITS_PER_LONG) {
      if (value) {
        currentWord |= (1 << ordinal)
      }
      ordinal + 1
    } else {
      byteCursor = writeCurrentWord() + 8
      currentWord = if (value) 1L else 0L
      1L
    }
  }

  override abstract def finish(ordinal: Long): ByteBuffer = {
    if (ordinal > 0) {
      // one more word required
      byteCursor = writeCurrentWord() + 8
    }
    super.finish(byteCursor)
  }
}
