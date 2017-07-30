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
import org.apache.spark.unsafe.Platform

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

  override protected[sql] def initializeCursor(columnBytes: AnyRef, cursor: Long,
      field: StructField): Long = {
    // baseCursor should never change after initialization
    baseCursor = cursor
    byteCursor = cursor
    // return current bit mask as the cursor so that is used and
    // updated in the next call; the byte position will happen once
    // every 64 calls so that is a member variable;
    // return max mask to force reading word in first nextBoolean call
    ColumnEncoding.MAX_BITMASK
  }

  override final def nextBoolean(columnBytes: AnyRef, mask: Long): Long = {
    val currentBitMask = mask << 1
    if (currentBitMask != 0L) currentBitMask
    else {
      currentWord = ColumnEncoding.readLong(columnBytes, byteCursor)
      byteCursor += 8
      1L
    }
  }

  override def absoluteBoolean(columnBytes: AnyRef, position: Int): Long = {
    val getPosition = position - numNullsUntilPosition(columnBytes, position) + 1
    val wordCursor = baseCursor + (getPosition >> 6) << 3
    currentWord = ColumnEncoding.readLong(columnBytes, wordCursor)
    // "cursor" is mod 64 and shift which is what currentWord will be masked with
    1L << (getPosition & 0x3f)
  }

  override final def readBoolean(columnBytes: AnyRef, mask: Long): Boolean =
    (currentWord & mask) != 0
}

trait BooleanBitSetEncoderBase
    extends ColumnEncoder with BooleanBitSetEncoding {

  /**
   * This stores the actual cursor. The cursor returned by writeBoolean
   * is actually the current index into the currentWord for best performance.
   */
  private var byteCursor: Long = _
  private var startByteCursor: Long = _
  private var currentWord: Long = _

  override def initSizeInBytes(dataType: DataType,
      initSize: Long, defSize: Int): Long = dataType match {
    // round to nearest 64 since each bit mask is a long word
    case BooleanType => ((initSize + 63) >>> 6) << 3
    case _ => throw new UnsupportedOperationException(
      s"Unexpected BooleanBitSet encoding for $dataType")
  }

  override def initialize(dataType: DataType, nullable: Boolean, initSize: Int,
      withHeader: Boolean, allocator: BufferAllocator): Long = {
    startByteCursor = super.initialize(dataType, nullable, initSize,
      withHeader, allocator)
    byteCursor = startByteCursor
    currentWord = 0L
    // returns the OR mask to use for currentWord
    1L
  }

  override private[sql] def decoderBeforeFinish: ColumnDecoder = {
    // can't depend on nullCount because even with zero count, it may have
    // allocated some null space at the start in advance
    val decoder = if (isNullable) new BooleanBitSetDecoderNullable else new BooleanBitSetDecoder
    decoder.initializeCursor(null, initializeNullsBeforeFinish(decoder), null)
    decoder
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
      byteCursor = writeCurrentWord() + 8L
      currentWord = 0L
      1L
    }
  }

  /**
   * Set the boolean at given position. Assumes withHeader was false in initialization.
   */
  final def writeBooleanAtPosition(position: Int, value: Boolean): Unit = {
    if (((columnEndPosition - columnBeginPosition) << 3) > position) {
      val bytePosition = columnBeginPosition + (position >>> 3)
      // mod 8 and shift
      val mask = 1 << (position & 0x7)
      val currentByte = Platform.getByte(columnBytes, bytePosition)
      if (value) {
        Platform.putByte(columnBytes, bytePosition, (currentByte | mask).toByte)
      } else {
        Platform.putByte(columnBytes, bytePosition, (currentByte & ~mask).toByte)
      }
    } else {
      throw new IndexOutOfBoundsException(s"Cannot write at position = $position " +
          s"sizeInBytes=${columnEndPosition - columnBeginPosition}")
    }
  }

  override def flushWithoutFinish(mask: Long): Long = {
    if (mask != 1L) {
      // one more word required
      byteCursor = writeCurrentWord() + 8L
    }
    byteCursor
  }

  override abstract def finish(mask: Long): ByteBuffer = {
    super.finish(flushWithoutFinish(mask))
  }

  override def encodedSize(mask: Long, dataBeginPosition: Long): Long = {
    if (mask != 1L) super.encodedSize(byteCursor + 8L, startByteCursor)
    else super.encodedSize(byteCursor, startByteCursor)
  }
}
