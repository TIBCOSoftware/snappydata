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

package org.apache.spark.sql.execution.columnar.encoding

import java.nio.ByteBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{Decimal, StructField}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Internal class to decode values from a single delta as obtained from
 * [[ColumnDeltaEncoder]]. Should not be used directly rather the combined
 * decoder [[MutatedColumnDecoder]] should be the one used.
 */
final class ColumnDeltaDecoder(realDecoder: ColumnDecoder) {

  private var deltaBytes: AnyRef = _
  private var deltaCursor: Long = _

  private var positionCursor: Long = _
  private var positionEndCursor: Long = _
  private var positionOrdinal: Int = _

  private[encoding] def initialize(buffer: ByteBuffer, field: StructField): Long = {
    val allocator = ColumnEncoding.getAllocator(buffer)
    val columnBytes = allocator.baseObject(buffer)
    var offset = allocator.baseOffset(buffer) + buffer.position()

    // initialize base decoder header first
    offset = realDecoder.initializeNulls(columnBytes, offset, field)

    // read the positions next
    val numPositions = ColumnEncoding.readInt(columnBytes, offset)
    offset += 4

    // find the start of data after padding
    deltaBytes = columnBytes

    positionEndCursor = offset + (numPositions << 2)
    val nextPosition = ColumnEncoding.readInt(columnBytes, offset)
    offset += 4
    positionCursor = offset

    // round to nearest word to get data start position
    offset = ((positionEndCursor + 7) >> 3) << 3

    // the actual cursor is tracked as a field while return value is the
    // next update position
    deltaCursor = realDecoder.initializeCursor(columnBytes, offset, field)

    nextPosition
  }

  private[encoding] def moveToNextPosition(): Int = {
    val cursor = positionCursor
    if (cursor < positionEndCursor) {
      val nextPosition = ColumnEncoding.readInt(deltaBytes, cursor)
      positionCursor += 4
      positionOrdinal += 1
      nextPosition
    } else {
      // convention used by MutableColumnDecoder to denote the end
      // which is greater than everything so will never get selected
      Int.MaxValue
    }
  }

  @inline def hasNulls: Boolean = realDecoder.hasNulls

  @inline def isNull: Int = realDecoder.isNull(deltaBytes, positionOrdinal)

  @inline def nextBoolean(): Unit = {
    deltaCursor = realDecoder.nextBoolean(deltaBytes, deltaCursor)
  }

  @inline def readBoolean: Boolean = realDecoder.readBoolean(deltaBytes, deltaCursor)

  @inline def nextByte(): Unit = {
    deltaCursor = realDecoder.nextByte(deltaBytes, deltaCursor)
  }

  @inline def readByte: Byte = realDecoder.readByte(deltaBytes, deltaCursor)

  @inline def nextShort(): Unit = {
    deltaCursor = realDecoder.nextShort(deltaBytes, deltaCursor)
  }

  @inline def readShort: Short = realDecoder.readShort(deltaBytes, deltaCursor)

  @inline def nextInt(): Unit = {
    deltaCursor = realDecoder.nextInt(deltaBytes, deltaCursor)
  }

  @inline def readInt: Int = realDecoder.readInt(deltaBytes, deltaCursor)

  @inline def nextLong(): Unit = {
    deltaCursor = realDecoder.nextLong(deltaBytes, deltaCursor)
  }

  @inline def readLong: Long = realDecoder.readLong(deltaBytes, deltaCursor)

  @inline def nextFloat(): Unit = {
    deltaCursor = realDecoder.nextFloat(deltaBytes, deltaCursor)
  }

  @inline def readFloat: Float = realDecoder.readFloat(deltaBytes, deltaCursor)

  @inline def nextDouble(): Unit = {
    deltaCursor = realDecoder.nextDouble(deltaBytes, deltaCursor)
  }

  @inline def readDouble: Double = realDecoder.readDouble(deltaBytes, deltaCursor)

  @inline def readDate(): Int = realDecoder.readDate(deltaBytes, deltaCursor)

  @inline def readTimestamp(): Long = realDecoder.readTimestamp(deltaBytes, deltaCursor)

  @inline def nextLongDecimal(): Unit = {
    deltaCursor = realDecoder.nextLongDecimal(deltaBytes, deltaCursor)
  }

  @inline def readLongDecimal(precision: Int, scale: Int): Decimal = {
    realDecoder.readLongDecimal(deltaBytes, precision, scale, deltaCursor)
  }

  @inline def nextDecimal(): Unit = {
    deltaCursor = realDecoder.nextDecimal(deltaBytes, deltaCursor)
  }

  @inline def readDecimal(precision: Int, scale: Int): Decimal = {
    realDecoder.readDecimal(deltaBytes, precision, scale, deltaCursor)
  }

  @inline def nextUTF8String(): Unit = {
    deltaCursor = realDecoder.nextUTF8String(deltaBytes, deltaCursor)
  }

  @inline def readUTF8String: UTF8String =
    realDecoder.readUTF8String(deltaBytes, deltaCursor)

  @inline def nextInterval(): Unit = {
    deltaCursor = realDecoder.nextInterval(deltaBytes, deltaCursor)
  }

  @inline def readInterval: CalendarInterval =
    realDecoder.readInterval(deltaBytes, deltaCursor)

  @inline def nextBinary(): Unit = {
    deltaCursor = realDecoder.nextBinary(deltaBytes, deltaCursor)
  }

  @inline def readBinary: Array[Byte] = realDecoder.readBinary(deltaBytes, deltaCursor)

  @inline def nextArray(): Unit = {
    deltaCursor = realDecoder.nextArray(deltaBytes, deltaCursor)
  }

  @inline def readArray: ArrayData = realDecoder.readArray(deltaBytes, deltaCursor)

  @inline def nextMap(): Unit = {
    deltaCursor = realDecoder.nextMap(deltaBytes, deltaCursor)
  }

  @inline def readMap: MapData = realDecoder.readMap(deltaBytes, deltaCursor)

  @inline def nextStruct(): Unit = {
    deltaCursor = realDecoder.nextStruct(deltaBytes, deltaCursor)
  }

  @inline def readStruct(numFields: Int): InternalRow = {
    realDecoder.readStruct(deltaBytes, numFields, deltaCursor)
  }
}
