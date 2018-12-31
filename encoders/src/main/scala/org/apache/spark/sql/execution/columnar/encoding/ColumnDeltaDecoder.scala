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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{Decimal, StructField}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Internal class to decode values from a single delta as obtained from
 * [[ColumnDeltaEncoder]]. Should not be used directly rather the combined
 * decoder [[UpdatedColumnDecoder]] should be the one used.
 */
final class ColumnDeltaDecoder(buffer: ByteBuffer, field: StructField) {

  private val (realDecoder, deltaBytes) =
    ColumnEncoding.getColumnDecoderAndBuffer(buffer, field, initialize)

  private var positionCursor: Long = _
  private var positionEndCursor: Long = _

  /** relative position being read currently in the underlying decoder */
  private var decoderPosition: Int = _
  /** relative position of the current non-null value in the underlying decoder */
  private var nonNullPosition: Int = _
  private var notNull: Boolean = _

  private def initialize(columnBytes: AnyRef, cursor: Long): Long = {
    // read the positions (skip the number of base rows)
    val numPositions = ColumnEncoding.readInt(columnBytes, cursor + 4)

    // initialize the start and end of mutated positions
    positionCursor = cursor + 8
    // relative positions are one behind so point to current on increment
    decoderPosition = -1
    nonNullPosition = -1

    positionEndCursor = positionCursor + (numPositions << 2)
    // round to nearest word to get data start position
    ((positionEndCursor + 7) >> 3) << 3
  }

  /**
   * Reads the current updated position in the column batch.
   */
  private[encoding] def readUpdatedPosition(): Int = {
    val cursor = positionCursor
    if (cursor < positionEndCursor) {
      ColumnEncoding.readInt(deltaBytes, cursor)
    } else {
      // convention used by ColumnDeltaDecoder to denote the end
      // which is greater than everything so will never get selected
      Int.MaxValue
    }
  }

  /**
   * Move the cursor to enable reading next updated position.
   */
  private[encoding] def moveUpdatePositionCursor(): Unit = {
    positionCursor += 4
    decoderPosition += 1
    notNull = !realDecoder.isNullAt(deltaBytes, decoderPosition)
    if (notNull) nonNullPosition += 1
  }

  @inline def readNotNull: Boolean = notNull

  @inline def readBoolean: Boolean =
    realDecoder.readBoolean(deltaBytes, nonNullPosition)

  @inline def readByte: Byte =
    realDecoder.readByte(deltaBytes, nonNullPosition)

  @inline def readShort: Short =
    realDecoder.readShort(deltaBytes, nonNullPosition)

  @inline def readInt: Int =
    realDecoder.readInt(deltaBytes, nonNullPosition)

  @inline def readLong: Long =
    realDecoder.readLong(deltaBytes, nonNullPosition)

  @inline def readFloat: Float =
    realDecoder.readFloat(deltaBytes, nonNullPosition)

  @inline def readDouble: Double =
    realDecoder.readDouble(deltaBytes, nonNullPosition)

  @inline def readDate: Int =
    realDecoder.readDate(deltaBytes, nonNullPosition)

  @inline def readTimestamp: Long =
    realDecoder.readTimestamp(deltaBytes, nonNullPosition)

  @inline def readLongDecimal(precision: Int, scale: Int): Decimal =
    realDecoder.readLongDecimal(deltaBytes, precision, scale, nonNullPosition)

  @inline def readDecimal(precision: Int, scale: Int): Decimal =
    realDecoder.readDecimal(deltaBytes, precision, scale, nonNullPosition)

  @inline def readUTF8String: UTF8String =
    realDecoder.readUTF8String(deltaBytes, nonNullPosition)

  @inline def readInterval: CalendarInterval =
    realDecoder.readInterval(deltaBytes, nonNullPosition)

  @inline def readBinary: Array[Byte] =
    realDecoder.readBinary(deltaBytes, nonNullPosition)

  @inline def readArray: ArrayData =
    realDecoder.readArray(deltaBytes, nonNullPosition)

  @inline def readMap: MapData =
    realDecoder.readMap(deltaBytes, nonNullPosition)

  @inline def readStruct(numFields: Int): InternalRow =
    realDecoder.readStruct(deltaBytes, numFields, nonNullPosition)

  @inline def close(): Unit = realDecoder.close()
}