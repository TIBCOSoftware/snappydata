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
 * decoder [[UpdatedColumnDecoder]] should be the one used.
 */
final class ColumnDeltaDecoder(buffer: ByteBuffer, field: StructField) {

  private val (realDecoder, deltaBytes) =
    ColumnEncoding.getColumnDecoderAndBuffer(buffer, field, initialize)

  private var nonNullOrdinal: Int = _
  private var numNulls: Int = _

  private var positionCursor: Long = _
  private var positionEndCursor: Long = _
  private var positionOrdinal: Int = _

  private def initialize(columnBytes: AnyRef, cursor: Long): Long = {
    // read the positions (skip the number of base rows)
    val numPositions = ColumnEncoding.readInt(columnBytes, cursor + 4)

    // initialize the start and end of mutated positions
    positionCursor = cursor + 8

    positionEndCursor = positionCursor + (numPositions << 2)
    // round to nearest word to get data start position
    ((positionEndCursor + 7) >> 3) << 3
  }

  private[encoding] def moveToNextPosition(): Int = {
    val cursor = positionCursor
    if (cursor < positionEndCursor) {
      positionCursor += 4
      ColumnEncoding.readInt(deltaBytes, cursor)
    } else {
      // convention used by ColumnDeltaDecoder to denote the end
      // which is greater than everything so will never get selected
      Int.MaxValue
    }
  }

  @inline def hasNulls: Boolean = realDecoder.hasNulls

  @inline def readNotNull: Boolean = {
    val n = realDecoder.numNulls(deltaBytes, positionOrdinal, numNulls)
    positionOrdinal += 1
    if (n >= 0) {
      numNulls = n
      true
    } else {
      numNulls = -n
      false
    }
  }

  @inline private[encoding] def nextNonNullOrdinal(): Unit = nonNullOrdinal += 1

  @inline def readBoolean: Boolean = {
    val ordinal = nonNullOrdinal
    nonNullOrdinal = ordinal + 1
    realDecoder.readBoolean(deltaBytes, ordinal)
  }

  @inline def readByte: Byte = {
    val ordinal = nonNullOrdinal
    nonNullOrdinal = ordinal + 1
    realDecoder.readByte(deltaBytes, ordinal)
  }

  @inline def readShort: Short = {
    val ordinal = nonNullOrdinal
    nonNullOrdinal = ordinal + 1
    realDecoder.readShort(deltaBytes, ordinal)
  }

  @inline def readInt: Int = {
    val ordinal = nonNullOrdinal
    nonNullOrdinal = ordinal + 1
    realDecoder.readInt(deltaBytes, ordinal)
  }

  @inline def readLong: Long = {
    val ordinal = nonNullOrdinal
    nonNullOrdinal = ordinal + 1
    realDecoder.readLong(deltaBytes, ordinal)
  }

  @inline def readFloat: Float = {
    val ordinal = nonNullOrdinal
    nonNullOrdinal = ordinal + 1
    realDecoder.readFloat(deltaBytes, ordinal)
  }

  @inline def readDouble: Double = {
    val ordinal = nonNullOrdinal
    nonNullOrdinal = ordinal + 1
    realDecoder.readDouble(deltaBytes, ordinal)
  }

  @inline def readDate: Int = {
    val ordinal = nonNullOrdinal
    nonNullOrdinal = ordinal + 1
    realDecoder.readDate(deltaBytes, ordinal)
  }

  @inline def readTimestamp: Long = {
    val ordinal = nonNullOrdinal
    nonNullOrdinal = ordinal + 1
    realDecoder.readTimestamp(deltaBytes, ordinal)
  }

  @inline def readLongDecimal(precision: Int, scale: Int): Decimal = {
    val ordinal = nonNullOrdinal
    nonNullOrdinal = ordinal + 1
    realDecoder.readLongDecimal(deltaBytes, precision, scale, ordinal)
  }

  @inline def readDecimal(precision: Int, scale: Int): Decimal = {
    val ordinal = nonNullOrdinal
    nonNullOrdinal = ordinal + 1
    realDecoder.readDecimal(deltaBytes, precision, scale, ordinal)
  }

  @inline def readUTF8String: UTF8String = {
    val ordinal = nonNullOrdinal
    nonNullOrdinal = ordinal + 1
    realDecoder.readUTF8String(deltaBytes, ordinal)
  }

  @inline def readInterval: CalendarInterval = {
    val ordinal = nonNullOrdinal
    nonNullOrdinal = ordinal + 1
    realDecoder.readInterval(deltaBytes, ordinal)
  }

  @inline def readBinary: Array[Byte] = {
    val ordinal = nonNullOrdinal
    nonNullOrdinal = ordinal + 1
    realDecoder.readBinary(deltaBytes, ordinal)
  }

  @inline def readArray: ArrayData = {
    val ordinal = nonNullOrdinal
    nonNullOrdinal = ordinal + 1
    realDecoder.readArray(deltaBytes, ordinal)
  }

  @inline def readMap: MapData = {
    val ordinal = nonNullOrdinal
    nonNullOrdinal = ordinal + 1
    realDecoder.readMap(deltaBytes, ordinal)
  }

  @inline def readStruct(numFields: Int): InternalRow = {
    val ordinal = nonNullOrdinal
    nonNullOrdinal = ordinal + 1
    realDecoder.readStruct(deltaBytes, numFields, ordinal)
  }
}
