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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, Decimal, StructField}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Internal class to decode values from a single delta as obtained from
 * [[ColumnDeltaEncoder]]. Should not be used directly rather the combined
 * decoder [[MutatedColumnDecoder]] should be the one used.
 */
private[encoding] final class ColumnDeltaDecoder(realDecoder: ColumnDecoder)
    extends ColumnDecoder {

  private var deltaBytes: AnyRef = _
  private var deltaCursor: Long = _

  private var positionCursor: Long = _
  private var positionEndCursor: Long = _
  private var positionOrdinal: Int = _

  override def typeId: Int = realDecoder.typeId

  override def supports(dataType: DataType): Boolean = realDecoder.supports(dataType)

  override protected[sql] def initializeNulls(columnBytes: AnyRef, cursor: Long,
      field: StructField): Long = realDecoder.initializeNulls(columnBytes, cursor, field)

  override protected[sql] def initializeCursor(columnBytes: AnyRef, cursor: Long,
      field: StructField): Long = {
    val initialCursor = realDecoder.initializeCursor(columnBytes, cursor, field)
    var offset = realDecoder.realCursor(initialCursor)
    val hasOwnCursor = initialCursor != offset
    // read the positions
    val numPositions = ColumnEncoding.readInt(columnBytes, offset)
    offset += 4

    // find the start of data after padding
    deltaBytes = columnBytes

    positionEndCursor = offset + (numPositions << 2)
    val nextPosition = ColumnEncoding.readInt(columnBytes, offset)
    offset += 4
    positionCursor = offset

    // round to nearest word to get data start position
    offset = ((positionEndCursor + 63) >> 6) << 6
    if (hasOwnCursor) {
      realDecoder.setRealCursor(offset)
      deltaCursor = initialCursor
    } else {
      deltaCursor = offset
    }
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

  override protected[sql] def hasNulls: Boolean = realDecoder.hasNulls

  override def isNull(columnBytes: AnyRef, ordinal: Int, mutated: Int): Int =
    throw new UnsupportedOperationException(s"isNull for $toString")

  def isNull: Int = realDecoder.isNull(deltaBytes, positionOrdinal, mutated = 0)

  def nextBoolean(): Unit = {
    deltaCursor = realDecoder.nextBoolean(deltaBytes, deltaCursor, mutated = 0)
  }

  def readBoolean: Boolean = realDecoder.readBoolean(deltaBytes, deltaCursor, mutated = 0)

  def nextByte(): Unit = {
    deltaCursor = realDecoder.nextByte(deltaBytes, deltaCursor, mutated = 0)
  }

  def readByte: Byte = realDecoder.readByte(deltaBytes, deltaCursor, mutated = 0)

  def nextShort(): Unit = {
    deltaCursor = realDecoder.nextShort(deltaBytes, deltaCursor, mutated = 0)
  }

  def readShort: Short = realDecoder.readShort(deltaBytes, deltaCursor, mutated = 0)

  def nextInt(): Unit = {
    deltaCursor = realDecoder.nextInt(deltaBytes, deltaCursor, mutated = 0)
  }

  def readInt: Int = realDecoder.readInt(deltaBytes, deltaCursor, mutated = 0)

  def nextLong(): Unit = {
    deltaCursor = realDecoder.nextLong(deltaBytes, deltaCursor, mutated = 0)
  }

  def readLong: Long = realDecoder.readLong(deltaBytes, deltaCursor, mutated = 0)

  def nextFloat(): Unit = {
    deltaCursor = realDecoder.nextFloat(deltaBytes, deltaCursor, mutated = 0)
  }

  def readFloat: Float = realDecoder.readFloat(deltaBytes, deltaCursor, mutated = 0)

  def nextDouble(): Unit = {
    deltaCursor = realDecoder.nextDouble(deltaBytes, deltaCursor, mutated = 0)
  }

  def readDouble: Double = realDecoder.readDouble(deltaBytes, deltaCursor, mutated = 0)

  def nextLongDecimal(): Unit = {
    deltaCursor = realDecoder.nextLongDecimal(deltaBytes, deltaCursor, mutated = 0)
  }

  def readLongDecimal(precision: Int, scale: Int): Decimal = {
    realDecoder.readLongDecimal(deltaBytes, precision, scale, deltaCursor, mutated = 0)
  }

  def nextDecimal(): Unit = {
    deltaCursor = realDecoder.nextDecimal(deltaBytes, deltaCursor, mutated = 0)
  }

  def readDecimal(precision: Int, scale: Int): Decimal = {
    realDecoder.readDecimal(deltaBytes, precision, scale, deltaCursor, mutated = 0)
  }

  def nextUTF8String(): Unit = {
    deltaCursor = realDecoder.nextUTF8String(deltaBytes, deltaCursor, mutated = 0)
  }

  def readUTF8String: UTF8String =
    realDecoder.readUTF8String(deltaBytes, deltaCursor, mutated = 0)

  def nextInterval(): Unit = {
    deltaCursor = realDecoder.nextInterval(deltaBytes, deltaCursor, mutated = 0)
  }

  def readInterval: CalendarInterval =
    realDecoder.readInterval(deltaBytes, deltaCursor, mutated = 0)

  def nextBinary(): Unit = {
    deltaCursor = realDecoder.nextBinary(deltaBytes, deltaCursor, mutated = 0)
  }

  def readBinary: Array[Byte] = realDecoder.readBinary(deltaBytes, deltaCursor, mutated = 0)

  def nextArray(): Unit = {
    deltaCursor = realDecoder.nextArray(deltaBytes, deltaCursor, mutated = 0)
  }

  def readArray: ArrayData = realDecoder.readArray(deltaBytes, deltaCursor, mutated = 0)

  def nextMap(): Unit = {
    deltaCursor = realDecoder.nextMap(deltaBytes, deltaCursor, mutated = 0)
  }

  def readMap: MapData = realDecoder.readMap(deltaBytes, deltaCursor, mutated = 0)

  def nextStruct(): Unit = {
    deltaCursor = realDecoder.nextStruct(deltaBytes, deltaCursor, mutated = 0)
  }

  def readStruct(numFields: Int): InternalRow = {
    realDecoder.readStruct(deltaBytes, numFields, deltaCursor, mutated = 0)
  }
}
