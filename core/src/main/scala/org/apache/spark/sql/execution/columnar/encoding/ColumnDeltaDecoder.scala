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
  private var nextPosition: Int = _
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
    deltaCursor = ((offset + (numPositions << 2) + 63) >> 6) << 6

    nextPosition = ColumnEncoding.readInt(columnBytes, offset)
    offset += 4

    if (hasOwnCursor) {
      realDecoder.setRealCursor(offset)
      positionCursor = initialCursor
    } else {
      positionCursor = offset
    }
    nextPosition
  }

  private[encoding] def moveNextPosition(): Int = {
    nextPosition = ColumnEncoding.readInt(deltaBytes, positionCursor)
    positionCursor += 4
    positionOrdinal += 1
    nextPosition
  }

  override protected def hasNulls: Boolean =
    throw new UnsupportedOperationException(s"hasNulls for $toString")

  override def notNull(columnBytes: AnyRef, ordinal: Int): Int =
    throw new UnsupportedOperationException(s"notNull for $toString")

  def notNull: Int = realDecoder.notNull(deltaBytes, positionOrdinal)

  def readBoolean: Boolean = {
    deltaCursor = realDecoder.nextBoolean(deltaBytes, deltaCursor)
    realDecoder.readBoolean(deltaBytes, deltaCursor)
  }

  def readByte: Byte = {
    deltaCursor = realDecoder.nextByte(deltaBytes, deltaCursor)
    realDecoder.readByte(deltaBytes, deltaCursor)
  }

  def readShort: Short = {
    deltaCursor = realDecoder.nextShort(deltaBytes, deltaCursor)
    realDecoder.readShort(deltaBytes, deltaCursor)
  }

  def readInt: Int = {
    deltaCursor = realDecoder.nextInt(deltaBytes, deltaCursor)
    realDecoder.readInt(deltaBytes, deltaCursor)
  }

  def readLong: Long = {
    deltaCursor = realDecoder.nextLong(deltaBytes, deltaCursor)
    realDecoder.readLong(deltaBytes, deltaCursor)
  }

  def readFloat: Float = {
    deltaCursor = realDecoder.nextFloat(deltaBytes, deltaCursor)
    realDecoder.readFloat(deltaBytes, deltaCursor)
  }

  def readDouble: Double = {
    deltaCursor = realDecoder.nextDouble(deltaBytes, deltaCursor)
    realDecoder.readDouble(deltaBytes, deltaCursor)
  }

  def readLongDecimal(precision: Int, scale: Int): Decimal = {
    deltaCursor = realDecoder.nextLongDecimal(deltaBytes, deltaCursor)
    realDecoder.readLongDecimal(deltaBytes, precision, scale, deltaCursor)
  }

  def readDecimal(precision: Int, scale: Int): Decimal = {
    deltaCursor = realDecoder.nextDecimal(deltaBytes, deltaCursor)
    realDecoder.readDecimal(deltaBytes, precision, scale, deltaCursor)
  }

  def readUTF8String: UTF8String = {
    deltaCursor = realDecoder.nextUTF8String(deltaBytes, deltaCursor)
    realDecoder.readUTF8String(deltaBytes, deltaCursor)
  }

  def readInterval: CalendarInterval = {
    deltaCursor = realDecoder.nextInterval(deltaBytes, deltaCursor)
    realDecoder.readInterval(deltaBytes, deltaCursor)
  }

  def readBinary: Array[Byte] = {
    deltaCursor = realDecoder.nextBinary(deltaBytes, deltaCursor)
    realDecoder.readBinary(deltaBytes, deltaCursor)
  }

  def readArray: ArrayData = {
    deltaCursor = realDecoder.nextArray(deltaBytes, deltaCursor)
    realDecoder.readArray(deltaBytes, deltaCursor)
  }

  def readMap: MapData = {
    deltaCursor = realDecoder.nextMap(deltaBytes, deltaCursor)
    realDecoder.readMap(deltaBytes, deltaCursor)
  }

  def readStruct(numFields: Int): InternalRow = {
    deltaCursor = realDecoder.nextStruct(deltaBytes, deltaCursor)
    realDecoder.readStruct(deltaBytes, numFields, deltaCursor)
  }
}
