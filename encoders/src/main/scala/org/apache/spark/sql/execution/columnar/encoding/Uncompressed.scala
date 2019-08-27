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

import java.math.{BigDecimal, BigInteger}

import com.pivotal.gemfirexd.internal.iapi.util.ReuseFactory

import org.apache.spark.sql.catalyst.util.{SerializedArray, SerializedMap, SerializedRow}
import org.apache.spark.sql.execution.columnar.encoding.ColumnEncoding.littleEndian
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

trait Uncompressed extends ColumnEncoding {

  final def typeId: Int = 0

  final def supports(dataType: DataType): Boolean = true
}

final class UncompressedDecoder(columnBytes: AnyRef, startCursor: Long,
    field: StructField, initDelta: (AnyRef, Long) => Long = ColumnEncoding.identityLong)
    extends UncompressedDecoderBase(columnBytes, startCursor, field,
      initDelta) with NotNullDecoder

final class UncompressedDecoderNullable(columnBytes: AnyRef, startCursor: Long,
    field: StructField, initDelta: (AnyRef, Long) => Long = ColumnEncoding.identityLong)
    extends UncompressedDecoderBase(columnBytes, startCursor, field,
      initDelta) with NullableDecoder

final class UncompressedEncoder
    extends NotNullEncoder with UncompressedEncoderBase

final class UncompressedEncoderNullable
    extends NullableEncoder with UncompressedEncoderBase

abstract class UncompressedDecoderBase(columnDataRef: AnyRef, startCursor: Long,
    field: StructField, initDelta: (AnyRef, Long) => Long)
    extends ColumnDecoder(columnDataRef, startCursor, field, initDelta) with Uncompressed {

  /**
   * The last value for "nonNullPosition" for variable width columns.
   * Starts at -1 so that first time increment will set it to 0.
   */
  private var lastNonNullPosition: Int = -1

  /**
   * The last size read for a variable width value. Used to
   * rewind back to previous position if required.
   */
  private var lastSize: Int = _

  override protected[sql] def initializeCursor(columnBytes: AnyRef, cursor: Long,
      dataType: DataType): Long = {
    currentCursor = cursor
    cursor
  }

  override def readBoolean(columnBytes: AnyRef, nonNullPosition: Int): Boolean =
    Platform.getByte(columnBytes, baseCursor + nonNullPosition) == 1

  override def readByte(columnBytes: AnyRef, nonNullPosition: Int): Byte =
    Platform.getByte(columnBytes, baseCursor + nonNullPosition)

  override def readShort(columnBytes: AnyRef, nonNullPosition: Int): Short =
    ColumnEncoding.readShort(columnBytes, baseCursor + (nonNullPosition << 1))

  override def readInt(columnBytes: AnyRef, nonNullPosition: Int): Int =
    ColumnEncoding.readInt(columnBytes, baseCursor + (nonNullPosition << 2))

  override def readLong(columnBytes: AnyRef, nonNullPosition: Int): Long =
    ColumnEncoding.readLong(columnBytes, baseCursor + (nonNullPosition << 3))

  override def readFloat(columnBytes: AnyRef, nonNullPosition: Int): Float =
    ColumnEncoding.readFloat(columnBytes, baseCursor + (nonNullPosition << 2))

  override def readDouble(columnBytes: AnyRef, nonNullPosition: Int): Double =
    ColumnEncoding.readDouble(columnBytes, baseCursor + (nonNullPosition << 3))

  override def readLongDecimal(columnBytes: AnyRef, precision: Int,
      scale: Int, nonNullPosition: Int): Decimal =
    Decimal.createUnsafe(ColumnEncoding.readLong(columnBytes,
      baseCursor + (nonNullPosition << 3)), precision, scale)

  private def setCursorAtPosition(columnBytes: AnyRef, nonNullPosition: Int,
      sizeWidth: Int, expectedPosition: Int): Unit = {
    if (nonNullPosition <= expectedPosition) {
      throw new IllegalStateException(s"Decoder map cursor cannot move back: " +
          s"lastPosition=${expectedPosition - 1} newPosition=$nonNullPosition")
    }
    var lastPosition = expectedPosition
    var cursor = currentCursor
    do {
      // first read is of keyArraySize and second of valueArraySize
      cursor += sizeWidth + ColumnEncoding.readInt(columnBytes, cursor)
      lastPosition += 1
    } while (nonNullPosition != lastPosition)
    currentCursor = cursor
  }

  private def setCursorForVariableWidth(columnBytes: AnyRef, nonNullPosition: Int,
      sizeWidth: Int = 4): Unit = {
    // check sequential calls else skip as much required
    if (nonNullPosition != lastNonNullPosition + 1) {
      // if same position accessed again then rewind to previous position (SNAP-2118);
      // this can happen for aggregation over inner join returning multiple rows for a
      // single streamed row (so then same value for aggregation is read multiple times)
      if (nonNullPosition == lastNonNullPosition) {
        currentCursor -= lastSize
        return
      } else {
        setCursorAtPosition(columnBytes, nonNullPosition, sizeWidth, lastNonNullPosition + 1)
      }
    }
    lastNonNullPosition = nonNullPosition
    lastSize = ColumnEncoding.readInt(columnBytes, currentCursor)
    currentCursor += sizeWidth
  }

  override def readDecimal(columnBytes: AnyRef, precision: Int,
      scale: Int, nonNullPosition: Int): Decimal = {
    Decimal.apply(new BigDecimal(new BigInteger(readBinary(columnBytes,
      nonNullPosition)), scale), precision, scale)
  }

  override def readUTF8String(columnBytes: AnyRef, nonNullPosition: Int): UTF8String = {
    setCursorForVariableWidth(columnBytes, nonNullPosition)
    val s = UTF8String.fromAddress(columnBytes, currentCursor, lastSize)
    currentCursor += lastSize
    s
  }

  override def readInterval(columnBytes: AnyRef, nonNullPosition: Int): CalendarInterval = {
    val cursor = baseCursor + (nonNullPosition * 12)
    val months = ColumnEncoding.readInt(columnBytes, cursor)
    val micros = ColumnEncoding.readLong(columnBytes, cursor + 4)
    new CalendarInterval(months, micros)
  }

  override def readBinary(columnBytes: AnyRef, nonNullPosition: Int): Array[Byte] = {
    setCursorForVariableWidth(columnBytes, nonNullPosition)
    val b = new Array[Byte](lastSize)
    Platform.copyMemory(columnBytes, currentCursor, b, Platform.BYTE_ARRAY_OFFSET, lastSize)
    currentCursor += lastSize
    b
  }

  override def readArray(columnBytes: AnyRef, nonNullPosition: Int): SerializedArray = {
    // size includes the 4 bytes for the size width
    setCursorForVariableWidth(columnBytes, nonNullPosition, sizeWidth = 0)
    // 4 bytes for size and then 4 bytes for number of elements
    val result = new SerializedArray(8)
    result.pointTo(columnBytes, currentCursor, lastSize)
    currentCursor += lastSize
    result
  }

  private def setCursorAtPositionForMap(columnBytes: AnyRef, nonNullPosition: Int,
      expectedPosition: Int): Unit = {
    if (nonNullPosition <= expectedPosition) {
      throw new IllegalStateException(s"Decoder map cursor cannot move back: " +
          s"lastPosition=${expectedPosition - 1} newPosition=$nonNullPosition")
    }
    var lastPosition = expectedPosition
    var cursor = currentCursor
    do {
      // first read is of keyArraySize and second of valueArraySize
      cursor += ColumnEncoding.readInt(columnBytes, cursor)
      cursor += ColumnEncoding.readInt(columnBytes, cursor)
      lastPosition += 1
    } while (nonNullPosition != lastPosition)
    currentCursor = cursor
  }

  private def setCursorForMap(columnBytes: AnyRef, nonNullPosition: Int): Unit = {
    // check sequential calls else skip as much required
    if (nonNullPosition != lastNonNullPosition + 1) {
      // if same position accessed again then rewind to previous position (SNAP-2118)
      if (nonNullPosition == lastNonNullPosition) {
        currentCursor -= lastSize
        return
      } else {
        setCursorAtPositionForMap(columnBytes, nonNullPosition, lastNonNullPosition + 1)
      }
    }
    lastNonNullPosition = nonNullPosition
  }

  override def readMap(columnBytes: AnyRef, nonNullPosition: Int): SerializedMap = {
    setCursorForMap(columnBytes, nonNullPosition)
    val result = new SerializedMap
    result.pointTo(columnBytes, currentCursor)
    // first read is of keyArraySize and second of valueArraySize
    lastSize = ColumnEncoding.readInt(columnBytes, currentCursor)
    lastSize += ColumnEncoding.readInt(columnBytes, currentCursor + lastSize)
    currentCursor += lastSize
    result
  }

  override def readStruct(columnBytes: AnyRef, numFields: Int,
      nonNullPosition: Int): SerializedRow = {
    setCursorForVariableWidth(columnBytes, nonNullPosition, sizeWidth = 0)
    // creates a SerializedRow with skipBytes = 4 and does not change the
    // cursor itself to get best 8-byte word alignment (the 4 bytes are
    //   subsumed in the null bit mask at the start)
    val result = new SerializedRow(4, numFields)
    result.pointTo(columnBytes, currentCursor, lastSize)
    currentCursor += lastSize
    result
  }
}

trait UncompressedEncoderBase extends ColumnEncoder with Uncompressed {

  override def writeBoolean(cursor: Long, value: Boolean): Long = {
    var position = cursor
    val b: Byte = if (value) 1 else 0
    if (position + 1 > columnEndPosition) {
      position = expand(position, 1)
    }
    Platform.putByte(columnBytes, position, b)
    updateLongStats(b)
    position + 1
  }

  override def writeByte(cursor: Long, value: Byte): Long = {
    var position = cursor
    if (position + 1 > columnEndPosition) {
      position = expand(position, 1)
    }
    Platform.putByte(columnBytes, position, value)
    updateLongStats(value)
    position + 1
  }

  override def writeShort(cursor: Long, value: Short): Long = {
    var position = cursor
    if (position + 2 > columnEndPosition) {
      position = expand(position, 2)
    }
    ColumnEncoding.writeShort(columnBytes, position, value)
    updateLongStats(value)
    position + 2
  }

  override def writeInt(cursor: Long, value: Int): Long = {
    var position = cursor
    if (position + 4 > columnEndPosition) {
      position = expand(position, 4)
    }
    ColumnEncoding.writeInt(columnBytes, position, value)
    updateLongStats(value)
    position + 4
  }

  override def writeLong(cursor: Long, value: Long): Long = {
    var position = cursor
    if (position + 8 > columnEndPosition) {
      position = expand(position, 8)
    }
    ColumnEncoding.writeLong(columnBytes, position, value)
    updateLongStats(value)
    position + 8
  }

  override def writeFloat(cursor: Long, value: Float): Long = {
    var position = cursor
    if (position + 4 > columnEndPosition) {
      position = expand(position, 4)
    }
    if (java.lang.Float.isNaN(value)) {
      if (littleEndian) Platform.putFloat(columnBytes, position, Float.NaN)
      else Platform.putInt(columnBytes, position,
        java.lang.Integer.reverseBytes(java.lang.Float.floatToIntBits(Float.NaN)))
    } else {
      if (littleEndian) Platform.putFloat(columnBytes, position, value)
      else Platform.putInt(columnBytes, position,
        java.lang.Integer.reverseBytes(java.lang.Float.floatToIntBits(value)))
      updateDoubleStats(value.toDouble)
    }
    position + 4
  }

  override def writeDouble(cursor: Long, value: Double): Long = {
    var position = cursor
    if (position + 8 > columnEndPosition) {
      position = expand(position, 8)
    }
    if (java.lang.Double.isNaN(value)) {
      if (littleEndian) Platform.putDouble(columnBytes, position, Double.NaN)
      else Platform.putLong(columnBytes, position,
        java.lang.Long.reverseBytes(java.lang.Double.doubleToLongBits(Double.NaN)))
    } else {
      if (littleEndian) Platform.putDouble(columnBytes, position, value)
      else Platform.putLong(columnBytes, position,
        java.lang.Long.reverseBytes(java.lang.Double.doubleToLongBits(value)))
      updateDoubleStats(value)
    }
    position + 8
  }

  override def writeLongDecimal(cursor: Long, value: Decimal,
      position: Int, precision: Int, scale: Int): Long = {
    if ((value eq null) || ((value.precision != precision ||
        value.scale != scale) && !value.changePrecision(precision, scale))) {
      if (isNullable) {
        writeIsNull(position)
        cursor
      }
      else writeLong(cursor, Decimal.ZERO.toUnscaledLong)
    } else {
      writeLong(cursor, value.toUnscaledLong)
    }
  }

  override def writeDecimal(cursor: Long, value: Decimal,
      position: Int, precision: Int, scale: Int): Long = {
    var decimal = value
    if ((value eq null) || ((value.precision != precision ||
        value.scale != scale) && !value.changePrecision(precision, scale))) {
      if (isNullable) {
        writeIsNull(position)
        return cursor
      } else {
        decimal = Decimal.ZERO
      }
    }
    val b = decimal.toJavaBigDecimal.unscaledValue.toByteArray
    updateDecimalStats(decimal)
    writeBinary(cursor, b)
  }

  override def writeInterval(cursor: Long, value: CalendarInterval): Long = {
    var months: Int = 0
    var microseconds: Long = 0L
    if (value ne null) {
      months = value.months
      microseconds = value.microseconds
    }
    val position = writeInt(cursor, months)
    writeLong(position, microseconds)
  }

  override def writeUTF8String(cursor: Long, value: UTF8String): Long = {
    var position = cursor
    val str = if (value ne null) value else UTF8String.EMPTY_UTF8
    val size = str.numBytes
    if (position + size + 4 > columnEndPosition) {
      position = expand(position, size + 4)
    }
    updateStringStats(str)
    ColumnEncoding.writeUTF8String(columnBytes, position,
      str.getBaseObject, str.getBaseOffset, size)
  }

  override def writeBinary(cursor: Long, value: Array[Byte]): Long = {
    var position = cursor
    val arr = if (value ne null) value else ReuseFactory.getZeroLenByteArray
    val size = arr.length
    if (position + size + 4 > columnEndPosition) {
      position = expand(position, size + 4)
    }
    val columnBytes = this.columnBytes
    ColumnEncoding.writeInt(columnBytes, position, size)
    position += 4
    Platform.copyMemory(arr, Platform.BYTE_ARRAY_OFFSET, columnBytes,
      position, size)
    position + size
  }

  override def writeBooleanUnchecked(cursor: Long, value: Boolean): Long = {
    val b: Byte = if (value) 1 else 0
    Platform.putByte(columnBytes, cursor, b)
    cursor + 1
  }

  override def writeByteUnchecked(cursor: Long, value: Byte): Long = {
    Platform.putByte(columnBytes, cursor, value)
    cursor + 1
  }

  override def writeShortUnchecked(cursor: Long, value: Short): Long = {
    ColumnEncoding.writeShort(columnBytes, cursor, value)
    cursor + 2
  }

  override def writeIntUnchecked(cursor: Long, value: Int): Long = {
    ColumnEncoding.writeInt(columnBytes, cursor, value)
    cursor + 4
  }

  override def writeLongUnchecked(cursor: Long, value: Long): Long = {
    ColumnEncoding.writeLong(columnBytes, cursor, value)
    cursor + 8
  }

  override def writeFloatUnchecked(cursor: Long, value: Float): Long = {
    if (java.lang.Float.isNaN(value)) {
      if (littleEndian) Platform.putFloat(columnBytes, cursor, Float.NaN)
      else Platform.putInt(columnBytes, cursor,
        java.lang.Integer.reverseBytes(java.lang.Float.floatToIntBits(Float.NaN)))
    } else {
      if (littleEndian) Platform.putFloat(columnBytes, cursor, value)
      else Platform.putInt(columnBytes, cursor,
        java.lang.Integer.reverseBytes(java.lang.Float.floatToIntBits(value)))
    }
    cursor + 4
  }

  override def writeDoubleUnchecked(cursor: Long, value: Double): Long = {
    if (java.lang.Double.isNaN(value)) {
      if (littleEndian) Platform.putDouble(columnBytes, cursor, Double.NaN)
      else Platform.putLong(columnBytes, cursor,
        java.lang.Long.reverseBytes(java.lang.Double.doubleToLongBits(Double.NaN)))
    } else {
      if (littleEndian) Platform.putDouble(columnBytes, cursor, value)
      else Platform.putLong(columnBytes, cursor,
        java.lang.Long.reverseBytes(java.lang.Double.doubleToLongBits(value)))
    }
    cursor + 8
  }

  override def writeUnsafeData(cursor: Long, baseObject: AnyRef,
      baseOffset: Long, numBytes: Int): Long = {
    var position = cursor
    if (position + numBytes > columnEndPosition) {
      position = expand(position, numBytes)
    }
    // assume size is already written as per skipBytes in SerializedRowData
    Platform.copyMemory(baseObject, baseOffset, columnBytes, position, numBytes)
    position + numBytes
  }
}
