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

import java.math.{BigDecimal, BigInteger}

import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeArrayData, UnsafeMapData, UnsafeRow}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

final class Uncompressed extends UncompressedBase with NotNullColumn

final class UncompressedNullable extends UncompressedBase with NullableColumn

abstract class UncompressedBase extends ColumnEncoding {

  import ColumnEncoding.littleEndian

  protected final var baseCursor = 0L

  def typeId: Int = 0

  def supports(dataType: DataType): Boolean = true

  override final def initializeDecoding(columnBytes: AnyRef,
      field: Attribute): Long = {
    val cursor = initializeNulls(columnBytes, field)
    // typeId takes 4 bytes for non-complex types else 0;
    // adjust cursor for the first next call to avoid extra checks in next
    // accounting for typeId size
    Utils.getSQLDataType(field.dataType) match {
      case BooleanType | ByteType => cursor + 3 // (typeIdSize - 1)
      case ShortType => cursor + 2 // (typeIdSize - 2)
      case IntegerType | FloatType | DateType => cursor // (typeIdSize - 4)
      case LongType | DoubleType | TimestampType =>
        cursor - 4 // (typeIdSize - 8)
      case CalendarIntervalType => cursor - 8 // (typeIdSize - 12)
      case StringType =>
        // this will check for zero value of cursor and adjust in first next
        baseCursor = cursor + 4 // typeIdSize
        0L
      case d: DecimalType if d.precision <= Decimal.MAX_LONG_DIGITS =>
        cursor - 4 // (typeIdSize - 8)
      case BinaryType | _: DecimalType |
           _: ArrayType | _: MapType | _: StructType =>
        // these will check for zero value of cursor and adjust in first next;
        // no typeId for complex types
        baseCursor = cursor
        0L
      case NullType => 0L // no role of cursor for NullType
      case t => throw new UnsupportedOperationException(s"Unsupported type $t")
    }
  }

  override def nextBoolean(columnBytes: AnyRef, cursor: Long): Long =
    cursor + 1

  override def readBoolean(columnBytes: AnyRef, cursor: Long): Boolean =
    Platform.getByte(columnBytes, cursor) == 1

  override def nextByte(columnBytes: AnyRef, cursor: Long): Long =
    cursor + 1

  override def readByte(columnBytes: AnyRef, cursor: Long): Byte =
    Platform.getByte(columnBytes, cursor)

  override def nextShort(columnBytes: AnyRef, cursor: Long): Long =
    cursor + 2

  override def readShort(columnBytes: AnyRef, cursor: Long): Short =
    if (littleEndian) Platform.getShort(columnBytes, cursor)
    else java.lang.Short.reverseBytes(Platform.getShort(columnBytes, cursor))

  override def nextInt(columnBytes: AnyRef, cursor: Long): Long =
    cursor + 4

  override def readInt(columnBytes: AnyRef, cursor: Long): Int =
    if (littleEndian) Platform.getInt(columnBytes, cursor)
    else java.lang.Integer.reverseBytes(Platform.getInt(columnBytes, cursor))

  override def nextLong(columnBytes: AnyRef, cursor: Long): Long =
    cursor + 8

  override def readLong(columnBytes: AnyRef, cursor: Long): Long = {
    val result = if (littleEndian) Platform.getLong(columnBytes, cursor)
    else java.lang.Long.reverseBytes(Platform.getLong(columnBytes, cursor))
    result
  }

  override def nextFloat(columnBytes: AnyRef, cursor: Long): Long =
    cursor + 4

  override def readFloat(columnBytes: AnyRef, cursor: Long): Float =
    if (littleEndian) Platform.getFloat(columnBytes, cursor)
    else java.lang.Float.intBitsToFloat(java.lang.Integer.reverseBytes(
      Platform.getInt(columnBytes, cursor)))

  override def nextDouble(columnBytes: AnyRef, cursor: Long): Long =
    cursor + 8

  override def readDouble(columnBytes: AnyRef, cursor: Long): Double =
    if (littleEndian) Platform.getDouble(columnBytes, cursor)
    else java.lang.Double.longBitsToDouble(java.lang.Long.reverseBytes(
      Platform.getLong(columnBytes, cursor)))

  override def nextLongDecimal(columnBytes: AnyRef, cursor: Long): Long =
    cursor + 8

  override def readLongDecimal(columnBytes: AnyRef, precision: Int,
      scale: Int, cursor: Long): Decimal =
    Decimal.createUnsafe(ColumnEncoding.readLong(columnBytes, cursor),
      precision, scale)

  override def nextDecimal(columnBytes: AnyRef, cursor: Long): Long = {
    // cursor == 0 indicates first call so don't increment cursor
    if (cursor != 0) {
      val size = ColumnEncoding.readInt(columnBytes, cursor)
      cursor + 4 + size
    } else {
      baseCursor
    }
  }

  override def readDecimal(columnBytes: AnyRef, precision: Int,
      scale: Int, cursor: Long): Decimal = {
    Decimal.apply(new BigDecimal(new BigInteger(readBinary(columnBytes,
      cursor)), scale), precision, scale)
  }

  override def nextUTF8String(columnBytes: AnyRef, cursor: Long): Long = {
    // cursor == 0 indicates first call so don't increment cursor
    if (cursor != 0) {
      val size = ColumnEncoding.readInt(columnBytes, cursor)
      cursor + 4 + size
    } else {
      baseCursor
    }
  }

  override def readUTF8String(columnBytes: AnyRef, cursor: Long): UTF8String =
    ColumnEncoding.readUTF8String(columnBytes, cursor)

  override def nextInterval(columnBytes: AnyRef, cursor: Long): Long =
    cursor + 12

  override def readInterval(columnBytes: AnyRef,
      cursor: Long): CalendarInterval = {
    val months = ColumnEncoding.readInt(columnBytes, cursor)
    val micros = ColumnEncoding.readLong(columnBytes, cursor + 4)
    new CalendarInterval(months, micros)
  }

  override def nextBinary(columnBytes: AnyRef, cursor: Long): Long = {
    // cursor == 0 indicates first call so don't increment cursor
    if (cursor != 0) {
      val size = ColumnEncoding.readInt(columnBytes, cursor)
      cursor + 4 + size
    } else {
      baseCursor
    }
  }

  override def readBinary(columnBytes: AnyRef, cursor: Long): Array[Byte] = {
    val size = ColumnEncoding.readInt(columnBytes, cursor)
    val b = new Array[Byte](size)
    Platform.copyMemory(columnBytes, cursor + 4, b,
      Platform.BYTE_ARRAY_OFFSET, size)
    b
  }

  override def readArray(columnBytes: AnyRef, cursor: Long): UnsafeArrayData = {
    val result = new UnsafeArrayData
    val size = ColumnEncoding.readInt(columnBytes, cursor)
    result.pointTo(columnBytes, cursor + 4, size)
    result
  }

  override def readMap(columnBytes: AnyRef, cursor: Long): UnsafeMapData = {
    val result = new UnsafeMapData
    val size = ColumnEncoding.readInt(columnBytes, cursor)
    result.pointTo(columnBytes, cursor + 4, size)
    result
  }

  override def readStruct(columnBytes: AnyRef, numFields: Int,
      cursor: Long): UnsafeRow = {
    val result = new UnsafeRow(numFields)
    val size = ColumnEncoding.readInt(columnBytes, cursor)
    result.pointTo(columnBytes, cursor + 4, size)
    result
  }

  /*
  override def writeBoolean(columnBytes: AnyRef, value: Boolean): Unit = {
    Platform.putByte(columnBytes, cursor, if (value) 1 else 0)
    cursor += 1
  }

  override def writeByte(columnBytes: AnyRef, value: Byte): Unit = {
    Platform.putByte(columnBytes, cursor, value)
    cursor += 1
  }

  override def writeShort(columnBytes: AnyRef, value: Short): Unit = {
    if (littleEndian) Platform.putShort(columnBytes, cursor, value)
    else Platform.putShort(columnBytes, cursor, java.lang.Short.reverseBytes(value))
    cursor += 2
  }

  override def writeInt(columnBytes: AnyRef, value: Int): Unit = {
    if (littleEndian) Platform.putInt(columnBytes, cursor, value)
    else Platform.putInt(columnBytes, cursor, java.lang.Integer.reverseBytes(value))
    cursor += 4
  }

  override def writeLong(columnBytes: AnyRef, value: Long): Unit = {
    if (littleEndian) Platform.putLong(columnBytes, cursor, value)
    else Platform.putLong(columnBytes, cursor, java.lang.Long.reverseBytes(value))
    cursor += 8
  }

  override def writeDecimal(columnBytes: AnyRef, value: Decimal,
      precision: Int): Unit = {
    if (precision <= Decimal.MAX_LONG_DIGITS) {
      writeLong(columnBytes, value.toUnscaledLong)
    } else {
      val b = value.toJavaBigDecimal.unscaledValue.toByteArray
      writeBinary(columnBytes, b)
    }
  }

  override def writeFloat(columnBytes: AnyRef, value: Float): Unit = {
    if (littleEndian) Platform.putFloat(columnBytes, cursor, value)
    else Platform.putInt(columnBytes, cursor, java.lang.Integer.reverseBytes(
      java.lang.Float.floatToIntBits(value)))
    cursor += 4
  }

  override def writeDouble(columnBytes: AnyRef, value: Double): Unit = {
    if (littleEndian) Platform.putDouble(columnBytes, cursor, value)
    else Platform.putLong(columnBytes, cursor, java.lang.Long.reverseBytes(
      java.lang.Double.doubleToLongBits(value)))
    cursor += 8
  }

  override def writeUTF8String(columnBytes: AnyRef, value: UTF8String): Unit = {
    val size = value.numBytes
    writeInt(columnBytes, size)
    Platform.copyMemory(value.getBaseObject, value.getBaseOffset, columnBytes,
      cursor, size)
    cursor += size
  }

  override def writeBinary(columnBytes: AnyRef, value: Array[Byte]): Unit = {
    val size = value.length
    writeInt(columnBytes, size)
    Platform.copyMemory(value, Platform.BYTE_ARRAY_OFFSET, columnBytes, cursor, size)
    cursor += size
  }

  override def writeInterval(columnBytes: AnyRef,
      value: CalendarInterval): Unit = {
    writeInt(columnBytes, value.months)
    writeLong(columnBytes, value.microseconds)
  }

  override def writeArray(columnBytes: AnyRef, value: UnsafeArrayData): Unit = {
    val size = value.getSizeInBytes
    writeInt(columnBytes, size)
    Platform.copyMemory(value.getBaseObject, value.getBaseOffset, columnBytes,
      cursor, size)
    cursor += size
  }

  override def writeMap(columnBytes: AnyRef, value: UnsafeMapData): Unit = {
    val size = value.getSizeInBytes
    writeInt(columnBytes, size)
    Platform.copyMemory(value.getBaseObject, value.getBaseOffset, columnBytes,
      cursor, size)
    cursor += size
  }

  override def writeStruct(columnBytes: AnyRef, value: UnsafeRow): Unit = {
    val size = value.getSizeInBytes
    writeInt(columnBytes, size)
    Platform.copyMemory(value.getBaseObject, value.getBaseOffset, columnBytes,
      cursor, size)
    cursor += size
  }
  */
}
