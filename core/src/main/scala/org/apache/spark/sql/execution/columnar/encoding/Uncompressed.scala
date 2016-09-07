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

private[columnar] final class Uncompressed
    extends UncompressedBase with NotNullColumn {

  override def initializeDecoding(columnBytes: Array[Byte],
      field: Attribute): Unit = {
    super.initializeDecoding(columnBytes, field)
    initializeDecodingBase(field.dataType)
  }

  override def initializeEncoding(dataType: DataType,
      batchSize: Int): Array[Byte] =
    new Array[Byte](dataType.defaultSize * batchSize + 4 /* for typeId */)
}

private[columnar] final class UncompressedNullable
    extends UncompressedBase with NullableColumn {

  override def initializeDecoding(columnBytes: Array[Byte],
      field: Attribute): Unit = {
    super.initializeDecoding(columnBytes, field)
    initializeDecodingBase(field.dataType)
  }

  override def initializeEncoding(dataType: DataType,
      batchSize: Int): Array[Byte] = {
    // max size for nulls is with bitmap (batchSize/8 bytes)
    val nullsSize = batchSize >>> 3
    new Array[Byte](dataType.defaultSize * batchSize + nullsSize + 5)
  }
}

private[columnar] abstract class UncompressedBase extends ColumnEncoding {

  import ColumnEncoding.littleEndian

  def typeId: Int = 0

  def supports(dataType: DataType): Boolean = true

  protected final def initializeDecodingBase(dataType: DataType): Unit = {
    // adjust cursor for the first next call to avoid extra checks in next
    Utils.getSQLDataType(dataType) match {
      case BooleanType | ByteType => cursor -= 1
      case ShortType => cursor -= 2
      case IntegerType | FloatType | DateType => cursor -= 4
      case LongType | DoubleType | TimestampType => cursor -= 8
      case CalendarIntervalType => cursor -= 12
      case d: DecimalType if d.precision <= Decimal.MAX_LONG_DIGITS =>
        cursor -= 8
      case StringType | BinaryType | _: DecimalType |
           _: ArrayType | _: MapType | _: StructType =>
        cursor -= 4 // read and skip null values in first call
      case NullType => // no role of cursor for NullType
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported type $dataType")
    }
  }

  override def readBoolean(bytes: Array[Byte]): Boolean =
    Platform.getByte(bytes, cursor) == 1

  override def nextBoolean(bytes: Array[Byte]): Unit =
    cursor += 1

  override def readByte(bytes: Array[Byte]): Byte =
    Platform.getByte(bytes, cursor)

  override def nextByte(bytes: Array[Byte]): Unit =
    cursor += 1

  override def readShort(bytes: Array[Byte]): Short =
    if (littleEndian) Platform.getShort(bytes, cursor)
    else java.lang.Short.reverseBytes(Platform.getShort(bytes, cursor))

  override def nextShort(bytes: Array[Byte]): Unit =
    cursor += 2

  override def readInt(bytes: Array[Byte]): Int =
    if (littleEndian) Platform.getInt(bytes, cursor)
    else java.lang.Integer.reverseBytes(Platform.getInt(bytes, cursor))

  override def nextInt(bytes: Array[Byte]): Unit =
    cursor += 4

  override def readLong(bytes: Array[Byte]): Long =
    if (littleEndian) Platform.getLong(bytes, cursor)
    else java.lang.Long.reverseBytes(Platform.getLong(bytes, cursor))

  override def nextLong(bytes: Array[Byte]): Unit =
    cursor += 8

  override def readFloat(bytes: Array[Byte]): Float =
    if (littleEndian) Platform.getFloat(bytes, cursor)
    else java.lang.Float.intBitsToFloat(java.lang.Integer.reverseBytes(
      Platform.getInt(bytes, cursor)))

  override def nextFloat(bytes: Array[Byte]): Unit =
    cursor += 4

  override def readDouble(bytes: Array[Byte]): Double =
    if (littleEndian) Platform.getDouble(bytes, cursor)
    else java.lang.Double.longBitsToDouble(java.lang.Long.reverseBytes(
      Platform.getLong(bytes, cursor)))

  override def nextDouble(bytes: Array[Byte]): Unit =
    cursor += 8

  override def readDecimal(bytes: Array[Byte], precision: Int,
      scale: Int): Decimal = {
    if (precision <= Decimal.MAX_LONG_DIGITS) {
      Decimal.createUnsafe(readLong(bytes), precision, scale)
    } else {
      Decimal.apply(new BigDecimal(new BigInteger(readBinary(bytes)),
        scale), precision, scale)
    }
  }

  override def nextDecimal(bytes: Array[Byte], precision: Int): Unit = {
    if (precision <= Decimal.MAX_LONG_DIGITS) {
      cursor += 8
    } else {
      val size = readInt(bytes)
      cursor += (4 + size)
    }
  }

  override def readUTF8String(columnBytes: Array[Byte]): UTF8String = {
    val size = readInt(columnBytes)
    UTF8String.fromAddress(columnBytes, cursor + 4, size)
  }

  override def nextUTF8String(columnBytes: Array[Byte]): Unit = {
    val size = readInt(columnBytes)
    cursor += (4 + size)
  }

  override def readBinary(bytes: Array[Byte]): Array[Byte] = {
    val size = readInt(bytes)
    val b = new Array[Byte](size)
    Platform.copyMemory(bytes, cursor + 4, b, Platform.BYTE_ARRAY_OFFSET, size)
    b
  }

  override def nextBinary(bytes: Array[Byte]): Unit = {
    val size = readInt(bytes)
    cursor += (4 + size)
  }

  override def readInterval(bytes: Array[Byte]): CalendarInterval = {
    val months = readInt(bytes)
    cursor += 4
    val micros = readLong(bytes)
    cursor -= 4
    new CalendarInterval(months, micros)
  }

  override def nextInterval(bytes: Array[Byte]): Unit =
    cursor += 12

  override def readArray(bytes: Array[Byte]): UnsafeArrayData = {
    val result = new UnsafeArrayData
    val size = readInt(bytes)
    result.pointTo(bytes, cursor + 4, size)
    result
  }

  override def readMap(bytes: Array[Byte]): UnsafeMapData = {
    val result = new UnsafeMapData
    val size = readInt(bytes)
    result.pointTo(bytes, cursor + 4, size)
    result
  }

  override def readStruct(bytes: Array[Byte], numFields: Int): UnsafeRow = {
    val result = new UnsafeRow(numFields)
    val size = readInt(bytes)
    result.pointTo(bytes, cursor + 4, size)
    result
  }

  override def initializeEncoding(dataType: DataType,
      batchSize: Int): Array[Byte] =
    throw new UnsupportedOperationException(s"initializeEncoding for $toString")

  override def writeBoolean(bytes: Array[Byte], value: Boolean): Unit = {
    Platform.putByte(bytes, cursor, if (value) 1 else 0)
    cursor += 1
  }

  override def writeByte(bytes: Array[Byte], value: Byte): Unit = {
    Platform.putByte(bytes, cursor, value)
    cursor += 1
  }

  override def writeShort(bytes: Array[Byte], value: Short): Unit = {
    if (littleEndian) Platform.putShort(bytes, cursor, value)
    else Platform.putShort(bytes, cursor, java.lang.Short.reverseBytes(value))
    cursor += 2
  }

  override def writeInt(bytes: Array[Byte], value: Int): Unit = {
    if (littleEndian) Platform.putInt(bytes, cursor, value)
    else Platform.putInt(bytes, cursor, java.lang.Integer.reverseBytes(value))
    cursor += 4
  }

  override def writeLong(bytes: Array[Byte], value: Long): Unit = {
    if (littleEndian) Platform.putLong(bytes, cursor, value)
    else Platform.putLong(bytes, cursor, java.lang.Long.reverseBytes(value))
    cursor += 8
  }

  override def writeDecimal(bytes: Array[Byte], value: Decimal,
      precision: Int): Unit = {
    if (precision <= Decimal.MAX_LONG_DIGITS) {
      writeLong(bytes, value.toUnscaledLong)
    } else {
      val b = value.toJavaBigDecimal.unscaledValue.toByteArray
      writeBinary(bytes, b)
    }
  }

  override def writeFloat(bytes: Array[Byte], value: Float): Unit = {
    if (littleEndian) Platform.putFloat(bytes, cursor, value)
    else Platform.putInt(bytes, cursor, java.lang.Integer.reverseBytes(
      java.lang.Float.floatToIntBits(value)))
    cursor += 4
  }

  override def writeDouble(bytes: Array[Byte], value: Double): Unit = {
    if (littleEndian) Platform.putDouble(bytes, cursor, value)
    else Platform.putLong(bytes, cursor, java.lang.Long.reverseBytes(
      java.lang.Double.doubleToLongBits(value)))
    cursor += 8
  }

  override def writeUTF8String(bytes: Array[Byte], value: UTF8String): Unit = {
    val size = value.numBytes
    writeInt(bytes, size)
    Platform.copyMemory(value.getBaseObject, value.getBaseOffset, bytes,
      cursor, size)
    cursor += size
  }

  override def writeBinary(bytes: Array[Byte], value: Array[Byte]): Unit = {
    val size = value.length
    writeInt(bytes, size)
    Platform.copyMemory(value, Platform.BYTE_ARRAY_OFFSET, bytes, cursor, size)
    cursor += size
  }

  override def writeInterval(bytes: Array[Byte],
      value: CalendarInterval): Unit = {
    writeInt(bytes, value.months)
    writeLong(bytes, value.microseconds)
  }

  override def writeArray(bytes: Array[Byte], value: UnsafeArrayData): Unit = {
    val size = value.getSizeInBytes
    writeInt(bytes, size)
    Platform.copyMemory(value.getBaseObject, value.getBaseOffset, bytes,
      cursor, size)
    cursor += size
  }

  override def writeMap(bytes: Array[Byte], value: UnsafeMapData): Unit = {
    val size = value.getSizeInBytes
    writeInt(bytes, size)
    Platform.copyMemory(value.getBaseObject, value.getBaseOffset, bytes,
      cursor, size)
    cursor += size
  }

  override def writeStruct(bytes: Array[Byte], value: UnsafeRow): Unit = {
    val size = value.getSizeInBytes
    writeInt(bytes, size)
    Platform.copyMemory(value.getBaseObject, value.getBaseOffset, bytes,
      cursor, size)
    cursor += size
  }
}
