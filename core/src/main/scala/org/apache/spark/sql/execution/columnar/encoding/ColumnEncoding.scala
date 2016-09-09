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

import java.lang.reflect.Field
import java.nio.ByteOrder

import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeArrayData, UnsafeMapData, UnsafeRow}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.collection.BitSet

abstract class ColumnEncoding {

  protected final var cursor = 0

  protected final var nullValues: Array[Byte] = _
  // protected final var nullValuesBitSet: BitSet = _
  protected final var nextNullOrdinal = 0
  protected final var nextNullCursor = 0
  protected final var nextNullCursorEnd = 0
  // protected final var nullValuesType: Byte = 0

  def typeId: Int

  def supports(dataType: DataType): Boolean

  private[columnar] def initializeNulls(columnBytes: Array[Byte],
      field: Attribute): Unit = {}

  def initializeDecoding(columnBytes: Array[Byte], field: Attribute): Unit = {}

  def notNull(columnBytes: Array[Byte], ordinal: Int): Byte

  def nextBoolean(columnBytes: Array[Byte]): Unit

  def readBoolean(columnBytes: Array[Byte]): Boolean

  def nextByte(columnBytes: Array[Byte]): Unit

  def readByte(columnBytes: Array[Byte]): Byte

  def nextShort(columnBytes: Array[Byte]): Unit

  def readShort(columnBytes: Array[Byte]): Short

  def nextInt(columnBytes: Array[Byte]): Unit

  def readInt(columnBytes: Array[Byte]): Int

  def nextLong(columnBytes: Array[Byte]): Unit

  def readLong(columnBytes: Array[Byte]): Long

  def nextFloat(columnBytes: Array[Byte]): Unit

  def readFloat(columnBytes: Array[Byte]): Float

  def nextDouble(columnBytes: Array[Byte]): Unit

  def readDouble(columnBytes: Array[Byte]): Double

  def nextDecimal(columnBytes: Array[Byte], precision: Int): Unit

  def readDecimal(columnBytes: Array[Byte], precision: Int,
      scale: Int): Decimal

  def nextUTF8String(columnBytes: Array[Byte]): Unit

  def readUTF8String(columnBytes: Array[Byte]): UTF8String

  def readDate(columnBytes: Array[Byte]): Int = readInt(columnBytes)

  def readTimestamp(columnBytes: Array[Byte]): Long = readLong(columnBytes)

  def nextInterval(columnBytes: Array[Byte]): Unit

  def readInterval(columnBytes: Array[Byte]): CalendarInterval

  def nextBinary(columnBytes: Array[Byte]): Unit

  def readBinary(columnBytes: Array[Byte]): Array[Byte]

  def readArray(columnBytes: Array[Byte]): UnsafeArrayData

  def readMap(columnBytes: Array[Byte]): UnsafeMapData

  def readStruct(columnBytes: Array[Byte], numFields: Int): UnsafeRow

  def wasNull(): Boolean = false

  def initializeEncoding(dataType: DataType, batchSize: Int): Array[Byte] =
    throw new UnsupportedOperationException(s"initializeEncoding for $toString")

  def writeBoolean(columnBytes: Array[Byte], value: Boolean): Unit =
    throw new UnsupportedOperationException(s"writeBoolean for $toString")

  def writeByte(columnBytes: Array[Byte], value: Byte): Unit =
    throw new UnsupportedOperationException(s"writeByte for $toString")

  def writeShort(columnBytes: Array[Byte], value: Short): Unit =
    throw new UnsupportedOperationException(s"writeShort for $toString")

  def writeInt(columnBytes: Array[Byte], value: Int): Unit =
    throw new UnsupportedOperationException(s"writeInt for $toString")

  def writeLong(columnBytes: Array[Byte], value: Long): Unit =
    throw new UnsupportedOperationException(s"writeLong for $toString")

  def writeFloat(columnBytes: Array[Byte], value: Float): Unit =
    throw new UnsupportedOperationException(s"writeFloat for $toString")

  def writeDouble(columnBytes: Array[Byte], value: Double): Unit =
    throw new UnsupportedOperationException(s"writeDouble for $toString")

  def writeDecimal(columnBytes: Array[Byte], value: Decimal,
      precision: Int): Unit =
    throw new UnsupportedOperationException(s"writeDecimal for $toString")

  def writeUTF8String(columnBytes: Array[Byte],
      value: UTF8String): Unit =
    throw new UnsupportedOperationException(s"writeUTF8String for $toString")

  def writeBinary(columnBytes: Array[Byte], value: Array[Byte]): Unit =
    throw new UnsupportedOperationException(s"writeBinary for $toString")

  def writeInterval(columnBytes: Array[Byte],
      value: CalendarInterval): Unit =
    throw new UnsupportedOperationException(s"writeInterval for $toString")

  def writeArray(columnBytes: Array[Byte],
      value: UnsafeArrayData): Unit =
    throw new UnsupportedOperationException(s"writeArray for $toString")

  def writeMap(columnBytes: Array[Byte], value: UnsafeMapData): Unit =
    throw new UnsupportedOperationException(s"writeMap for $toString")

  def writeStruct(columnBytes: Array[Byte], value: UnsafeRow): Unit =
    throw new UnsupportedOperationException(s"writeStruct for $toString")
}

object ColumnEncoding {

  private[columnar] val bitSetWords: Field = {
    val f = classOf[BitSet].getDeclaredField("words")
    f.setAccessible(true)
    f
  }

  private[columnar] val BITS_PER_LONG = 64

  val littleEndian: Boolean = ByteOrder.nativeOrder == ByteOrder.LITTLE_ENDIAN

  val allEncodings: Array[(DataType, Boolean) => ColumnEncoding] = Array(
    createPassThrough,
    createRunLengthEncoding,
    createDictionaryEncoding,
    createBooleanBitSetEncoding,
    createIntDeltaEncoding,
    createLongDeltaEncoding
  )

  def getColumnDecoder(columnBytes: Array[Byte],
      field: Attribute): ColumnEncoding = {
    val typeId = initializeDecoding(columnBytes)
    val dataType = field.dataType
    val encoding = allEncodings(typeId)(dataType, field.nullable)
    if (encoding.typeId != typeId) {
      throw new AssertionError(s"typeId for $encoding = ${encoding.typeId} " +
          s"does not match typeId = $typeId in global registration")
    }
    if (!encoding.supports(dataType)) {
      throw new AssertionError(s"Encoder bug? Unsupported type $dataType " +
          s"for encoding $encoding")
    }
    encoding.initializeNulls(columnBytes, field)
    encoding.initializeDecoding(columnBytes, field)
    encoding
  }

  private[columnar] def createPassThrough(dataType: DataType,
      nullable: Boolean): ColumnEncoding =
    if (nullable) new UncompressedNullable else new Uncompressed

  private[columnar] def createRunLengthEncoding(dataType: DataType,
      nullable: Boolean): ColumnEncoding = dataType match {
    case BooleanType | ByteType | ShortType |
         IntegerType | DateType | LongType | TimestampType | StringType =>
      if (nullable) new RunLengthEncodingNullable else new RunLengthEncoding
    case _ => throw new UnsupportedOperationException(
      s"RunLengthEncoding not supported for $dataType")
  }

  private[columnar] def createDictionaryEncoding(dataType: DataType,
      nullable: Boolean): ColumnEncoding = dataType match {
    case StringType | IntegerType | DateType | LongType | TimestampType =>
      if (nullable) new DictionaryEncodingNullable
      else new DictionaryEncoding
    case _ => throw new UnsupportedOperationException(
      s"DictionaryEncoding not supported for $dataType")
  }

  private[columnar] def createBooleanBitSetEncoding(dataType: DataType,
      nullable: Boolean): ColumnEncoding = dataType match {
    case BooleanType =>
      if (nullable) new BooleanBitSetEncodingNullable
      else new BooleanBitSetEncoding
    case _ => throw new UnsupportedOperationException(
      s"BooleanBitSetEncoding not supported for $dataType")
  }

  private[columnar] def createIntDeltaEncoding(dataType: DataType,
      nullable: Boolean): ColumnEncoding = dataType match {
    case IntegerType | DateType =>
      if (nullable) new IntDeltaEncodingNullable else new IntDeltaEncoding
    case _ => throw new UnsupportedOperationException(
      s"IntDeltaEncoding not supported for $dataType")
  }

  private[columnar] def createLongDeltaEncoding(dataType: DataType,
      nullable: Boolean): ColumnEncoding = dataType match {
    case LongType | TimestampType =>
      if (nullable) new LongDeltaEncodingNullable else new LongDeltaEncoding
    case _ => throw new UnsupportedOperationException(
      s"LongDeltaEncoding not supported for $dataType")
  }

  private[columnar] final def readInt(columnBytes: Array[Byte],
      cursor: Int): Int = if (littleEndian) {
    Platform.getInt(columnBytes, cursor)
  } else {
    java.lang.Integer.reverseBytes(Platform.getInt(columnBytes, cursor))
  }

  private[columnar] final def readLong(columnBytes: Array[Byte],
      cursor: Int): Long = if (littleEndian) {
    Platform.getLong(columnBytes, cursor)
  } else {
    java.lang.Long.reverseBytes(Platform.getLong(columnBytes, cursor))
  }

  private[columnar] def initializeDecoding(columnBytes: Array[Byte]): Int = {
    // read and skip null values array at the start, then read the typeId
    var cursor = Platform.BYTE_ARRAY_OFFSET
    val nullValuesSize = readInt(columnBytes, cursor)
    cursor += (4 + nullValuesSize)
    readInt(columnBytes, cursor)
  }
}

trait NotNullColumn extends ColumnEncoding {

  override private[columnar] final def initializeNulls(
      columnBytes: Array[Byte], field: Attribute): Unit = {
    cursor = Platform.BYTE_ARRAY_OFFSET
    val numNullValues = ColumnEncoding.readInt(columnBytes, cursor)
    if (numNullValues != 0) {
      throw new IllegalStateException(
        s"$numNullValues null values found in NOT NULL column $field")
    }
    cursor += 8 // skip numNullValues and typeId
  }

  override def notNull(bytes: Array[Byte], ordinal: Int): Byte = 1
}

trait NullableColumn extends ColumnEncoding {

  private final def updateNextNullOrdinal() {
    if (nextNullCursor < nextNullCursorEnd) {
      nextNullOrdinal = ColumnEncoding.readInt(nullValues, nextNullCursor)
      nextNullCursor += 4
    } else nextNullOrdinal = -1
    /*
    nullValuesType match {
      case 0 =>
        // 0 indicates bytes for null indexes
        if (nextNullCursor < nextNullCursorEnd) {
          nextNullOrdinal = nullValues(nextNullCursor)
          nextNullCursor += 1
        } else nextNullOrdinal = -1
      case 1 =>
        // 1 indicates shorts for null indexes
        if (nextNullCursor < nextNullCursorEnd) {
          val s1 = nullValues(nextNullCursor)
          nextNullCursor += 1
          nextNullOrdinal = (s1 << 8) + nullValues(nextNullCursor)
          nextNullCursor += 1
        } else nextNullOrdinal = -1
      case _ =>
        // 2 indicates a bit map
        nextNullOrdinal = nullValuesBitSet.nextSetBit(nextNullOrdinal)
    }
    */
  }

  override private[columnar] final def initializeNulls(
      columnBytes: Array[Byte], field: Attribute): Unit = {
    cursor = Platform.BYTE_ARRAY_OFFSET
    val nullValuesSize = ColumnEncoding.readInt(columnBytes, cursor)
    cursor += 4
    if (nullValuesSize > 0) {
      // copying instead of keeping pointer will help in better cache alignment
      nullValues = new Array[Byte](nullValuesSize)
      Platform.copyMemory(columnBytes, cursor, nullValues,
        Platform.BYTE_ARRAY_OFFSET, nullValuesSize)
      cursor += nullValuesSize
      nextNullCursor = Platform.BYTE_ARRAY_OFFSET
      nextNullCursorEnd = nextNullCursor + nullValuesSize
      updateNextNullOrdinal()
    } else nextNullOrdinal = -1
    cursor += 4 // skip typeId
    /*
    nullValuesType = columnBytes(0)
    cursor = Platform.BYTE_ARRAY_OFFSET + 1
    // copying instead of keeping pointer will help in better cache alignment
    // since these are at the start of the byte array
    if (nullValuesType <= 1) {
      nullValues = super.readBinary(columnBytes, 0)
      nullValuesBitSet = null
      if (nullValues.length > 0) {
        nullCursorEnd = nullValues.length
        updateNextNullOrdinal()
      }
      else {
        nullValues = null
        nextNullOrdinal = -1
      }
    } else {
      nullValues = null
      nullValuesBitSet = null
      val numWords = ColumnEncoding.readInt(columnBytes, cursor)
      cursor += 4
      if (numWords > 0) {
        try {
          nullValuesBitSet = new BitSet(numWords << 6)
          // set the internal words field of BitSet directly
          val words = ColumnEncodingBase.bitSetWords.get(
            nullValuesBitSet).asInstanceOf[Array[Long]]
          var index = 0
          while (index < numWords) {
            words(index) = ColumnEncoding.readLong(columnBytes, cursor)
            cursor += 8
            index += 1
          }
          nextNullOrdinal = nullValuesBitSet.nextSetBit(0)
        } catch {
          case e: IllegalAccessException => throw new RuntimeException(e)
        }
      } else nextNullOrdinal = -1
    }
    */
  }

  override final def notNull(bytes: Array[Byte], ordinal: Int): Byte = {
    if (ordinal != nextNullOrdinal) 1
    else {
      updateNextNullOrdinal()
      0
    }
  }
}
