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

private[columnar] abstract class ColumnEncoding {

  private[columnar] final var cursor = 0

  def typeId: Int

  def supports(dataType: DataType): Boolean

  def initializeDecoding(columnBytes: Array[Byte], field: Attribute): Unit

  def readNull(columnBytes: Array[Byte], ordinal: Int): Boolean

  def readBoolean(columnBytes: Array[Byte]): Boolean

  def nextBoolean(columnBytes: Array[Byte]): Unit

  def readByte(columnBytes: Array[Byte]): Byte

  def nextByte(columnBytes: Array[Byte]): Unit

  def readShort(columnBytes: Array[Byte]): Short

  def nextShort(columnBytes: Array[Byte]): Unit

  def readInt(columnBytes: Array[Byte]): Int

  def nextInt(columnBytes: Array[Byte]): Unit

  def readLong(columnBytes: Array[Byte]): Long

  def nextLong(columnBytes: Array[Byte]): Unit

  def readFloat(columnBytes: Array[Byte]): Float

  def nextFloat(columnBytes: Array[Byte]): Unit

  def readDouble(columnBytes: Array[Byte]): Double

  def nextDouble(columnBytes: Array[Byte]): Unit

  def readDecimal(columnBytes: Array[Byte], precision: Int,
      scale: Int): Decimal

  def nextDecimal(columnBytes: Array[Byte], precision: Int): Unit

  def readUTF8String(columnBytes: Array[Byte]): UTF8String

  def nextUTF8String(columnBytes: Array[Byte]): Unit

  def readBinary(columnBytes: Array[Byte]): Array[Byte]

  def nextBinary(columnBytes: Array[Byte]): Unit

  def readInterval(columnBytes: Array[Byte]): CalendarInterval

  def nextInterval(columnBytes: Array[Byte]): Unit

  def readArray(columnBytes: Array[Byte]): UnsafeArrayData

  def readMap(columnBytes: Array[Byte]): UnsafeMapData

  def readStruct(columnBytes: Array[Byte], numFields: Int): UnsafeRow

  def initializeEncoding(dataType: DataType, batchSize: Int): Array[Byte]

  def writeBoolean(columnBytes: Array[Byte], value: Boolean): Unit

  def writeByte(columnBytes: Array[Byte], value: Byte): Unit

  def writeShort(columnBytes: Array[Byte], value: Short): Unit

  def writeInt(columnBytes: Array[Byte], value: Int): Unit

  def writeLong(columnBytes: Array[Byte], value: Long): Unit

  def writeFloat(columnBytes: Array[Byte], value: Float): Unit

  def writeDouble(columnBytes: Array[Byte], value: Double): Unit

  def writeDecimal(columnBytes: Array[Byte], value: Decimal,
      precision: Int): Unit

  def writeUTF8String(columnBytes: Array[Byte], value: UTF8String): Unit

  def writeBinary(columnBytes: Array[Byte], value: Array[Byte]): Unit

  def writeInterval(columnBytes: Array[Byte], value: CalendarInterval): Unit

  def writeArray(columnBytes: Array[Byte], value: UnsafeArrayData): Unit

  def writeMap(columnBytes: Array[Byte], value: UnsafeMapData): Unit

  def writeStruct(columnBytes: Array[Byte], value: UnsafeRow): Unit
}

object ColumnEncoding {

  private[columnar] val baseOffset: Int =
    Platform.BYTE_ARRAY_OFFSET + 4 /* for typeId */

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

  def getColumnEncoding(columnBytes: Array[Byte], dataType: DataType,
      nullable: Boolean): ColumnEncoding = {
    val typeId = if (littleEndian) {
      Platform.getInt(columnBytes, Platform.BYTE_ARRAY_OFFSET)
    } else {
      java.lang.Integer.reverseBytes(
        Platform.getInt(columnBytes, Platform.BYTE_ARRAY_OFFSET))
    }
    val encoding = allEncodings(typeId)(dataType, nullable)
    if (encoding.typeId != typeId) {
      throw new AssertionError(s"typeId for $encoding = ${encoding.typeId} " +
          s"does not match typeId = $typeId in global registration")
    }
    if (!encoding.supports(dataType)) {
      throw new AssertionError(s"Encoder bug? Unsupported type $dataType " +
          s"for encoding $encoding")
    }
    encoding
  }

  def getColumnDecoder(columnBytes: Array[Byte], numRows: Int,
      field: Attribute): ColumnEncoding = {
    val encoding = getColumnEncoding(columnBytes,
      field.dataType, field.nullable)
    encoding.initializeDecoding(columnBytes, field)
    encoding
  }

  private[columnar] def createPassThrough(dataType: DataType,
      nullable: Boolean): ColumnEncoding =
    if (nullable) new UncompressedNullable else new Uncompressed

  private[columnar] def createRunLengthEncoding(dataType: DataType,
      nullable: Boolean): ColumnEncoding = dataType match {
    case BooleanType | ByteType | ShortType |
         IntegerType | LongType | StringType =>
      if (nullable) new RunLengthEncodingNullable else new RunLengthEncoding
    case _ => throw new UnsupportedOperationException(
      s"RunLengthEncoding not supported for $dataType")
  }

  private[columnar] def createDictionaryEncoding(dataType: DataType,
      nullable: Boolean): ColumnEncoding = dataType match {
    case StringType =>
      if (nullable) new DictionaryEncodingNullable[UTF8String]
      else new DictionaryEncoding[UTF8String]
    case IntegerType =>
      if (nullable) new DictionaryEncodingNullable[Int]
      else new DictionaryEncoding[Int]
    case LongType =>
      if (nullable) new DictionaryEncodingNullable[Long]
      else new DictionaryEncoding[Long]
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
    case IntegerType =>
      if (nullable) new IntDeltaEncodingNullable else new IntDeltaEncoding
    case _ => throw new UnsupportedOperationException(
      s"IntDeltaEncoding not supported for $dataType")
  }

  private[columnar] def createLongDeltaEncoding(dataType: DataType,
      nullable: Boolean): ColumnEncoding = dataType match {
    case LongType =>
      if (nullable) new LongDeltaEncodingNullable else new LongDeltaEncoding
    case _ => throw new UnsupportedOperationException(
      s"LongDeltaEncoding not supported for $dataType")
  }
}

private[columnar] trait NotNullColumn extends ColumnEncoding {

  override def initializeDecoding(columnBytes: Array[Byte],
      field: Attribute): Unit = {
    cursor = ColumnEncoding.baseOffset
    val numNullValues = Platform.getInt(columnBytes, cursor)
    if (numNullValues != 0) {
      throw new IllegalStateException(
        s"Null values found in NOT NULL column $field")
    }
    cursor += 4
  }

  override final def readNull(bytes: Array[Byte], ordinal: Int): Boolean = false
}

private[columnar] trait NullableColumn extends ColumnEncoding {

  private[this] final var nullValues: Array[Byte] = _
  // private[this] final var nullValuesBitSet: BitSet = _
  private[this] final var nextNullOrdinal = 0
  private[this] final var nextNullCursor = 0
  private[this] final var nextNullCursorEnd = 0
  // private[this] final var nullValuesType: Byte = 0

  private final def updateNextNullOrdinal() {
    if (nextNullCursor < nextNullCursorEnd) {
      nextNullOrdinal = Platform.getInt(nullValues, nextNullCursor)
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

  override def initializeDecoding(columnBytes: Array[Byte],
      field: Attribute): Unit = {
    cursor = ColumnEncoding.baseOffset
    // copying instead of keeping pointer will help in better cache alignment
    val nullValuesSize = Platform.getInt(columnBytes, cursor)
    cursor += 4
    if (nullValuesSize > 0) {
      nullValues = new Array[Byte](nullValuesSize)
      Platform.copyMemory(columnBytes, cursor, nullValues,
        Platform.BYTE_ARRAY_OFFSET, nullValuesSize)
      cursor += nullValuesSize
      nextNullCursor = Platform.BYTE_ARRAY_OFFSET
      nextNullCursorEnd = nextNullCursor + nullValuesSize
      updateNextNullOrdinal()
    } else nextNullOrdinal = -1
    /*
    nullValuesType = columnBytes(4)
    cursor = Platform.BYTE_ARRAY_OFFSET + 4 /* for typeId */ + 1
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
      val numWords = Platform.getInt(columnBytes, cursor)
      cursor += 4
      if (numWords > 0) {
        try {
          nullValuesBitSet = new BitSet(numWords << 6)
          // set the internal words field of BitSet directly
          val words = ColumnEncodingBase.bitSetWords.get(
            nullValuesBitSet).asInstanceOf[Array[Long]]
          var index = 0
          while (index < numWords) {
            words(index) = Platform.getLong(columnBytes, cursor)
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

  override final def readNull(bytes: Array[Byte], ordinal: Int): Boolean = {
    if (ordinal != nextNullOrdinal) false
    else {
      updateNextNullOrdinal()
      true
    }
  }
}
