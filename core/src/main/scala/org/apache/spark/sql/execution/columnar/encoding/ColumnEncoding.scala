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
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.collection.BitSet

abstract class ColumnEncoding {

  def typeId: Int

  def supports(dataType: DataType): Boolean

  protected def initializeNulls(columnBytes: AnyRef,
      field: Attribute): Long

  def initializeDecoding(columnBytes: AnyRef, field: Attribute): Long = {
    val cursor = initializeNulls(columnBytes, field)
    val dataType = Utils.getSQLDataType(field.dataType)
    // no typeId for complex types
    dataType match {
      case d: DecimalType =>
        if (d.precision > Decimal.MAX_LONG_DIGITS) cursor else cursor + 4
      case BinaryType | _: ArrayType | _: MapType | _: StructType => cursor
      case _ => cursor + 4
    }
  }

  /**
   * Returns 1 to indicate that column value was not-null,
   * 0 to indicate that it was null and -1 to indicate that
   * <code>wasNull()</code> needs to be invoked after the
   * appropriate read method.
   */
  def notNull(columnBytes: AnyRef, ordinal: Int): Int

  def nextBoolean(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextBoolean for $toString")

  def readBoolean(columnBytes: AnyRef, cursor: Long): Boolean =
    throw new UnsupportedOperationException(s"readBoolean for $toString")

  def nextByte(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextByte for $toString")

  def readByte(columnBytes: AnyRef, cursor: Long): Byte =
    throw new UnsupportedOperationException(s"readByte for $toString")

  def nextShort(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextShort for $toString")

  def readShort(columnBytes: AnyRef, cursor: Long): Short =
    throw new UnsupportedOperationException(s"readShort for $toString")

  def nextInt(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextInt for $toString")

  def readInt(columnBytes: AnyRef, cursor: Long): Int =
    throw new UnsupportedOperationException(s"readInt for $toString")

  def nextLong(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextLong for $toString")

  def readLong(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"readLong for $toString")

  def nextFloat(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextFloat for $toString")

  def readFloat(columnBytes: AnyRef, cursor: Long): Float =
    throw new UnsupportedOperationException(s"readFloat for $toString")

  def nextDouble(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextDouble for $toString")

  def readDouble(columnBytes: AnyRef, cursor: Long): Double =
    throw new UnsupportedOperationException(s"readDouble for $toString")

  def nextLongDecimal(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextLongDecimal for $toString")

  def readLongDecimal(columnBytes: AnyRef, precision: Int,
      scale: Int, cursor: Long): Decimal =
    throw new UnsupportedOperationException(s"readLongDecimal for $toString")

  def nextDecimal(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextDecimal for $toString")

  def readDecimal(columnBytes: AnyRef, precision: Int,
      scale: Int, cursor: Long): Decimal =
    throw new UnsupportedOperationException(s"readDecimal for $toString")

  def nextUTF8String(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextUTF8String for $toString")

  def readUTF8String(columnBytes: AnyRef, cursor: Long): UTF8String =
    throw new UnsupportedOperationException(s"readUTF8String for $toString")

  def readDate(columnBytes: AnyRef, cursor: Long): Int =
    readInt(columnBytes, cursor)

  def readTimestamp(columnBytes: AnyRef, cursor: Long): Long =
    readLong(columnBytes, cursor)

  def nextInterval(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextInterval for $toString")

  def readInterval(columnBytes: AnyRef, cursor: Long): CalendarInterval =
    throw new UnsupportedOperationException(s"readInterval for $toString")

  def nextBinary(columnBytes: AnyRef, cursor: Long): Long =
    throw new UnsupportedOperationException(s"nextBinary for $toString")

  def readBinary(columnBytes: AnyRef, cursor: Long): Array[Byte] =
    throw new UnsupportedOperationException(s"readBinary for $toString")

  def readArray(columnBytes: AnyRef, cursor: Long): UnsafeArrayData =
    throw new UnsupportedOperationException(s"readArray for $toString")

  def readMap(columnBytes: AnyRef, cursor: Long): UnsafeMapData =
    throw new UnsupportedOperationException(s"readMap for $toString")

  def readStruct(columnBytes: AnyRef, numFields: Int,
      cursor: Long): UnsafeRow =
    throw new UnsupportedOperationException(s"readStruct for $toString")

  /**
   * Only to be used for implementations (ResultSet adapter) that need to check
   * for null after having invoked the appropriate read method.
   * The <code>notNull</code> method should return -1 for such implementations.
   */
  def wasNull(): Boolean = false

  def initializeEncoding(dataType: DataType, batchSize: Int): AnyRef =
    throw new UnsupportedOperationException(s"initializeEncoding for $toString")

  def writeBoolean(columnBytes: AnyRef, value: Boolean): Unit =
    throw new UnsupportedOperationException(s"writeBoolean for $toString")

  def writeByte(columnBytes: AnyRef, value: Byte): Unit =
    throw new UnsupportedOperationException(s"writeByte for $toString")

  def writeShort(columnBytes: AnyRef, value: Short): Unit =
    throw new UnsupportedOperationException(s"writeShort for $toString")

  def writeInt(columnBytes: AnyRef, value: Int): Unit =
    throw new UnsupportedOperationException(s"writeInt for $toString")

  def writeLong(columnBytes: AnyRef, value: Long): Unit =
    throw new UnsupportedOperationException(s"writeLong for $toString")

  def writeFloat(columnBytes: AnyRef, value: Float): Unit =
    throw new UnsupportedOperationException(s"writeFloat for $toString")

  def writeDouble(columnBytes: AnyRef, value: Double): Unit =
    throw new UnsupportedOperationException(s"writeDouble for $toString")

  def writeLongDecimal(columnBytes: AnyRef, value: Decimal,
      precision: Int): Unit =
    throw new UnsupportedOperationException(s"writeLongDecimal for $toString")

  def writeDecimal(columnBytes: AnyRef, value: Decimal,
      precision: Int): Unit =
    throw new UnsupportedOperationException(s"writeDecimal for $toString")

  def writeUTF8String(columnBytes: AnyRef,
      value: UTF8String): Unit =
    throw new UnsupportedOperationException(s"writeUTF8String for $toString")

  def writeBinary(columnBytes: AnyRef, value: Array[Byte]): Unit =
    throw new UnsupportedOperationException(s"writeBinary for $toString")

  def writeInterval(columnBytes: AnyRef,
      value: CalendarInterval): Unit =
    throw new UnsupportedOperationException(s"writeInterval for $toString")

  def writeArray(columnBytes: AnyRef,
      value: UnsafeArrayData): Unit =
    throw new UnsupportedOperationException(s"writeArray for $toString")

  def writeMap(columnBytes: AnyRef, value: UnsafeMapData): Unit =
    throw new UnsupportedOperationException(s"writeMap for $toString")

  def writeStruct(columnBytes: AnyRef, value: UnsafeRow): Unit =
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

  val allEncodings: Array[(AnyRef, DataType, Boolean) => ColumnEncoding] = Array(
    createPassThrough,
    createRunLengthEncoding,
    createDictionaryEncoding,
    createBooleanBitSetEncoding,
    createIntDeltaEncoding,
    createLongDeltaEncoding
  )

  def getColumnDecoder(columnBytes: Array[Byte],
      field: Attribute): ColumnEncoding = {
    // read and skip null values array at the start, then read the typeId
    var cursor = Platform.BYTE_ARRAY_OFFSET
    val nullValuesSize = readInt(columnBytes, cursor) << 2
    cursor += (4 + nullValuesSize)

    val dataType = Utils.getSQLDataType(field.dataType)
    // no typeId for complex types
    val typeId = dataType match {
      case d: DecimalType if d.precision > Decimal.MAX_LONG_DIGITS => 0
      case BinaryType | _: ArrayType | _: MapType | _: StructType => 0
      case _ => readInt(columnBytes, cursor)
    }
    if (typeId >= allEncodings.length) {
      throw new IllegalStateException(s"Unknown encoding typeId = $typeId " +
          s"for $dataType($field) bytes: ${columnBytes.toSeq}")
    }
    val encoding = allEncodings(typeId)(columnBytes, dataType,
      // use NotNull version if field is marked so or no nulls in the batch
      field.nullable && nullValuesSize > 0)
    if (encoding.typeId != typeId) {
      throw new IllegalStateException(s"typeId for $encoding = " +
          s"${encoding.typeId} does not match $typeId in global registration")
    }
    if (!encoding.supports(dataType)) {
      throw new IllegalStateException("Encoder bug? Unsupported type " +
          s"$dataType for encoding $encoding")
    }
    encoding
  }

  private[columnar] def createPassThrough(columnBytes: AnyRef,
      dataType: DataType, nullable: Boolean): ColumnEncoding =
    if (nullable) new UncompressedNullable else new Uncompressed

  private[columnar] def createRunLengthEncoding(columnBytes: AnyRef,
      dataType: DataType, nullable: Boolean): ColumnEncoding = dataType match {
    case BooleanType | ByteType | ShortType |
         IntegerType | DateType | LongType | TimestampType | StringType =>
      if (nullable) new RunLengthEncodingNullable else new RunLengthEncoding
    case _ => throw new UnsupportedOperationException(
      s"RunLengthEncoding not supported for $dataType")
  }

  private[columnar] def createDictionaryEncoding(columnBytes: AnyRef,
      dataType: DataType, nullable: Boolean): ColumnEncoding = dataType match {
    case StringType | IntegerType | DateType | LongType | TimestampType =>
      if (nullable) new DictionaryEncodingNullable
      else new DictionaryEncoding
    case _ => throw new UnsupportedOperationException(
      s"DictionaryEncoding not supported for $dataType")
  }

  private[columnar] def createBooleanBitSetEncoding(columnBytes: AnyRef,
      dataType: DataType, nullable: Boolean): ColumnEncoding = dataType match {
    case BooleanType =>
      if (nullable) new BooleanBitSetEncodingNullable
      else new BooleanBitSetEncoding
    case _ => throw new UnsupportedOperationException(
      s"BooleanBitSetEncoding not supported for $dataType")
  }

  private[columnar] def createIntDeltaEncoding(columnBytes: AnyRef,
      dataType: DataType, nullable: Boolean): ColumnEncoding = dataType match {
    case IntegerType | DateType =>
      if (nullable) new IntDeltaEncodingNullable else new IntDeltaEncoding
    case _ => throw new UnsupportedOperationException(
      s"IntDeltaEncoding not supported for $dataType")
  }

  private[columnar] def createLongDeltaEncoding(columnBytes: AnyRef,
      dataType: DataType, nullable: Boolean): ColumnEncoding = dataType match {
    case LongType | TimestampType =>
      if (nullable) new LongDeltaEncodingNullable else new LongDeltaEncoding
    case _ => throw new UnsupportedOperationException(
      s"LongDeltaEncoding not supported for $dataType")
  }

  private[columnar] final def readShort(columnBytes: AnyRef,
      cursor: Long): Int = if (littleEndian) {
    Platform.getShort(columnBytes, cursor)
  } else {
    java.lang.Short.reverseBytes(Platform.getShort(columnBytes, cursor))
  }

  private[columnar] final def readInt(columnBytes: AnyRef,
      cursor: Long): Int = if (littleEndian) {
    Platform.getInt(columnBytes, cursor)
  } else {
    java.lang.Integer.reverseBytes(Platform.getInt(columnBytes, cursor))
  }

  private[columnar] final def readLong(columnBytes: AnyRef,
      cursor: Long): Long = if (littleEndian) {
    Platform.getLong(columnBytes, cursor)
  } else {
    java.lang.Long.reverseBytes(Platform.getLong(columnBytes, cursor))
  }

  private[columnar] final def readUTF8String(columnBytes: AnyRef,
      cursor: Long): UTF8String = {
    val size = readInt(columnBytes, cursor)
    UTF8String.fromAddress(columnBytes, cursor + 4, size)
  }
}

trait NotNullColumn extends ColumnEncoding {

  override protected final def initializeNulls(
      columnBytes: AnyRef, field: Attribute): Long = {
    val cursor = Platform.BYTE_ARRAY_OFFSET
    val numNullValues = ColumnEncoding.readInt(columnBytes, cursor)
    if (numNullValues != 0) {
      throw new IllegalStateException(
        s"$numNullValues null values found in NOT NULL column $field")
    }
    cursor + 4 // skip numNullValues
  }

  override final def notNull(columnBytes: AnyRef, ordinal: Int): Int = 1
}

trait NullableColumn extends ColumnEncoding {

  protected final var nullValues: Array[Byte] = _
  // protected final var nullValuesBitSet: BitSet = _
  protected final var nextNullOrdinal = 0
  protected final var nextNullCursor = 0
  protected final var nextNullCursorEnd = 0
  // protected final var nullValuesType: Byte = 0

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

  override protected final def initializeNulls(
      columnBytes: AnyRef, field: Attribute): Long = {
    val cursor = Platform.BYTE_ARRAY_OFFSET
    val nullValuesSize = ColumnEncoding.readInt(columnBytes, cursor) << 2
    if (nullValuesSize > 0) {
      // copying instead of keeping pointer will help in better cache alignment
      nullValues = new Array[Byte](nullValuesSize)
      Platform.copyMemory(columnBytes, cursor + 4, nullValues,
        Platform.BYTE_ARRAY_OFFSET, nullValuesSize)
      nextNullCursor = Platform.BYTE_ARRAY_OFFSET
      nextNullCursorEnd = nextNullCursor + nullValuesSize
      updateNextNullOrdinal()
    } else nextNullOrdinal = -1
    cursor + 4 + nullValuesSize
    /*
    nullValuesType = columnBytes(0)
    cursor = Platform.BYTE_ARRAY_OFFSET + 1
    // copying instead of keeping pointer will help in better cache alignment
    // since these are at the start of the byte array
    if (nullValuesType <= 1) {
      nullValues = readBinary(columnBytes, 0)
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

  override final def notNull(columnBytes: AnyRef, ordinal: Int): Int = {
    if (ordinal != nextNullOrdinal) 1
    else {
      updateNextNullOrdinal()
      0
    }
  }
}
