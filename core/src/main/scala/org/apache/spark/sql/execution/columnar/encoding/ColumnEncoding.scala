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

import io.snappydata.util.{BitSet, StringUtils}

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, UnsafeArrayData, UnsafeMapData, UnsafeRow}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.collection.BitSet

/**
 * Base class for encoding and decoding in columnar form. Memory layout of
 * the bytes for a set of column values is:
 * {{{
 *   .----------------------- Encoding scheme (4 bytes)
 *   |   .------------------- Null bitset size as number of longs N (4 bytes)
 *   |   |   .--------------- Null bitset longs (8 x N bytes,
 *                                               empty if null count is zero)
 *   |   |   |     .--------- Encoded non-null elements
 *   V   V   V     V
 *   +---+---+-----+---------+
 *   |   |   | ... | ... ... |
 *   +---+---+-----+---------+
 *    \-----/ \-------------/
 *     header      body
 * }}}
 */
trait ColumnEncoding {

  def typeId: Int

  def supports(dataType: DataType): Boolean
}

// TODO: SW: check perf after removing the columnBytes argument to decoders
// if its same, then remove since it will help free up many registers
abstract class ColumnDecoder extends ColumnEncoding {

  protected def hasNulls: Boolean

  protected def initializeNulls(columnBytes: AnyRef,
      cursor: Long, field: StructField): Long

  protected def initializeCursor(columnBytes: AnyRef, cursor: Long,
      field: StructField): Long

  def initialize(columnBytes: Array[Byte], field: StructField): Long = {
    initialize(columnBytes, Platform.BYTE_ARRAY_OFFSET, field)
  }

  def initialize(columnBytes: AnyRef, cursor: Long,
      field: StructField): Long = {
    initializeCursor(columnBytes,
      initializeNulls(columnBytes, cursor, field), field)
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

  def getStringDictionary: Array[UTF8String] = null

  def readDictionaryIndex(columnBytes: AnyRef, cursor: Long): Int = -1

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
}

trait ColumnEncoder extends ColumnEncoding {

  protected final var allocator: ColumnAllocator = _
  protected final var columnData: ColumnData = _
  protected final var columnBytes: AnyRef = _
  protected final var reuseColumnData: ColumnData = _
  protected final var reuseUsedSize: Int = _

  protected final var _lowerLong: Long = _
  protected final var _upperLong: Long = _
  protected final var _lowerDouble: Double = _
  protected final var _upperDouble: Double = _
  protected final var _lowerStr: UTF8String = _
  protected final var _upperStr: UTF8String = _
  protected final var _lowerDecimal: Decimal = _
  protected final var _upperDecimal: Decimal = _

  def sizeInBytes(cursor: Long): Long = cursor - columnData.baseOffset

  def defaultSize(dataType: DataType): Int = dataType match {
    case CalendarIntervalType => 12 // uses 12 and not 16 bytes
    case _ => dataType.defaultSize
  }

  protected def initializeNulls(initSize: Int): Int

  final def initialize(field: StructField, initSize: Int): Long = {
    initialize(field, initSize, HeapAllocator)
  }

  protected def initializeLimits(): Unit = {
    _lowerLong = Long.MaxValue
    _upperLong = Long.MinValue
    _lowerDouble = Double.MaxValue
    _upperDouble = Double.MinValue
    _lowerStr = null
    _upperStr = null
    _lowerDecimal = null
    _upperDecimal = null
  }

  def initialize(field: StructField, initSize: Int,
      allocator: ColumnAllocator): Long = {
    this.allocator = allocator
    val dataType = Utils.getSQLDataType(field.dataType)
    val defSize = defaultSize(dataType)

    // initialize the lower and upper limits
    initializeLimits()

    val numNullBytes = initializeNulls(initSize)
    if (reuseColumnData eq null) {
      val initByteSize = 8L /* typeId + nullsSize */ + defSize.toLong * initSize
      columnData = allocator.allocate(initByteSize)
      columnBytes = columnData.bytes
    } else {
      // for primitive types optimistically trim to exact size
      dataType match {
        case BooleanType | ByteType | ShortType | IntegerType | LongType |
             DateType | TimestampType | FloatType | DoubleType
          if reuseUsedSize != reuseColumnData.sizeInBytes =>
          columnData = allocator.allocate(reuseUsedSize)
          columnBytes = columnData.bytes
          allocator.release(reuseColumnData)

        case _ =>
          columnData = reuseColumnData
          columnBytes = reuseColumnData.bytes
      }
      reuseColumnData = null
      reuseUsedSize = 0
    }
    var cursor = columnData.baseOffset
    // typeId followed by nulls bitset size and space for values
    ColumnEncoding.writeInt(columnBytes, cursor, typeId)
    cursor += 4
    // write the number of null bytes
    ColumnEncoding.writeInt(columnBytes, cursor, numNullBytes)
    cursor + 4 + numNullBytes
  }

  /** Expand the underlying bytes if required and return the new cursor */
  protected final def expand(cursor: Long, required: Long): Long = {
    val numWritten = cursor - columnData.baseOffset
    columnData = allocator.expand(columnData, cursor, required)
    columnBytes = columnData.bytes
    columnData.baseOffset + numWritten
  }

  final def lowerLong: Long = _lowerLong

  final def upperLong: Long = _upperLong

  final def lowerDouble: Double = _lowerDouble

  final def upperDouble: Double = _upperDouble

  final def lowerDecimal: Decimal = _lowerDecimal

  final def upperDecimal: Decimal = _upperDecimal

  final def lowerString: UTF8String = _lowerStr

  final def upperString: UTF8String = _upperStr

  @inline protected final def updateLongStats(value: Long): Unit = {
    val lower = _lowerLong
    if (value < lower) {
      _lowerLong = value
      // check for first write case
      if (lower == Long.MaxValue) _upperLong = value
    } else if (value > _upperLong) {
      _upperLong = value
    }
  }

  @inline protected final def updateDoubleStats(value: Double): Unit = {
    val lower = _lowerDouble
    if (value < lower) {
      // check for first write case
      if (lower == Double.MaxValue) _upperDouble = value
      _lowerDouble = value
    } else if (value > _upperDouble) {
      _upperDouble = value
    }
  }

  @inline protected final def updateStringStats(value: UTF8String): Unit = {
    if (value ne null) {
      val lower = _lowerStr
      // check for first write case
      if (lower eq null) {
        _lowerStr = value
        _upperStr = value
      } else if (value.compare(lower) < 0) {
        _lowerStr = value
      } else if (value.compare(_upperStr) > 0) {
        _upperStr = value
      }
    }
  }

  @inline protected final def updateStringStatsClone(value: UTF8String): Unit = {
    if (value ne null) {
      val lower = _lowerStr
      // check for first write case
      if (lower eq null) {
        val valueClone = StringUtils.cloneIfRequired(value)
        _lowerStr = valueClone
        _upperStr = valueClone
      } else if (value.compare(lower) < 0) {
        _lowerStr = StringUtils.cloneIfRequired(value)
      } else if (value.compare(_upperStr) > 0) {
        _upperStr = StringUtils.cloneIfRequired(value)
      }
    }
  }

  @inline protected final def updateDecimalStats(value: Decimal): Unit = {
    if (value ne null) {
      val lower = _lowerDecimal
      // check for first write case
      if (lower eq null) {
        _lowerDecimal = value
        _upperDecimal = value
      } else if (value.compare(lower) < 0) {
        _lowerDecimal = value
      } else if (value.compare(_upperDecimal) > 0) {
        _upperDecimal = value
      }
    }
  }

  def nullCount: Int

  def writeIsNull(ordinal: Int): Unit

  def writeBoolean(cursor: Long, value: Boolean): Long =
    throw new UnsupportedOperationException(s"writeBoolean for $toString")

  def writeByte(cursor: Long, value: Byte): Long =
    throw new UnsupportedOperationException(s"writeByte for $toString")

  def writeShort(cursor: Long, value: Short): Long =
    throw new UnsupportedOperationException(s"writeShort for $toString")

  def writeInt(cursor: Long, value: Int): Long =
    throw new UnsupportedOperationException(s"writeInt for $toString")

  def writeLong(cursor: Long, value: Long): Long =
    throw new UnsupportedOperationException(s"writeLong for $toString")

  def writeFloat(cursor: Long, value: Float): Long =
    throw new UnsupportedOperationException(s"writeFloat for $toString")

  def writeDouble(cursor: Long, value: Double): Long =
    throw new UnsupportedOperationException(s"writeDouble for $toString")

  def writeLongDecimal(cursor: Long, value: Decimal,
      precision: Int, scale: Int): Long =
    throw new UnsupportedOperationException(s"writeLongDecimal for $toString")

  def writeDecimal(cursor: Long, value: Decimal,
      precision: Int, scale: Int): Long =
    throw new UnsupportedOperationException(s"writeDecimal for $toString")

  def writeDate(cursor: Long, value: Int): Long =
    writeInt(cursor, value)

  def writeTimestamp(cursor: Long, value: Long): Long =
    writeLong(cursor, value)

  def writeInterval(cursor: Long, value: CalendarInterval): Long =
    throw new UnsupportedOperationException(s"writeInterval for $toString")

  def writeUTF8String(cursor: Long, value: UTF8String): Long =
    throw new UnsupportedOperationException(s"writeUTF8String for $toString")

  def writeBinary(cursor: Long, value: Array[Byte]): Long =
    throw new UnsupportedOperationException(s"writeBinary for $toString")

  def writeArray(cursor: Long, value: UnsafeArrayData): Long =
    throw new UnsupportedOperationException(s"writeArray for $toString")

  def writeMap(cursor: Long, value: UnsafeMapData): Long =
    throw new UnsupportedOperationException(s"writeMap for $toString")

  def writeStruct(cursor: Long, value: UnsafeRow): Long =
    throw new UnsupportedOperationException(s"writeStruct for $toString")

  final def finish(cursor: Long): AnyRef = finish(cursor, 0L)

  protected def writeNulls(columnBytes: AnyRef, cursor: Long): Long

  protected def finish(cursor: Long, offset: Long): AnyRef

  protected final def releaseForReuse(columnData: ColumnData,
      newSize: Long): Unit = {
    if (reuseColumnData ne null) {
      allocator.release(reuseColumnData)
    }
    if (newSize <= Int.MaxValue) {
      reuseColumnData = columnData
      reuseUsedSize = newSize.toInt
    } else {
      reuseColumnData = null
      reuseUsedSize = 0
    }
  }
}

object ColumnEncoding {

  private[columnar] val bitSetWords: Field = {
    val f = classOf[BitSet].getDeclaredField("words")
    f.setAccessible(true)
    f
  }

  private[columnar] val BITS_PER_LONG = 64

  val littleEndian: Boolean = ByteOrder.nativeOrder == ByteOrder.LITTLE_ENDIAN

  val allDecoders: Array[(DataType, Boolean) => ColumnDecoder] = Array(
    createUncompressedDecoder,
    createRunLengthDecoder,
    createDictionaryDecoder,
    createBigDictionaryDecoder,
    createBooleanBitSetDecoder,
    createIntDeltaDecoder,
    createLongDeltaDecoder
  )

  def getColumnDecoder(columnBytes: Array[Byte],
      field: StructField): ColumnDecoder = {
    getColumnDecoder(columnBytes, Platform.BYTE_ARRAY_OFFSET, field)
  }

  def getColumnDecoder(columnBytes: AnyRef, offset: Long,
      field: StructField): ColumnDecoder = {
    // typeId at the start followed by null bit set values
    var cursor = offset
    val typeId = readInt(columnBytes, cursor)
    cursor += 4
    val dataType = Utils.getSQLDataType(field.dataType)
    if (typeId >= allDecoders.length) {
      val bytesStr = columnBytes match {
        case null => ""
        case bytes: Array[Byte] => s" bytes: ${bytes.toSeq}"
        case _ => ""
      }
      throw new IllegalStateException(s"Unknown encoding typeId = $typeId " +
          s"for $dataType($field)$bytesStr")
    }

    val numNullWords = readInt(columnBytes, cursor)
    val decoder = allDecoders(typeId)(dataType,
      // use NotNull version if field is marked so or no nulls in the batch
      field.nullable && numNullWords > 0)
    if (decoder.typeId != typeId) {
      throw new IllegalStateException(s"typeId for $decoder = " +
          s"${decoder.typeId} does not match $typeId in global registration")
    }
    if (!decoder.supports(dataType)) {
      throw new IllegalStateException("Encoder bug? Unsupported type " +
          s"$dataType for $decoder")
    }
    decoder
  }

  def getColumnEncoder(field: StructField): ColumnEncoder = {
    // TODO: SW: Only uncompressed + dictionary encoding for a start.
    // Need to add RunLength and BooleanBitSet by default (others on explicit
    //    compression level with LZ4/LZF for binary/complex data)
    Utils.getSQLDataType(field.dataType) match {
      case StringType => createDictionaryEncoder(StringType, field.nullable)
      case dataType => createUncompressedEncoder(dataType, field.nullable)
    }
  }

  private[columnar] def createUncompressedDecoder(dataType: DataType,
      nullable: Boolean): ColumnDecoder =
    if (nullable) new UncompressedDecoderNullable else new UncompressedDecoder

  private[columnar] def createRunLengthDecoder(dataType: DataType,
      nullable: Boolean): ColumnDecoder = dataType match {
    case BooleanType | ByteType | ShortType |
         IntegerType | DateType | LongType | TimestampType | StringType =>
      if (nullable) new RunLengthDecoderNullable else new RunLengthDecoder
    case _ => throw new UnsupportedOperationException(
      s"RunLengthDecoder not supported for $dataType")
  }

  private[columnar] def createDictionaryDecoder(dataType: DataType,
      nullable: Boolean): ColumnDecoder = dataType match {
    case StringType | IntegerType | DateType | LongType | TimestampType =>
      if (nullable) new DictionaryDecoderNullable
      else new DictionaryDecoder
    case _ => throw new UnsupportedOperationException(
      s"DictionaryDecoder not supported for $dataType")
  }

  private[columnar] def createBigDictionaryDecoder(dataType: DataType,
      nullable: Boolean): ColumnDecoder = dataType match {
    case StringType | IntegerType | DateType | LongType | TimestampType =>
      if (nullable) new BigDictionaryDecoderNullable
      else new BigDictionaryDecoder
    case _ => throw new UnsupportedOperationException(
      s"BigDictionaryDecoder not supported for $dataType")
  }

  private[columnar] def createBooleanBitSetDecoder(dataType: DataType,
      nullable: Boolean): ColumnDecoder = dataType match {
    case BooleanType =>
      if (nullable) new BooleanBitSetDecoderNullable
      else new BooleanBitSetDecoder
    case _ => throw new UnsupportedOperationException(
      s"BooleanBitSetDecoder not supported for $dataType")
  }

  private[columnar] def createIntDeltaDecoder(dataType: DataType,
      nullable: Boolean): ColumnDecoder = dataType match {
    case IntegerType | DateType =>
      if (nullable) new IntDeltaDecoderNullable else new IntDeltaDecoder
    case _ => throw new UnsupportedOperationException(
      s"IntDeltaDecoder not supported for $dataType")
  }

  private[columnar] def createLongDeltaDecoder(dataType: DataType,
      nullable: Boolean): ColumnDecoder = dataType match {
    case LongType | TimestampType =>
      if (nullable) new LongDeltaDecoderNullable else new LongDeltaDecoder
    case _ => throw new UnsupportedOperationException(
      s"LongDeltaDecoder not supported for $dataType")
  }

  private[columnar] def createUncompressedEncoder(dataType: DataType,
      nullable: Boolean): ColumnEncoder = {
    if (nullable) new UncompressedEncoderNullable else new UncompressedEncoder
  }

  private[columnar] def createDictionaryEncoder(dataType: DataType,
      nullable: Boolean): ColumnEncoder = dataType match {
    case StringType | IntegerType | DateType | LongType | TimestampType =>
      if (nullable) new DictionaryEncoderNullable else new DictionaryEncoder
    case _ => throw new UnsupportedOperationException(
      s"DictionaryEncoder not supported for $dataType")
  }

  @inline private[columnar] final def readShort(columnBytes: AnyRef,
      cursor: Long): Int = if (littleEndian) {
    Platform.getShort(columnBytes, cursor)
  } else {
    java.lang.Short.reverseBytes(Platform.getShort(columnBytes, cursor))
  }

  @inline private[columnar] final def readInt(columnBytes: AnyRef,
      cursor: Long): Int = if (littleEndian) {
    Platform.getInt(columnBytes, cursor)
  } else {
    java.lang.Integer.reverseBytes(Platform.getInt(columnBytes, cursor))
  }

  @inline private[columnar] final def readLong(columnBytes: AnyRef,
      cursor: Long): Long = if (littleEndian) {
    Platform.getLong(columnBytes, cursor)
  } else {
    java.lang.Long.reverseBytes(Platform.getLong(columnBytes, cursor))
  }

  @inline private[columnar] final def readUTF8String(columnBytes: AnyRef,
      cursor: Long): UTF8String = {
    val size = readInt(columnBytes, cursor)
    UTF8String.fromAddress(columnBytes, cursor + 4, size)
  }

  @inline private[columnar] final def writeShort(columnBytes: AnyRef,
      cursor: Long, value: Short): Unit = if (littleEndian) {
    Platform.putShort(columnBytes, cursor, value)
  } else {
    Platform.putShort(columnBytes, cursor, java.lang.Short.reverseBytes(value))
  }

  @inline private[columnar] final def writeInt(columnBytes: AnyRef,
      cursor: Long, value: Int): Unit = if (littleEndian) {
    Platform.putInt(columnBytes, cursor, value)
  } else {
    Platform.putInt(columnBytes, cursor, java.lang.Integer.reverseBytes(value))
  }

  @inline private[columnar] final def writeLong(columnBytes: AnyRef,
      cursor: Long, value: Long): Unit = if (littleEndian) {
    Platform.putLong(columnBytes, cursor, value)
  } else {
    Platform.putLong(columnBytes, cursor, java.lang.Long.reverseBytes(value))
  }

  @inline private[columnar] final def writeUTF8String(columnBytes: AnyRef,
      cursor: Long, value: UTF8String, size: Int): Long = {
    ColumnEncoding.writeInt(columnBytes, cursor, size)
    val position = cursor + 4
    Platform.copyMemory(value.getBaseObject, value.getBaseOffset, columnBytes,
      position, size)
    position + size
  }
}

private[columnar] case class ColumnStatsSchema(fieldName: String,
    dataType: DataType) {
  val upperBound: AttributeReference = AttributeReference(
    fieldName + ".upperBound", dataType)()
  val lowerBound: AttributeReference = AttributeReference(
    fieldName + ".lowerBound", dataType)()
  val nullCount: AttributeReference = AttributeReference(
    fieldName + ".nullCount", IntegerType, nullable = false)()
  val count: AttributeReference = AttributeReference(
    fieldName + ".count", IntegerType, nullable = false)()

  val schema = Seq(lowerBound, upperBound, nullCount, count)
}


final class ColumnData(val bytes: AnyRef, val baseOffset: Long,
    val endPosition: Long) {

  def sizeInBytes: Long = endPosition - baseOffset
}

trait ColumnAllocator {

  /** Allocate data block of given size. */
  def allocate(size: Long): ColumnData

  /**
   * Expand given column data to new capacity.
   *
   * @return the new expanded column byte object and the end position
   *         (baseOffset + capacity)
   */
  def expand(columnData: ColumnData, cursor: Long, required: Long): ColumnData

  /**
   * Copies data from the specified source data holder, beginning at the
   * specified position, to the specified position of the destination data
   * holder. Both source and destination data holders should have been
   * allocated by instances of the same ColumnAllocator implementation.
   */
  def copy(source: AnyRef, sourcePos: Long,
      dest: AnyRef, destPos: Long, size: Long): Unit

  /** Release data block allocated previously using allocate or expand. */
  def release(columnData: ColumnData): Unit
}

object HeapAllocator extends ColumnAllocator {

  private def baseOffset: Long = Platform.BYTE_ARRAY_OFFSET

  private def checkSize(size: Long): Int = {
    if (size < Int.MaxValue) size.toInt
    else {
      throw new ArrayIndexOutOfBoundsException(
        s"Invalid size/index = $size. Max allowed = ${Int.MaxValue - 1}.")
    }
  }

  override def allocate(size: Long): ColumnData = {
    new ColumnData(new Array[Byte](checkSize(size)), baseOffset,
      baseOffset + size)
  }

  override def expand(columnData: ColumnData, cursor: Long,
      required: Long): ColumnData = {
    val columnBytes = columnData.bytes.asInstanceOf[Array[Byte]]
    val currentUsed = cursor - baseOffset
    val minRequired = currentUsed + required
    // double the size
    val newLength = math.min(math.max(columnBytes.length << 1L,
      minRequired), Int.MaxValue).asInstanceOf[Int]
    if (newLength < minRequired) {
      throw new ArrayIndexOutOfBoundsException(
        s"Cannot allocate more than $newLength bytes but required $minRequired")
    }
    val newBytes = new Array[Byte](newLength)
    System.arraycopy(columnBytes, 0, newBytes, 0, currentUsed.toInt)
    new ColumnData(newBytes, baseOffset, newLength + baseOffset)
  }

  override def copy(source: AnyRef, sourcePos: Long,
      dest: AnyRef, destPos: Long, size: Long): Unit = {
    System.arraycopy(source, checkSize(sourcePos - baseOffset), dest,
      checkSize(destPos - baseOffset), checkSize(size))
  }

  override def release(columnData: ColumnData): Unit = {}
}

trait NotNullDecoder extends ColumnDecoder {

  override protected final def hasNulls: Boolean = false

  protected def initializeNulls(columnBytes: AnyRef,
      cursor: Long, field: StructField): Long = {
    val numNullWords = ColumnEncoding.readInt(columnBytes, cursor + 4)
    if (numNullWords != 0) {
      throw new IllegalStateException(
        s"Nulls bitset of size $numNullWords found in NOT NULL column $field")
    }
    cursor + 8 // skip typeId and nullValuesSize
  }

  override final def notNull(columnBytes: AnyRef, ordinal: Int): Int = 1
}

trait NullableDecoder extends ColumnDecoder {

  protected final var nullWords: Array[Long] = _
  protected final var nextNullOrdinal = 0

  override protected final def hasNulls: Boolean = true

  private final def updateNextNullOrdinal() {
    nextNullOrdinal = BitSet.nextSetBit(nullWords, nextNullOrdinal)
  }

  protected def initializeNulls(columnBytes: AnyRef,
      cursor: Long, field: StructField): Long = {
    var position = cursor + 4
    // skip typeId
    val numNullWords = ColumnEncoding.readInt(columnBytes, position)
    assert(numNullWords > 0,
      s"Expected valid null values but got length = $numNullWords")
    // copying instead of keeping pointer will help in better cache pipeline
    position += 4
    nullWords = new Array[Long](numNullWords)
    var index = 0
    while (index < numNullWords) {
      nullWords(index) = ColumnEncoding.readLong(columnBytes, position)
      position += 8
      index += 1
    }
    updateNextNullOrdinal()
    position
  }

  override final def notNull(columnBytes: AnyRef, ordinal: Int): Int = {
    if (ordinal != nextNullOrdinal) 1
    else {
      updateNextNullOrdinal()
      0
    }
  }
}

trait NotNullEncoder extends ColumnEncoder {

  override protected def initializeNulls(initSize: Int): Int = 0

  override def nullCount: Int = 0

  override def writeIsNull(ordinal: Int): Unit =
    throw new UnsupportedOperationException(s"writeIsNull for $toString")

  override protected def writeNulls(columnBytes: AnyRef, cursor: Long): Long = {
    cursor
  }

  override protected def finish(cursor: Long, offset: Long): AnyRef = {
    val requiredEndPos = cursor + offset
    if (requiredEndPos == columnData.endPosition) columnBytes
    else {
      // copy to exact size
      val newSize = requiredEndPos - columnData.baseOffset
      val newColumnData = allocator.allocate(newSize)
      val newColumnBytes = newColumnData.bytes
      // using safe copy for heap data to get proper bounds exception
      allocator.copy(columnBytes, columnData.baseOffset, newColumnBytes,
        newColumnData.baseOffset, cursor - columnData.baseOffset)
      // reuse this columnData in next round if possible
      releaseForReuse(columnData, newSize)
      columnData = newColumnData
      columnBytes = newColumnBytes
      columnBytes
    }
  }
}

trait NullableEncoder extends NotNullEncoder {

  protected final var maxNulls: Long = _
  protected final var nullWords: Array[Long] = _
  protected final var initialNumWords: Int = _

  @inline private def getNumWords(nullWords: Array[Long]): Int = {
    var numWords = nullWords.length
    while (numWords > 0 && nullWords(numWords - 1) == 0L) numWords -= 1
    numWords
  }

  override protected def initializeNulls(initSize: Int): Int = {
    if (nullWords eq null) {
      val numWords = BitSet.numWords(initSize)
      maxNulls = numWords.toLong << 6L
      nullWords = new Array[Long](numWords)
      initialNumWords = numWords
      numWords << 3
    } else {
      // trim trailing empty words
      val numWords = getNumWords(nullWords)
      initialNumWords = numWords
      // clear rest of the words
      var i = 0
      while (i < numWords) {
        nullWords(i) = 0L
        i += 1
      }
      numWords << 3
    }
  }

  override def nullCount: Int = BitSet.cardinality(nullWords)

  override def writeIsNull(ordinal: Int): Unit = {
    if (ordinal < maxNulls) {
      BitSet.set(nullWords, ordinal)
    } else {
      // expand
      val oldNulls = nullWords
      val oldLen = oldNulls.length
      val newLen = oldLen << 1
      nullWords = new Array[Long](newLen)
      maxNulls = newLen << 6L
      System.arraycopy(oldNulls, 0, nullWords, 0, oldLen)
      BitSet.set(nullWords, ordinal)
    }
  }

  override protected def writeNulls(columnBytes: AnyRef, cursor: Long): Long = {
    // trim trailing empty words
    val nullWords = this.nullWords
    val numWords = getNumWords(nullWords)
    writeNulls(columnBytes, cursor, numWords)
  }

  private def writeNulls(columnBytes: AnyRef, cursor: Long,
      numWords: Int): Long = {
    var position = cursor
    var index = 0
    while (index < numWords) {
      ColumnEncoding.writeLong(columnBytes, position, nullWords(index))
      position += 8
      index += 1
    }
    position
  }

  override protected def finish(cursor: Long, offset: Long): AnyRef = {
    // trim trailing empty words
    val nullWords = this.nullWords
    val numWords = getNumWords(nullWords)
    // check if the number of words to be written matches the space that
    // was left at initialization; as an optimization allow for larger
    // space left at initialization when one full data copy can be avoided
    val baseOffset = columnData.baseOffset
    if (initialNumWords == numWords) {
      writeNulls(columnBytes, baseOffset + 8, numWords)
      super.finish(cursor, offset)
    } else if (initialNumWords > numWords &&
        cursor + offset == columnData.endPosition) {
      // write till initialNumWords and not just numWords to clear any
      // trailing empty bytes (required since ColumnData can be reused)
      writeNulls(columnBytes, baseOffset + 8, initialNumWords)
      columnBytes
    } else {
      // make space for writing nulls at the start (for efficient decoding)
      val numNullBytes = numWords << 3
      val initialNullBytes = initialNumWords << 3
      val oldSize = cursor - baseOffset
      val newSize = oldSize + offset + numNullBytes - initialNullBytes
      val newColumnData = allocator.allocate(newSize)
      val newColumnBytes = newColumnData.bytes
      var position = newColumnData.baseOffset
      ColumnEncoding.writeInt(newColumnBytes, position, typeId)
      position += 4
      ColumnEncoding.writeInt(newColumnBytes, position, numNullBytes)
      position += 4
      // copy the rest of bytes
      allocator.copy(columnBytes, baseOffset + 8 + initialNullBytes,
        newColumnBytes, position + numNullBytes, oldSize - 8 - initialNullBytes)

      // reuse this columnData in next round if possible
      releaseForReuse(columnData, newSize)
      columnData = newColumnData
      columnBytes = newColumnBytes

      // write the null words
      writeNulls(newColumnBytes, position, numWords)
      newColumnBytes
    }
  }
}
