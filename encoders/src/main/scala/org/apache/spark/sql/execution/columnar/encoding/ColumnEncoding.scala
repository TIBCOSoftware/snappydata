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

import java.nio.{ByteBuffer, ByteOrder}

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.gemstone.gemfire.internal.shared.unsafe.DirectBufferAllocator
import com.gemstone.gemfire.internal.shared.{BufferAllocator, ClientSharedUtils, HeapBufferAllocator}
import io.snappydata.util.StringUtils

import org.apache.spark.memory.MemoryManagerCallback.memoryManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.UnsafeRow.calculateBitSetWidthInBytes
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.execution.columnar.encoding.ColumnEncoding.checkBufferSize
import org.apache.spark.sql.sources.JdbcExtendedUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Base class for encoding and decoding in columnar form. Memory layout of
 * the bytes for a set of column values is:
 * {{{
 *    .----------------------- Encoding scheme (4 bytes)
 *   |    .------------------- Null bitset size as number of longs N (4 bytes)
 *   |   |
 *   |   |   .---------------- Null bitset longs (8 x N bytes,
 *   |   |   |                                    empty if null count is zero)
 *   |   |   |     .---------- Encoded non-null elements
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

// Removing the columnBytes argument to decoders (and storing within)
// results in significant deterioration in basic ColumnCacheBenchmark.
abstract class ColumnDecoder(columnDataRef: AnyRef, startCursor: Long,
    field: StructField, initDelta: (AnyRef, Long) => Long = ColumnEncoding.identityLong)
    extends ColumnEncoding {

  protected[sql] final val baseCursor: Long = {
    // initDelta is null only in tests
    if (startCursor != 0L && (initDelta ne null)) {
      initializeCursor(columnDataRef, initDelta(columnDataRef,
        initializeNulls(columnDataRef, startCursor, field)), field.dataType)
    } else startCursor
  }

  /** Used by some decoders to track the current sequential cursor. */
  protected final var currentCursor: Long = _

  /**
   * Not used by decoders themselves but by delta writer that stores the
   * current number of nulls and nonNullPosition for the decoder.
   * Initialized to -1 so that first increment starts at 0.
   */
  protected[sql] final var numNulls: Int = _
  protected[sql] final var nonNullPosition: Int = -1

  protected[sql] def hasNulls: Boolean

  protected[sql] def initializeNulls(columnBytes: AnyRef,
      startCursor: Long, field: StructField): Long

  protected[sql] def initializeCursor(columnBytes: AnyRef, cursor: Long,
      dataType: DataType): Long

  /**
   * Return the next position of a null value.
   */
  def getNextNullPosition: Int

  /**
   * Move to next null position >= given ordinal and return number of nulls so far.
   */
  def moveToNextNull(columnBytes: AnyRef, ordinal: Int, numNulls: Int): Int

  def readBoolean(columnBytes: AnyRef, nonNullPosition: Int): Boolean =
    throw new UnsupportedOperationException(s"readBoolean for $toString")

  def readByte(columnBytes: AnyRef, nonNullPosition: Int): Byte =
    throw new UnsupportedOperationException(s"readByte for $toString")

  def readShort(columnBytes: AnyRef, nonNullPosition: Int): Short =
    throw new UnsupportedOperationException(s"readShort for $toString")

  def readInt(columnBytes: AnyRef, nonNullPosition: Int): Int =
    throw new UnsupportedOperationException(s"readInt for $toString")

  def readLong(columnBytes: AnyRef, nonNullPosition: Int): Long =
    throw new UnsupportedOperationException(s"readLong for $toString")

  def readFloat(columnBytes: AnyRef, nonNullPosition: Int): Float =
    throw new UnsupportedOperationException(s"readFloat for $toString")

  def readDouble(columnBytes: AnyRef, nonNullPosition: Int): Double =
    throw new UnsupportedOperationException(s"readDouble for $toString")

  def readLongDecimal(columnBytes: AnyRef, precision: Int, scale: Int,
      nonNullPosition: Int): Decimal =
    throw new UnsupportedOperationException(s"readLongDecimal for $toString")

  def readDecimal(columnBytes: AnyRef, precision: Int, scale: Int,
      nonNullPosition: Int): Decimal =
    throw new UnsupportedOperationException(s"readDecimal for $toString")

  def readUTF8String(columnBytes: AnyRef, nonNullPosition: Int): UTF8String =
    throw new UnsupportedOperationException(s"readUTF8String for $toString")

  def getStringDictionary: StringDictionary = null

  def readDictionaryIndex(columnBytes: AnyRef, nonNullPosition: Int): Int = -1

  def readDate(columnBytes: AnyRef, nonNullPosition: Int): Int =
    readInt(columnBytes, nonNullPosition)

  def readTimestamp(columnBytes: AnyRef, nonNullPosition: Int): Long =
    readLong(columnBytes, nonNullPosition)

  def readInterval(columnBytes: AnyRef, nonNullPosition: Int): CalendarInterval =
    throw new UnsupportedOperationException(s"readInterval for $toString")

  def readBinary(columnBytes: AnyRef, nonNullPosition: Int): Array[Byte] =
    throw new UnsupportedOperationException(s"readBinary for $toString")

  def readArray(columnBytes: AnyRef, nonNullPosition: Int): ArrayData =
    throw new UnsupportedOperationException(s"readArray for $toString")

  def readMap(columnBytes: AnyRef, nonNullPosition: Int): MapData =
    throw new UnsupportedOperationException(s"readMap for $toString")

  def readStruct(columnBytes: AnyRef, numFields: Int,
      nonNullPosition: Int): InternalRow =
    throw new UnsupportedOperationException(s"readStruct for $toString")

  /**
   * Close and relinquish all resources of this encoder.
   * The encoder may no longer be usable after this call.
   */
  def close(): Unit = {}
}

trait ColumnEncoder extends ColumnEncoding {

  protected[sql] final var allocator: BufferAllocator = _
  private final var finalAllocator: BufferAllocator = _
  protected[sql] final var columnData: ByteBuffer = _
  protected[sql] final var columnBeginPosition: Long = _
  protected[sql] final var columnEndPosition: Long = _
  protected[sql] final var columnBytes: AnyRef = _
  protected[sql] final var reuseUsedSize: Int = _
  protected final var forComplexType: Boolean = _

  protected final var _lowerLong: Long = _
  protected final var _upperLong: Long = _
  protected final var _lowerDouble: Double = _
  protected final var _upperDouble: Double = _
  protected final var _lowerStr: UTF8String = _
  protected final var _upperStr: UTF8String = _
  protected final var _lowerDecimal: Decimal = _
  protected final var _upperDecimal: Decimal = _

  /**
   * Get the allocator for the final data to be sent for storage.
   * It is on-heap for now in embedded mode while off-heap for
   * connector mode to minimize copying in both cases.
   * This should be changed to use the matching allocator as per the
   * storage being used by column store in embedded mode.
   */
  protected final def storageAllocator: BufferAllocator = {
    if (finalAllocator ne null) finalAllocator
    else {
      finalAllocator = GemFireCacheImpl.getCurrentBufferAllocator
      finalAllocator
    }
  }

  protected final def isAllocatorFinal: Boolean =
    allocator.getClass eq storageAllocator.getClass

  protected[sql] final def setAllocator(allocator: BufferAllocator): Unit = {
    if (this.allocator ne allocator) {
      this.allocator = allocator
      this.finalAllocator = null
    }
  }

  def sizeInBytes(cursor: Long): Long = cursor - columnBeginPosition

  def defaultSize(dataType: DataType): Int = dataType match {
    case CalendarIntervalType => 12 // uses 12 and not 16 bytes
    case _ => dataType.defaultSize
  }

  def initSizeInBytes(dataType: DataType, initSize: Long, defSize: Int): Long = {
    initSize * defSize
  }

  protected[sql] def initializeNulls(initSize: Int): Int

  final def initialize(field: StructField, initSize: Int, withHeader: Boolean): Long = {
    initialize(field, initSize, withHeader, GemFireCacheImpl.getCurrentBufferAllocator)
  }

  protected def initializeLimits(): Unit = {
    if (forComplexType) {
      // set limits for complex types that will never be hit
      _lowerLong = Long.MinValue
      _upperLong = Long.MaxValue
      _lowerDouble = Double.MinValue
      _upperDouble = Double.MaxValue
    } else {
      // for other cases set limits that will be hit on first attempt
      _lowerLong = Long.MaxValue
      _upperLong = Long.MinValue
      _lowerDouble = Double.MaxValue
      _upperDouble = Double.MinValue
    }
    _lowerStr = null
    _upperStr = null
    _lowerDecimal = null
    _upperDecimal = null
  }

  final def initialize(field: StructField, initSize: Int,
      withHeader: Boolean, allocator: BufferAllocator): Long =
    initialize(JdbcExtendedUtils.getSQLDataType(field.dataType), field.nullable,
      initSize, withHeader, allocator)

  /**
   * Initialize this ColumnEncoder.
   *
   * @param dataType   DataType of the field to be written
   * @param nullable   True if the field is nullable, false otherwise
   * @param initSize   Initial estimated number of elements to be written
   * @param withHeader True if header is to be written to data (typeId etc)
   * @param allocator  the [[BufferAllocator]] to use for the data
   * @return initial position of the cursor that caller must use to write
   */
  final def initialize(dataType: DataType, nullable: Boolean, initSize: Int,
      withHeader: Boolean, allocator: BufferAllocator): Long = {
    initialize(dataType, nullable, initSize, withHeader, allocator, minBufferSize = -1)
  }

  /**
   * Initialize this ColumnEncoder.
   *
   * @param dataType      DataType of the field to be written
   * @param nullable      True if the field is nullable, false otherwise
   * @param initSize      Initial estimated number of elements to be written
   * @param withHeader    True if header is to be written to data (typeId etc)
   * @param allocator     the [[BufferAllocator]] to use for the data
   * @param minBufferSize the minimum size of initial buffer to use (ignored if <= 0)
   * @return initial position of the cursor that caller must use to write
   */
  def initialize(dataType: DataType, nullable: Boolean, initSize: Int,
      withHeader: Boolean, allocator: BufferAllocator, minBufferSize: Int): Long = {
    setAllocator(allocator)
    val defSize = defaultSize(dataType)

    this.forComplexType = dataType match {
      case _: ArrayType | _: MapType | _: StructType => true
      case _ => false
    }

    val numNullWords = initializeNulls(initSize)
    val numNullBytes = numNullWords << 3

    // initialize the lower and upper limits
    initializeLimits()

    var baseSize = numNullBytes.toLong
    if (withHeader) {
      baseSize += 8L /* typeId + nullsSize */
    }
    if ((columnData eq null) || (columnData.limit() < (baseSize + defSize))) {
      var initByteSize = 0L
      if (reuseUsedSize > baseSize) {
        initByteSize = reuseUsedSize
      } else {
        initByteSize = initSizeInBytes(dataType, initSize, defSize) + baseSize
      }
      setSource(allocator.allocate(checkBufferSize(math.max(initByteSize, minBufferSize)),
        ColumnEncoding.BUFFER_OWNER), releaseOld = true)
    } else {
      // for primitive types optimistically trim to exact size
      dataType match {
        case BooleanType | ByteType | ShortType | IntegerType | LongType |
             DateType | TimestampType | FloatType | DoubleType
          if reuseUsedSize > 0 && isAllocatorFinal &&
              reuseUsedSize != columnData.limit() =>
          setSource(allocator.allocate(reuseUsedSize,
            ColumnEncoding.BUFFER_OWNER), releaseOld = true)

        case _ => // continue to use the previous columnData
      }
    }
    reuseUsedSize = 0
    if (withHeader) {
      var cursor = ensureCapacity(columnBeginPosition, 8 + numNullBytes)
      // typeId followed by nulls bitset size and space for values
      ColumnEncoding.writeInt(columnBytes, cursor, typeId)
      cursor += 4
      // write the initial number of null words (will be updated in writeNulls
      //   except for non-nullable columns where zero written here is final)
      ColumnEncoding.writeInt(columnBytes, cursor, numNullBytes)
      cursor + 4L + numNullBytes
    } else columnBeginPosition
  }

  final def baseOffset: Long = columnBeginPosition

  def offset(cursor: Long): Long = cursor - columnBeginPosition

  final def buffer: AnyRef = columnBytes

  /**
   * Write any internal structures (e.g. dictionary) of the encoder that would
   * normally be written by [[finish]] after the header and null bit mask.
   */
  def writeInternals(columnBytes: AnyRef, cursor: Long): Long = cursor

  protected[sql] final def setSource(buffer: ByteBuffer,
      releaseOld: Boolean): Unit = {
    if (buffer ne columnData) {
      if (releaseOld && (columnData ne null)) {
        allocator.release(columnData)
      }
      columnData = buffer
      columnBytes = allocator.baseObject(buffer)
      columnBeginPosition = allocator.baseOffset(buffer)
    }
    columnEndPosition = columnBeginPosition + buffer.limit()
  }

  protected[sql] final def clearSource(newSize: Int, releaseData: Boolean): Unit = {
    if (columnData ne null) {
      if (releaseData) {
        allocator.release(columnData)
      }
      columnData = null
      columnBytes = null
      columnBeginPosition = 0
      columnEndPosition = 0
    }
    reuseUsedSize = newSize
  }

  protected final def copyTo(dest: ByteBuffer, srcOffset: Int,
      endOffset: Int): Unit = {
    val src = columnData
    // buffer to buffer copy after position reset for source
    val position = src.position()
    val limit = src.limit()

    if (position != srcOffset) src.position(srcOffset)
    if (limit != endOffset) src.limit(endOffset)

    dest.put(src)

    // move back position and limit to original values
    src.position(position)
    src.limit(limit)
  }

  /** Expand the underlying bytes if required and return the new cursor */
  protected final def expand(cursor: Long, required: Int): Long = {
    val numWritten = cursor - columnBeginPosition
    setSource(allocator.expand(columnData, required,
      ColumnEncoding.BUFFER_OWNER), releaseOld = true)
    columnBeginPosition + numWritten
  }

  final def ensureCapacity(cursor: Long, required: Int): Long = {
    if ((cursor + required) <= columnEndPosition) cursor
    else expand(cursor, required)
  }

  final def lowerLong: Long = _lowerLong

  final def upperLong: Long = _upperLong

  final def lowerDouble: Double = _lowerDouble

  final def upperDouble: Double = _upperDouble

  final def lowerString: UTF8String = _lowerStr

  final def upperString: UTF8String = _upperStr

  final def lowerDecimal: Decimal = _lowerDecimal

  final def upperDecimal: Decimal = _upperDecimal

  protected final def updateLongStats(value: Long): Unit = {
    val lower = _lowerLong
    if (value < lower) {
      _lowerLong = value
      // check for first write case
      if (lower == Long.MaxValue) _upperLong = value
    } else if (value > _upperLong) {
      _upperLong = value
    }
  }

  protected final def updateDoubleStats(value: Double): Unit = {
    val lower = _lowerDouble
    if (value < lower) {
      // check for first write case
      if (lower == Double.MaxValue) _upperDouble = value
      _lowerDouble = value
    } else if (value > _upperDouble) {
      _upperDouble = value
    }
  }

  protected final def updateStringStats(value: UTF8String): Unit = {
    if (value ne null) {
      val lower = _lowerStr
      // check for first write case
      if (lower eq null) {
        if (!forComplexType) {
          val valueClone = StringUtils.cloneIfRequired(value)
          _lowerStr = valueClone
          _upperStr = valueClone
        }
      } else if (value.compare(lower) < 0) {
        _lowerStr = StringUtils.cloneIfRequired(value)
      } else if (value.compare(_upperStr) > 0) {
        _upperStr = StringUtils.cloneIfRequired(value)
      }
    }
  }

  protected final def updateDecimalStats(value: Decimal): Unit = {
    if (value ne null) {
      val lower = _lowerDecimal
      // check for first write case
      if (lower eq null) {
        if (!forComplexType) {
          _lowerDecimal = value
          _upperDecimal = value
        }
      } else if (value.compare(lower) < 0) {
        _lowerDecimal = value
      } else if (value.compare(_upperDecimal) > 0) {
        _upperDecimal = value
      }
    }
  }

  def nullCount: Int

  def isNullable: Boolean

  def writeIsNull(position: Int): Unit

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
      position: Int, precision: Int, scale: Int): Long =
    throw new UnsupportedOperationException(s"writeLongDecimal for $toString")

  def writeDecimal(cursor: Long, value: Decimal,
      position: Int, precision: Int, scale: Int): Long =
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

  def writeBooleanUnchecked(cursor: Long, value: Boolean): Long =
    throw new UnsupportedOperationException(s"writeBooleanUnchecked for $toString")

  def writeByteUnchecked(cursor: Long, value: Byte): Long =
    throw new UnsupportedOperationException(s"writeByteUnchecked for $toString")

  def writeShortUnchecked(cursor: Long, value: Short): Long =
    throw new UnsupportedOperationException(s"writeShortUnchecked for $toString")

  def writeIntUnchecked(cursor: Long, value: Int): Long =
    throw new UnsupportedOperationException(s"writeIntUnchecked for $toString")

  def writeLongUnchecked(cursor: Long, value: Long): Long =
    throw new UnsupportedOperationException(s"writeLongUnchecked for $toString")

  def writeFloatUnchecked(cursor: Long, value: Float): Long =
    throw new UnsupportedOperationException(s"writeFloatUnchecked for $toString")

  def writeDoubleUnchecked(cursor: Long, value: Double): Long =
    throw new UnsupportedOperationException(s"writeDoubleUnchecked for $toString")

  def writeUnsafeData(cursor: Long, baseObject: AnyRef, baseOffset: Long,
      numBytes: Int): Long =
    throw new UnsupportedOperationException(s"writeUnsafeData for $toString")

  // Helper methods for writing complex types and elements inside them.

  /**
   * Temporary offset results to be read by generated code immediately
   * after initializeComplexType, so not an issue for nested types.
   */
  protected final var baseTypeOffset: Long = _
  protected final var baseDataOffset: Long = _

  @inline final def setOffsetAndSize(cursor: Long, fieldOffset: Long,
      baseOffset: Long, size: Int): Unit = {
    val relativeOffset = cursor - columnBeginPosition - baseOffset
    val offsetAndSize = (relativeOffset << 32L) | size.toLong
    Platform.putLong(columnBytes, columnBeginPosition + fieldOffset,
      offsetAndSize)
  }

  final def getBaseTypeOffset: Long = baseTypeOffset

  final def getBaseDataOffset: Long = baseDataOffset

  /**
   * Complex types are written similar to UnsafeRows while respecting platform
   * endianness (format is always little endian) so appropriate for storage.
   * Also have other minor differences related to size writing and interval
   * type handling. General layout looks like below:
   * {{{
   *   .--------------------------- Optional total size including itself (4 bytes)
   *   |   .----------------------- Optional number of elements (4 bytes)
   *   |   |   .------------------- Null bitset longs (8 x (N / 8) bytes)
   *   |   |   |
   *   |   |   |     .------------- Offsets+Sizes of elements (8 x N bytes)
   *   |   |   |     |     .------- Variable length elements
   *   V   V   V     V     V
   *   +---+---+-----+-------------+
   *   |   |   | ... | ... ... ... |
   *   +---+---+-----+-------------+
   *    \-----/ \-----------------/
   *     header      body
   * }}}
   * The above generic layout is used for ARRAY and STRUCT types.
   *
   * The total size of the data is written for top-level complex types. Nested
   * complex objects write their sizes in the "Offsets+Sizes" portion in the
   * respective parent object.
   *
   * ARRAY types also write the number of elements in the array in the header
   * while STRUCT types skip it since it is fixed in the meta-data.
   *
   * The null bitset follows the header. To keep the reads aligned at 8 byte
   * boundaries while preserving space, the implementation will combine the
   * header and the null bitset portion, then pad them together at 8 byte
   * boundary (in particular it will consider header as some additional empty
   * fields in the null bitset itself).
   *
   * After this follows the "Offsets+Sizes" which keeps the offset and size
   * for variable length elements. Fixed length elements less than 8 bytes
   * in size are written directly in the offset+size portion. Variable length
   * elements have their offsets (from start of this array) and sizes encoded
   * in this portion as a long (4 bytes for each of offset and size). Fixed
   * width elements that are greater than 8 bytes are encoded like variable
   * length elements. [[CalendarInterval]] is the only type currently that
   * is of that nature whose "months" portion is encoded into the size
   * while the "microseconds" portion is written into variable length part.
   *
   * MAP types are written as an ARRAY of keys followed by ARRAY of values
   * like in Spark. To keep things simpler both ARRAYs always have the
   * optional size header at their respective starts which together determine
   * the total size of the encoded MAP object. For nested MAP types, the
   * total size is skipped from the "Offsets+Sizes" portion and only
   * the offset is written (which is the start of key ARRAY).
   */
  final def initializeComplexType(cursor: Long, numElements: Int,
      skipBytes: Int, writeNumElements: Boolean): Long = {
    val numNullBytes = calculateBitSetWidthInBytes(
      numElements + (skipBytes << 3))
    // space for nulls and offsets at the start
    val fixedWidth = numNullBytes + (numElements << 3)
    var position = cursor
    if (position + fixedWidth > columnEndPosition) {
      position = expand(position, fixedWidth)
    }
    val baseTypeOffset = offset(position).toInt
    // initialize the null bytes to zeros
    allocator.fill(columnData, 0, baseTypeOffset, numNullBytes)
    this.baseTypeOffset = baseTypeOffset
    this.baseDataOffset = baseTypeOffset + numNullBytes
    if (writeNumElements) {
      writeIntUnchecked(position + skipBytes - 4, numElements)
    }
    position + fixedWidth
  }

  private final def writeStructData(cursor: Long, value: AnyRef, size: Int,
      valueOffset: Long, fieldOffset: Long, baseOffset: Long): Long = {
    val alignedSize = ((size + 7) >>> 3) << 3
    // Write the bytes to the variable length portion.
    var position = cursor
    if (position + alignedSize > columnEndPosition) {
      position = expand(position, alignedSize)
    }
    setOffsetAndSize(position, fieldOffset, baseOffset, size)
    Platform.copyMemory(value, valueOffset, columnBytes, position, size)
    position + alignedSize
  }

  final def writeStructUTF8String(cursor: Long, value: UTF8String,
      fieldOffset: Long, baseOffset: Long): Long = {
    writeStructData(cursor, value.getBaseObject, value.numBytes(),
      value.getBaseOffset, fieldOffset, baseOffset)
  }

  final def writeStructBinary(cursor: Long, value: Array[Byte],
      fieldOffset: Long, baseOffset: Long): Long = {
    writeStructData(cursor, value, value.length, Platform.BYTE_ARRAY_OFFSET,
      fieldOffset, baseOffset)
  }

  final def writeStructDecimal(cursor: Long, value: Decimal,
      fieldOffset: Long, baseOffset: Long): Long = {
    // assume precision and scale are matching and ensured by caller
    val bytes = value.toJavaBigDecimal.unscaledValue.toByteArray
    writeStructData(cursor, bytes, bytes.length, Platform.BYTE_ARRAY_OFFSET,
      fieldOffset, baseOffset)
  }

  final def writeStructInterval(cursor: Long, value: CalendarInterval,
      fieldOffset: Long, baseOffset: Long): Long = {
    var position = cursor
    if (position + 8 > columnEndPosition) {
      position = expand(position, 8)
    }
    // write months in the size field itself instead of using separate bytes
    setOffsetAndSize(position, fieldOffset, baseOffset, value.months)
    Platform.putLong(columnBytes, position, value.microseconds)
    position + 8
  }

  /** flush any pending data when [[finish]] is not being invoked explicitly */
  def flushWithoutFinish(cursor: Long): Long = cursor

  /**
   * Finish encoding the current column and return the data as a ByteBuffer.
   * The encoder can be reused for new column data of same type again.
   */
  def finish(cursor: Long, size: Int): ByteBuffer

  /**
   * The final size of the encoder column (excluding header and nulls) which should match
   * that occupied after [[finish]] but without writing anything.
   */
  def encodedSize(cursor: Long, dataBeginPosition: Long): Long = cursor - dataBeginPosition

  /**
   * Close and relinquish all resources of this encoder.
   * The encoder may no longer be usable after this call.
   */
  def close(): Unit = close(releaseData = true)

  /**
   * Close and relinquish all resources of this encoder.
   * The encoder may no longer be usable after this call.
   */
  private[sql] def close(releaseData: Boolean): Unit = {
    clearSource(newSize = 0, releaseData)
  }

  protected[sql] def getNumNullWords(size: Int): (Int, Int)

  protected[sql] def writeNulls(columnBytes: AnyRef, cursor: Long,
      numWords: Int, numNulls: Int): Long

  protected final def releaseForReuse(newSize: Int): Unit = {
    columnData.rewind()
    reuseUsedSize = newSize
  }
}

object ColumnEncoding {

  private[columnar] val DICTIONARY_TYPE_ID = 2

  private[columnar] val BIG_DICTIONARY_TYPE_ID = 3

  /**
   * The limit up-to which string dictionary will be pre-populated with UTF8String objects
   * in decoder else the objects will be created on-the-fly to avoid GC issues.
   */
  val CACHED_DICTIONARY_LIMIT = 1000

  private[columnar] val BUFFER_OWNER = "ENCODER"

  private[columnar] val BITS_PER_LONG = 64

  private[columnar] val MAX_BITMASK = 1L << 63

  private[columnar] val ULIMIT_POSITION = 0x3fffffff

  private[columnar] val NO_NULLS = (0, 0)

  private[columnar] val identityLong: (AnyRef, Long) => Long = (_: AnyRef, l: Long) => l

  /** maximum number of null words that can be allowed to go waste in storage */
  private[columnar] val MAX_WASTED_WORDS_FOR_NULLS = 8

  private[columnar] val encodingClassName = s"${classOf[ColumnEncoding].getName}$$.MODULE$$"

  private[columnar] val NULLS_OWNER = "NULL_WORDS"

  /**
   * Return true if the platform byte-order is little-endian and false otherwise.
   */
  val littleEndian: Boolean = ByteOrder.nativeOrder == ByteOrder.LITTLE_ENDIAN

  val allDecoders: Array[(AnyRef, Long, StructField, (AnyRef, Long) => Long,
      DataType, Boolean) => ColumnDecoder] = Array(
    createUncompressedDecoder,
    createRunLengthDecoder,
    createDictionaryDecoder,
    createBigDictionaryDecoder,
    createBooleanBitSetDecoder
  )

  final def checkBufferSize(size: Long): Int = {
    if (size >= 0 && size < Int.MaxValue) size.toInt
    else {
      throw new ArrayIndexOutOfBoundsException(
        s"Invalid size/index = $size. Max allowed = ${Int.MaxValue - 1}.")
    }
  }

  def getAllocator(buffer: ByteBuffer): BufferAllocator =
    if (buffer.isDirect) DirectBufferAllocator.instance()
    else HeapBufferAllocator.instance()

  def getColumnDecoder(buffer: ByteBuffer, field: StructField): ColumnDecoder =
    getColumnDecoder(buffer, field, ColumnEncoding.identityLong)

  def getColumnDecoderAndBuffer(buffer: ByteBuffer,
      field: StructField, initDelta: (AnyRef, Long) => Long): (ColumnDecoder, AnyRef) = {
    val allocator = getAllocator(buffer)
    val columnBytes = allocator.baseObject(buffer)
    (getColumnDecoder(buffer, field, initDelta), columnBytes)
  }

  final def getColumnDecoder(buffer: ByteBuffer,
      field: StructField, initDelta: (AnyRef, Long) => Long): ColumnDecoder = {
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    val allocator = getAllocator(buffer)
    val columnBytes = allocator.baseObject(buffer)
    // typeId at the start followed by null bit set values
    val typeId = try {
      buffer.getInt(buffer.position())
    } catch {
      case t: Throwable =>
        throw new IllegalStateException(s"Failed to read typeId from " +
            s"buffer=0x${ClientSharedUtils.toHexString(buffer)}: $t")
    }
    val cursor = allocator.baseOffset(buffer) + buffer.position() + 4
    val dataType = JdbcExtendedUtils.getSQLDataType(field.dataType)
    if (typeId >= allDecoders.length) {
      throw new IllegalStateException(s"Unknown encoding typeId = $typeId " +
          s"for $dataType($field) bytes=0x${ClientSharedUtils.toHexString(buffer)}")
    }

    val decoder = allDecoders(typeId)(columnBytes, cursor, field, initDelta, dataType,
      // use NotNull version if field is marked so
      // don't use NotNull in case this batch has no nulls because switching
      // between NotNull and Nullable decoders between batches causes JVM inlining
      // to take a hit in many cases
      field.nullable)
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

  def getColumnEncoder(field: StructField): ColumnEncoder =
    getColumnEncoder(JdbcExtendedUtils.getSQLDataType(field.dataType), field.nullable)

  def getColumnEncoder(dataType: DataType, nullable: Boolean): ColumnEncoder = {
    // TODO: SW: add RunLength by default
    dataType match {
      case StringType => createDictionaryEncoder(StringType, nullable)
      case BooleanType => createBooleanBitSetEncoder(BooleanType, nullable)
      case _ => createUncompressedEncoder(dataType, nullable)
    }
  }

  private[columnar] def createUncompressedDecoder(columnBytes: AnyRef, cursor: Long,
      field: StructField, initDelta: (AnyRef, Long) => Long,
      dataType: DataType, nullable: Boolean): ColumnDecoder =
    if (nullable) new UncompressedDecoderNullable(columnBytes, cursor, field, initDelta)
    else new UncompressedDecoder(columnBytes, cursor, field, initDelta)

  private[columnar] def createRunLengthDecoder(columnBytes: AnyRef, cursor: Long,
      field: StructField, initDelta: (AnyRef, Long) => Long,
      dataType: DataType, nullable: Boolean): ColumnDecoder = dataType match {
    case BooleanType | ByteType | ShortType |
         IntegerType | DateType | LongType | TimestampType | StringType =>
      if (nullable) new RunLengthDecoderNullable(columnBytes, cursor, field, initDelta)
      else new RunLengthDecoder(columnBytes, cursor, field, initDelta)
    case _ => throw new UnsupportedOperationException(
      s"RunLengthDecoder not supported for $dataType")
  }

  private[columnar] def createDictionaryDecoder(columnBytes: AnyRef, cursor: Long,
      field: StructField, initDelta: (AnyRef, Long) => Long,
      dataType: DataType, nullable: Boolean): ColumnDecoder = dataType match {
    case StringType | IntegerType | DateType | LongType | TimestampType =>
      if (nullable) new DictionaryDecoderNullable(columnBytes, cursor, field, initDelta)
      else new DictionaryDecoder(columnBytes, cursor, field, initDelta)
    case _ => throw new UnsupportedOperationException(
      s"DictionaryDecoder not supported for $dataType")
  }

  private[columnar] def createBigDictionaryDecoder(columnBytes: AnyRef, cursor: Long,
      field: StructField, initDelta: (AnyRef, Long) => Long,
      dataType: DataType, nullable: Boolean): ColumnDecoder = dataType match {
    case StringType | IntegerType | DateType | LongType | TimestampType =>
      if (nullable) new BigDictionaryDecoderNullable(columnBytes, cursor, field, initDelta)
      else new BigDictionaryDecoder(columnBytes, cursor, field, initDelta)
    case _ => throw new UnsupportedOperationException(
      s"BigDictionaryDecoder not supported for $dataType")
  }

  private[columnar] def createBooleanBitSetDecoder(columnBytes: AnyRef, cursor: Long,
      field: StructField, initDelta: (AnyRef, Long) => Long,
      dataType: DataType, nullable: Boolean): ColumnDecoder = dataType match {
    case BooleanType =>
      if (nullable) new BooleanBitSetDecoderNullable(columnBytes, cursor, field, initDelta)
      else new BooleanBitSetDecoder(columnBytes, cursor, field, initDelta)
    case _ => throw new UnsupportedOperationException(
      s"BooleanBitSetDecoder not supported for $dataType")
  }

  private[columnar] def createUncompressedEncoder(dataType: DataType,
      nullable: Boolean): ColumnEncoder =
    if (nullable) new UncompressedEncoderNullable else new UncompressedEncoder

  private[columnar] def createDictionaryEncoder(dataType: DataType,
      nullable: Boolean): ColumnEncoder = dataType match {
    case StringType | IntegerType | DateType | LongType | TimestampType =>
      if (nullable) new DictionaryEncoderNullable else new DictionaryEncoder
    case _ => throw new UnsupportedOperationException(
      s"DictionaryEncoder not supported for $dataType")
  }

  private[columnar] def createBooleanBitSetEncoder(dataType: DataType,
      nullable: Boolean): ColumnEncoder = dataType match {
    case BooleanType => if (nullable) new BooleanBitSetEncoderNullable
    else new BooleanBitSetEncoder
    case _ => throw new UnsupportedOperationException(
      s"BooleanBitSetEncoder not supported for $dataType")
  }

  @inline final def readShort(columnBytes: AnyRef,
      cursor: Long): Short = if (littleEndian) {
    Platform.getShort(columnBytes, cursor)
  } else {
    java.lang.Short.reverseBytes(Platform.getShort(columnBytes, cursor))
  }

  @inline final def readInt(columnBytes: AnyRef,
      cursor: Long): Int = if (littleEndian) {
    Platform.getInt(columnBytes, cursor)
  } else {
    java.lang.Integer.reverseBytes(Platform.getInt(columnBytes, cursor))
  }

  @inline final def readLong(columnBytes: AnyRef,
      cursor: Long): Long = if (littleEndian) {
    Platform.getLong(columnBytes, cursor)
  } else {
    java.lang.Long.reverseBytes(Platform.getLong(columnBytes, cursor))
  }

  @inline final def readFloat(columnBytes: AnyRef,
      cursor: Long): Float = if (littleEndian) {
    Platform.getFloat(columnBytes, cursor)
  } else {
    java.lang.Float.intBitsToFloat(java.lang.Integer.reverseBytes(
      Platform.getInt(columnBytes, cursor)))
  }

  @inline final def readDouble(columnBytes: AnyRef,
      cursor: Long): Double = if (littleEndian) {
    Platform.getDouble(columnBytes, cursor)
  } else {
    java.lang.Double.longBitsToDouble(java.lang.Long.reverseBytes(
      Platform.getLong(columnBytes, cursor)))
  }

  @inline final def readUTF8String(columnBytes: AnyRef,
      cursor: Long): UTF8String = {
    val size = readInt(columnBytes, cursor)
    UTF8String.fromAddress(columnBytes, cursor + 4, size)
  }

  def stringFromDictionaryCode(dictVar: String, bufferVar: String, indexVar: String): String =
    s"$dictVar.getString($bufferVar, $indexVar)"

  @inline final def writeShort(columnBytes: AnyRef,
      cursor: Long, value: Short): Unit = if (littleEndian) {
    Platform.putShort(columnBytes, cursor, value)
  } else {
    Platform.putShort(columnBytes, cursor, java.lang.Short.reverseBytes(value))
  }

  @inline final def writeInt(columnBytes: AnyRef,
      cursor: Long, value: Int): Unit = if (littleEndian) {
    Platform.putInt(columnBytes, cursor, value)
  } else {
    Platform.putInt(columnBytes, cursor, java.lang.Integer.reverseBytes(value))
  }

  @inline final def writeLong(columnBytes: AnyRef,
      cursor: Long, value: Long): Unit = if (littleEndian) {
    Platform.putLong(columnBytes, cursor, value)
  } else {
    Platform.putLong(columnBytes, cursor, java.lang.Long.reverseBytes(value))
  }

  final def writeUTF8String(columnBytes: AnyRef,
      cursor: Long, base: AnyRef, offset: Long, size: Int): Long = {
    ColumnEncoding.writeInt(columnBytes, cursor, size)
    val position = cursor + 4
    Platform.copyMemory(base, offset, columnBytes, position, size)
    position + size
  }

  /** Write a short run-length as either 1 or 2 bytes */
  final def writeRunLength(columnBytes: AnyRef, cursor: Long, run: Int): Long = {
    if (run > 0x7f) {
      Platform.putByte(columnBytes, cursor, ((run & 0x7f) | 0x80).toByte)
      Platform.putByte(columnBytes, cursor + 1, (run >>> 7).toByte)
      cursor + 2
    } else {
      Platform.putByte(columnBytes, cursor, run.toByte)
      cursor + 1
    }
  }
}

/**
 * Full stats row has "nullCount" as non-nullable while delta stats row has it as nullable.
 */
case class ColumnStatsSchema(fieldName: String,
    dataType: DataType, nullCountNullable: Boolean) {
  val lowerBound: AttributeReference = AttributeReference(
    fieldName + ".lowerBound", dataType)()
  val upperBound: AttributeReference = AttributeReference(
    fieldName + ".upperBound", dataType)()
  val nullCount: AttributeReference = AttributeReference(
    fieldName + ".nullCount", IntegerType, nullCountNullable)()

  val schema = Seq(lowerBound, upperBound, nullCount)

  assert(schema.length == ColumnStatsSchema.NUM_STATS_PER_COLUMN)
}

object ColumnStatsSchema {
  val NUM_STATS_PER_COLUMN = 3
  val COUNT_INDEX_IN_SCHEMA = 0

  val COUNT_ATTRIBUTE: AttributeReference = AttributeReference(
    "batchCount", IntegerType, nullable = false)()

  def numStatsColumns(schemaSize: Int): Int = schemaSize * NUM_STATS_PER_COLUMN + 1
}

trait NotNullDecoder extends ColumnDecoder {

  override protected[sql] final def hasNulls: Boolean = false

  protected[sql] def initializeNulls(columnBytes: AnyRef,
      startCursor: Long, field: StructField): Long = {
    val numNullBytes = ColumnEncoding.readInt(columnBytes, startCursor)
    if (numNullBytes != 0) {
      throw new IllegalStateException(
        s"Nulls bitset of size $numNullBytes found in NOT NULL column $field")
    }
    startCursor + 4
  }

  override final def getNextNullPosition: Int = Int.MaxValue

  override final def moveToNextNull(columnBytes: AnyRef, ordinal: Int, numNulls: Int): Int = 0
}

/**
 * Nulls are stored either as a bitset or as a sequence of positions
 * (or inverse sequence of missing positions) if the number of nulls
 * is small (or non-nulls is small). This is indicated by the count
 * field which is negative for the latter case. The decoder for latter
 * keeps track of next null position and compares against that.
 */
trait NullableDecoder extends ColumnDecoder {

  private[this] final var dataCursor: Long = _
  private[this] final var numNullWords: Int = _
  private[this] final var nextNullPosition: Int = _

  override protected[sql] final def hasNulls: Boolean = dataCursor != Long.MaxValue

  protected[sql] def initializeNulls(columnBytes: AnyRef,
      startCursor: Long, field: StructField): Long = {
    var cursor = startCursor
    var numNullBytes = ColumnEncoding.readInt(columnBytes, cursor)
    cursor += 4
    // for no-nulls case mark dataCursor to be so
    if (numNullBytes == 0) {
      this.dataCursor = Long.MaxValue
      this.numNullWords = 0
      this.nextNullPosition = Int.MaxValue
    } else {
      this.dataCursor = cursor
      if (numNullBytes < 0) {
        // position encoding
        numNullBytes = -numNullBytes
        this.numNullWords = 0 // indicates position encoding
      } else {
        this.numNullWords = numNullBytes >>> 3
      }
      // expect it to be a factor of 8
      assert((numNullBytes & 0x7) == 0,
        s"Expected valid null values but got length = $numNullBytes")
      // initialize the first nextNullPosition
      this.nextNullPosition = -1
      moveToNextNull(columnBytes, 0, -1)
      // skip null bit set
      cursor += numNullBytes
    }
    cursor
  }

  override final def getNextNullPosition: Int = nextNullPosition

  override final def moveToNextNull(columnBytes: AnyRef, ordinal: Int,
      numNulls: Int): Int = {
    if (numNullWords == 0) {
      // position encoding
      val baseOffset = this.dataCursor
      var n = numNulls
      while (true) {
        n += 1
        val nextNull = ColumnEncoding.readInt(columnBytes, baseOffset + (n << 2))
        if (ordinal <= nextNull) {
          nextNullPosition = nextNull
          return n
        }
      }
    }
    // bitset encoding
    findNextBitSetPosition(columnBytes, ordinal, numNulls)
  }

  private def findNextBitSetPosition(columnBytes: AnyRef, ordinal: Int,
      numNulls: Int): Int = {
    // check the common case first where ordinal is <= the next null position
    val nextNull = BitSet.nextSetBit(columnBytes, dataCursor, nextNullPosition + 1, numNullWords)
    if (ordinal <= nextNull) {
      nextNullPosition = nextNull
      numNulls + 1
    } else findBitSetPosition(columnBytes, ordinal, nextNull, numNulls + 1)
  }

  private def findBitSetPosition(columnBytes: AnyRef, ordinal: Int,
      nullPosition: Int, numNulls: Int): Int = {
    val baseOffset = this.dataCursor
    val numWords = this.numNullWords
    // find the next null after given ordinal and count the additional nulls till ordinal
    // new nextNullPosition must be inclusive of current ordinal because generated
    // code will check for nullability of ordinal separately
    nextNullPosition = BitSet.nextSetBit(columnBytes, baseOffset, ordinal, numWords)
    // find new number of nulls from the previous null position till ordinal
    // (search has to be aligned to word boundary)
    val searchWord = nullPosition >>> 6 // a word holds 64 bits
    // if word boundary is first word then faster to do full recount
    if (searchWord == 0) {
      // count excludes the ordinal itself since nextNullPosition above includes it
      BitSet.cardinality(columnBytes, baseOffset, ordinal, numWords)
    } else {
      // address required by BitSet methods is in bytes
      val searchOffset = baseOffset + (searchWord << 3)
      val searchOffsetInWord = nullPosition & 0x3f
      // count from the word boundary
      val countFrom = BitSet.cardinality(columnBytes, searchOffset,
        // skipping "searchWord" words from start so ordinal, numWords adjusted accordingly
        ordinal - (searchWord << 6), numWords - searchWord)
      // reduce the number of nulls till the position within the word (excluding
      //   searchOffsetInWord as the passed num does not include the previous nextNullPosition)
      numNulls + countFrom - BitSet.cardinality(columnBytes, searchOffset, searchOffsetInWord, 1)
    }
  }
}

trait NotNullEncoder extends ColumnEncoder {

  override protected[sql] def initializeNulls(initSize: Int): Int = 0

  override def nullCount: Int = 0

  override def isNullable: Boolean = false

  override def writeIsNull(position: Int): Unit =
    throw new UnsupportedOperationException(s"writeIsNull for $toString")

  override protected[sql] def getNumNullWords(size: Int): (Int, Int) = ColumnEncoding.NO_NULLS

  override protected[sql] def writeNulls(columnBytes: AnyRef, cursor: Long,
      numWords: Int, numNulls: Int): Long = {
    assert(numWords == 0)
    assert(numNulls == 0)
    ColumnEncoding.writeInt(columnBytes, cursor, 0)
    cursor + 4
  }

  override def finish(cursor: Long, size: Int): ByteBuffer = {
    val newSize = checkBufferSize(cursor - columnBeginPosition)
    // check if need to shrink byte array since it is stored as is in region
    // avoid copying only if final shape of object in region is same
    // else copy is required in any case and columnData can be reused
    if (cursor == columnEndPosition && isAllocatorFinal) {
      val columnData = this.columnData
      clearSource(newSize, releaseData = false)
      // mark its allocation for storage
      if (columnData.isDirect) {
        memoryManager.changeOffHeapOwnerToStorage(columnData, allowNonAllocator = false)
      }
      columnData
    } else {
      // copy to exact size
      val newColumnData = storageAllocator.allocateForStorage(newSize)
      copyTo(newColumnData, srcOffset = 0, newSize)
      newColumnData.rewind()
      // reuse this columnData in next round if possible
      releaseForReuse(newSize)
      newColumnData
    }
  }
}

trait NullableEncoder extends NotNullEncoder {

  protected[this] final var maxNulls: Long = _
  protected[this] final var nullWords: ByteBuffer = _
  protected[this] final var nullWordsObj: AnyRef = _
  protected[this] final var nullWordsBaseOffset: Long = _
  protected[this] final var initialNumWords: Int = _

  @inline private[this] final def nullWordAt(index: Int): Long =
    Platform.getLong(nullWordsObj, nullWordsBaseOffset + (index << 3))

  private def trimmedNumNullWords: Int = {
    var numWords = nullWords.limit() >> 3
    while (numWords > 0 && nullWordAt(numWords - 1) == 0L) numWords -= 1
    numWords
  }

  private def setNullWords(buffer: ByteBuffer, clearFromPosition: Int): Unit = {
    nullWords = buffer
    allocator.clearPostAllocate(buffer, clearFromPosition)
    nullWordsObj = allocator.baseObject(nullWords)
    nullWordsBaseOffset = allocator.baseOffset(nullWords)
  }

  override protected[sql] def getNumNullWords(size: Int): (Int, Int) = {
    val numWords = trimmedNumNullWords
    val numNulls = nullCount(numWords)
    // change to position encoding in case number of nulls is small
    if (numNulls > 0 && numNulls <= (size >>> 4)) {
      // negative numWords indicates position encoding
      // each position will take half a word so ceil to word size
      // also add space for one extra terminating integer (Int.MaxValue)
      (-((numNulls >>> 1) + 1), numNulls)
    } else (numWords, numNulls)
  }

  override protected[sql] def initializeNulls(initSize: Int): Int = {
    var numWords = 0
    if (nullWords ne null) {
      numWords = nullWords.limit() >> 3
      BufferAllocator.releaseBuffer(nullWords)
    } else {
      numWords = math.max(1, calculateBitSetWidthInBytes(initSize) >>> 3)
    }
    maxNulls = numWords.toLong << 6L
    setNullWords(allocator.allocate(numWords << 3, ColumnEncoding.NULLS_OWNER), 0)
    initialNumWords = numWords
    numWords
  }

  override def nullCount: Int = {
    nullCount(trimmedNumNullWords)
  }

  private def nullCount(numWords: Int): Int = {
    var sum = 0
    var i = 0
    while (i < numWords) {
      sum += java.lang.Long.bitCount(nullWordAt(i))
      i += 1
    }
    sum
  }

  override def isNullable: Boolean = true

  override def writeIsNull(position: Int): Unit = {
    if (position < maxNulls) {
      BitSet.set(nullWordsObj, nullWordsBaseOffset, position)
    } else {
      // expand
      val limit = nullWords.limit()
      val oldLen = limit >> 3
      // ensure that position fits (SNAP-1760)
      val newLen = math.max((oldLen * 3) >> 1, (position >> 6) + 1)
      setNullWords(allocator.expand(nullWords, (newLen - oldLen) << 3,
        ColumnEncoding.NULLS_OWNER), limit)
      maxNulls = newLen.toLong << 6L
      BitSet.set(nullWordsObj, nullWordsBaseOffset, position)
    }
  }

  override protected[sql] def writeNulls(columnBytes: AnyRef, cursor: Long,
      numWords: Int, numNulls: Int): Long = {
    var writeCursor = cursor
    val nullWords = this.nullWords
    if (numWords >= 0) {
      ColumnEncoding.writeInt(columnBytes, writeCursor, numWords << 3)
      writeCursor += 4
      // write the null words
      var index = 0
      while (index < numWords) {
        ColumnEncoding.writeLong(columnBytes, writeCursor, nullWords(index))
        writeCursor += 8
        index += 1
      }
    } else {
      // write null positions with negative size to indicate it
      val numWordsAbs = -numWords
      ColumnEncoding.writeInt(columnBytes, writeCursor, -(numWordsAbs << 3))
      writeCursor += 4

      var index = 0
      val numNullWords = trimmedNumNullWords
      while (index < numNullWords) {
        var word = nullWords(index)
        if (word != 0L) {
          // keep finding the next set bit in current word
          var absoluteIndex = index << 6
          var nextPos = java.lang.Long.numberOfTrailingZeros(word)
          while (nextPos < 64) {
            absoluteIndex += nextPos
            ColumnEncoding.writeInt(columnBytes, writeCursor, absoluteIndex)
            writeCursor += 4
            word >>>= nextPos
            word >>>= 1 // two separate shifts to deal with shift of 64 (which is turned to 0)
            absoluteIndex += 1
            nextPos = if (word != 0L) java.lang.Long.numberOfTrailingZeros(word) else 64
          }
        }
        index += 1
      }
      // number of nulls must exactly match number written
      val bytesWritten = writeCursor - cursor - 4
      assert((bytesWritten & 0x3) == 0,
        s"Unexpected number of null bytes written = $bytesWritten")
      assert(numNulls == (bytesWritten >> 2),
        s"Internal error: nulls expected = $numNulls but written = ${bytesWritten >> 2}")
      // terminate with Int.MaxValue to avoid extra checks in decoding
      ColumnEncoding.writeInt(columnBytes, writeCursor, Int.MaxValue)
      writeCursor += 4
      // move cursor to end of padding; padding for even numNulls due to terminal Int.MaxValue
      if ((numNulls & 0x1) == 0) writeCursor += 4
      assert(writeCursor == cursor + 4 + (numWordsAbs << 3))
    }
    writeCursor
  }

  private def allowWastedWords(cursor: Long, numWords: Int): Boolean = {
    numWords > 0 && initialNumWords > numWords &&
        (initialNumWords - numWords) < ColumnEncoding.MAX_WASTED_WORDS_FOR_NULLS &&
        cursor == columnEndPosition
  }

  override def finish(cursor: Long, size: Int): ByteBuffer = {
    // trim trailing empty words
    val (numWords, numNulls) = getNumNullWords(size)
    val numWordsAbs = math.abs(numWords)
    // check if the number of words to be written matches the space that
    // was left at initialization; as an optimization allow for larger
    // space left at initialization when one full data copy can be avoided
    val baseOffset = columnBeginPosition
    if (initialNumWords == numWordsAbs) {
      writeNulls(columnBytes, baseOffset + 4, numWords, numNulls)
      super.finish(cursor, size)
    } else if (allowWastedWords(cursor, numWords)) {
      // write till initialNumWords and not just numWords to clear any
      // trailing empty bytes (required since ColumnData can be reused)
      writeNulls(columnBytes, baseOffset + 4, initialNumWords, numNulls)
      super.finish(cursor, size)
    } else {
      // make space (or shrink) for writing nulls at the start
      val numNullBytes = numWordsAbs << 3
      val initialNullBytes = initialNumWords << 3
      val oldSize = cursor - baseOffset
      val newSize = checkBufferSize(oldSize + numNullBytes - initialNullBytes)
      val storageAllocator = this.storageAllocator
      val newColumnData = storageAllocator.allocateForStorage(newSize)

      // first copy the rest of the bytes skipping header and nulls
      val srcOffset = 8 + initialNullBytes
      val destOffset = 8 + numNullBytes
      newColumnData.position(destOffset)
      copyTo(newColumnData, srcOffset, oldSize.toInt)
      newColumnData.rewind()

      // reuse this columnData in next round if possible but
      // skip if there was a large wastage in this round
      if (math.abs(initialNumWords - numWordsAbs) < ColumnEncoding.MAX_WASTED_WORDS_FOR_NULLS) {
        releaseForReuse(newSize)
      } else {
        clearSource(newSize, releaseData = true)
      }

      // now write the header including nulls
      val newColumnBytes = storageAllocator.baseObject(newColumnData)
      val writeCursor = storageAllocator.baseOffset(newColumnData)
      // write the typeId
      ColumnEncoding.writeInt(newColumnBytes, writeCursor, typeId)
      // write the null words
      writeNulls(newColumnBytes, writeCursor + 4, numWords, numNulls)
      newColumnData
    }
  }

  override def close(): Unit = {
    if (nullWords ne null) {
      BufferAllocator.releaseBuffer(nullWords)
      nullWords = null
    }
    close(releaseData = true)
  }
}
