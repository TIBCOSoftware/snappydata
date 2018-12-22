/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
/*
 * Adapted from Spark's UnsafeRow having the license below:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.util

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecializedGetters, UnsafeRow}
import org.apache.spark.sql.execution.columnar.encoding.{BitSet, ColumnEncoding}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.hash.Murmur3_x86_32
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Like [[UnsafeRow]] uses raw memory bytes to encode the data in a row instead
 * of objects. The difference is that it pays attention to endianness to use a
 * consistent endian format (little-endian) so is suitable for storage.
 */
final class SerializedRow extends InternalRow with SerializedRowData {

  /**
   * Construct a new SerializedRow. The resulting row won't be usable until
   * `pointTo()` has been called, since the value returned by this constructor
   * is equivalent to a null pointer.
   *
   * @param skipBytes number of bytes to skip at the start
   * @param numFields the number of fields in this row
   */
  def this(skipBytes: Int, numFields: Int) = {
    this()
    this.skipBytes = skipBytes
    this.nFields = numFields
    this.bitSetWidthInBytes = calculateBitSetWidthInBytes(numFields)
  }

  /**
   * Copies this row, returning a self-contained SerializedRow that stores
   * its data in an internal byte array.
   */
  override def copy(): SerializedRow = {
    val copy = new SerializedRow(skipBytes, nFields)
    val dataCopy = new Array[Byte](sizeInBytes)
    Platform.copyMemory(baseObject, baseOffset, dataCopy,
      Platform.BYTE_ARRAY_OFFSET, sizeInBytes)
    copy.pointTo(dataCopy, Platform.BYTE_ARRAY_OFFSET, sizeInBytes)
    copy
  }

  override def setNullAt(i: Int): Unit = {
    throw new UnsupportedOperationException("not implemented")
  }

  override def update(i: Int, value: Any): Unit = {
    throw new UnsupportedOperationException("not implemented")
  }
}

/**
 * Contains the main code for `SerializedRow` and `SerializedArray`.
 */
trait SerializedRowData extends SpecializedGetters
    with Externalizable with KryoSerializable {

  protected final var baseObject: AnyRef = _
  protected final var baseOffset: Long = _

  /**
   * The number of bytes reserved for size or something at the start that need
   * to be skipped when reading data otherwise. This is not adjusted in
   * baseOffset since those reserved bytes are part of the data that need to
   * be retained when copying etc.
   */
  protected final var skipBytes: Int = _

  /** The number of fields in this row, used for calculating the bitset width. */
  protected final var nFields: Int = _

  /** The width of the null tracking bit set, in bytes */
  protected final var bitSetWidthInBytes: Int = _

  /** The size of this row's backing data, in bytes) */
  protected final var sizeInBytes: Int = _

  @inline protected final def getFieldCursor(ordinal: Int): Long =
    baseOffset + bitSetWidthInBytes + (ordinal << 3L)

  @inline protected final def indexIsValid(index: Int): Boolean =
    index >= 0 && index < nFields

  protected final def indexInvalid(index: Int): AssertionError =
    new AssertionError(s"index $index should be >= 0 and < $nFields")

  @inline private final def readInt(ordinal: Int): Int =
    ColumnEncoding.readInt(baseObject, getFieldCursor(ordinal))

  @inline private final def readLong(ordinal: Int): Long =
    ColumnEncoding.readLong(baseObject, getFieldCursor(ordinal))

  @inline private final def readBinary(ordinal: Int): Array[Byte] = {
    val offsetAndSize = readLong(ordinal)
    val offset = (offsetAndSize >> 32).toInt
    val size = offsetAndSize.toInt
    val bytes = new Array[Byte](size)
    Platform.copyMemory(baseObject, baseOffset + offset, bytes,
      Platform.BYTE_ARRAY_OFFSET, size)
    bytes
  }

  final def calculateBitSetWidthInBytes(numFields: Int): Int = {
    // subsume skipBytes in the nulls bit set
    UnsafeRow.calculateBitSetWidthInBytes(numFields + (skipBytes << 3))
  }

  //////////////////////////////////////////////////////////////////////////////
  // Public methods
  //////////////////////////////////////////////////////////////////////////////

  final def getBaseObject: AnyRef = baseObject

  final def getBaseOffset: Long = baseOffset

  final def getSkipBytes: Int = skipBytes

  final def getSizeInBytes: Int = sizeInBytes

  final def numFields: Int = nFields

  /**
   * Update this SerializedRow to point to different backing data.
   *
   * @param baseObject  the base object
   * @param baseOffset  the offset within the base object
   * @param sizeInBytes the size of this row's backing data, in bytes
   */
  def pointTo(baseObject: AnyRef, baseOffset: Long, sizeInBytes: Int): Unit = {
    assert(nFields >= 0, s"numColumns ($nFields) should >= 0")
    this.baseObject = baseObject
    this.baseOffset = baseOffset
    this.sizeInBytes = sizeInBytes
  }

  final def get(ordinal: Int, dataType: DataType): AnyRef = dataType match {
    case NullType => null
    case _ if isNullAt(ordinal) => null
    case IntegerType | DateType => Int.box(getInt(ordinal))
    case LongType | TimestampType => Long.box(getLong(ordinal))
    case StringType => getUTF8String(ordinal)
    case DoubleType => Double.box(getDouble(ordinal))
    case d: DecimalType => getDecimal(ordinal, d.precision, d.scale)
    case FloatType => Float.box(getFloat(ordinal))
    case BooleanType => Boolean.box(getBoolean(ordinal))
    case ByteType => Byte.box(getByte(ordinal))
    case ShortType => Short.box(getShort(ordinal))
    case BinaryType => getBinary(ordinal)
    case CalendarIntervalType => getInterval(ordinal)
    case _: ArrayType => getArray(ordinal)
    case _: MapType => getMap(ordinal)
    case s: StructType => getStruct(ordinal, s.size)
    case u: UserDefinedType[_] => get(ordinal, u.sqlType)
    case _ => throw new UnsupportedOperationException(
      s"Unsupported data type ${dataType.simpleString}")
  }

  final def isNullAt(ordinal: Int): Boolean = {
    if (indexIsValid(ordinal)) {
      BitSet.isSet(baseObject, baseOffset, ordinal + (skipBytes << 3),
        bitSetWidthInBytes >> 3)
    } else throw indexInvalid(ordinal)
  }

  final def getBoolean(ordinal: Int): Boolean = {
    if (indexIsValid(ordinal)) {
      Platform.getBoolean(baseObject, getFieldCursor(ordinal))
    } else throw indexInvalid(ordinal)
  }

  final def getByte(ordinal: Int): Byte = {
    if (indexIsValid(ordinal)) {
      Platform.getByte(baseObject, getFieldCursor(ordinal))
    } else throw indexInvalid(ordinal)
  }

  final def getShort(ordinal: Int): Short = {
    if (indexIsValid(ordinal)) {
      ColumnEncoding.readShort(baseObject, getFieldCursor(ordinal))
    } else throw indexInvalid(ordinal)
  }

  final def getInt(ordinal: Int): Int = {
    if (indexIsValid(ordinal)) readInt(ordinal)
    else throw indexInvalid(ordinal)
  }

  final def getLong(ordinal: Int): Long = {
    if (indexIsValid(ordinal)) readLong(ordinal)
    else throw indexInvalid(ordinal)
  }

  final def getFloat(ordinal: Int): Float = {
    if (indexIsValid(ordinal)) {
      ColumnEncoding.readFloat(baseObject, getFieldCursor(ordinal))
    } else throw indexInvalid(ordinal)
  }

  final def getDouble(ordinal: Int): Double = {
    if (indexIsValid(ordinal)) {
      ColumnEncoding.readDouble(baseObject, getFieldCursor(ordinal))
    } else throw indexInvalid(ordinal)
  }

  final def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = {
    // index validity checked in the isNullAt call
    if (isNullAt(ordinal)) null
    else if (precision <= Decimal.MAX_LONG_DIGITS) {
      Decimal.createUnsafe(readLong(ordinal), precision, scale)
    } else {
      val bytes = readBinary(ordinal)
      val bigInteger = new java.math.BigInteger(bytes)
      val javaDecimal = new java.math.BigDecimal(bigInteger, scale)
      Decimal.apply(javaDecimal, precision, scale)
    }
  }

  final def getUTF8String(ordinal: Int): UTF8String = {
    // index validity checked in the isNullAt call
    if (isNullAt(ordinal)) null
    else {
      val offsetAndSize = readLong(ordinal)
      val offset = (offsetAndSize >> 32).toInt
      val size = offsetAndSize.toInt
      UTF8String.fromAddress(baseObject, baseOffset + offset, size)
    }
  }

  final def getBinary(ordinal: Int): Array[Byte] = {
    // index validity checked in the isNullAt call
    if (isNullAt(ordinal)) null
    else readBinary(ordinal)
  }

  final def getInterval(ordinal: Int): CalendarInterval = {
    // index validity checked in the isNullAt call
    if (isNullAt(ordinal)) null
    else {
      val offsetAndSize = readLong(ordinal)
      val offset = (offsetAndSize >> 32).toInt
      val months = offsetAndSize.toInt
      val micros = ColumnEncoding.readLong(baseObject, baseOffset + offset)
      new CalendarInterval(months, micros)
    }
  }

  final def getStruct(ordinal: Int, numFields: Int): SerializedRow = {
    // index validity checked in the isNullAt call
    if (isNullAt(ordinal)) null
    else {
      val offsetAndSize = readLong(ordinal)
      val offset = (offsetAndSize >> 32).toInt
      val size = offsetAndSize.toInt
      val row = new SerializedRow(0, numFields)
      row.pointTo(baseObject, baseOffset + offset, size)
      row
    }
  }

  final def getArray(ordinal: Int): SerializedArray = {
    // index validity checked in the isNullAt call
    if (isNullAt(ordinal)) null
    else {
      val offsetAndSize = readLong(ordinal)
      val offset = (offsetAndSize >> 32).toInt
      val size = offsetAndSize.toInt
      val array = new SerializedArray
      array.pointTo(baseObject, baseOffset + offset, size)
      array
    }
  }

  final def getMap(ordinal: Int): SerializedMap = {
    // index validity checked in the isNullAt call
    if (isNullAt(ordinal)) null
    else {
      val offsetAndSize = readLong(ordinal)
      val offset = (offsetAndSize >> 32).toInt
      val map = new SerializedMap
      map.pointTo(baseObject, baseOffset + offset)
      map
    }
  }

  final def anyNull: Boolean = {
    if (skipBytes == 0) {
      BitSet.anySet(baseObject, baseOffset, bitSetWidthInBytes >> 3)
    } else {
      // need to account separately for skipBytes
      var offset = baseOffset + skipBytes
      // ceil skipBytes to word size of 8 and read byte-wise till there
      val endSkip = baseOffset + ((skipBytes + 7) >>> 3) << 3
      val endOffset = offset + bitSetWidthInBytes
      while (offset < endSkip) {
        if (Platform.getByte(baseObject, offset) != 0) {
          return true
        }
        offset += 1
      }
      while (offset < endOffset) {
        if (Platform.getLong(baseObject, offset) != 0) {
          return true
        }
        offset += 8
      }
      false
    }
  }

  override final def hashCode: Int = {
    // noinspection HashCodeUsesVar
    Murmur3_x86_32.hashUnsafeWords(baseObject, baseOffset, sizeInBytes, 42)
  }

  override final def equals(other: Any): Boolean = other match {
    case o: SerializedRowData =>
      (sizeInBytes == o.sizeInBytes) && ByteArrayMethods.arrayEquals(
        baseObject, baseOffset, o.baseObject, o.baseOffset, sizeInBytes)
    case _: InternalRow => throw new IllegalArgumentException(
      s"Cannot compare SerializedRow to ${other.getClass.getName}")
    case _ => false
  }

  /**
   * Returns the underlying bytes for this row.
   */
  final def toBytes: Array[Byte] = {
    if (baseOffset == Platform.BYTE_ARRAY_OFFSET) baseObject match {
      case bytes: Array[Byte] if bytes.length == sizeInBytes => return bytes
      case _ => // fallback to full copy
    }
    val bytes = new Array[Byte](sizeInBytes)
    Platform.copyMemory(baseObject, baseOffset, bytes,
      Platform.BYTE_ARRAY_OFFSET, sizeInBytes)
    bytes
  }

  override final def toString: String = {
    val sb = new StringBuilder("[")
    val baseOffset = this.baseOffset
    var i = 0
    while (i < sizeInBytes) {
      if (i != 0) sb.append(',')
      // platform endianness deliberately ignored below
      sb.append(java.lang.Long.toHexString(Platform.getLong(baseObject,
        baseOffset + i)))
      i += 8
    }
    sb.append(']')
    sb.toString
  }

  @throws[IOException]
  override final def writeExternal(out: ObjectOutput): Unit = {
    val bytes = toBytes
    out.writeInt(bytes.length)
    out.writeInt(this.skipBytes)
    out.writeInt(this.nFields)
    out.write(bytes)
  }

  @throws[IOException]
  @throws[ClassNotFoundException]
  override final def readExternal(in: ObjectInput): Unit = {
    this.sizeInBytes = in.readInt
    this.skipBytes = in.readInt
    this.nFields = in.readInt
    this.bitSetWidthInBytes = calculateBitSetWidthInBytes(nFields)
    this.baseOffset = Platform.BYTE_ARRAY_OFFSET
    val bytes = new Array[Byte](sizeInBytes)
    in.readFully(bytes)
    this.baseObject = bytes
  }

  override final def write(kryo: Kryo, out: Output): Unit = {
    val bytes = toBytes
    out.writeInt(bytes.length)
    out.writeVarInt(this.skipBytes, true)
    out.writeVarInt(this.nFields, true)
    out.write(bytes)
  }

  override final def read(kryo: Kryo, in: Input): Unit = {
    this.sizeInBytes = in.readInt
    this.skipBytes = in.readVarInt(true)
    this.nFields = in.readVarInt(true)
    this.bitSetWidthInBytes = calculateBitSetWidthInBytes(nFields)
    this.baseOffset = Platform.BYTE_ARRAY_OFFSET
    val bytes = new Array[Byte](sizeInBytes)
    in.read(bytes)
    this.baseObject = bytes
  }
}
