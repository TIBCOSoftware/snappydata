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
package org.apache.spark.sql.catalyst.util

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}

import org.apache.spark.sql.execution.columnar.encoding.ColumnEncoding
import org.apache.spark.unsafe.Platform

/**
 * An implementation of [[MapData]] serialized like `SerializedRow` elements
 * that pays attention to the platform endianness and uses a consistent endian
 * format (little-endian) so is suitable for storage.
 */
// TODO: optimize this to use a serialized hash map and corresponding plan
// additions for direct hash based lookup
final class SerializedMap extends MapData
    with Externalizable with KryoSerializable {

  private var baseObject: AnyRef = _
  private var baseOffset: Long = _

  // The size of this map's backing data, in bytes.
  // The 4-bytes header of key array `numBytes` is also included, so it's
  // actually equal to 4 + key array numBytes + value array numBytes.
  private var sizeInBytes: Int = _

  private val keys = new SerializedArray(8)
  private val values = new SerializedArray(8)

  def getBaseObject: AnyRef = baseObject

  def getBaseOffset: Long = baseOffset

  def getSizeInBytes: Int = sizeInBytes

  /**
   * Update this SerializedMap to point to different backing data.
   *
   * @param baseObject the base object
   * @param baseOffset the offset within the base object
   */
  def pointTo(baseObject: AnyRef, baseOffset: Long): Unit = {
    // [sumedh] The reason for writing key and value array sizes separately
    // rather than writing total size and key size (like in UnsafeMapData)
    // is that former allows to cleanly align reads on word size of 8 bytes
    // for both keys and values. This has a small overhead of having to read
    // two sizes in decoder nextMap calls. One option can be skipBytes=12
    // when reading keys but avoiding that additional work since optimizing
    // Map needs a completely different layout for hashing in any case.
    var offset = baseOffset
    val keyArraySize = ColumnEncoding.readInt(baseObject, offset)
    assert(keyArraySize >= 8, s"keyArraySize ($keyArraySize) should >= 8")
    keys.pointTo(baseObject, offset, keyArraySize)

    offset += keyArraySize
    val valueArraySize = ColumnEncoding.readInt(baseObject, offset)
    assert(valueArraySize >= 8, s"valueArraySize ($valueArraySize) should >= 8")
    values.pointTo(baseObject, offset, valueArraySize)

    assert(keys.numElements == values.numElements)

    this.baseObject = baseObject
    this.baseOffset = baseOffset
    this.sizeInBytes = keyArraySize + valueArraySize
  }

  def numElements: Int = keys.numElements

  def keyArray: SerializedArray = keys

  def valueArray: SerializedArray = values

  def copy: SerializedMap = {
    val copy = new SerializedMap
    val dataCopy = new Array[Byte](sizeInBytes)
    Platform.copyMemory(baseObject, baseOffset, dataCopy,
      Platform.BYTE_ARRAY_OFFSET, sizeInBytes)
    copy.pointTo(dataCopy, Platform.BYTE_ARRAY_OFFSET)
    copy
  }

  /**
   * Returns the underlying bytes for this row.
   */
  def toBytes: Array[Byte] = {
    if (baseOffset == Platform.BYTE_ARRAY_OFFSET) baseObject match {
      case bytes: Array[Byte] if bytes.length == sizeInBytes => return bytes
      case _ => // fallback to full copy
    }
    val bytes = new Array[Byte](sizeInBytes)
    Platform.copyMemory(baseObject, baseOffset, bytes,
      Platform.BYTE_ARRAY_OFFSET, sizeInBytes)
    bytes
  }

  override def toString: String = {
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
  override def writeExternal(out: ObjectOutput): Unit = {
    val bytes = toBytes
    out.writeInt(bytes.length)
    out.write(bytes)
  }

  @throws[IOException]
  @throws[ClassNotFoundException]
  override def readExternal(in: ObjectInput): Unit = {
    val size = in.readInt
    val bytes = new Array[Byte](size)
    in.readFully(bytes)
    pointTo(bytes, Platform.BYTE_ARRAY_OFFSET)
    if (size != sizeInBytes) {
      throw new IOException(
        s"Expected to read a SerializedMap of size = $size but got $sizeInBytes")
    }
  }

  override def write(kryo: Kryo, out: Output): Unit = {
    val bytes = toBytes
    out.writeInt(bytes.length)
    out.write(bytes)
  }

  override def read(kryo: Kryo, in: Input): Unit = {
    val size = in.readInt
    val bytes = new Array[Byte](size)
    in.read(bytes)
    pointTo(bytes, Platform.BYTE_ARRAY_OFFSET)
    if (size != sizeInBytes) {
      throw new IOException(
        s"Expected to read a SerializedMap of size = $size but got $sizeInBytes")
    }
  }
}
