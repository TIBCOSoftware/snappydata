/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import org.apache.spark.sql.execution.columnar.encoding.ColumnEncoding
import org.apache.spark.unsafe.Platform

/**
 * An implementation of [[MapData]] serialized like `SerializedRow` elements
 * that pays attention to the platform endianness and uses a consistent endian
 * format (little-endian) so is suitable for storage.
 */
// TODO: optimize this to use a serialized hash map and corresponding plan
// additions for direct hash based lookup
final class SerializedMap extends MapData {

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
}
