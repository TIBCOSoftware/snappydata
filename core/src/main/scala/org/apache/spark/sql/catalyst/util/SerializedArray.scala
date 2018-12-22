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
package org.apache.spark.sql.catalyst.util

import org.apache.spark.sql.execution.columnar.encoding.ColumnEncoding
import org.apache.spark.unsafe.Platform

/**
 * An implementation of [[ArrayData]] serialized like a `SerializedRow`
 * that pays attention to the platform endianness and uses a consistent
 * endian format (little-endian) so is suitable for storage.
 */
final class SerializedArray(_skipBytes: Int = 4)
    extends ArrayData with SerializedRowData {

  this.skipBytes = _skipBytes

  /**
   * Update this SerializedArray to point to different backing data.
   *
   * @param baseObject  the base object
   * @param baseOffset  the offset within the base object
   * @param sizeInBytes the size of this array's backing data, in bytes
   */
  override def pointTo(baseObject: AnyRef, baseOffset: Long,
      sizeInBytes: Int): Unit = {
    assert(skipBytes >= 4, s"skipBytes ($skipBytes) should be >= 4")
    // Read the number of elements from the first 4 bytes or if skipBytes
    // is set to something other than default of 4, then adjust it to find
    // the position where number of elements is written.
    val numElements = ColumnEncoding.readInt(baseObject,
      baseOffset + skipBytes - 4)
    assert(numElements >= 0, s"numElements ($numElements) should >= 0")
    this.baseObject = baseObject
    this.baseOffset = baseOffset
    this.nFields = numElements
    this.bitSetWidthInBytes = calculateBitSetWidthInBytes(numElements)
    this.sizeInBytes = sizeInBytes
  }

  override def numElements: Int = nFields

  override def copy(): SerializedArray = {
    val copy = new SerializedArray(skipBytes)
    val dataCopy = new Array[Byte](sizeInBytes)
    Platform.copyMemory(baseObject, baseOffset, dataCopy,
      Platform.BYTE_ARRAY_OFFSET, sizeInBytes)
    copy.pointTo(dataCopy, Platform.BYTE_ARRAY_OFFSET, sizeInBytes)
    copy
  }

  override def array: Array[Any] =
    throw new UnsupportedOperationException("not supported on SerializedArray")
}
