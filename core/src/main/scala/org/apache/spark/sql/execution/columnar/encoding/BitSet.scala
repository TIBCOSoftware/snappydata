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

package org.apache.spark.sql.execution.columnar.encoding

import org.apache.spark.unsafe.Platform

/**
 * Static methods for working with fixed-size bitsets stored elsewhere
 * in a byte array or long array. Similar to Spark's <code>BitSetMethods</code>
 * but respects platform endian-ness so is suitable for storage.
 */
object BitSet {

  /**
   * Sets the bit at the specified index.
   */
  def set(baseObject: AnyRef, baseAddress: Long, position: Int): Unit = {
    val bytePosition = baseAddress + (position >> 3)
    // mod 8 and shift for setting into appropriate byte
    val mask = 1 << (position & 0x7)
    val currentByte = Platform.getByte(baseObject, bytePosition)
    Platform.putByte(baseObject, bytePosition, (currentByte | mask).toByte)
  }

  /**
   * Clears the bit at the specified index.
   */
  def clear(baseObject: AnyRef, baseAddress: Long, position: Int): Unit = {
    val bytePosition = baseAddress + (position >> 3)
    // mod 8 and shift for clearing from appropriate byte
    val mask = 1 << (position & 0x7)
    val currentByte = Platform.getByte(baseObject, bytePosition)
    Platform.putByte(baseObject, bytePosition, (currentByte & ~mask).toByte)
  }

  /**
   * Returns true if the bit is set at the specified index
   * given maximum size of nulls bitmask in bytes.
   */
  def isSet(baseObject: AnyRef, baseAddress: Long, position: Int, maxBytes: Int): Boolean = {
    val byteIndex = position >> 3
    if (byteIndex < maxBytes) {
      val bytePosition = baseAddress + byteIndex
      val currentByte = Platform.getByte(baseObject, bytePosition)
      // mod 8 and shift
      val mask = 1 << (position & 0x7)
      (currentByte & mask) != 0
    }
    else false
  }

  /**
   * Returns true if any bit is set.
   */
  def anySet(baseObject: AnyRef, baseAddress: Long, sizeInBytes: Long): Boolean = {
    var address = baseAddress
    val endAddress = baseAddress + sizeInBytes
    while (address < endAddress) {
      // to just check the presence, endian-ness can be ignored
      if (Platform.getLong(baseObject, address) != 0) return true
      address += 8
    }
    false
  }

  /**
   * Returns the index of the first bit that is set to true that occurs on or after
   * the specified starting index. If no such bit exists then Int.MaxValue is returned.
   */
  def nextSetBit(baseObject: AnyRef, baseAddress: Long, startIndex: Int,
      sizeInBytes: Int): Int = {
    // round to nearest word
    var byteIndex = (startIndex >> 6) << 3
    if (byteIndex < sizeInBytes) {
      // mod 64 gives the number of bits to skip in current word
      val indexInWord = startIndex & 0x3f
      // get as a long for best efficiency in little-endian format
      // i.e. LSB first since that is the way bytes have been written
      var longVal = ColumnEncoding.readLong(baseObject,
        baseAddress + byteIndex) >> indexInWord
      if (longVal != 0) {
        return startIndex + java.lang.Long.numberOfTrailingZeros(longVal)
      }
      // find the next set bit in the rest of the bitSet reading as longs
      // for best efficiency
      byteIndex += 8
      while (byteIndex < sizeInBytes) {
        longVal = ColumnEncoding.readLong(baseObject, baseAddress + byteIndex)
        if (longVal != 0) {
          return (byteIndex << 3) + java.lang.Long.numberOfTrailingZeros(longVal)
        }
        byteIndex += 8
      }
    }
    Int.MaxValue
  }

  /**
   * Number of bits set before given position (exclusive).
   */
  def cardinality(baseObject: AnyRef, baseAddress: Long,
      position: Int, sizeInBytes: Int): Int = {
    val posNumBytes = position >>> 3
    var pos = 0
    val numBytesToCheck = if (sizeInBytes >= posNumBytes) {
      pos = position & 0x3f
      (posNumBytes >>> 3) << 3
    } else {
      // sizeInBytes should be a multiple of 8 so can check all as words
      if ((sizeInBytes & 0x7) != 0) {
        throw new IllegalStateException(
          s"sizeInBytes=$sizeInBytes is not a multiple of 8 (position=$position)")
      }
      sizeInBytes
    }
    var numNulls = 0
    var i = 0
    while (i < numBytesToCheck) {
      // ignoring endian-ness when getting the full count
      val word = Platform.getLong(baseObject, baseAddress + i)
      if (word != 0L) numNulls += java.lang.Long.bitCount(word)
      i += 8
    }
    // last word may remain where position may be in the middle of the word
    if (pos != 0) {
      val word = ColumnEncoding.readLong(baseObject, baseAddress + i)
      if (word != 0) {
        // mask the bits after or at position
        numNulls += java.lang.Long.bitCount(word & ((1L << pos.toLong) - 1L))
      }
    }
    numNulls
  }
}
