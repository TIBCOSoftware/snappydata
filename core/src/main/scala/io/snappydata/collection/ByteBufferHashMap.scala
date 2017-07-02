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

package io.snappydata.collection

import java.nio.ByteBuffer

import com.gemstone.gemfire.internal.shared.BufferAllocator
import com.gemstone.gemfire.internal.shared.unsafe.UnsafeHolder

import org.apache.spark.sql.execution.columnar.encoding.ColumnEncoding
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.types.UTF8String

/**
 * A HashMap implementation using a serialized ByteBuffer for key data and
 * another one for value data. Key data is required to hold fixed-width
 * values while the value data will be written back-to-back as new data
 * is inserted into the map. Key data is stored in the following format:
 * {{{
 *     .------------------------ Offset into value data (4 bytes)
 *     |   .-------------------- Hash code (4 bytes)
 *     |   |
 *     |   |        .----------- Fixed-width fields of key data
 *     |   |        |        .-- Padding for 8 byte word alignment
 *     V   V        V        V
 *   +---+---+-------------+---+
 *   |   |   | ... ... ... |   |
 *   +---+---+-------------+---+
 *    \-----/ \------------/\--/
 *     header      body    padding
 * }}}
 * If key has variable length data, then it should be appended to the
 * value data. The offset+hash code is read as a single long where LSB
 * is used for hash code while MSB is used for offset, so the two can
 * be reverse in actual memory layout on big-endian machines. Since there
 * is no disk storage of this map so no attempt is made to have consistent
 * endianness or memory layout of the data.
 *
 * Rehash of the map (when loadFactor exceeds) moves around the above key
 * fields to create a new array as per the new hash locations.
 * The value fields are left untouched with the headers of keys having the
 * offsets into value array as before the rehash.
 */
final class ByteBufferHashMap(initialCapacity: Int, val loadFactor: Double,
    keySize: Int, private val valueSize: Int,
    private val allocator: BufferAllocator,
    var keyData: ByteBufferData = null,
    var valueData: ByteBufferData = null,
    var valueDataPosition: Long = 0L) {

  // round to word size adding 8 bytes for header (offset + hashcode)
  private val fixedKeySize = ((keySize + 15) >>> 3) << 3
  private var _capacity = ObjectHashSet.nextPowerOf2(
    initialCapacity, loadFactor)
  private var _size = 0
  private var growThreshold = (loadFactor * _capacity).toInt

  private var mask = _capacity - 1

  if (keyData eq null) {
    val buffer = allocator.allocate(_capacity * fixedKeySize, "HASHMAP")
    // clear the key data
    allocator.clearPostAllocate(buffer)
    keyData = new ByteBufferData(buffer, allocator)
  }
  if (valueData eq null) {
    valueData = new ByteBufferData(allocator.allocate(_capacity * valueSize,
      "HASHMAP"), allocator)
    valueDataPosition = valueData.baseOffset
  }

  def size: Int = _size

  def valueDataSize: Long = valueDataPosition - valueData.baseOffset

  /**
   * Add a new string to the map for dictionaries. The key field has the
   * index of the value i.e. (n - 1) for nth distinct string added to the map,
   * with the offset into the value. The string itself is stored back to back
   * in the value portion with its size at the start being variable length.
   * This exactly matches the end format of the dictionary encoding that
   * stores the dictionary string back-to-back in index order and expected
   * by DictionaryDecoders. So the encoder can use the final
   * value serialized array as is for putting into the encoded column batch
   * (followed by the dictionary indexes of actual values themselves).
   *
   * The encoded values are read in the initialization of DictionaryDecoder
   * and put into an array, and looked up by its readUTF8String method.
   */
  def addDictionaryString(key: UTF8String): Int = {
    val hash = key.hashCode()
    val mapKeyObject = keyData.baseObject
    val mapKeyBaseOffset = keyData.baseOffset
    val fixedKeySize = this.fixedKeySize
    val mask = this.mask
    var pos = hash & mask
    var delta = 1
    while (true) {
      val mapKeyOffset = mapKeyBaseOffset + fixedKeySize * pos
      val mapKey = Platform.getLong(mapKeyObject, mapKeyOffset)
      // offset will at least be 4 so mapKey can never be zero when occupied
      if (mapKey != 0L) {
        // first compare the hash codes
        val mapKeyHash = mapKey.toInt
        // equalsSize will include check for 4 bytes of numBytes itself
        if (hash == mapKeyHash && valueData.equalsSize((mapKey >>> 32L).toInt - 4,
          key.getBaseObject, key.getBaseOffset, key.numBytes())) {
          return Platform.getInt(mapKeyObject, mapKeyOffset + 8)
        } else {
          // quadratic probing with position increase by 1, 2, 3, ...
          pos = (pos + delta) & mask
          delta += 1
        }
      } else {
        // insert into the map and rehash if required
        val relativeOffset = newInsert(key)
        val newIndex = _size
        Platform.putLong(mapKeyObject, mapKeyOffset,
          (relativeOffset.toLong << 32) | (hash & 0xffffffffL))
        Platform.putInt(mapKeyObject, mapKeyOffset + 8, newIndex)
        handleNewInsert()
        // return negative of index to indicate insert
        return -newIndex - 1
      }
    }
    throw new AssertionError("not expected to reach")
  }

  def duplicate(): ByteBufferHashMap = {
    new ByteBufferHashMap(_capacity - 1, loadFactor, keySize, valueSize,
      allocator, keyData.duplicate(), valueData.duplicate(), valueData.baseOffset)
  }

  def reset(): Unit = {
    keyData.reset(clearMemory = true)
    // no need to clear valueData since it will be overwritten completely
    valueData.reset(clearMemory = false)
    valueDataPosition = valueData.baseOffset
    _size = 0
  }

  def release(): Unit = {
    keyData.release(allocator)
    valueData.release(allocator)
    keyData = null
    valueData = null
  }

  private def newInsert(s: UTF8String): Int = {
    // write into the valueData ByteBuffer growing it if required
    val numBytes = s.numBytes()
    var position = valueDataPosition
    val dataSize = position - valueData.baseOffset
    if (position + numBytes + 4 > valueData.endPosition) {
      valueData = valueData.resize(numBytes + 4, allocator)
      position = valueData.baseOffset + dataSize
    }
    valueDataPosition = ColumnEncoding.writeUTF8String(valueData.baseObject,
      position, s.getBaseObject, s.getBaseOffset, numBytes)
    // return the relative offset to the start excluding numBytes
    (dataSize + 4).toInt
  }

  /**
   * Double the table's size and re-hash everything.
   */
  private def handleNewInsert(): Unit = {
    _size += 1
    // check and trigger a rehash if load factor exceeded
    if (_size <= growThreshold) return

    val fixedKeySize = this.fixedKeySize
    val newCapacity = ObjectHashSet.checkCapacity(_capacity << 1, loadFactor)
    val newKeyBuffer = allocator.allocate(newCapacity * fixedKeySize, "HASHMAP")
    // clear the key data
    allocator.clearPostAllocate(newKeyBuffer)
    val newKeyData = new ByteBufferData(newKeyBuffer, allocator)
    val newKeyObject = newKeyData.baseObject
    val newKeyBaseOffset = newKeyData.baseOffset
    val newMask = newCapacity - 1

    val keyData = this.keyData
    val keyObject = keyData.baseObject
    var keyOffset = keyData.baseOffset
    val keyEndPosition = keyData.endPosition

    while (keyOffset < keyEndPosition) {
      val key = Platform.getLong(keyObject, keyOffset)
      if (key != 0L) {
        var newPos = key.toInt & newMask
        var delta = 1
        var keepGoing = true
        while (keepGoing) {
          val newOffset = newKeyBaseOffset + fixedKeySize * newPos
          if (Platform.getLong(newKeyObject, newOffset) == 0L) {
            // Inserting the key at newPos
            Platform.putLong(newKeyObject, newOffset, key)
            if (fixedKeySize > 8) {
              UnsafeHolder.getUnsafe.copyMemory(keyObject, keyOffset + 8,
                newKeyObject, newOffset + 8, fixedKeySize - 8)
            }
            keepGoing = false
          } else {
            newPos = (newPos + delta) & newMask
            delta += 1
          }
        }
      }
      keyOffset += fixedKeySize
    }

    _capacity = newCapacity
    this.growThreshold = (loadFactor * newCapacity).toInt
    this.mask = newMask
    keyData.release(allocator)
    this.keyData = newKeyData
  }
}

final class ByteBufferData private(val buffer: ByteBuffer,
    val baseObject: AnyRef, val baseOffset: Long, val endPosition: Long) {

  def this(buffer: ByteBuffer, baseObject: AnyRef, baseOffset: Long) = {
    this(buffer, baseObject, baseOffset, baseOffset + buffer.limit())
  }

  def this(buffer: ByteBuffer, allocator: BufferAllocator) = {
    this(buffer, allocator.baseObject(buffer), allocator.baseOffset(buffer))
  }

  def capacity: Int = (endPosition - baseOffset).toInt

  def equals(srcOffset: Int, oBase: AnyRef, oBaseOffset: Long,
      size: Int): Boolean = {
    ByteArrayMethods.arrayEquals(baseObject, baseOffset + srcOffset,
      oBase, oBaseOffset, size)
  }

  /** Check equality assuming size is written on this data at the start. */
  def equalsSize(srcOffset: Int, oBase: AnyRef, oBaseOffset: Long,
      size: Int): Boolean = {
    val baseObject = this.baseObject
    val offset = this.baseOffset + srcOffset
    // below is ColumnEncoding.readInt and not Platform.readInt because the
    // write is using ColumnEncoding.writeUTF8String which writes the size
    // using former (which respects endianness)
    ColumnEncoding.readInt(baseObject, offset) == size && ByteArrayMethods
        .arrayEquals(baseObject, offset + 4, oBase, oBaseOffset, size)
  }

  def resize(required: Int, allocator: BufferAllocator): ByteBufferData = {
    val buffer = allocator.expand(this.buffer, required, "HASHMAP")
    val baseOffset = allocator.baseOffset(buffer)
    new ByteBufferData(buffer, allocator.baseObject(buffer), baseOffset,
      baseOffset + buffer.limit())
  }

  def duplicate(): ByteBufferData = {
    new ByteBufferData(buffer, baseObject, baseOffset, endPosition)
  }

  def reset(clearMemory: Boolean): Unit = {
    if (clearMemory) {
      UnsafeHolder.getUnsafe.setMemory(baseObject, baseOffset,
        // use capacity which is likely to be factor of 8 where setMemory
        // will be more efficient
        buffer.capacity(), 0)
    }
    buffer.rewind()
  }

  def release(allocator: BufferAllocator): Unit = {
    allocator.release(buffer)
  }
}
