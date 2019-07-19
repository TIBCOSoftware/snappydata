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

package io.snappydata.collection

import java.nio.ByteBuffer

import com.gemstone.gemfire.internal.shared.{BufferAllocator, BufferSizeLimitExceededException}

import org.apache.spark.TaskContext
import org.apache.spark.memory.{MemoryConsumer, TaskMemoryManager}
import org.apache.spark.sql.collection.SharedUtils
import org.apache.spark.sql.execution.columnar.encoding.ColumnEncoding
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods

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
class ByteBufferHashMap(initialCapacity: Int, val loadFactor: Double,
    keySize: Int, protected val valueSize: Int,
    protected val allocator: BufferAllocator,
    protected var keyData: ByteBufferData = null,
    protected var valueData: ByteBufferData = null,
    protected var valueDataPosition: Long = 0L,
    val approxMaxCapacity: Int = Integer.MAX_VALUE) {
  val taskContext: TaskContext = TaskContext.get()
  private var maxSizeReached: Boolean = false
  private[this] val consumer = if (taskContext ne null) {
    new ByteBufferHashMapMemoryConsumer(SharedUtils.taskMemoryManager(taskContext))
  } else null

  if ((taskContext ne null)) {
    freeMemoryOnTaskCompletion()
  }
  // round to word size adding 8 bytes for header (offset + hashcode)
  private val fixedKeySize = ((keySize + 15) >>> 3) << 3
  private var _capacity = SharedUtils.nextPowerOf2(initialCapacity)

  private var _size = 0
  private var growThreshold = (loadFactor * _capacity).toInt

  private var mask = _capacity - 1
  private[this] var _maxMemory: Long = _
  if (keyData eq null) {
    val buffer = allocator.allocate(_capacity * fixedKeySize, "HASHMAP")
    // clear the key data
    allocator.clearPostAllocate(buffer, 0)
    keyData = new ByteBufferData(buffer, allocator)
    acquireMemory(keyData.capacity)
    _maxMemory += keyData.capacity
  }
  if (valueData eq null) {
    require(_capacity * valueSize < approxMaxCapacity)
    valueData = new ByteBufferData(allocator.allocate(_capacity * valueSize,
      "HASHMAP"), allocator)
    valueDataPosition = valueData.baseOffset
    acquireMemory(valueData.capacity)
    _maxMemory += valueData.capacity
  }



  final def getKeyData: ByteBufferData = this.keyData

  final def getValueData: ByteBufferData = this.valueData

  final def size: Int = _size

  final def valueDataSize: Long = valueDataPosition - valueData.baseOffset

  final def capacity: Int = _capacity
  def maxMemory: Long = _maxMemory

  /**
   * Insert raw bytes with given hash code into the map if not present.
   * The key bytes for comparison is assumed to be at the start having
   * "numKeyBytes" size, while the total size including value is "numBytes".
   * Normally one would have serialized form of key bytes followed by
   * self-contained serialized form of value bytes (i.e. including its size).
   *
   * This method will handle writing only the 8 byte key header while
   * any additional fixed-width bytes to be tracked in read/write should be
   * taken care of in [[handleExisting]] and [[handleNew]].
   * These bytes are not part of key equality check itself.
   *
   * @param baseObject  the base object for the bytes as required by Unsafe API
   * @param baseOffset  the base offset for the bytes as required by Unsafe API
   * @param numKeyBytes number of bytes used by the key which should be at the
   *                    start of "baseObject" with value after that
   * @param numBytes    the total number of bytes used by key and value together
   *                    (excluding the four bytes of "numKeyBytes" itself)
   * @param hash        the hash code of key bytes
   */
  final def putBufferIfAbsent(baseObject: AnyRef, baseOffset: Long, numKeyBytes: Int,
      numBytes: Int, hash: Int): Int = {
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
        // first compare the hash codes followed by "equalsSize" that will
        // include the check for 4 bytes of numKeyBytes itself
        val valueStartOffset = (mapKey >>> 32L).toInt - 4
        if (hash == mapKey.toInt && valueData.equalsSize(valueStartOffset,
          baseObject, baseOffset, numKeyBytes)) {
          return handleExisting(mapKeyObject, mapKeyOffset, valueStartOffset + 4)
        } else {
          // quadratic probing (increase delta)
          pos = (pos + delta) & mask
          delta += 1
        }
      } else {
        if (maxSizeReached) {
          throw ByteBufferHashMap.bsle
        }
        // insert into the map and rehash if required
        val relativeOffset = newInsert(baseObject, baseOffset, numKeyBytes, numBytes)
        Platform.putLong(mapKeyObject, mapKeyOffset,
          (relativeOffset << 32L) | (hash & 0xffffffffL))
        try {
          return handleNew(mapKeyObject, mapKeyOffset, relativeOffset)
        } catch {
          case bsle: BufferSizeLimitExceededException =>
            maxSizeReached = true
            Platform.putLong(mapKeyObject, mapKeyOffset, 0L)
            throw bsle
        }
      }
    }
    0 // not expected to reach
  }

  final def reset(): Unit = {
    keyData.reset(clearMemory = true)
    // no need to clear valueData since it will be overwritten completely
    valueData.reset(clearMemory = false)
    valueDataPosition = valueData.baseOffset
    _size = 0
    this.maxSizeReached = false
  }

  final def release(): Unit = {
    keyData.release(allocator)
    valueData.release(allocator)
    keyData = null
    valueData = null
    this.maxSizeReached = false
  }

  protected def handleExisting(mapKeyObject: AnyRef, mapKeyOffset: Long,
    valueStartOffset: Int): Int = {
    // 0 indicates existing
    0
  }

  protected def handleNew(mapKeyObject: AnyRef, mapKeyOffset: Long, valueStartOffset: Int): Int = {
    handleNewInsert()
    // 1 indicates new insert
    1
  }

  protected final def newInsert(baseObject: AnyRef, baseOffset: Long,
      numKeyBytes: Int, numBytes: Int): Int = {
    // write into the valueData ByteBuffer growing it if required
    var position = valueDataPosition
    val dataSize = position - valueData.baseOffset
    if (position + numBytes + 4 > valueData.endPosition) {
      val oldCapacity = valueData.capacity
      valueData = valueData.resize(numBytes + 4, allocator, approxMaxCapacity)
      position = valueData.baseOffset + dataSize
      acquireMemory(valueData.capacity - oldCapacity)
      _maxMemory += valueData.capacity - oldCapacity
    }
    val valueBaseObject = valueData.baseObject
    // write the key size followed by the full key+value bytes
    ColumnEncoding.writeInt(valueBaseObject, position, numKeyBytes)
    position += 4
    Platform.copyMemory(baseObject, baseOffset, valueBaseObject, position, numBytes)
    valueDataPosition = position + numBytes
    // return the relative offset to the start excluding numKeyBytes
    (dataSize + 4).toInt
  }

  /**
   * Double the table's size and re-hash everything.
   */
  protected final def handleNewInsert(): Unit = {
    _size += 1
    // check and trigger a rehash if load factor exceeded
    if (_size <= growThreshold) return

    val fixedKeySize = this.fixedKeySize
    val newCapacity = SharedUtils.checkCapacity(_capacity << 1)
    val newKeyBuffer = allocator.allocate(newCapacity * fixedKeySize, "HASHMAP")
    acquireMemory(_capacity * fixedKeySize)
    _maxMemory += _capacity * fixedKeySize
    // clear the key data
    allocator.clearPostAllocate(newKeyBuffer, 0)
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
              Platform.copyMemory(keyObject, keyOffset + 8,
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

  private def acquireMemory(required: Long): Unit = {
    if (consumer ne null) {
      consumer.acquireMemory(required)
    }
  }

  private def freeMemoryOnTaskCompletion(): Unit = {
    taskContext.addTaskCompletionListener { _ =>
      consumer.freeMemory(_maxMemory)
    }
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

  def resize(required: Int, allocator: BufferAllocator, maxCapacity: Int): ByteBufferData = {
    val currentUsed = this.buffer.limit;
    val currentCapacity = this.buffer.capacity;
    if (maxCapacity - currentUsed < required ) {
      throw new BufferSizeLimitExceededException(currentUsed, required, maxCapacity);
    }
    // the expand will not full respect max capacity , but that is ok , ...
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
      Platform.setMemory(baseObject, baseOffset,
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
object ByteBufferHashMap {
  val bsle = new BufferSizeLimitExceededException("ByteBufferData capacity reached to max")
}


final class ByteBufferHashMapMemoryConsumer(taskMemoryManager: TaskMemoryManager)
  extends MemoryConsumer(taskMemoryManager) {
  override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
}
