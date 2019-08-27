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
/*
 * Portions adapted from Spark's OpenHashSet having the license below.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.snappydata.collection

import java.util.{Iterator => JIterator}

import scala.reflect.ClassTag

import com.gemstone.gemfire.internal.shared.ClientResolverUtils
import com.gemstone.gemfire.internal.size.ReflectionSingleObjectSizer

import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.sql.collection.SharedUtils
import org.apache.spark.storage.TaskResultBlockId
import org.apache.spark.{SparkEnv, TaskContext}

/**
 * A fast hash set implementation for non-null data. This hash set supports
 * insertions and updates, but not deletions. It is much faster than Java's
 * standard HashSet while using much less memory overhead.
 * <p>
 * A special feature of this set is that it allows using the key objects
 * for storing additional data too and allows update of the same by the new
 * passed in key when it matches existing key in the map. Hence it can be
 * used as a more efficient map that uses a single object for both key and
 * value parts (and user's key object can be coded to be so).
 * <p>
 * Adapted from Spark's OpenHashSet implementation. It deliberately uses
 * java interfaces to keep byte code overheads minimal.
 */
final class ObjectHashSet[T <: AnyRef : ClassTag](initialCapacity: Int,
    loadFactor: Double, numColumns: Int, longLived: Boolean = false)
    extends java.lang.Iterable[T] with Serializable {

  val taskContext: TaskContext = TaskContext.get()

  private[this] val consumer = if (taskContext ne null) {
    new ObjectHashSetMemoryConsumer(SharedUtils.taskMemoryManager(taskContext))
  } else null

  if (!longLived && (taskContext ne null)) {
    freeMemoryOnTaskCompletion()
  }

  private[this] var objectSize: Long = -1L
  private[this] var _maxMemory: Long = _

  private[this] var _capacity: Int = SharedUtils.nextPowerOf2(initialCapacity)
  private[this] var _size: Int = 0
  private[this] var _growThreshold: Int = (loadFactor * _capacity).toInt

  private[this] var _mask: Int = _capacity - 1
  private[this] var _data: Array[T] = newArray(_capacity)
  private[this] var _keyIsUnique: Boolean = true
  private[this] var _minValues: Array[Long] = _
  private[this] var _maxValues: Array[Long] = _

  // acquire initial memory
  acquireMemoryForMap(_capacity.toLong)

  private[this] def newArray(capacity: Int): Array[T] =
    implicitly[ClassTag[T]].newArray(capacity)

  def size: Int = _size

  def mask: Int = _mask

  def data: Array[T] = _data

  def keyIsUnique: Boolean = _keyIsUnique

  def maxMemory: Long = _maxMemory

  def addLong(key: Long, default: Long => T): T = {
    val hash = ClientResolverUtils.fastHashLong(key)
    val data = _data
    val mask = _mask
    var pos = hash & mask
    var delta = 1
    while (true) {
      val mapKey = data(pos)
      if (mapKey ne null) {
        val longKey = mapKey.asInstanceOf[LongKey]
        if (longKey.l == key) {
          // update
          return longKey.asInstanceOf[T]
        } else {
          // quadratic probing (increase delta)
          pos = (pos + delta) & mask
          delta += 1
        }
      } else {
        val entry = default(key)
        // insert into the map and rehash if required
        data(pos) = entry.asInstanceOf[T]
        handleNewInsert(pos)
        return entry
      }
    }
    throw new AssertionError("not expected to reach")
  }

  def setKeyIsUnique(unique: Boolean): Unit = {
    if (_keyIsUnique != unique) _keyIsUnique = unique
  }

  private def initArray(values: Array[Long], init: Long): Unit = {
    var index = 0
    val len = values.length
    while (index < len) {
      values(index) = init
      index += 1
    }
  }

  def updateLimits(v: Long, ordinal: Int): Unit = {
    if (_minValues == null) {
      _minValues = new Array[Long](numColumns)
      initArray(_minValues, Long.MaxValue)
    }
    val currentMin = _minValues(ordinal)
    if (v < currentMin) {
      _minValues(ordinal) = v
    }
    if (_maxValues == null) {
      _maxValues = new Array[Long](numColumns)
      initArray(_maxValues, Long.MinValue)
    }
    val currentMax = _maxValues(ordinal)
    if (v > currentMax) {
      _maxValues(ordinal) = v
    }
  }

  def getMinValue(ordinal: Int): Long = _minValues match {
    case null => Long.MaxValue // no value in map so skip everything
    case minValues => minValues(ordinal)
  }

  def getMaxValue(ordinal: Int): Long = _maxValues match {
    case null => Long.MinValue // no value in map so skip everything
    case maxValues => maxValues(ordinal)
  }

  def toArray: Array[AnyRef] = {
    val result = new Array[AnyRef](size)
    val iter = iterator
    var i = 0
    var next: AnyRef = iter.next()
    while (next ne null) {
      result(i) = next
      next = iter.next()
      i += 1
    }
    result
  }

  override def iterator: JIterator[T] = new JIterator[T] {

    private[this] var _pos = -1

    override def hasNext: Boolean =
      throw new UnsupportedOperationException("not expected to be invoked")

    override def remove(): Unit =
      throw new UnsupportedOperationException("not expected to be invoked")

    override def next(): T = {
      val data = _data
      val size = data.length
      var pos = _pos + 1
      while (pos < size) {
        val d = data(pos)
        if (d ne null) {
          _pos = pos
          return d
        }
        pos += 1
      }
      _pos = size
      null.asInstanceOf[T]
    }
  }

  def handleNewInsert(pos: Int): Boolean = {
    if (objectSize == -1) {
      entrySize(pos)
      // adjust memory requirements after first insert
      acquireMemoryForMap(_capacity.toLong, onlyValues = true)
    }
    _size += 1
    // check and trigger a rehash if load factor exceeded
    if (_size <= _growThreshold) {
      false
    } else {
      rehash()
      true
    }
  }

  private def entrySize(pos: Int): Unit = {
    objectSize = ReflectionSingleObjectSizer.INSTANCE.sizeof(data(pos))
  }

  private def acquireMemoryForMap(capacity: Long, onlyValues: Boolean = false): Unit = {
    // Probably overestimating as some of the array cell might be empty
    if (!onlyValues) {
      val refSize = capacity * ReflectionSingleObjectSizer.REFERENCE_SIZE
      acquireMemory(refSize)
      _maxMemory += refSize
    }

    // Also add potential memory usage by objects
    if (objectSize > 0L) {
      val valSize = capacity * objectSize
      acquireMemory(valSize)
      _maxMemory += valSize
    }
  }

  /**
   * Double the table's size and re-hash everything.
   * Caller must check for overloaded set before triggering a rehash.
   */
  private def rehash(): Unit = {
    val capacity = _capacity
    val data = _data

    acquireMemoryForMap(capacity.toLong)

    val newCapacity = SharedUtils.checkCapacity(capacity << 1)
    val newData = newArray(newCapacity)
    val newMask = newCapacity - 1

    var oldPos = 0
    while (oldPos < capacity) {
      val d = data(oldPos)
      if (d ne null) {
        var newPos = d.hashCode() & newMask
        var i = 1
        var keepGoing = true
        // No need to check for equality here when we insert.
        while (keepGoing) {
          if (newData(newPos) eq null) {
            // Inserting the key at newPos
            newData(newPos) = d
            keepGoing = false
          } else {
            val delta = i
            newPos = (newPos + delta) & newMask
            i += 1
          }
        }
      }
      oldPos += 1
    }

    _capacity = newCapacity
    _data = newData
    _mask = newMask
    _growThreshold = (loadFactor * newCapacity).toInt
  }

  private def acquireMemory(required: Long): Unit = {
    if (longLived) {
      val blockId = TaskResultBlockId(taskContext.taskAttemptId())
      SparkEnv.get.memoryManager.acquireStorageMemory(blockId, required, MemoryMode.ON_HEAP)
    } else if (consumer ne null) {
      consumer.acquireMemory(required)
    }
  }

  private def freeMemoryOnTaskCompletion(): Unit = {
    taskContext.addTaskCompletionListener { _ =>
      consumer.freeMemory(_maxMemory)
    }
  }

  def freeStorageMemory(): Unit = {
    assert(longLived, "Method valid for only long lived hashsets")
    val sparkEnv = SparkEnv.get
    if (sparkEnv ne null) {
      sparkEnv.memoryManager.releaseStorageMemory(_maxMemory, MemoryMode.ON_HEAP)
    }
  }
}

abstract class LongKey(val l: Long) {

  override def hashCode(): Int = ClientResolverUtils.fastHashLong(l)

  override def equals(obj: Any): Boolean = obj match {
    case o: LongKey => l == o.l
    case _ => false
  }

  override def toString: String = String.valueOf(l)
}

final class ObjectHashSetMemoryConsumer(taskMemoryManager: TaskMemoryManager)
    extends MemoryConsumer(taskMemoryManager) {
  override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
}
