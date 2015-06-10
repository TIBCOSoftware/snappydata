/*
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

package org.apache.spark.sql.collection

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow
import org.apache.spark.sql.types.DataType

import scala.reflect.ClassTag

/**
 * A fast hash map implementation for nullable keys. This hash map supports
 * insertions and updates, but not deletions. This map is about 5X faster than
 * java.util.HashMap, while using much less space overhead.
 *
 * Under the hood, it uses our MultiColumnOpenHashSet implementation.
 */
private[spark] class MultiColumnOpenHashMap[@specialized(Long, Int, Double) V: ClassTag](
  val columns: Array[Int],
  val types: Array[DataType],
  val numColumns: Int,
  val initialCapacity: Int,
  val loadFactor: Double)
    extends SegmentMap[Row, V]
    with Iterable[(Row, V)]
    with Serializable {

  def this(columns: Array[Int], types: Array[DataType], initialCapacity: Int) =
    this(columns, types, columns.length, initialCapacity, 0.7)

  def this(columns: Array[Int], types: Array[DataType]) =
    this(columns, types, 64)

  protected var _keySet = new MultiColumnOpenHashSet(columns, null, types,
    numColumns, initialCapacity, loadFactor)

  // Init in constructor (instead of in declaration) to work around
  // a Scala compiler specialization bug that would generate two arrays
  // (one for Object and one for specialized T).
  private var _values: Array[V] = _
  _values = new Array[V](_keySet.capacity)

  @transient private var _oldValues: Array[V] = null

  // Treat the null key differently so we can use nulls in "data"
  // to represent empty items.
  private var noNullValue = true
  private var nullValue: V = null.asInstanceOf[V]

  override def size: Int = if (noNullValue) _keySet.size else _keySet.size + 1

  override def isEmpty: Boolean = _keySet.size == 0 && noNullValue

  /** Tests whether this map contains a binding for a key. */
  def contains(r: Row): Boolean = {
    if (r != null) {
      val keySet = _keySet
      keySet.getPos(r, keySet.getHash(r)) != MultiColumnOpenHashSet.INVALID_POS
    } else {
      !noNullValue
    }
  }
  /** Tests whether this map contains a binding for a key. */
  override def contains(r: Row, hash: Int): Boolean = {
    if (r != null) {
      _keySet.getPos(r, hash) != MultiColumnOpenHashSet.INVALID_POS
    } else {
      !noNullValue
    }
  }

  /** Get the value for a given key */
  def apply(r: Row): V = {
    if (r != null) {
      val keySet = _keySet
      val pos = keySet.getPos(r, keySet.getHash(r))
      if (pos >= 0) {
        _values(pos)
      } else {
        null.asInstanceOf[V]
      }
    } else {
      nullValue
    }
  }
  /** Get the value for a given key */
  override def apply(r: Row, hash: Int): V = {
    if (r != null) {
      val pos = _keySet.getPos(r, hash)
      if (pos >= 0) {
        _values(pos)
      } else {
        null.asInstanceOf[V]
      }
    } else {
      nullValue
    }
  }

  /** Optionally get the value for a given key */
  def get(r: Row): Option[V] = {
    if (r != null) {
      val keySet = _keySet
      val pos = keySet.getPos(r, keySet.getHash(r))
      if (pos >= 0) {
        Some(_values(pos))
      } else {
        None
      }
    } else {
      None
    }
  }

  /** Set the value for a key */
  def update(r: Row, v: V): Unit = {
    if (r != null) {
      val keySet = _keySet
      val pos = keySet.addWithoutResize(r, keySet.getHash(r))
      if ((pos & MultiColumnOpenHashSet.NONEXISTENCE_MASK) != 0) {
        _values(pos & MultiColumnOpenHashSet.POSITION_MASK) = v
        keySet.rehashIfNeeded(r, grow, move)
      } else {
        _values(pos) = v
      }
    } else {
      if (noNullValue) {
        noNullValue = false
      }
      nullValue = v
    }
  }
  /** Set the value for a key */
  override def update(r: Row, hash: Int, v: V): Boolean = {
    if (r != null) {
      val keySet = _keySet
      val pos = keySet.addWithoutResize(r, hash)
      if ((pos & MultiColumnOpenHashSet.NONEXISTENCE_MASK) != 0) {
        _values(pos & MultiColumnOpenHashSet.POSITION_MASK) = v
        keySet.rehashIfNeeded(r, grow, move)
        true
      } else {
        _values(pos) = v
        false
      }
    } else {
      if (noNullValue) {
        noNullValue = false
        nullValue = v
        true
      } else {
        nullValue = v
        false
      }
    }
  }

  /**
   * If the key doesn't exist yet in the hash map, set its value to
   * defaultValue; otherwise, set its value to mergeValue(oldValue).
   *
   * @return the newly updated value.
   */
  def changeValue(r: Row, change: ChangeValue[Row, V]): V = {
    if (r != null) {
      val keySet = _keySet
      val pos = keySet.addWithoutResize(r, keySet.getHash(r))
      if ((pos & MultiColumnOpenHashSet.NONEXISTENCE_MASK) != 0) {
        val newValue = change.defaultValue(r)
        _values(pos & MultiColumnOpenHashSet.POSITION_MASK) = newValue
        keySet.rehashIfNeeded(r, grow, move)
        newValue
      } else {
        _values(pos) = change.mergeValue(r, _values(pos))
        _values(pos)
      }
    } else {
      if (noNullValue) {
        noNullValue = false
        nullValue = change.defaultValue(r)
      } else {
        nullValue = change.mergeValue(r, nullValue)
      }
      nullValue
    }
  }
  /**
   * If the key doesn't exist yet in the hash map, set its value to
   * defaultValue; otherwise, set its value to mergeValue(oldValue).
   *
   * @return true if new value was added, false if it was merged and null
   *         if the default/merge calls returned null and nothing was done
   */
  override def changeValue(r: Row, hash: Int,
                           change: ChangeValue[Row, V]): Option[Boolean] = {
    if (r != null) {
      val keySet = _keySet
      val pos = keySet.addWithoutResize(r, hash)
      if ((pos & MultiColumnOpenHashSet.NONEXISTENCE_MASK) != 0) {
        val v = change.defaultValue(r)
        if (v != null) {
          _values(pos & MultiColumnOpenHashSet.POSITION_MASK) = v
          keySet.rehashIfNeeded(r, grow, move)
          SegmentMap.TRUE_OPTION
        }
        else None
      } else {
        val v = change.mergeValue(r, _values(pos))
        if (v != null) {
          _values(pos) = v
          SegmentMap.FALSE_OPTION
        }
        else None
      }
    } else {
      if (noNullValue) {
        noNullValue = false
        nullValue = change.defaultValue(r)
        SegmentMap.TRUE_OPTION
      } else {
        nullValue = change.mergeValue(r, nullValue)
        SegmentMap.FALSE_OPTION
      }
    }
  }

  override def fold[U](init: U)(f: (Row, V, U) => U): U = {
    var v = init
    // first check for null value
    if (!noNullValue) {
      v = f(null, nullValue, v)
    }
    // next go through the entire map
    val keySet = _keySet
    val values = _values
    val currentKey: SpecificMutableRow = keySet.newEmptyValueAsRow()
    var pos = keySet.nextPos(0)
    while (pos >= 0) {
      keySet.fillValueAsRow(pos, currentKey)
      v = f(currentKey, values(pos), v)
      pos = keySet.nextPos(pos + 1)
    }
    v
  }

  override def iterator: Iterator[(Row, V)] = new Iterator[(Row, V)] {
    var pos = -1
    val currentKey: SpecificMutableRow = _keySet.newEmptyValueAsRow()
    var nextPair: (Row, V) = computeNextPair()

    /**
     * Get the next value we should return from next(),
     * or null if we're finished iterating
     */
    def computeNextPair(): (Row, V) = {
      if (pos == -1) { // Treat position -1 as looking at the null value
        pos += 1
        if (!noNullValue) {
          return (null, nullValue)
        }
      }
      pos = _keySet.nextPos(pos)
      if (pos >= 0) {
        _keySet.fillValueAsRow(pos, currentKey)
        val ret = (currentKey, _values(pos))
        pos += 1
        ret
      } else {
        null
      }
    }

    override def hasNext: Boolean = nextPair != null

    override def next(): (Row, V) = {
      val pair = nextPair
      nextPair = computeNextPair()
      pair
    }
  }

  final def valuesIterator: Iterator[V] = new Iterator[V] {
    var pos = -1
    var nextV: V = computeNextV()

    /**
     * Get the next value we should return from next(),
     * or null if we're finished iterating
     */
    def computeNextV(): V = {
      if (pos == -1) { // Treat position -1 as looking at the null value
        pos += 1
        if (!noNullValue) {
          return nullValue
        }
      }
      pos = _keySet.nextPos(pos)
      if (pos >= 0) {
        val ret = _values(pos)
        pos += 1
        ret
      } else {
        null.asInstanceOf[V]
      }
    }

    override def hasNext: Boolean = nextV != null
    override def next(): V = {
      val v = nextV
      nextV = computeNextV()
      v
    }
  }

  // The following member variables are declared as protected instead of
  // private for the specialization to work (specialized class extends the
  // non-specialized one and needs access to the "private" variables).
  // They also should have been val's. We use var because there is
  // a Scala compiler bug that would throw illegal access error at runtime
  // if they are declared as val's.
  protected var grow = (newCapacity: Int) => {
    _oldValues = _values
    _values = new Array[V](newCapacity)
  }

  protected var move = (oldPos: Int, newPos: Int) => {
    _values(newPos) = _oldValues(oldPos)
  }
}
