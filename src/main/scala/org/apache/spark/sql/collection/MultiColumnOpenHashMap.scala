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

import scala.collection.generic.CanBuildFrom
import scala.collection.{IterableLike, Iterator, mutable}
import scala.reflect.ClassTag

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow
import org.apache.spark.sql.collection.MultiColumnOpenHashSet._
import org.apache.spark.sql.types.DataType

/**
 * A fast hash map implementation for nullable keys. This hash map supports
 * insertions and updates, but not deletions. This map is about 5X faster than
 * java.util.HashMap, while using much less space overhead.
 *
 * Under the hood, it uses our MultiColumnOpenHashSet implementation.
 */
final class MultiColumnOpenHashMap[@specialized(Long, Int, Double) V: ClassTag](
    _columns: Array[Int],
    _types: Array[DataType],
    _numColumns: Int,
    _initialCapacity: Int,
    _loadFactor: Double)
    extends SegmentMap[Row, V]
    with mutable.Map[Row, V]
    with mutable.MapLike[Row, V, MultiColumnOpenHashMap[V]]
    with Iterable[(Row, V)]
    with IterableLike[(Row, V), MultiColumnOpenHashMap[V]]
    with mutable.Builder[(Row, V), MultiColumnOpenHashMap[V]]
    with Serializable {

  self =>

  def this(columns: Array[Int], types: Array[DataType], initialCapacity: Int) =
    this(columns, types, columns.length, initialCapacity, 0.7)

  def this(columns: Array[Int], types: Array[DataType]) =
    this(columns, types, 64)

  private val _keySet = new MultiColumnOpenHashSet(_columns, _types,
    _numColumns, _initialCapacity, _loadFactor)

  // Init in constructor (instead of in declaration) to work around
  // a Scala compiler specialization bug that would generate two arrays
  // (one for Object and one for specialized T).
  private[sql] var _values: Array[V] = _
  _values = new Array[V](_keySet.capacity)

  @transient private var _oldValues: Array[V] = null

  // Treat the null row differently so we can use nulls in "data"
  // to represent empty items.
  private[sql] var noNullValue = true
  private[sql] var nullValue: V = null.asInstanceOf[V]

  override def size: Int = if (noNullValue) _keySet.size else _keySet.size + 1

  override def isEmpty: Boolean = _keySet.isEmpty && noNullValue

  override def nonEmpty: Boolean = _keySet.nonEmpty || noNullValue

  /** Tests whether this map contains a binding for a row. */
  private def contains_(r: Row, columnHandler: ColumnHandler): Boolean = {
    val keySet = _keySet
    keySet.getPos(r, keySet.getHash(r, columnHandler),
      columnHandler) != INVALID_POS
  }

  /** Tests whether this map contains a binding for a row. */
  override def contains(r: Row): Boolean = {
    if (r != null) {
      contains_(r, _keySet.getColumnHandler(r))
    } else {
      !noNullValue
    }
  }

  /** Tests whether this map contains a binding for a projected row. */
  def contains(r: SpecificMutableRow): Boolean = {
    if (r != null) {
      contains_(r, _keySet.getColumnHandler(r))
    } else {
      !noNullValue
    }
  }

  /** Tests whether this map contains a binding for a row. */
  override def contains(r: Row, hash: Int): Boolean = {
    if (r != null) {
      val keySet = _keySet
      keySet.getPos(r, hash, keySet.getColumnHandler(r)) != INVALID_POS
    } else {
      !noNullValue
    }
  }

  /** Get the value for a given row */
  private def get_(r: Row, columnHandler: ColumnHandler): V = {
    if (r != null) {
      val keySet = _keySet
      val pos = keySet.getPos(r, keySet.getHash(r, columnHandler),
        columnHandler)
      if (pos >= 0) {
        _values(pos)
      } else {
        null.asInstanceOf[V]
      }
    } else {
      nullValue
    }
  }

  /** Get the value for a given row */
  override def apply(r: Row): V = {
    if (r != null) {
      get_(r, _keySet.getColumnHandler(r))
    } else {
      nullValue
    }
  }

  /** Get the value for a given row */
  def apply(r: SpecificMutableRow): V = {
    if (r != null) {
      get_(r, _keySet.getColumnHandler(r))
    } else {
      nullValue
    }
  }

  /** Get the value for a given row */
  override def apply(r: Row, hash: Int): V = {
    if (r != null) {
      val keySet = _keySet
      val pos = keySet.getPos(r, hash, keySet.getColumnHandler(r))
      if (pos >= 0) {
        _values(pos)
      } else {
        null.asInstanceOf[V]
      }
    } else {
      nullValue
    }
  }

  /** Optionally get the value for a given row */
  override def get(r: Row): Option[V] = Option(apply(r))

  /** Optionally get the value for a given row */
  def get(r: SpecificMutableRow): Option[V] = Option(apply(r))

  /** Set the value for a row */
  private def putValue(r: Row, v: V, hash: Int,
      columnHandler: ColumnHandler): Option[V] = {
    val keySet = _keySet
    val pos = keySet.addWithoutResize(r, hash, columnHandler)
    if ((pos & NONEXISTENCE_MASK) != 0) {
      _values(pos & POSITION_MASK) = v
      keySet.rehashIfNeeded(r, grow, move)
      None
    } else {
      val oldV = _values(pos)
      _values(pos) = v
      Option(oldV)
    }
  }

  /** Set the value for special null row */
  private def putNull(v: V): Option[V] = {
    if (noNullValue) {
      noNullValue = false
      nullValue = v
      None
    } else {
      val oldV = nullValue
      nullValue = v
      Option(oldV)
    }
  }

  /** Set the value for a row */
  override def put(r: Row, v: V): Option[V] = {
    if (r != null) {
      val keySet = _keySet
      val columnHandler = keySet.getColumnHandler(r)
      putValue(r, v, keySet.getHash(r, columnHandler), columnHandler)
    } else {
      putNull(v)
    }
  }

  /** Set the value for a row */
  override def update(r: Row, v: V) {
    if (r != null) {
      val keySet = _keySet
      val columnHandler = keySet.getColumnHandler(r)
      putValue(r, v, keySet.getHash(r, columnHandler), columnHandler)
    } else {
      putNull(v)
    }
  }

  /** Set the value for a row */
  def update(r: SpecificMutableRow, v: V) {
    if (r != null) {
      val keySet = _keySet
      val columnHandler = keySet.getColumnHandler(r)
      putValue(r, v, keySet.getHash(r, columnHandler), columnHandler)
    } else {
      putNull(v)
    }
  }

  /** Set the value for a row given pre-computed hash */
  override def update(r: Row, hash: Int, v: V): Boolean = {
    if (r != null) {
      putValue(r, v, hash, _keySet.getColumnHandler(r)).isEmpty
    } else {
      putNull(v).isEmpty
    }
  }

  override def +=(elem: (Row, V)) = {
    self.update(elem._1, elem._2)
    self
  }

  override def remove(row: Row) = throw new UnsupportedOperationException

  override def -=(row: Row) = throw new UnsupportedOperationException

  override def empty: MultiColumnOpenHashMap[V] = {
    val keySet = _keySet
    new MultiColumnOpenHashMap[V](keySet.columns, keySet.types,
      keySet.numColumns, 1, keySet.loadFactor)
  }

  /**
   * If the row doesn't exist yet in the hash map, set its value to
   * defaultValue; otherwise, set its value to mergeValue(oldValue).
   *
   * @return true if new value was added, false if it was merged and null
   *         if the default/merge calls returned null and nothing was done
   */
  private def changeValue(r: Row, hash: Int, columnHandler: ColumnHandler,
      change: ChangeValue[Row, V]): Option[Boolean] = {
    val keySet = _keySet
    val pos = keySet.addWithoutResize(r, hash, columnHandler)
    if ((pos & NONEXISTENCE_MASK) != 0) {
      val v = change.defaultValue(r)
      if (v != null) {
        _values(pos & POSITION_MASK) = v
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
  }

  /** Change value for the special null row */
  private def changeValueForNull(c: ChangeValue[Row, V]): Option[Boolean] = {
    if (noNullValue) {
      noNullValue = false
      nullValue = c.defaultValue(null)
      SegmentMap.TRUE_OPTION
    } else {
      nullValue = c.mergeValue(null, nullValue)
      SegmentMap.FALSE_OPTION
    }
  }

  /**
   * If the row doesn't exist yet in the hash map, set its value to
   * defaultValue; otherwise, set its value to mergeValue(oldValue).
   *
   * @return the newly updated value.
   */
  def changeValue(r: Row, change: ChangeValue[Row, V]): Option[Boolean] = {
    if (r != null) {
      val keySet = _keySet
      val columnHandler = keySet.getColumnHandler(r)
      changeValue(r, keySet.getHash(r, columnHandler), columnHandler, change)
    } else {
      changeValueForNull(change)
    }
  }

  /**
   * If the row doesn't exist yet in the hash map, set its value to
   * defaultValue; otherwise, set its value to mergeValue(oldValue).
   *
   * @return the newly updated value.
   */
  def changeValue(r: SpecificMutableRow,
      change: ChangeValue[Row, V]): Option[Boolean] = {
    if (r != null) {
      val keySet = _keySet
      val columnHandler = keySet.getColumnHandler(r)
      changeValue(r, keySet.getHash(r, columnHandler), columnHandler, change)
    } else {
      changeValueForNull(change)
    }
  }

  /**
   * If the row doesn't exist yet in the hash map, set its value to
   * defaultValue; otherwise, set its value to mergeValue(oldValue).
   *
   * @return true if new value was added, false if it was merged and null
   *         if the default/merge calls returned null and nothing was done
   */
  override def changeValue(r: Row, hash: Int,
      change: ChangeValue[Row, V]): Option[Boolean] = {
    if (r != null) {
      changeValue(r, hash, _keySet.getColumnHandler(r), change)
    } else {
      changeValueForNull(change)
    }
  }

  override def foldValues[U](init: U)(f: (V, U) => U): U = {
    var v = init
    // first check for null value
    if (!noNullValue) {
      v = f(nullValue, v)
    }
    // next go through the entire map
    val bitset = _keySet.getBitSet
    val values = _values
    var pos = bitset.nextSetBit(0)
    while (pos >= 0) {
      v = f(values(pos), v)
      pos = bitset.nextSetBit(pos + 1)
    }
    v
  }

  override def foldEntries[U](init: U)(f: (Row, V, U) => U): U = {
    var v = init
    // first check for null value
    if (!noNullValue) {
      v = f(null, nullValue, v)
    }
    // next go through the entire map
    val keySet = _keySet
    val bitset = keySet.getBitSet
    val values = _values
    val currentKey = keySet.newEmptyValueAsRow()
    var pos = bitset.nextSetBit(0)
    while (pos >= 0) {
      keySet.fillValueAsRow(pos, currentKey)
      v = f(currentKey, values(pos), v)
      pos = bitset.nextSetBit(pos + 1)
    }
    v
  }

  def mapValues[B: ClassTag](f: V => B): MultiColumnOpenHashMap[B] = {
    val keySet = self._keySet
    val values = self._values
    val capacity = keySet.capacity
    val otherMap = new MultiColumnOpenHashMap[B](keySet.columns, keySet.types,
      keySet.numColumns, capacity, keySet.loadFactor)
    val otherValues = otherMap._values
    if (capacity == otherMap._keySet.capacity) {
      val bitset = keySet.getBitSet
      keySet.copyTo(otherMap._keySet)
      // map exact array by array
      var pos = bitset.nextSetBit(0)
      while (pos >= 0) {
        otherValues(pos) = f(values(pos))
        pos = bitset.nextSetBit(pos + 1)
      }
      if (!self.noNullValue) {
        otherMap.noNullValue = false
        otherMap.nullValue = f(self.nullValue)
      }
    }
    else {
      // for some reason capacity is still different so use slower row by row
      self.iteratorRowReuse.foreach { case (row, v) =>
        otherMap.update(row, f(v))
      }
    }
    otherMap
  }

  def groupBy[K](groupOp: (Row, V) => Row,
      combineOp: (V, V) => V): MultiColumnOpenHashMap[V] = {
    val m = newBuilder
    for ((row, value) <- this.iteratorRowReuse) {
      val key = groupOp(row, value)
      m.changeValue(key, new ChangeValue[Row, V] {
        override def defaultValue(k: Row) = value

        override def mergeValue(k: Row, v: V): V = combineOp(v, value)
      })
    }
    m
  }

  private def newBuilder[B: ClassTag](keySet: MultiColumnOpenHashSet) = {
    new MultiColumnOpenHashMap[B](keySet.columns, keySet.types,
      keySet.numColumns, keySet.capacity, keySet.loadFactor)
  }

  override protected[this] def newBuilder = newBuilder[V](self._keySet)

  implicit def canBuildFrom[B: ClassTag] =
    new CanBuildFrom[MultiColumnOpenHashMap[V], (Row, B),
        MultiColumnOpenHashMap[B]] {

      override def apply(from: MultiColumnOpenHashMap[V]) =
        newBuilder(from._keySet)

      override def apply() = newBuilder[B](self._keySet)
    }

  abstract private class HashIterator[A] extends Iterator[A] {

    final val bitset = _keySet.getBitSet
    var pos = bitset.nextSetBit(0)
    var nextVal = if (noNullValue) computeNext() else valueForNullKey

    def valueForNullKey: A

    def newEmptyRow(): SpecificMutableRow

    def buildResult(row: SpecificMutableRow, v: V): A

    /**
     * Get the next value we should return from next(),
     * or null if we're finished iterating
     */
    private final def computeNext(): A = {
      if (pos >= 0) {
        val row = newEmptyRow()
        val ret = if (row != null) {
          _keySet.fillValueAsRow(pos, row)
          buildResult(row, _values(pos))
        }
        else {
          _values(pos).asInstanceOf[A]
        }
        pos = bitset.nextSetBit(pos + 1)
        ret
      } else {
        null.asInstanceOf[A]
      }
    }

    override final def hasNext: Boolean = nextVal != null

    override final def next(): A = {
      val a = nextVal
      nextVal = computeNext()
      a
    }
  }

  override def iterator: Iterator[(Row, V)] = new HashIterator[(Row, V)] {

    def valueForNullKey = (null.asInstanceOf[Row], nullValue)

    def newEmptyRow() = _keySet.newEmptyValueAsRow()

    def buildResult(row: SpecificMutableRow, v: V) = (row.asInstanceOf[Row], v)
  }

  def iteratorRowReuse: Iterator[(SpecificMutableRow, V)] =
    new HashIterator[(SpecificMutableRow, V)] {

      private[this] val currentRow = _keySet.newEmptyValueAsRow()

      def valueForNullKey = (null.asInstanceOf[SpecificMutableRow], nullValue)

      def newEmptyRow() = currentRow

      def buildResult(row: SpecificMutableRow, v: V) = (row, v)
    }

  override def foreach[U](f: ((Row, V)) => U) = iteratorRowReuse.foreach(f)

  /* Override to avoid tuple allocation */
  override def keysIterator: Iterator[Row] = new HashIterator[Row] {

    def valueForNullKey = null.asInstanceOf[Row]

    def newEmptyRow() = _keySet.newEmptyValueAsRow()

    def buildResult(row: SpecificMutableRow, v: V): Row = row
  }

  def keysIteratorRowReuse: Iterator[SpecificMutableRow] =
    new HashIterator[SpecificMutableRow] {

      private[this] val currentRow = _keySet.newEmptyValueAsRow()

      def valueForNullKey = null.asInstanceOf[SpecificMutableRow]

      def newEmptyRow() = currentRow

      def buildResult(row: SpecificMutableRow, v: V) = row
    }

  /* Override to avoid tuple allocation */
  override def valuesIterator: Iterator[V] = new HashIterator[V] {

    def valueForNullKey = nullValue

    def newEmptyRow() = null

    def buildResult(row: SpecificMutableRow, v: V) = v
  }

  override def clear() {
    // first clear the values array and value for null key
    val bitset = _keySet.getBitSet
    val nullV = null.asInstanceOf[V]
    val values = _values
    var pos = bitset.nextSetBit(0)
    while (pos >= 0) {
      values(pos) = nullV
      pos = bitset.nextSetBit(pos + 1)
    }
    noNullValue = true
    nullValue = nullV
    _oldValues = null
    // next clear the key set
    _keySet.clear()
  }

  override def result(): MultiColumnOpenHashMap[V] = self

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
