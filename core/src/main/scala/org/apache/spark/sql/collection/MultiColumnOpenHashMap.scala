/*
 * Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
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

package org.apache.spark.sql.collection

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.ArrayBuffer
import scala.collection.{IterableLike, Iterator, mutable}
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodeAndComment
import org.apache.spark.sql.collection.MultiColumnOpenHashSet._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.DataType

/**
 * A fast hash map implementation for nullable keys. This hash map supports
 * insertions and updates, but not deletions. This map is about as fast as
 * any other hashmap, while using much less space overhead.
 *
 * Under the hood, it uses the MultiColumnOpenHashSet implementation.
 */
final class MultiColumnOpenHashMap[@specialized(Long, Int, Double) V: ClassTag](
    _columns: Array[Int],
    _types: Array[DataType],
    _numColumns: Int,
    _initialCapacity: Int,
    _loadFactor: Double,
    qcsColHandlerOption: Option[ColumnHandler])
    extends SegmentMap[Row, V]
    with mutable.Map[Row, V]
    with mutable.MapLike[Row, V, MultiColumnOpenHashMap[V]]
    with Iterable[(Row, V)]
    with IterableLike[(Row, V), MultiColumnOpenHashMap[V]]
    with mutable.Builder[(Row, V), MultiColumnOpenHashMap[V]]
    with Serializable {

  self =>

  def this(columns: Array[Int], types: Array[DataType], initialCapacity: Int) =
    this(columns, types, columns.length, initialCapacity, 0.7, None)

  def this(columns: Array[Int], types: Array[DataType]) =
    this(columns, types, 64)

  private val _keySet = new MultiColumnOpenHashSet(_columns, _types,
    _numColumns, _initialCapacity, _loadFactor, qcsColHandlerOption)

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

  override def nonEmpty: Boolean = _keySet.nonEmpty || !noNullValue

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
  def contains(r: WrappedInternalRow): Boolean = {
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
    get_(r, _keySet.getColumnHandler(r))
  }

  /** Get the value for a given row */
  def apply(r: WrappedInternalRow): V = {
    get_(r, _keySet.getColumnHandler(r))
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
  def get(r: WrappedInternalRow): Option[V] = Option(apply(r))

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
  def update(r: WrappedInternalRow, v: V) {
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
      keySet.numColumns, 1, keySet.loadFactor, qcsColHandlerOption)
  }

  /**
   * If the row doesn't exist yet in the hash map, set its value to
   * defaultValue; otherwise, set its value to mergeValue(oldValue).
   *
   * @return true if new value was added, false if it was merged and null
   *         if the default/merge calls returned null and nothing was done
   */
  private def changeValue(r: Row, hash: Int, columnHandler: ColumnHandler,
      change: ChangeValue[Row, V]): java.lang.Boolean = {
    val keySet = _keySet
    val pos = keySet.addWithoutResize(r, hash, columnHandler)
    if ((pos & NONEXISTENCE_MASK) != 0) {
      val v = change.defaultValue(r)
      if (v != null) {
        _values(pos & POSITION_MASK) = v
        keySet.rehashIfNeeded(r, grow, move)
        java.lang.Boolean.TRUE
      } else null
    } else {
      val v = change.mergeValue(r, _values(pos))
      if (v != null) {
        _values(pos) = v
        java.lang.Boolean.FALSE
      } else null
    }
  }

  /** Change value for the special null row */
  private def changeValueForNull(c: ChangeValue[Row, V]): java.lang.Boolean = {
    if (noNullValue) {
      noNullValue = false
      nullValue = c.defaultValue(null)
      java.lang.Boolean.TRUE
    } else {
      nullValue = c.mergeValue(null, nullValue)
      java.lang.Boolean.FALSE
    }
  }

  /**
   * If the row doesn't exist yet in the hash map, set its value to
   * defaultValue; otherwise, set its value to mergeValue(oldValue).
   *
   * @return true if new value was added, false if it was merged and null
   *         if the default/merge calls returned null and nothing was done
   */
  private def changeValue(r: WrappedInternalRow, hash: Int,
      columnHandler: ColumnHandler,
      change: ChangeValue[Row, V]): java.lang.Boolean = {
    val keySet = _keySet
    val pos = keySet.addWithoutResize(r, hash, columnHandler)
    if ((pos & NONEXISTENCE_MASK) != 0) {
      val v = change.defaultValue(r)
      if (v != null) {
        _values(pos & POSITION_MASK) = v
        keySet.rehashIfNeeded(r, grow, move)
        java.lang.Boolean.TRUE
      } else null
    } else {
      val v = change.mergeValue(r, _values(pos))
      if (v != null) {
        _values(pos) = v
        java.lang.Boolean.FALSE
      } else null
    }
  }

  /**
   * If the row doesn't exist yet in the hash map, set its value to
   * defaultValue; otherwise, set its value to mergeValue(oldValue).
   *
   * @return the newly updated value.
   */
  def changeValue(r: Row, change: ChangeValue[Row, V]): java.lang.Boolean = {
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
      change: ChangeValue[Row, V], isLocal: Boolean): java.lang.Boolean = {
    if (r != null) {
      changeValue(r, hash, _keySet.getColumnHandler(r), change)
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
  def changeValue(r: WrappedInternalRow, hash: Int,
      change: ChangeValue[Row, V]): java.lang.Boolean = {
    if (r != null) {
      changeValue(r, hash, _keySet.getColumnHandler(r), change)
    } else {
      changeValueForNull(change)
    }
  }

  override def foldValues[U](init: U, f: (Int, V, U) => U, reset: Boolean = false): U = {
    var v = init
    // first check for null value
    if (!noNullValue) {
      v = f(-1, nullValue, v)
    }
    // next go through the entire map
    val bitset = _keySet.getBitSet
    val values = _values
    var pos = bitset.nextSetBit(0)
    while (pos >= 0) {
      v = f(-1, values(pos), v)
      pos = bitset.nextSetBit(pos + 1)
    }
    v
  }

  override def foldEntries[U](init: U, copyIfRequired: Boolean,
      f: (Row, V, U) => U): U = {
    var v = init
    // first check for null value
    if (!noNullValue) {
      v = f(null, nullValue, v)
    }
    // next go through the entire map
    val keySet = _keySet
    val bitset = keySet.getBitSet
    var currentKey = if (copyIfRequired) null else keySet.newEmptyValueAsRow()
    val values = _values
    var pos = bitset.nextSetBit(0)
    while (pos >= 0) {
      if (copyIfRequired) {
        currentKey = keySet.newEmptyValueAsRow()
      }
      keySet.fillValueAsRow(pos, currentKey)
      v = f(currentKey, values(pos), v)
      pos = bitset.nextSetBit(pos + 1)
    }
    v
  }

  def groupBy[K](groupOp: (Row, V) => Row,
      combineOp: (V, V) => V): MultiColumnOpenHashMap[V] = {
    val m = newBuilder
    for ((row, value) <- this.iteratorRowReuse) {
      val key = groupOp(row, value)
      m.changeValue(key, new ChangeValue[Row, V] {

        override def keyCopy(k: Row): Row = k.copy()

        override def defaultValue(k: Row) = value

        // This is placeholder. Need to implement if needed.
        override def mergeValueNoNull(row: Row, sr: V): (V, Boolean, Boolean) = {
          (mergeValue(row, sr), false, false)
        }

        override def mergeValue(k: Row, v: V): V = combineOp(v, value)
      })
    }
    m
  }

  private def newBuilder[B: ClassTag](keySet: MultiColumnOpenHashSet) = {
    new MultiColumnOpenHashMap[B](keySet.columns, keySet.types,
      keySet.numColumns, keySet.capacity, keySet.loadFactor, qcsColHandlerOption)
  }

  override protected[this] def newBuilder = newBuilder[V](self._keySet)

  implicit def canBuildFrom[B: ClassTag] =
    new CanBuildFrom[MultiColumnOpenHashMap[V], (Row, B),
        MultiColumnOpenHashMap[B]] {

      override def apply(from: MultiColumnOpenHashMap[V]) =
        newBuilder(from._keySet)

      override def apply() = newBuilder[B](self._keySet)
    }

  abstract private class HashIterator[A](useCachedRow: Boolean = false)
      extends Iterator[A] {

    final val bitset = _keySet.getBitSet
    var pos = bitset.nextSetBit(0)

    // this is in parent class to allow initializing before first use in
    // computeNext (else will need to make it lazy val etc in child classes)
    private[this] final val currentRow =
      if (useCachedRow) _keySet.newEmptyValueAsRow() else null

    var nextVal = if (noNullValue) computeNext() else valueForNullKey

    def valueForNullKey: A

    def newEmptyRow(): ReusableRow

    def buildResult(row: ReusableRow, v: V): A

    /**
     * Get the next value we should return from next(),
     * or null if we're finished iterating
     */
    private final def computeNext(): A = {
      if (pos >= 0) {
        val row = if (useCachedRow) currentRow else newEmptyRow()
        val ret = if (row != null) {
          _keySet.fillValueAsRow(pos, row)
          buildResult(row, _values(pos))
        } else {
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

    def buildResult(row: ReusableRow, v: V) = (row.asInstanceOf[Row], v)
  }

  def iteratorRowReuse: Iterator[(ReusableRow, V)] =
    new HashIterator[(ReusableRow, V)](true) {

      def valueForNullKey = (null.asInstanceOf[ReusableRow], nullValue)

      def newEmptyRow() = null

      def buildResult(row: ReusableRow, v: V) = (row, v)
    }

  override def foreach[U](f: ((Row, V)) => U) = iteratorRowReuse.foreach(f)

  /* Override to avoid tuple allocation */
  override def keysIterator: Iterator[Row] = new HashIterator[Row] {

    def valueForNullKey = null.asInstanceOf[Row]

    def newEmptyRow() = _keySet.newEmptyValueAsRow()

    def buildResult(row: ReusableRow, v: V): Row = row
  }

  def keysIteratorRowReuse: Iterator[ReusableRow] =
    new HashIterator[ReusableRow](true) {

      def valueForNullKey = null.asInstanceOf[ReusableRow]

      def newEmptyRow() = null

      def buildResult(row: ReusableRow, v: V) = row
    }

  /* Override to avoid tuple allocation */
  override def valuesIterator: Iterator[V] = new HashIterator[V] {

    def valueForNullKey = nullValue

    def newEmptyRow() = null

    def buildResult(row: ReusableRow, v: V) = v
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
