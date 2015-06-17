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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.BitSet

import scala.util.hashing.MurmurHash3

/**
 * A simple, fast hash set optimized for non-null insertion-only use case,
 * where keys are never removed.
 *
 * The underlying implementation uses Scala compiler's specialization to
 * generate optimized storage for two primitive types (Long and Int).
 * It is much faster than Java's standard HashSet while incurring much less
 * memory overhead. This can serve as building blocks for higher level
 * data structures such as an optimized HashMap.
 *
 * This MultiColumnOpenHashSet is designed to serve as building blocks for
 * higher level data structures such as an optimized hash map. Compared with
 * standard hash set implementations, this class provides its various callbacks
 * interfaces (e.g. allocateFunc, moveFunc) and interfaces to retrieve
 * the position of a key in the underlying array.
 *
 * It uses quadratic probing with a power-of-2 hash table size,
 * which is guaranteed to explore all spaces for each key
 * (see http://en.wikipedia.org/wiki/Quadratic_probing).
 */
final class MultiColumnOpenHashSet(var columns: Array[Int],
                                   var types: Array[DataType],
                                   var numColumns: Int,
                                   var initialCapacity: Int,
                                   var loadFactor: Double)
  extends Iterable[SpecificMutableRow]
  with Serializable {

  require(initialCapacity <= (1 << 29),
    "Can't make capacity bigger than 2^29 elements")
  require(initialCapacity >= 1, "Invalid initial capacity")
  require(loadFactor < 1.0, "Load factor must be less than 1.0")
  require(loadFactor > 0.0, "Load factor must be greater than 0.0")

  import MultiColumnOpenHashSet._

  def this(columns: Array[Int], types: Array[DataType], initialCapacity: Int) =
    this(columns, types, columns.length, initialCapacity, 0.7)

  def this(columns: Array[Int], types: Array[DataType]) =
    this(columns, types, 64)

  // for serialization
  def this() = this(Array.emptyIntArray, Array.empty, 0, 1, 0.1)

  private var _columnHandler: ColumnHandler = _
  private var _projectionColumnHandler: ColumnHandler = _

  private var _capacity, _mask, _growThreshold = 0
  private var _size = 0

  initColumnHandlersAndCapacity(SegmentMap.nextPowerOf2(initialCapacity))

  private[sql] def getColumnHandler(r: Row) =
    if (r.length == numColumns) _projectionColumnHandler else _columnHandler

  private[sql] def getColumnHandler(r: SpecificMutableRow) =
    if (r.length == numColumns) _projectionColumnHandler else _columnHandler

  private var _bitset = new BitSet(_capacity)

  def getBitSet: BitSet = _bitset

  private var _data: Array[Any] = _
  _data = _columnHandler.initDataContainer(_capacity)

  private def initColumnHandlersAndCapacity(capacity: Int) = {
    _columnHandler = newColumnHandler(columns, types, numColumns)
    _projectionColumnHandler = newColumnHandler((0 until numColumns).toArray,
      types, numColumns)
    _capacity = capacity
    _mask = capacity - 1
    _growThreshold = (loadFactor * capacity).toInt
  }

  /** Number of elements in the set. */
  override def size: Int = _size

  override def isEmpty: Boolean = _size == 0

  override def nonEmpty: Boolean = _size != 0

  /** The capacity of the set (i.e. size of the underlying array). */
  def capacity: Int = _capacity

  /** Return true if this set contains the specified element. */
  def contains(row: Row): Boolean = {
    val columnHandler = getColumnHandler(row)
    getPos(row, columnHandler.hash(row), columnHandler) != INVALID_POS
  }

  /** Return true if this set contains the specified projected row. */
  def contains(row: SpecificMutableRow): Boolean = {
    val columnHandler = getColumnHandler(row)
    getPos(row, columnHandler.hash(row), columnHandler) != INVALID_POS
  }

  /**
   * Add projected columns from a row to the set. If the set is over capacity
   * after the insertion, grow the set and rehash all elements.
   */
  def add(row: Row) {
    val columnHandler = getColumnHandler(row)
    addWithoutResize(row, columnHandler.hash(row), columnHandler)
    rehashIfNeeded(row, grow, move)
  }

  /**
   * Add a projected row to the set. If the set is over capacity after
   * the insertion, grow the set and rehash all elements.
   */
  def add(row: SpecificMutableRow) {
    val columnHandler = getColumnHandler(row)
    addWithoutResize(row, columnHandler.hash(row), columnHandler)
    rehashIfNeeded(row, grow, move)
  }

  /**
   * Add an element to the set. This one differs from add in that it doesn't
   * trigger rehashing. The caller is responsible for calling rehashIfNeeded.
   *
   * Use (returnValue & POSITION_MASK) to get the actual position, and
   * (returnValue & NONEXISTENCE_MASK) == 0 for prior existence.
   *
   * @return The position where the key is placed, plus the highest order bit
   *         is set if the key does not exists previously.
   */
  private[sql] def addWithoutResize(row: Row, hash: Int,
                                    columnHandler: ColumnHandler): Int = {
    var pos = hash & _mask
    var delta = 1
    val data = _data
    while (true) {
      if (!_bitset.get(pos)) {
        // This is a new key.
        columnHandler.setValue(data, pos, row)
        _bitset.set(pos)
        _size += 1
        return pos | NONEXISTENCE_MASK
      } else if (columnHandler.equals(data, pos, row)) {
        // Found an existing key.
        return pos
      } else {
        // quadratic probing with values increase by 1, 2, 3, ...
        pos = (pos + delta) & _mask
        delta += 1
      }
    }
    throw new RuntimeException("Should never reach here.")
  }

  /**
   * Rehash the set if it is overloaded.
   * @param row A parameter unused in the function, but to force the
   *            Scala compiler to specialize this method.
   * @param allocateFunc Callback invoked when we are allocating
   *                     a new, larger array.
   * @param moveFunc Callback invoked when we move the key from one position
   *                 (in the old array) to a new position (in the new array).
   */
  def rehashIfNeeded(row: Row, allocateFunc: (Int) => Unit,
                     moveFunc: (Int, Int) => Unit) {
    if (_size > _growThreshold) {
      rehash(row, allocateFunc, moveFunc)
    }
  }

  def getHash(row: Row, columnHandler: ColumnHandler): Int =
    columnHandler.hash(row)

  /**
   * Return the position of the element in the underlying array,
   * or INVALID_POS if it is not found.
   */
  def getPos(row: Row, hash: Int, columnHandler: ColumnHandler): Int = {
    var pos = hash & _mask
    var delta = 1
    val data = _data
    while (true) {
      if (!_bitset.get(pos)) {
        return INVALID_POS
      } else if (columnHandler.equals(data, pos, row)) {
        return pos
      } else {
        // quadratic probing with values increase by 1, 2, 3, ...
        pos = (pos + delta) & _mask
        delta += 1
      }
    }
    throw new RuntimeException("Should never reach here.")
  }

  def newEmptyValueAsRow() = _columnHandler.newMutableRow()

  /**
   * Return the value at the specified position as a Row,
   * filling into the given MutableRow.
   */
  def fillValueAsRow(pos: Int, row: SpecificMutableRow) =
    _columnHandler.fillValue(_data, pos, row)

  override def iterator: Iterator[SpecificMutableRow] =
    new Iterator[SpecificMutableRow] {

      final val bitset = _bitset
      var pos = bitset.nextSetBit(0)

      override def hasNext: Boolean = pos != INVALID_POS

      override def next(): SpecificMutableRow = {
        val row = newEmptyValueAsRow()
        _columnHandler.fillValue(_data, pos, row)
        pos = bitset.nextSetBit(pos + 1)
        row
      }
    }

  def iteratorRowReuse: Iterator[SpecificMutableRow] =
    new Iterator[SpecificMutableRow] {

      final val bitset = _bitset
      final val currentRow = newEmptyValueAsRow()
      var pos = bitset.nextSetBit(0)

      override def hasNext: Boolean = pos != INVALID_POS

      override def next(): SpecificMutableRow = {
        _columnHandler.fillValue(_data, pos, currentRow)
        pos = bitset.nextSetBit(pos + 1)
        currentRow
      }
    }

  def clear(): Unit = {
    _data = _columnHandler.initDataContainer(_capacity)
    _bitset = new BitSet(_bitset.capacity)
    _size = 0
  }

  /** Copy this set to given set clearing it if it is not empty */
  def copyTo(other: MultiColumnOpenHashSet): Unit = {
    val capacity = this.capacity
    if (capacity == other.capacity) {
      // do direct array copies
      _data.indices.foreach { index =>
        System.arraycopy(_data(index), 0, other._data(index), 0, capacity)
      }
      other._bitset = getBitSet.copyTo(other.getBitSet)
      other._size = size
    }
    else {
      if (other.nonEmpty) other.clear()
      foreach(other.add)
    }
  }

  /**
   * Double the table's size and re-hash everything. We are not really using k,
   * but it is declared so Scala compiler can specialize this method
   * (which leads to calling the specialized version of putInto).
   *
   * @param row A parameter unused in the function, but to force the
   *            Scala compiler to specialize this method.
   * @param allocateFunc Callback invoked when we are allocating
   *                     a new, larger array.
   * @param moveFunc Callback invoked when we move the key from one position
   *                 (in the old array) to a new position (in the new array).
   */
  private def rehash(row: Row, allocateFunc: (Int) => Unit,
                     moveFunc: (Int, Int) => Unit) {
    val newCapacity = _capacity * 2
    allocateFunc(newCapacity)
    val columnHandler = this._columnHandler
    val newBitset = new BitSet(newCapacity)
    val newData = columnHandler.initDataContainer(newCapacity)
    val newMask = newCapacity - 1
    val data = _data

    var oldPos = 0
    while (oldPos < capacity) {
      if (_bitset.get(oldPos)) {
        var newPos = columnHandler.hash(data, oldPos) & newMask
        var i = 1
        var keepGoing = true
        // No need to check for equality here when we insert so this has
        // one less if branch than the similar code path in addWithoutResize.
        while (keepGoing) {
          if (!newBitset.get(newPos)) {
            // Inserting the key at newPos
            columnHandler.copyValue(data, oldPos, newData, newPos)
            newBitset.set(newPos)
            moveFunc(oldPos, newPos)
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

    _bitset = newBitset
    _data = newData
    _capacity = newCapacity
    _mask = newMask
    _growThreshold = (loadFactor * newCapacity).toInt
  }
}

private[sql] object MultiColumnOpenHashSet {

  val INVALID_POS = -1
  val NONEXISTENCE_MASK = 0x80000000
  val POSITION_MASK = 0xEFFFFFF

  /**
   * A set of specialized column type handlers esp for single primitive
   * column types for array creation, hashing avoid boxing hash code
   * computation etc.
   */
  abstract sealed class ColumnHandler extends Serializable {

    val columns: Array[Int]

    def numColumns: Int = columns.length

    def getMutableValue(index: Int): MutableValue

    def initDataContainer(capacity: Int): Array[Any]

    def hash(row: Row): Int

    def hash(data: Array[Any], pos: Int): Int

    def equals(data: Array[Any], pos: Int, row: Row): Boolean

    def fillValue(data: Array[Any], pos: Int, row: SpecificMutableRow): Unit

    def setValue(data: Array[Any], pos: Int, row: Row): Unit

    def copyValue(data: Array[Any], pos: Int, newData: Array[Any],
                  newPos: Int): Unit

    final def newMutableRow(): SpecificMutableRow = {
      val ncols = numColumns
      val row = new Array[MutableValue](ncols)
      (0 until ncols).foreach { i =>
        row(i) = getMutableValue(i)
      }
      new SpecificMutableRow(row)
    }

    final def hashInt(i: Int): Int = {
      MurmurHash3.finalizeHash(MurmurHash3.mixLast(
        MurmurHash3.arraySeed, i), 0)
    }

    final def hashLong(l: Long): Int = {
      MurmurHash3.finalizeHash(MurmurHash3.mixLast(MurmurHash3.mix(
        MurmurHash3.arraySeed, l.toInt), (l >>> 32).toInt), 0)
    }
  }

  def newColumnHandler(columns: Array[Int], types: Array[DataType],
                       numColumns: Int) = {
    if (numColumns == 1) {
      val col = columns(0)
      types(0) match {
        case LongType => new LongHandler(col)
        case IntegerType => new IntHandler(col)
        case DoubleType => new DoubleHandler(col)
        case FloatType => new FloatHandler(col)
        case BooleanType => new BooleanHandler(col)
        case ByteType => new ByteHandler(col)
        case ShortType => new ShortHandler(col)
        // use INT for DATE -- see comment in SpecificMutableRow constructor
        case DateType => new IntHandler(col)
        case _ => new SingleColumnHandler(col)
      }
    } else {
      new MultiColumnHandler(columns, numColumns, types)
    }
  }

  final class LongHandler(val col: Int) extends ColumnHandler {

    override val columns = Array[Int](col)

    override def getMutableValue(index: Int): MutableValue = new MutableLong

    override def initDataContainer(capacity: Int): Array[Any] = {
      Array[Any](new Array[Long](capacity))
    }

    override def hash(row: Row): Int = {
      hashLong(row.getLong(col))
    }

    override def hash(data: Array[Any], pos: Int): Int = {
      hashLong(data(0).asInstanceOf[Array[Long]](pos))
    }

    override def equals(data: Array[Any], pos: Int, row: Row): Boolean =
      data(0).asInstanceOf[Array[Long]](pos) == row.getLong(col)

    override def fillValue(data: Array[Any], pos: Int,
                           row: SpecificMutableRow) =
      row.setLong(0, data(0).asInstanceOf[Array[Long]](pos))

    override def setValue(data: Array[Any], pos: Int, row: Row) =
      data(0).asInstanceOf[Array[Long]](pos) = row.getLong(col)

    override def copyValue(data: Array[Any], pos: Int, newData: Array[Any],
                           newPos: Int) =
      newData(0).asInstanceOf[Array[Long]](newPos) =
        data(0).asInstanceOf[Array[Long]](pos)
  }

  final class IntHandler(val col: Int) extends ColumnHandler {

    override val columns = Array[Int](col)

    override def getMutableValue(index: Int): MutableValue = new MutableInt

    override def initDataContainer(capacity: Int): Array[Any] = {
      Array[Any](new Array[Int](capacity))
    }

    override def hash(row: Row): Int = hashInt(row.getInt(col))

    override def hash(data: Array[Any], pos: Int): Int =
      hashInt(data(0).asInstanceOf[Array[Int]](pos))

    override def equals(data: Array[Any], pos: Int, row: Row): Boolean =
      data(0).asInstanceOf[Array[Int]](pos) == row.getInt(col)

    override def fillValue(data: Array[Any], pos: Int,
                           row: SpecificMutableRow) =
      row.setInt(0, data(0).asInstanceOf[Array[Int]](pos))

    override def setValue(data: Array[Any], pos: Int, row: Row) =
      data(0).asInstanceOf[Array[Int]](pos) = row.getInt(col)

    override def copyValue(data: Array[Any], pos: Int, newData: Array[Any],
                           newPos: Int) =
      newData(0).asInstanceOf[Array[Int]](newPos) =
        data(0).asInstanceOf[Array[Int]](pos)
  }

  final class DoubleHandler(val col: Int) extends ColumnHandler {

    override val columns = Array[Int](col)

    override def getMutableValue(index: Int): MutableValue = new MutableDouble

    override def initDataContainer(capacity: Int): Array[Any] = {
      Array[Any](new Array[Double](capacity))
    }

    override def hash(row: Row): Int = {
      hashLong(java.lang.Double.doubleToRawLongBits(row.getDouble(col)))
    }

    override def hash(data: Array[Any], pos: Int): Int = {
      hashLong(java.lang.Double.doubleToRawLongBits(
        data(0).asInstanceOf[Array[Double]](pos)))
    }

    override def equals(data: Array[Any], pos: Int, row: Row): Boolean =
      data(0).asInstanceOf[Array[Double]](pos) == row.getDouble(col)

    override def fillValue(data: Array[Any], pos: Int,
                           row: SpecificMutableRow) =
      row.setDouble(0, data(0).asInstanceOf[Array[Double]](pos))

    override def setValue(data: Array[Any], pos: Int, row: Row) =
      data(0).asInstanceOf[Array[Double]](pos) = row.getDouble(col)

    override def copyValue(data: Array[Any], pos: Int, newData: Array[Any],
                           newPos: Int) =
      newData(0).asInstanceOf[Array[Double]](newPos) =
        data(0).asInstanceOf[Array[Double]](pos)
  }

  final class FloatHandler(val col: Int) extends ColumnHandler {

    override val columns = Array[Int](col)

    override def getMutableValue(index: Int): MutableValue = new MutableFloat

    override def initDataContainer(capacity: Int): Array[Any] = {
      Array[Any](new Array[Float](capacity))
    }

    override def hash(row: Row): Int =
      hashInt(java.lang.Float.floatToRawIntBits(row.getFloat(col)))

    override def hash(data: Array[Any], pos: Int): Int = hashInt(java.lang
      .Float.floatToRawIntBits(data(0).asInstanceOf[Array[Float]](pos)))

    override def equals(data: Array[Any], pos: Int, row: Row): Boolean =
      data(0).asInstanceOf[Array[Float]](pos) == row.getFloat(col)

    override def fillValue(data: Array[Any], pos: Int,
                           row: SpecificMutableRow) =
      row.setFloat(0, data(0).asInstanceOf[Array[Float]](pos))

    override def setValue(data: Array[Any], pos: Int, row: Row) =
      data(0).asInstanceOf[Array[Float]](pos) = row.getFloat(col)

    override def copyValue(data: Array[Any], pos: Int, newData: Array[Any],
                           newPos: Int) =
      newData(0).asInstanceOf[Array[Float]](newPos) =
        data(0).asInstanceOf[Array[Float]](pos)
  }

  final class BooleanHandler(val col: Int) extends ColumnHandler {

    override val columns = Array[Int](col)

    override def getMutableValue(index: Int): MutableValue = new MutableBoolean

    override def initDataContainer(capacity: Int): Array[Any] = {
      Array[Any](new Array[Boolean](capacity))
    }

    override def hash(row: Row): Int = if (row.getBoolean(col)) 1 else 0

    override def hash(data: Array[Any], pos: Int): Int =
      if (data(0).asInstanceOf[Array[Boolean]](pos)) 1 else 0

    override def equals(data: Array[Any], pos: Int, row: Row): Boolean =
      data(0).asInstanceOf[Array[Boolean]](pos) == row.getBoolean(col)

    override def fillValue(data: Array[Any], pos: Int,
                           row: SpecificMutableRow) =
      row.setBoolean(0, data(0).asInstanceOf[Array[Boolean]](pos))

    override def setValue(data: Array[Any], pos: Int, row: Row) =
      data(0).asInstanceOf[Array[Boolean]](pos) = row.getBoolean(col)

    override def copyValue(data: Array[Any], pos: Int, newData: Array[Any],
                           newPos: Int) =
      newData(0).asInstanceOf[Array[Boolean]](newPos) =
        data(0).asInstanceOf[Array[Boolean]](pos)
  }

  final class ByteHandler(val col: Int) extends ColumnHandler {

    override val columns = Array[Int](col)

    override def getMutableValue(index: Int): MutableValue = new MutableByte

    override def initDataContainer(capacity: Int): Array[Any] = {
      Array[Any](new Array[Byte](capacity))
    }

    override def hash(row: Row): Int = row.getByte(col)

    override def hash(data: Array[Any], pos: Int): Int =
      data(0).asInstanceOf[Array[Byte]](pos)

    override def equals(data: Array[Any], pos: Int, row: Row): Boolean =
      data(0).asInstanceOf[Array[Byte]](pos) == row.getByte(col)

    override def fillValue(data: Array[Any], pos: Int,
                           row: SpecificMutableRow) =
      row.setByte(0, data(0).asInstanceOf[Array[Byte]](pos))

    override def setValue(data: Array[Any], pos: Int, row: Row) =
      data(0).asInstanceOf[Array[Byte]](pos) = row.getByte(col)

    override def copyValue(data: Array[Any], pos: Int, newData: Array[Any],
                           newPos: Int) =
      newData(0).asInstanceOf[Array[Byte]](newPos) =
        data(0).asInstanceOf[Array[Byte]](pos)
  }

  final class ShortHandler(val col: Int) extends ColumnHandler {

    override val columns = Array[Int](col)

    override def getMutableValue(index: Int): MutableValue = new MutableShort

    override def initDataContainer(capacity: Int): Array[Any] = {
      Array[Any](new Array[Short](capacity))
    }

    override def hash(row: Row): Int = row.getShort(col)

    override def hash(data: Array[Any], pos: Int): Int =
      data(0).asInstanceOf[Array[Short]](pos)

    override def equals(data: Array[Any], pos: Int, row: Row): Boolean =
      data(0).asInstanceOf[Array[Short]](pos) == row.getShort(col)

    override def fillValue(data: Array[Any], pos: Int,
                           row: SpecificMutableRow) =
      row.setShort(0, data(0).asInstanceOf[Array[Short]](pos))

    override def setValue(data: Array[Any], pos: Int, row: Row) =
      data(0).asInstanceOf[Array[Short]](pos) = row.getShort(col)

    override def copyValue(data: Array[Any], pos: Int, newData: Array[Any],
                           newPos: Int) =
      newData(0).asInstanceOf[Array[Short]](newPos) =
        data(0).asInstanceOf[Array[Short]](pos)
  }

  final class SingleColumnHandler(val col: Int) extends ColumnHandler {

    override val columns = Array[Int](col)

    override def getMutableValue(index: Int): MutableValue = new MutableAny

    override def initDataContainer(capacity: Int): Array[Any] = {
      Array[Any](new Array[Any](capacity))
    }

    override def hash(row: Row): Int = hashInt(row(col).##)

    override def hash(data: Array[Any], pos: Int): Int =
      hashInt(data(0).asInstanceOf[Array[Any]](pos).##)

    override def equals(data: Array[Any], pos: Int, row: Row): Boolean =
      data(0).asInstanceOf[Array[Any]](pos).equals(row(col))

    override def fillValue(data: Array[Any], pos: Int,
                           row: SpecificMutableRow) =
      row.update(0, data(0).asInstanceOf[Array[Any]](pos))

    override def setValue(data: Array[Any], pos: Int, row: Row) =
      data(0).asInstanceOf[Array[Any]](pos) = row(col)

    override def copyValue(data: Array[Any], pos: Int, newData: Array[Any],
                           newPos: Int) =
      newData(0).asInstanceOf[Array[Any]](newPos) =
        data(0).asInstanceOf[Array[Any]](pos)
  }

  // TODO: can generate code using quasi-quotes and lose all the
  // single column implementations above.
  // See children of CodeGenerator like GenerateProjection/GenerateOrdering
  // for examples of using quasi-quotes with Toolbox to generate code.
  // Note that it is an expensive operation so should only be done when
  // this is known to be used for things like Sampled tables or GROUP BY.
  final class MultiColumnHandler(override val columns: Array[Int],
                                 override val numColumns: Int,
                                 val types: Array[DataType])
    extends ColumnHandler {

    override def getMutableValue(index: Int): MutableValue = {
      types(index) match {
        case LongType => new MutableLong
        case IntegerType => new MutableInt
        case DoubleType => new MutableDouble
        case FloatType => new MutableFloat
        case BooleanType => new MutableBoolean
        case ByteType => new MutableByte
        case ShortType => new MutableShort
        // use INT for DATE -- see comment in SpecificMutableRow constructor
        case DateType => new MutableInt
        case _ => new MutableAny
      }
    }

    override def initDataContainer(capacity: Int): Array[Any] = {
      val ncols = this.numColumns
      val data = new Array[Any](ncols)
      var i = 0
      while (i < ncols) {
        types(i) match {
          case LongType => data(i) = new Array[Long](capacity)
          case IntegerType => data(i) = new Array[Int](capacity)
          case DoubleType => data(i) = new Array[Double](capacity)
          case FloatType => data(i) = new Array[Float](capacity)
          case BooleanType => data(i) = new Array[Boolean](capacity)
          case ByteType => data(i) = new Array[Byte](capacity)
          case ShortType => data(i) = new Array[Short](capacity)
          // use INT for DATE -- see comment in SpecificMutableRow constructor
          case DateType => data(i) = new Array[Int](capacity)
          case _ => data(i) = new Array[Any](capacity)
        }
        i += 1
      }
      data
    }

    override def hash(row: Row): Int = {
      val cols = this.columns
      val ncols = this.numColumns
      val types = this.types
      var h = MurmurHash3.arraySeed
      var i = 0
      while (i < ncols) {
        val col = cols(i)
        types(i) match {
          case LongType =>
            val l = row.getLong(col)
            h = MurmurHash3.mix(h, l.toInt)
            h = MurmurHash3.mix(h, (l >>> 32).toInt)
          case IntegerType => h = MurmurHash3.mix(h, row.getInt(cols(i)))
          case DoubleType =>
            val l = java.lang.Double.doubleToRawLongBits(
              row.getDouble(cols(i)))
            h = MurmurHash3.mix(h, l.toInt)
            h = MurmurHash3.mix(h, (l >>> 32).toInt)
          case FloatType => h = MurmurHash3.mix(h,
            java.lang.Float.floatToRawIntBits(row.getFloat(cols(i))))
          case BooleanType => h = MurmurHash3.mix(h,
            if (row.getBoolean(cols(i))) 1 else 0)
          case ByteType => h = MurmurHash3.mix(h, row.getByte(cols(i)))
          case ShortType => h = MurmurHash3.mix(h, row.getShort(cols(i)))
          case DateType => h = MurmurHash3.mix(h, row.getInt(cols(i)))
          case _ => h = MurmurHash3.mix(h, row(cols(i)).##)
        }
        i += 1
      }
      MurmurHash3.finalizeHash(h, ncols)
    }

    override def hash(data: Array[Any], pos: Int): Int = {
      val ncols = this.numColumns
      val types = this.types
      var h = MurmurHash3.arraySeed
      var i = 0
      while (i < ncols) {
        types(i) match {
          case LongType =>
            val l = data(i).asInstanceOf[Array[Long]](pos)
            h = MurmurHash3.mix(h, l.toInt)
            h = MurmurHash3.mix(h, (l >>> 32).toInt)
          case IntegerType =>
            h = MurmurHash3.mix(h, data(i).asInstanceOf[Array[Int]](pos))
          case DoubleType =>
            val l = java.lang.Double.doubleToRawLongBits(
              data(i).asInstanceOf[Array[Long]](pos))
            h = MurmurHash3.mix(h, l.toInt)
            h = MurmurHash3.mix(h, (l >>> 32).toInt)
          case FloatType => h = MurmurHash3.mix(h, java.lang.Float
            .floatToRawIntBits(data(i).asInstanceOf[Array[Float]](pos)))
          case BooleanType => h = MurmurHash3.mix(h,
            if (data(i).asInstanceOf[Array[Boolean]](pos)) 1 else 0)
          case ByteType =>
            h = MurmurHash3.mix(h, data(i).asInstanceOf[Array[Byte]](pos))
          case ShortType =>
            h = MurmurHash3.mix(h, data(i).asInstanceOf[Array[Short]](pos))
          case DateType =>
            h = MurmurHash3.mix(h, data(i).asInstanceOf[Array[Int]](pos))
          case _ =>
            h = MurmurHash3.mix(h, data(i).asInstanceOf[Array[Any]](pos).##)
        }
        i += 1
      }
      MurmurHash3.finalizeHash(h, ncols)
    }

    override def equals(data: Array[Any], pos: Int, row: Row): Boolean = {
      val cols = this.columns
      val ncols = this.numColumns
      val types = this.types
      var i = 0
      while (i < ncols) {
        types(i) match {
          case LongType =>
            if (data(i).asInstanceOf[Array[Long]](pos) != row.getLong(cols(i)))
              return false
          case IntegerType =>
            if (data(i).asInstanceOf[Array[Int]](pos) != row.getInt(cols(i)))
              return false
          case DoubleType =>
            if (data(i).asInstanceOf[Array[Double]](pos) != row.getDouble(cols(i)))
              return false
          case FloatType =>
            if (data(i).asInstanceOf[Array[Float]](pos) != row.getFloat(cols(i)))
              return false
          case BooleanType =>
            if (data(i).asInstanceOf[Array[Boolean]](pos) != row.getBoolean(cols(i)))
              return false
          case ByteType =>
            if (data(i).asInstanceOf[Array[Byte]](pos) != row.getByte(cols(i)))
              return false
          case ShortType =>
            if (data(i).asInstanceOf[Array[Short]](pos) != row.getShort(cols(i)))
              return false
          case DateType =>
            if (data(i).asInstanceOf[Array[Int]](pos) != row.getInt(cols(i)))
              return false
          case _ =>
            if (!data(i).asInstanceOf[Array[Any]](pos).equals(row(cols(i))))
              return false
        }
        i += 1
      }
      true
    }

    override def fillValue(data: Array[Any], pos: Int,
                           row: SpecificMutableRow) = {
      val ncols = this.numColumns
      val types = this.types
      var i = 0
      while (i < ncols) {
        types(i) match {
          case LongType =>
            row.setLong(i, data(i).asInstanceOf[Array[Long]](pos))
          case IntegerType =>
            row.setInt(i, data(i).asInstanceOf[Array[Int]](pos))
          case DoubleType =>
            row.setDouble(i, data(i).asInstanceOf[Array[Double]](pos))
          case FloatType =>
            row.setFloat(i, data(i).asInstanceOf[Array[Float]](pos))
          case BooleanType =>
            row.setBoolean(i, data(i).asInstanceOf[Array[Boolean]](pos))
          case ByteType =>
            row.setByte(i, data(i).asInstanceOf[Array[Byte]](pos))
          case ShortType =>
            row.setShort(i, data(i).asInstanceOf[Array[Short]](pos))
          case DateType =>
            row.setInt(i, data(i).asInstanceOf[Array[Int]](pos))
          case _ =>
            row.update(i, data(i).asInstanceOf[Array[Any]](pos))
        }
        i += 1
      }
    }

    override def setValue(data: Array[Any], pos: Int, row: Row) = {
      val cols = this.columns
      val ncols = this.numColumns
      val types = this.types
      var i = 0
      while (i < ncols) {
        types(i) match {
          case LongType =>
            data(i).asInstanceOf[Array[Long]](pos) = row.getLong(cols(i))
          case IntegerType =>
            data(i).asInstanceOf[Array[Int]](pos) = row.getInt(cols(i))
          case DoubleType =>
            data(i).asInstanceOf[Array[Double]](pos) = row.getDouble(cols(i))
          case FloatType =>
            data(i).asInstanceOf[Array[Float]](pos) = row.getFloat(cols(i))
          case BooleanType =>
            data(i).asInstanceOf[Array[Boolean]](pos) = row.getBoolean(cols(i))
          case ByteType =>
            data(i).asInstanceOf[Array[Byte]](pos) = row.getByte(cols(i))
          case ShortType =>
            data(i).asInstanceOf[Array[Short]](pos) = row.getShort(cols(i))
          case DateType =>
            data(i).asInstanceOf[Array[Int]](pos) = row.getInt(cols(i))
          case _ =>
            data(i).asInstanceOf[Array[Any]](pos) = row(cols(i))
        }
        i += 1
      }
    }

    override def copyValue(data: Array[Any], pos: Int, newData: Array[Any],
                           newPos: Int) = {
      val ncols = this.numColumns
      val types = this.types
      var i = 0
      while (i < ncols) {
        types(i) match {
          case LongType =>
            newData(i).asInstanceOf[Array[Long]](newPos) =
              data(i).asInstanceOf[Array[Long]](pos)
          case IntegerType =>
            newData(i).asInstanceOf[Array[Int]](newPos) =
              data(i).asInstanceOf[Array[Int]](pos)
          case DoubleType =>
            newData(i).asInstanceOf[Array[Double]](newPos) =
              data(i).asInstanceOf[Array[Double]](pos)
          case FloatType =>
            newData(i).asInstanceOf[Array[Float]](newPos) =
              data(i).asInstanceOf[Array[Float]](pos)
          case BooleanType =>
            newData(i).asInstanceOf[Array[Boolean]](newPos) =
              data(i).asInstanceOf[Array[Boolean]](pos)
          case ByteType =>
            newData(i).asInstanceOf[Array[Byte]](newPos) =
              data(i).asInstanceOf[Array[Byte]](pos)
          case ShortType =>
            newData(i).asInstanceOf[Array[Short]](newPos) =
              data(i).asInstanceOf[Array[Short]](pos)
          case DateType =>
            newData(i).asInstanceOf[Array[Int]](newPos) =
              data(i).asInstanceOf[Array[Int]](pos)
          case _ =>
            newData(i).asInstanceOf[Array[Any]](newPos) =
              data(i).asInstanceOf[Array[Any]](pos)
        }
        i += 1
      }
    }
  }

  private def grow1(newSize: Int) {}

  private def move1(oldPos: Int, newPos: Int) {}

  private val grow = grow1 _
  private val move = move1 _
}
