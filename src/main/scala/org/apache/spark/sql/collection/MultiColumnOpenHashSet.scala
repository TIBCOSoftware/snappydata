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
final class MultiColumnOpenHashSet(val columns: Array[Int],
                                   val expressions: Array[Expression],
                                   val types: Array[DataType],
                                   val numColumns: Int,
                                   val initialCapacity: Int,
                                   val loadFactor: Double) extends Serializable {

  require(initialCapacity <= (1 << 29),
    "Can't make capacity bigger than 2^29 elements")
  require(initialCapacity >= 1, "Invalid initial capacity")
  require(loadFactor < 1.0, "Load factor must be less than 1.0")
  require(loadFactor > 0.0, "Load factor must be greater than 0.0")

  import MultiColumnOpenHashSet._

  def this(columns: Array[Int], types: Array[DataType], initialCapacity: Int) =
    this(columns, null, types, columns.length, initialCapacity, 0.7)

  def this(columns: Array[Int], types: Array[DataType]) =
    this(columns, types, 64)

  def this(expressions: Array[Expression], types: Array[DataType],
           initialCapacity: Int) =
    this(null, expressions, types, expressions.length, initialCapacity, 0.7)

  def this(expressions: Array[Expression], types: Array[DataType]) =
    this(expressions, types, 64)

  private val columnHandler: ColumnHandler =
    newColumnHandler(columns, types, numColumns)

  private var _capacity = SegmentMap.nextPowerOf2(initialCapacity)
  private var _mask = _capacity - 1
  private var _size = 0
  private var _growThreshold = (loadFactor * _capacity).toInt

  private var _bitset = new BitSet(_capacity)

  def getBitSet: BitSet = _bitset

  private var _data: Array[Any] = _
  _data = columnHandler.initDataContainer(_capacity)

  /** Number of elements in the set. */
  def size: Int = _size

  /** The capacity of the set (i.e. size of the underlying array). */
  def capacity: Int = _capacity

  /** Return true if this set contains the specified element. */
  def contains(row: Row): Boolean =
    getPos(row, columnHandler.hash(row)) != INVALID_POS

  /**
   * Add an element to the set. If the set is over capacity after the insertion,
   * grow the set and rehash all elements.
   */
  def add(row: Row) {
    addWithoutResize(row, columnHandler.hash(row))
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
  def addWithoutResize(row: Row, hash: Int): Int = {
    val colHandler = this.columnHandler
    var pos = hash & _mask
    var delta = 1
    val data = _data
    while (true) {
      if (!_bitset.get(pos)) {
        // This is a new key.
        colHandler.setValue(data, pos, row)
        _bitset.set(pos)
        _size += 1
        return pos | NONEXISTENCE_MASK
      } else if (colHandler.equals(data, pos, row)) {
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

  def getHash(row: Row): Int = columnHandler.hash(row)

  /**
   * Return the position of the element in the underlying array,
   * or INVALID_POS if it is not found.
   */
  def getPos(row: Row, hash: Int): Int = {
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

  def newEmptyValueAsRow(): SpecificMutableRow = columnHandler.newMutableRow()

  /**
   * Return the value at the specified position as a Row,
   * filling into the given MutableRow.
   */
  def fillValueAsRow(pos: Int, row: SpecificMutableRow) =
    columnHandler.fillValue(_data, pos, row)

  /*
  /** Set the value at the specified position. */
  def setValue(pos: Int, row: Row) =
    columnHandler.setValue(_data, pos, row)
  */

  def iterator: Iterator[Row] = new Iterator[Row] {
    var currentRow: SpecificMutableRow = newEmptyValueAsRow()
    var pos = nextPos(0)

    override def hasNext: Boolean = pos != INVALID_POS

    override def next(): Row = {
      columnHandler.fillValue(_data, pos, currentRow)
      pos = nextPos(pos + 1)
      currentRow
    }
  }

  /**
   * Return the next position with an element stored, starting
   * from the given position inclusively.
   */
  def nextPos(fromPos: Int): Int = _bitset.nextSetBit(fromPos)

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
    val colHandler = this.columnHandler
    val newBitset = new BitSet(newCapacity)
    val newData = colHandler.initDataContainer(newCapacity)
    val newMask = newCapacity - 1
    val data = _data

    var oldPos = 0
    while (oldPos < capacity) {
      if (_bitset.get(oldPos)) {
        var newPos = colHandler.hash(data, oldPos) & newMask
        var i = 1
        var keepGoing = true
        // No need to check for equality here when we insert so this has
        // one less if branch than the similar code path in addWithoutResize.
        while (keepGoing) {
          if (!newBitset.get(newPos)) {
            // Inserting the key at newPos
            colHandler.copyValue(data, oldPos, newData, newPos)
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

private[spark] object MultiColumnOpenHashSet {

  val INVALID_POS = -1
  val NONEXISTENCE_MASK = 0x80000000
  val POSITION_MASK = 0xEFFFFFF

  /**
   * A set of specialized column type handlers esp for single primitive
   * column types for array creation, hashing avoid boxing hash code
   * computation etc.
   */
  abstract sealed class ColumnHandler extends Serializable {

    def initDataContainer(capacity: Int): Array[Any]

    def hash(row: Row): Int

    def hash(data: Array[Any], pos: Int): Int

    def equals(data: Array[Any], pos: Int, row: Row): Boolean

    def newMutableRow(): SpecificMutableRow

    def fillValue(data: Array[Any], pos: Int, row: SpecificMutableRow): Unit

    def setValue(data: Array[Any], pos: Int, row: Row): Unit

    def copyValue(data: Array[Any], pos: Int, newData: Array[Any],
                  newPos: Int): Unit

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
      val ctype = types(0)
      ctype match {
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

    override def newMutableRow(): SpecificMutableRow =
      new SpecificMutableRow(Array[MutableValue](new MutableLong))

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

  final class LongExprHandler(val expr: Expression) extends ColumnHandler {
    override def initDataContainer(capacity: Int): Array[Any] = {
      Array[Any](new Array[Long](capacity))
    }

    override def hash(row: Row): Int = {
      hashLong(expr.eval(row).asInstanceOf[Long])
    }

    override def hash(data: Array[Any], pos: Int): Int = {
      hashLong(data(0).asInstanceOf[Array[Long]](pos))
    }

    override def equals(data: Array[Any], pos: Int, row: Row): Boolean =
      data(0).asInstanceOf[Array[Long]](pos) == expr.eval(row).asInstanceOf[Long]

    override def newMutableRow(): SpecificMutableRow =
      new SpecificMutableRow(Array[MutableValue](new MutableLong))

    override def fillValue(data: Array[Any], pos: Int,
                           row: SpecificMutableRow) =
      row.setLong(0, data(0).asInstanceOf[Array[Long]](pos))

    override def setValue(data: Array[Any], pos: Int, row: Row) =
      data(0).asInstanceOf[Array[Long]](pos) = expr.eval(row).asInstanceOf[Long]

    override def copyValue(data: Array[Any], pos: Int, newData: Array[Any],
                           newPos: Int) =
      newData(0).asInstanceOf[Array[Long]](newPos) =
        data(0).asInstanceOf[Array[Long]](pos)
  }

  final class IntHandler(val col: Int) extends ColumnHandler {
    override def initDataContainer(capacity: Int): Array[Any] = {
      Array[Any](new Array[Int](capacity))
    }

    override def hash(row: Row): Int = hashInt(row.getInt(col))

    override def hash(data: Array[Any], pos: Int): Int =
      hashInt(data(0).asInstanceOf[Array[Int]](pos))

    override def equals(data: Array[Any], pos: Int, row: Row): Boolean =
      data(0).asInstanceOf[Array[Int]](pos) == row.getInt(col)

    override def newMutableRow(): SpecificMutableRow =
      new SpecificMutableRow(Array[MutableValue](new MutableInt))

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

  final class IntExprHandler(val expr: Expression) extends ColumnHandler {
    override def initDataContainer(capacity: Int): Array[Any] = {
      Array[Any](new Array[Int](capacity))
    }

    override def hash(row: Row): Int = hashInt(expr.eval(row).asInstanceOf[Int])

    override def hash(data: Array[Any], pos: Int): Int =
      hashInt(data(0).asInstanceOf[Array[Int]](pos))

    override def equals(data: Array[Any], pos: Int, row: Row): Boolean =
      data(0).asInstanceOf[Array[Int]](pos) == expr.eval(row).asInstanceOf[Int]

    override def newMutableRow(): SpecificMutableRow =
      new SpecificMutableRow(Array[MutableValue](new MutableInt))

    override def fillValue(data: Array[Any], pos: Int,
                           row: SpecificMutableRow) =
      row.setInt(0, data(0).asInstanceOf[Array[Int]](pos))

    override def setValue(data: Array[Any], pos: Int, row: Row) =
      data(0).asInstanceOf[Array[Int]](pos) = expr.eval(row).asInstanceOf[Int]

    override def copyValue(data: Array[Any], pos: Int, newData: Array[Any],
                           newPos: Int) =
      newData(0).asInstanceOf[Array[Int]](newPos) =
        data(0).asInstanceOf[Array[Int]](pos)
  }

  final class DoubleHandler(val col: Int) extends ColumnHandler {
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

    override def newMutableRow(): SpecificMutableRow =
      new SpecificMutableRow(Array[MutableValue](new MutableDouble))

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

  final class DoubleExprHandler(val expr: Expression) extends ColumnHandler {
    override def initDataContainer(capacity: Int): Array[Any] = {
      Array[Any](new Array[Double](capacity))
    }

    override def hash(row: Row): Int = {
      hashLong(java.lang.Double.doubleToRawLongBits(
        expr.eval(row).asInstanceOf[Double]))
    }

    override def hash(data: Array[Any], pos: Int): Int = {
      hashLong(java.lang.Double.doubleToRawLongBits(
        data(0).asInstanceOf[Array[Double]](pos)))
    }

    override def equals(data: Array[Any], pos: Int, row: Row): Boolean =
      data(0).asInstanceOf[Array[Double]](pos) == expr.eval(
        row).asInstanceOf[Double]

    override def newMutableRow(): SpecificMutableRow =
      new SpecificMutableRow(Array[MutableValue](new MutableDouble))

    override def fillValue(data: Array[Any], pos: Int,
                           row: SpecificMutableRow) =
      row.setDouble(0, data(0).asInstanceOf[Array[Double]](pos))

    override def setValue(data: Array[Any], pos: Int, row: Row) =
      data(0).asInstanceOf[Array[Double]](pos) = expr.eval(
        row).asInstanceOf[Double]

    override def copyValue(data: Array[Any], pos: Int, newData: Array[Any],
                           newPos: Int) =
      newData(0).asInstanceOf[Array[Double]](newPos) =
        data(0).asInstanceOf[Array[Double]](pos)
  }

  final class FloatHandler(val col: Int) extends ColumnHandler {
    override def initDataContainer(capacity: Int): Array[Any] = {
      Array[Any](new Array[Float](capacity))
    }

    override def hash(row: Row): Int =
      hashInt(java.lang.Float.floatToRawIntBits(row.getFloat(col)))

    override def hash(data: Array[Any], pos: Int): Int = hashInt(java.lang
      .Float.floatToRawIntBits(data(0).asInstanceOf[Array[Float]](pos)))

    override def equals(data: Array[Any], pos: Int, row: Row): Boolean =
      data(0).asInstanceOf[Array[Float]](pos) == row.getFloat(col)

    override def newMutableRow(): SpecificMutableRow =
      new SpecificMutableRow(Array[MutableValue](new MutableFloat))

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

  final class FloatExprHandler(val expr: Expression) extends ColumnHandler {
    override def initDataContainer(capacity: Int): Array[Any] = {
      Array[Any](new Array[Float](capacity))
    }

    override def hash(row: Row): Int =
      hashInt(java.lang.Float.floatToRawIntBits(
        expr.eval(row).asInstanceOf[Float]))

    override def hash(data: Array[Any], pos: Int): Int = hashInt(java.lang
      .Float.floatToRawIntBits(data(0).asInstanceOf[Array[Float]](pos)))

    override def equals(data: Array[Any], pos: Int, row: Row): Boolean =
      data(0).asInstanceOf[Array[Float]](pos) == expr.eval(
        row).asInstanceOf[Float]

    override def newMutableRow(): SpecificMutableRow =
      new SpecificMutableRow(Array[MutableValue](new MutableFloat))

    override def fillValue(data: Array[Any], pos: Int,
                           row: SpecificMutableRow) =
      row.setFloat(0, data(0).asInstanceOf[Array[Float]](pos))

    override def setValue(data: Array[Any], pos: Int, row: Row) =
      data(0).asInstanceOf[Array[Float]](pos) = expr.eval(
        row).asInstanceOf[Float]

    override def copyValue(data: Array[Any], pos: Int, newData: Array[Any],
                           newPos: Int) =
      newData(0).asInstanceOf[Array[Float]](newPos) =
        data(0).asInstanceOf[Array[Float]](pos)
  }

  final class BooleanHandler(val col: Int) extends ColumnHandler {
    override def initDataContainer(capacity: Int): Array[Any] = {
      Array[Any](new Array[Boolean](capacity))
    }

    override def hash(row: Row): Int = if (row.getBoolean(col)) 1 else 0

    override def hash(data: Array[Any], pos: Int): Int =
      if (data(0).asInstanceOf[Array[Boolean]](pos)) 1 else 0

    override def equals(data: Array[Any], pos: Int, row: Row): Boolean =
      data(0).asInstanceOf[Array[Boolean]](pos) == row.getBoolean(col)

    override def newMutableRow(): SpecificMutableRow =
      new SpecificMutableRow(Array[MutableValue](new MutableBoolean))

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

  final class BooleanExprHandler(val expr: Expression) extends ColumnHandler {
    override def initDataContainer(capacity: Int): Array[Any] = {
      Array[Any](new Array[Boolean](capacity))
    }

    override def hash(row: Row): Int =
      if (expr.eval(row).asInstanceOf[Boolean]) 1 else 0

    override def hash(data: Array[Any], pos: Int): Int =
      if (data(0).asInstanceOf[Array[Boolean]](pos)) 1 else 0

    override def equals(data: Array[Any], pos: Int, row: Row): Boolean =
      data(0).asInstanceOf[Array[Boolean]](pos) == expr.eval(
        row).asInstanceOf[Boolean]

    override def newMutableRow(): SpecificMutableRow =
      new SpecificMutableRow(Array[MutableValue](new MutableBoolean))

    override def fillValue(data: Array[Any], pos: Int,
                           row: SpecificMutableRow) =
      row.setBoolean(0, data(0).asInstanceOf[Array[Boolean]](pos))

    override def setValue(data: Array[Any], pos: Int, row: Row) =
      data(0).asInstanceOf[Array[Boolean]](pos) = expr.eval(
        row).asInstanceOf[Boolean]

    override def copyValue(data: Array[Any], pos: Int, newData: Array[Any],
                           newPos: Int) =
      newData(0).asInstanceOf[Array[Boolean]](newPos) =
        data(0).asInstanceOf[Array[Boolean]](pos)
  }

  final class ByteHandler(val col: Int) extends ColumnHandler {
    override def initDataContainer(capacity: Int): Array[Any] = {
      Array[Any](new Array[Byte](capacity))
    }

    override def hash(row: Row): Int = row.getByte(col)

    override def hash(data: Array[Any], pos: Int): Int =
      data(0).asInstanceOf[Array[Byte]](pos)

    override def equals(data: Array[Any], pos: Int, row: Row): Boolean =
      data(0).asInstanceOf[Array[Byte]](pos) == row.getByte(col)

    override def newMutableRow(): SpecificMutableRow =
      new SpecificMutableRow(Array[MutableValue](new MutableByte))

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

  final class ByteExprHandler(val expr: Expression) extends ColumnHandler {
    override def initDataContainer(capacity: Int): Array[Any] = {
      Array[Any](new Array[Byte](capacity))
    }

    override def hash(row: Row): Int = expr.eval(row).asInstanceOf[Byte]

    override def hash(data: Array[Any], pos: Int): Int =
      data(0).asInstanceOf[Array[Byte]](pos)

    override def equals(data: Array[Any], pos: Int, row: Row): Boolean =
      data(0).asInstanceOf[Array[Byte]](pos) == expr.eval(row).asInstanceOf[Byte]

    override def newMutableRow(): SpecificMutableRow =
      new SpecificMutableRow(Array[MutableValue](new MutableByte))

    override def fillValue(data: Array[Any], pos: Int,
                           row: SpecificMutableRow) =
      row.setByte(0, data(0).asInstanceOf[Array[Byte]](pos))

    override def setValue(data: Array[Any], pos: Int, row: Row) =
      data(0).asInstanceOf[Array[Byte]](pos) = expr.eval(row).asInstanceOf[Byte]

    override def copyValue(data: Array[Any], pos: Int, newData: Array[Any],
                           newPos: Int) =
      newData(0).asInstanceOf[Array[Byte]](newPos) =
        data(0).asInstanceOf[Array[Byte]](pos)
  }

  final class ShortHandler(val col: Int) extends ColumnHandler {
    override def initDataContainer(capacity: Int): Array[Any] = {
      Array[Any](new Array[Short](capacity))
    }

    override def hash(row: Row): Int = row.getShort(col)

    override def hash(data: Array[Any], pos: Int): Int =
      data(0).asInstanceOf[Array[Short]](pos)

    override def equals(data: Array[Any], pos: Int, row: Row): Boolean =
      data(0).asInstanceOf[Array[Short]](pos) == row.getShort(col)

    override def newMutableRow(): SpecificMutableRow =
      new SpecificMutableRow(Array[MutableValue](new MutableShort))

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

  final class ShortExprHandler(val expr: Expression) extends ColumnHandler {
    override def initDataContainer(capacity: Int): Array[Any] = {
      Array[Any](new Array[Short](capacity))
    }

    override def hash(row: Row): Int = expr.eval(row).asInstanceOf[Short]

    override def hash(data: Array[Any], pos: Int): Int =
      data(0).asInstanceOf[Array[Short]](pos)

    override def equals(data: Array[Any], pos: Int, row: Row): Boolean =
      data(0).asInstanceOf[Array[Short]](pos) == expr.eval(
        row).asInstanceOf[Short]

    override def newMutableRow(): SpecificMutableRow =
      new SpecificMutableRow(Array[MutableValue](new MutableShort))

    override def fillValue(data: Array[Any], pos: Int,
                           row: SpecificMutableRow) =
      row.setShort(0, data(0).asInstanceOf[Array[Short]](pos))

    override def setValue(data: Array[Any], pos: Int, row: Row) =
      data(0).asInstanceOf[Array[Short]](pos) = expr.eval(
        row).asInstanceOf[Short]

    override def copyValue(data: Array[Any], pos: Int, newData: Array[Any],
                           newPos: Int) =
      newData(0).asInstanceOf[Array[Short]](newPos) =
        data(0).asInstanceOf[Array[Short]](pos)
  }

  final class SingleColumnHandler(val col: Int) extends ColumnHandler {
    override def initDataContainer(capacity: Int): Array[Any] = {
      Array[Any](new Array[Any](capacity))
    }

    override def hash(row: Row): Int = hashInt(row(col).##)

    override def hash(data: Array[Any], pos: Int): Int =
      hashInt(data(0).asInstanceOf[Array[Any]](pos).##)

    override def equals(data: Array[Any], pos: Int, row: Row): Boolean =
      data(0).asInstanceOf[Array[Any]](pos).equals(row(col))

    override def newMutableRow(): SpecificMutableRow =
      new SpecificMutableRow(Array[MutableValue](new MutableAny))

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

  final class SingleColumnExprHandler(val expr: Expression) extends ColumnHandler {
    override def initDataContainer(capacity: Int): Array[Any] = {
      Array[Any](new Array[Any](capacity))
    }

    override def hash(row: Row): Int = hashInt(expr.eval(row).##)

    override def hash(data: Array[Any], pos: Int): Int =
      hashInt(data(0).asInstanceOf[Array[Any]](pos).##)

    override def equals(data: Array[Any], pos: Int, row: Row): Boolean =
      data(0).asInstanceOf[Array[Any]](pos).equals(expr.eval(row))

    override def newMutableRow(): SpecificMutableRow =
      new SpecificMutableRow(Array[MutableValue](new MutableAny))

    override def fillValue(data: Array[Any], pos: Int,
                           row: SpecificMutableRow) =
      row.update(0, data(0).asInstanceOf[Array[Any]](pos))

    override def setValue(data: Array[Any], pos: Int, row: Row) =
      data(0).asInstanceOf[Array[Any]](pos) = expr.eval(row)

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
  final class MultiColumnHandler(val cols: Array[Int],
                                 val ncols: Int,
                                 val types: Array[DataType])
    extends ColumnHandler {

    override def initDataContainer(capacity: Int): Array[Any] = {
      val ncols = this.ncols
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
      val cols = this.cols
      val ncols = this.ncols
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
      val ncols = this.ncols
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
      val cols = this.cols
      val ncols = this.ncols
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

    override def newMutableRow(): SpecificMutableRow = {
      val ncols = this.ncols
      val row = new Array[MutableValue](ncols)
      var i = 0
      while (i < ncols) {
        types(i) match {
          case LongType =>
            row(i) = new MutableLong
          case IntegerType =>
            row(i) = new MutableInt
          case DoubleType =>
            row(i) = new MutableDouble
          case FloatType =>
            row(i) = new MutableFloat
          case BooleanType =>
            row(i) = new MutableBoolean
          case ByteType =>
            row(i) = new MutableByte
          case ShortType =>
            row(i) = new MutableShort
          case DateType =>
            // use INT for DATE -- see comment in SpecificMutableRow constructor
            row(i) = new MutableInt
          case _ =>
            row(i) = new MutableAny
        }
        i += 1
      }
      new SpecificMutableRow(row)
    }

    override def fillValue(data: Array[Any], pos: Int,
                           row: SpecificMutableRow) = {
      val ncols = this.ncols
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
      val cols = this.cols
      val ncols = this.ncols
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
      val ncols = this.ncols
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
