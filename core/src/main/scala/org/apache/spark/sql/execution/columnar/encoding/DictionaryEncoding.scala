/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

import java.nio.ByteBuffer

import com.gemstone.gnu.trove.TLongArrayList

import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.{LongKey, ObjectHashSet, StringKey}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

trait DictionaryEncoding extends ColumnEncoding {

  override def typeId: Int = 2

  val BIG_DICTIONARY_TYPE_ID = 3

  override final def supports(dataType: DataType): Boolean = dataType match {
    case StringType | IntegerType | DateType | LongType | TimestampType => true
    case _ => false
  }
}

final class DictionaryDecoder
    extends DictionaryDecoderBase with NotNullDecoder

final class DictionaryDecoderNullable
    extends DictionaryDecoderBase with NullableDecoder

final class BigDictionaryDecoder
    extends BigDictionaryDecoderBase with NotNullDecoder

final class BigDictionaryDecoderNullable
    extends BigDictionaryDecoderBase with NullableDecoder

final class DictionaryEncoder
    extends NotNullEncoder with DictionaryEncoderBase

final class DictionaryEncoderNullable
    extends NullableEncoder with DictionaryEncoderBase

abstract class DictionaryDecoderBase
    extends ColumnDecoder with DictionaryEncoding {

  protected[this] final var stringDictionary: Array[UTF8String] = _
  protected[this] final var intDictionary: Array[Int] = _
  protected[this] final var longDictionary: Array[Long] = _

  override protected def initializeCursor(columnBytes: AnyRef, cursor: Long,
      field: StructField): Long = {
    var position = cursor
    val numElements = ColumnEncoding.readInt(columnBytes, position)
    // last index in the dictionary is for null element
    val dictionaryLen = if (hasNulls) numElements + 1 else numElements
    var index = 0
    position += 4
    Utils.getSQLDataType(field.dataType) match {
      case StringType =>
        stringDictionary = new Array[UTF8String](dictionaryLen)
        while (index < numElements) {
          val s = ColumnEncoding.readUTF8String(columnBytes, position)
          stringDictionary(index) = s
          position += (4 + s.numBytes())
          index += 1
        }
      case IntegerType | DateType =>
        intDictionary = new Array[Int](dictionaryLen)
        while (index < numElements) {
          intDictionary(index) = ColumnEncoding.readInt(columnBytes, position)
          position += 4
          index += 1
        }
      case LongType | TimestampType =>
        longDictionary = new Array[Long](dictionaryLen)
        while (index < numElements) {
          longDictionary(index) = ColumnEncoding.readLong(columnBytes, position)
          position += 8
          index += 1
        }
      case _ => throw new UnsupportedOperationException(
        s"DictionaryEncoding not supported for ${field.dataType}")
    }
    position - 2 // move cursor back so that first next call increments it
  }

  override def nextUTF8String(columnBytes: AnyRef, cursor: Long): Long =
    cursor + 2

  override def readUTF8String(columnBytes: AnyRef,
      cursor: Long): UTF8String =
    stringDictionary(ColumnEncoding.readShort(columnBytes, cursor))

  override final def getStringDictionary: Array[UTF8String] =
    stringDictionary

  override def readDictionaryIndex(columnBytes: AnyRef, cursor: Long): Int =
    if (ColumnEncoding.littleEndian) {
      Platform.getShort(columnBytes, cursor)
    } else {
      java.lang.Short.reverseBytes(Platform.getShort(columnBytes, cursor))
    }

  override def nextInt(columnBytes: AnyRef, cursor: Long): Long =
    cursor + 2

  override def readInt(columnBytes: AnyRef, cursor: Long): Int =
    intDictionary(ColumnEncoding.readShort(columnBytes, cursor))

  override def nextLong(columnBytes: AnyRef, cursor: Long): Long =
    cursor + 2

  override def readLong(columnBytes: AnyRef, cursor: Long): Long =
    longDictionary(ColumnEncoding.readShort(columnBytes, cursor))
}

abstract class BigDictionaryDecoderBase extends DictionaryDecoderBase {

  override def typeId: Int = BIG_DICTIONARY_TYPE_ID

  override protected def initializeCursor(columnBytes: AnyRef, cursor: Long,
      field: StructField): Long = {
    // move cursor further back for 4 byte integer index reads
    super.initializeCursor(columnBytes, cursor, field) - 2
  }

  override final def nextUTF8String(columnBytes: AnyRef, cursor: Long): Long =
    cursor + 4

  override final def readUTF8String(columnBytes: AnyRef,
      cursor: Long): UTF8String =
    stringDictionary(ColumnEncoding.readInt(columnBytes, cursor))

  override def readDictionaryIndex(columnBytes: AnyRef, cursor: Long): Int =
    if (ColumnEncoding.littleEndian) {
      Platform.getInt(columnBytes, cursor)
    } else {
      java.lang.Integer.reverseBytes(Platform.getInt(columnBytes, cursor))
    }

  override final def nextInt(columnBytes: AnyRef, cursor: Long): Long =
    cursor + 4

  override final def readInt(columnBytes: AnyRef, cursor: Long): Int =
    intDictionary(ColumnEncoding.readInt(columnBytes, cursor))

  override final def nextLong(columnBytes: AnyRef, cursor: Long): Long =
    cursor + 4

  override final def readLong(columnBytes: AnyRef, cursor: Long): Long =
    longDictionary(ColumnEncoding.readInt(columnBytes, cursor))
}

trait DictionaryEncoderBase extends ColumnEncoder with DictionaryEncoding {

  private[this] final var stringMap: ObjectHashSet[StringIndexKey] = _
  // list is more efficient than array for iteration in finish() with
  // its overall performance being 5-10% higher in inserts;
  // reason seems to be better cache pre-fetch for list references due to next
  // reference rather than array where reference value does not get pre-fetched
  private[this] final var stringHead: StringIndexKey = _
  private[this] final var stringTail: StringIndexKey = _
  private[this] final var numStrings: Int = _

  private[this] final var longMap: ObjectHashSet[LongIndexKey] = _
  private[this] final var longArray: TLongArrayList = _
  private[this] final var isIntMap: Boolean = _

  @transient private[this] final var isShortDictionary: Boolean = _
  @transient private[this] final var dictionarySize: Long = _

  override def typeId: Int = if (isShortDictionary) 2 else BIG_DICTIONARY_TYPE_ID

  override def sizeInBytes(cursor: Long): Long = {
    if (dictionarySize > 0 || (stringMap ne null)) {
      cursor - columnBeginPosition + dictionarySize
    } else if (isIntMap) {
      cursor - columnBeginPosition + (longArray.size() << 2)
    } else {
      cursor - columnBeginPosition + (longArray.size() << 4)
    }
  }

  override def defaultSize(dataType: DataType): Int = dataType match {
    // reduce size by a factor of 2 assuming some compression
    case StringType => dataType.defaultSize >>> 1
    case _ => dataType.defaultSize // accommodate values without expansion
  }

  protected def initializeIndexBytes(initSize: Int, minSize: Int): Unit = {
    // 2 byte indexes for short dictionary while 4 bytes for big dictionary
    val numBytes = if (isShortDictionary) initSize << 1L else initSize << 2L
    if ((reuseColumnData eq null) || reuseColumnData.remaining() < minSize) {
      setSource(allocator.allocate(numBytes))
      if (reuseColumnData ne null) {
        allocator.release(reuseColumnData)
        reuseColumnData = null
      }
    } else {
      setSource(reuseColumnData)
      reuseColumnData = null
    }
  }

  override def initialize(field: StructField, initSize: Int,
      withHeader: Boolean, allocator: ColumnAllocator): Long = {
    assert(withHeader, "DictionaryEncoding not supported without header")

    Utils.getSQLDataType(field.dataType) match {
      case StringType =>
        // re-use the previous map if not too large
        // previous index values in the map would have been reset to -1
        if ((stringMap eq null) || stringMap.size > 512) {
          stringMap = new ObjectHashSet[StringIndexKey](128, 0.6, 1)
        }
      case t =>
        longMap = new ObjectHashSet[LongIndexKey](128, 0.6, 1)
        longArray = new TLongArrayList(128)
        isIntMap = t.isInstanceOf[IntegerType]
    }
    dictionarySize = 0
    this.allocator = allocator
    initializeLimits()
    initializeNulls(initSize)
    // start with the short dictionary having 2 byte indexes
    isShortDictionary = true
    initializeIndexBytes(initSize, 0)
    // return the cursor for index bytes
    columnBeginPosition
  }

  protected final def switchToBigDictionary(numUsedIndexes: Int,
      newNumIndexes: Int): Long = {
    // mark as a big dictionary
    isShortDictionary = false
    val oldIndexData = columnData
    val oldIndexBytes = columnBytes
    var oldCursor = columnBeginPosition
    // initialize new index array having at least the current dictionary size
    initializeIndexBytes(newNumIndexes, newNumIndexes << 2)
    var cursor = columnBeginPosition
    // copy over short indexes from previous index bytes
    var i = 0
    while (i < numUsedIndexes) {
      ColumnEncoding.writeInt(columnBytes, cursor,
        ColumnEncoding.readShort(oldIndexBytes, oldCursor))
      oldCursor += 2
      cursor += 4
      i += 1
    }
    allocator.release(oldIndexData)
    cursor
  }

  protected final def writeIndex(cursor: Long, index: Int): Long = {
    // expand the index array if required
    if (isShortDictionary) {
      var position = cursor
      if (position + 2 > columnEndPosition) {
        position = expand(position, 2)
      }
      ColumnEncoding.writeShort(columnBytes, position, index.toShort)
      position + 2
    } else {
      var position = cursor
      if (position + 4 > columnEndPosition) {
        position = expand(position, 4)
      }
      ColumnEncoding.writeInt(columnBytes, position, index)
      position + 4
    }
  }

  override final def writeUTF8String(cursor: Long, value: UTF8String): Long = {
    var position = cursor
    // add or get from dictionary
    val key = stringMap.addString(value, StringInit)
    // update stats only if new key was added
    var index = key.index
    if (index == -1) {
      dictionarySize += 4 + key.numBytes
      updateStringStats(key)
      index = numStrings
      numStrings += 1
      key.index = index
      if (index == Short.MaxValue && isShortDictionary) {
        val numUsedIndexes = ((position - columnBeginPosition) >> 1).toInt
        // allocate with increased size
        position = switchToBigDictionary(numUsedIndexes,
          (numUsedIndexes << 2) / 3)
      }
      val stringTail = this.stringTail
      if (stringTail ne null) {
        stringTail.next = key
        this.stringTail = key
      } else {
        this.stringHead = key
        this.stringTail = key
      }
    }
    updateCount()
    // write the index
    writeIndex(position, index)
  }

  override final def writeInt(cursor: Long, value: Int): Long = {
    // write just like longs (finish will adjust for actual size in long array)
    writeLong(cursor, value)
  }

  override final def writeLong(cursor: Long, value: Long): Long = {
    var position = cursor
    // add or get from dictionary
    val key = longMap.addLong(value, LongInit)
    // update stats only if new key was added
    var index = key.index
    if (index == -1) {
      index = longArray.size()
      key.index = index
      if (index == Short.MaxValue && isShortDictionary) {
        val numUsedIndexes = ((position - columnBeginPosition) >> 1).toInt
        // allocate with increased size
        position = switchToBigDictionary(numUsedIndexes,
          (numUsedIndexes << 2) / 3)
      }
      longArray.add(key.l)
      updateLongStats(value)
    }
    // write the index
    writeIndex(position, index)
  }

  override def finish(indexCursor: Long): ByteBuffer = {
    val numIndexBytes = (indexCursor - this.columnBeginPosition).toInt
    var numElements = this.numStrings
    if (stringMap eq null) {
      numElements = longArray.size
      if (isIntMap) {
        dictionarySize = numElements << 2
      } else {
        dictionarySize = numElements << 4
      }
    }
    // create the final data array of exact size that is known at this point
    val numNullWords = getNumNullWords
    val dataSize = 4L /* dictionary size */ + dictionarySize + numIndexBytes
    val columnData = finalAllocator.allocate(ColumnEncoding.checkBufferSize(
      8L /* typeId + number of nulls */ + (numNullWords << 3L) + dataSize))
    val columnBytes = if (columnData.hasArray) columnData.array() else null
    val baseOffset = getBaseOffset(columnData)
    var cursor = baseOffset
    // typeId
    ColumnEncoding.writeInt(columnBytes, cursor, typeId)
    cursor += 4
    // number of nulls
    ColumnEncoding.writeInt(columnBytes, cursor, numNullWords)
    cursor += 4
    // write the null bytes
    cursor = writeNulls(columnBytes, cursor, numNullWords)

    // write the dictionary and then the indexes

    // write the size of dictionary
    ColumnEncoding.writeInt(columnBytes, cursor, numElements)
    cursor += 4
    // write the dictionary elements
    if (stringMap ne null) {
      if (numElements > 0) {
        stringTail.next = null
        var current = stringHead
        do {
          // reset index for re-use in next batch
          current.index = -1
          cursor = ColumnEncoding.writeUTF8String(columnBytes, cursor,
            current.base, current.offset, current.numBytes)
          current = current.next
        } while (current ne null)
        this.stringHead = null
        this.stringTail = null
        this.numStrings = 0
      }
    } else if (isIntMap) {
      var index = 0
      while (index < numElements) {
        val l = longArray.getQuick(index)
        ColumnEncoding.writeInt(columnBytes, cursor, l.toInt)
        cursor += 4
        index += 1
      }
    } else {
      var index = 0
      while (index < numElements) {
        val l = longArray.getQuick(index)
        ColumnEncoding.writeLong(columnBytes, cursor, l)
        cursor += 8
        index += 1
      }
    }
    // lastly copy the index bytes
    val position = columnData.position()
    columnData.position((cursor - baseOffset).toInt)
    copyTo(columnData, 0, numIndexBytes)
    columnData.position(position)

    // reuse this index data in next round if possible
    releaseForReuse(this.columnData, numIndexBytes)
    clearSource()

    columnData
  }
}

private final class StringIndexKey(_base: AnyRef, _offset: Long,
    _numBytes: Int, var index: Int, var next: StringIndexKey)
    extends StringKey(_base, _offset, _numBytes) {

  def this(s: UTF8String) = {
    this(s.getBaseObject, s.getBaseOffset, s.numBytes(), -1, null)
  }
}

private object StringInit extends (UTF8String => StringIndexKey) {
  override def apply(s: UTF8String): StringIndexKey = {
    if (s.getBaseOffset == Platform.BYTE_ARRAY_OFFSET &&
        s.getBaseObject.asInstanceOf[Array[Byte]].length == s.numBytes()) {
      // no clone required
      new StringIndexKey(s)
    } else {
      // clone into StringIndexKey
      val numBytes = s.numBytes()
      val bytes = new Array[Byte](numBytes)
      Platform.copyMemory(s.getBaseObject, s.getBaseOffset,
        bytes, Platform.BYTE_ARRAY_OFFSET, numBytes)
      new StringIndexKey(bytes, Platform.BYTE_ARRAY_OFFSET, numBytes, -1, null)
    }
  }
}

private final class LongIndexKey(_l: Long, var index: Int)
    extends LongKey(_l)

private object LongInit extends (Long => LongIndexKey) {
  override def apply(l: Long): LongIndexKey = {
    new LongIndexKey(l, -1)
  }
}
