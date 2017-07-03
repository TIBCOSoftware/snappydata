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

import com.gemstone.gemfire.internal.shared.BufferAllocator
import com.gemstone.gnu.trove.TLongArrayList
import io.snappydata.collection.{ByteBufferHashMap, LongKey, ObjectHashSet}

import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatEntry
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

  /**
   * Initialization will fill in the dictionaries as written by the
   * DictionaryEncoder. For string maps it reads in the value array
   * written using [[ByteBufferHashMap]] by the encoder expecting the
   * size of UTF8 encoded string followed by the string contents.
   * Long and integer dictionaries are still using the old ObjectHashSet
   * which needs to be moved to [[ByteBufferHashMap]] once DictionaryEncoder
   * adds support for long/integer dictionary encoding.
   */
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

  /**
   * Serialized off-heap map used for strings. This is to minimize the objects
   * created to help GC issues in bulk inserts. The serialized map uses
   * a single serialized array for fixed-width keys and another for values.
   *
   * Strings are added using [[ByteBufferHashMap.addDictionaryString]]
   * method which returns the index of the string in the dictionary.
   */
  private[this] final var stringMap: ByteBufferHashMap = _

  private[this] final var longMap: ObjectHashSet[LongIndexKey] = _
  private[this] final var longArray: TLongArrayList = _
  private[this] final var isIntMap: Boolean = _

  @transient private[this] final var isShortDictionary: Boolean = _

  override def typeId: Int = if (isShortDictionary) 2 else BIG_DICTIONARY_TYPE_ID

  override def sizeInBytes(cursor: Long): Long = {
    if (stringMap ne null) {
      cursor - columnBeginPosition + stringMap.valueDataSize
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

  protected def initializeIndexBytes(initSize: Int,
      releaseOld: Boolean): Unit = {
    if (!releaseOld || (columnData eq null)) {
      // 2 byte indexes for short dictionary while 4 bytes for big dictionary
      val numBytes = if (isShortDictionary) initSize << 1L else initSize << 2L
      setSource(allocator.allocate(numBytes, ColumnEncoding.BUFFER_OWNER),
        releaseOld)
    }
  }

  override def initialize(field: StructField, initSize: Int,
      withHeader: Boolean, allocator: BufferAllocator): Long = {
    assert(withHeader, "DictionaryEncoding not supported without header")

    setAllocator(allocator)
    Utils.getSQLDataType(field.dataType) match {
      case StringType =>
        if (stringMap eq null) {
          // assume some level of compression with dictionary encoding
          val mapSize = math.min(math.max(initSize >>> 1, 128), 1024)
          // keySize is 4 since need to store dictionary index
          stringMap = new ByteBufferHashMap(mapSize, 0.6, 4,
            StringType.defaultSize, allocator)
        } else {
          // reuse the previous dictionary data but release the shell objects
          stringMap = stringMap.duplicate()
          stringMap.reset()
        }
      case t =>
        // assume some level of compression with dictionary encoding
        val mapSize = if (longMap ne null) longMap.size
        else math.min(math.max(initSize >>> 1, 128), 1024)
        longMap = new ObjectHashSet[LongIndexKey](mapSize, 0.6, 1, false)
        longArray = new TLongArrayList(mapSize)
        isIntMap = t.isInstanceOf[IntegerType]
    }
    initializeLimits()
    initializeNulls(initSize)
    // start with the short dictionary having 2 byte indexes
    isShortDictionary = true
    initializeIndexBytes(initSize, releaseOld = true)
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
    // force new columnData creation since need to copy in different format
    initializeIndexBytes(newNumIndexes, releaseOld = false)
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
    var index = stringMap.addDictionaryString(value)
    // update stats only if new key was added
    if (index < 0) {
      updateStringStats(value)
      index = -index - 1
      if (index == Short.MaxValue && isShortDictionary) {
        val numUsedIndexes = ((position - columnBeginPosition) >> 1).toInt
        // allocate with increased size
        position = switchToBigDictionary(numUsedIndexes,
          (numUsedIndexes << 2) / 3)
      }
    }
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
    var numElements: Int = 0
    val dictionarySize = if (stringMap ne null) {
      numElements = stringMap.size
      stringMap.valueDataSize
    } else {
      numElements = longArray.size
      if (isIntMap) numElements << 2
      else numElements << 4
    }
    // create the final data array of exact size that is known at this point
    val numNullWords = getNumNullWords
    val dataSize = 4L /* dictionary size */ + dictionarySize + numIndexBytes
    val storageAllocator = this.storageAllocator
    // serialization header size + typeId + number of nulls
    val headerSize = ColumnFormatEntry.VALUE_HEADER_SIZE + 8L
    val columnData = storageAllocator.allocateForStorage(ColumnEncoding
        .checkBufferSize(headerSize + (numNullWords << 3L) + dataSize))
    val columnBytes = storageAllocator.baseObject(columnData)
    val baseOffset = storageAllocator.baseOffset(columnData)
    // skip serialization header which will be filled in by ColumnFormatValue
    var cursor = baseOffset + ColumnFormatEntry.VALUE_HEADER_SIZE
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
      // dictionary is already in serialized form
      if (numElements > 0) {
        Platform.copyMemory(stringMap.valueData.baseObject,
          stringMap.valueData.baseOffset, columnBytes, cursor, dictionarySize)
        cursor += dictionarySize
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
    copyTo(columnData, srcOffset = 0, numIndexBytes)
    columnData.position(position)

    // reuse this index data in next round if possible
    releaseForReuse(numIndexBytes)

    columnData
  }

  override def close(): Unit = {
    super.close()
    if ((stringMap ne null) && (stringMap.keyData ne null)) {
      stringMap.release()
    }
    stringMap = null
    longMap = null
    longArray = null
  }
}

private final class LongIndexKey(_l: Long, var index: Int)
    extends LongKey(_l)

private object LongInit extends (Long => LongIndexKey) {
  override def apply(l: Long): LongIndexKey = {
    new LongIndexKey(l, -1)
  }
}
