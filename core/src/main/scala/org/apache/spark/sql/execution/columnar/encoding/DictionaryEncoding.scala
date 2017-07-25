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
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

trait DictionaryEncoding extends ColumnEncoding {

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

  override def typeId: Int = ColumnEncoding.DICTIONARY_TYPE_ID

  /**
   * Initialization will fill in the dictionaries as written by the
   * DictionaryEncoder. For string maps it reads in the value array
   * written using [[ByteBufferHashMap]] by the encoder expecting the
   * size of UTF8 encoded string followed by the string contents.
   * Long and integer dictionaries are still using the old ObjectHashSet
   * which needs to be moved to [[ByteBufferHashMap]] once DictionaryEncoder
   * adds support for long/integer dictionary encoding.
   */
  override protected[sql] def initializeCursor(columnBytes: AnyRef, cursor: Long,
      field: StructField): Long = {
    val numElements = ColumnEncoding.readInt(columnBytes, cursor)
    // last index in the dictionary is for null element
    val dictionaryLen = if (hasNulls) numElements + 1 else numElements
    // move cursor back so that first next call increments it
    initializeDictionary(columnBytes, cursor + 4, numElements, dictionaryLen,
      field.dataType) - 2
  }

  private def initializeDictionary(columnBytes: AnyRef, cursor: Long,
      numElements: Int, dictionaryLen: Int, dataType: DataType): Long = {
    var position = cursor
    var index = 0
    Utils.getSQLDataType(dataType) match {
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
        s"DictionaryEncoding not supported for $dataType")
    }
    baseCursor = position
    position
  }

  private[sql] def initializeBeforeFinish(dictionaryBytes: AnyRef,
      dictionaryCursor: Long, dictionaryLen: Int, cursor: Long, dataType: DataType): Long = {
    initializeDictionary(dictionaryBytes, dictionaryCursor,
      dictionaryLen, dictionaryLen, dataType)
    cursor - 2 // move cursor back so that first next call increments it
  }

  override def nextUTF8String(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    cursor + 2

  override def absoluteUTF8String(columnBytes: AnyRef, position: Int): Long = {
    // TODO: PERF: optimize for local index access case by filling
    // in the dictionary array lazily on access
    baseCursor + ((position - numNullsUntilPosition(columnBytes, position) + 1) << 1)
  }

  override def readUTF8String(columnBytes: AnyRef,
      cursor: Long, mutated: Int): UTF8String =
    stringDictionary(ColumnEncoding.readShort(columnBytes, cursor))

  override final def getStringDictionary: Array[UTF8String] =
    stringDictionary

  override def readDictionaryIndex(columnBytes: AnyRef, cursor: Long, mutated: Int): Int =
    if (ColumnEncoding.littleEndian) {
      Platform.getShort(columnBytes, cursor)
    } else {
      java.lang.Short.reverseBytes(Platform.getShort(columnBytes, cursor))
    }

  override def nextInt(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    cursor + 2

  override def absoluteInt(columnBytes: AnyRef, position: Int): Long =
    absoluteUTF8String(columnBytes, position)

  override def readInt(columnBytes: AnyRef, cursor: Long, mutated: Int): Int =
    intDictionary(ColumnEncoding.readShort(columnBytes, cursor))

  override def nextLong(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    cursor + 2

  override def absoluteLong(columnBytes: AnyRef, position: Int): Long =
    absoluteUTF8String(columnBytes, position)

  override def readLong(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    longDictionary(ColumnEncoding.readShort(columnBytes, cursor))
}

abstract class BigDictionaryDecoderBase extends DictionaryDecoderBase {

  override def typeId: Int = ColumnEncoding.BIG_DICTIONARY_TYPE_ID

  override protected[sql] def initializeCursor(columnBytes: AnyRef, cursor: Long,
      field: StructField): Long = {
    // move cursor further back for 4 byte integer index reads
    super.initializeCursor(columnBytes, cursor, field) - 2
  }

  override private[sql] def initializeBeforeFinish(dictionaryBytes: AnyRef,
      dictionaryCursor: Long, dictionaryLen: Int, cursor: Long, dataType: DataType): Long = {
    // move cursor further back for 4 byte integer index reads
    super.initializeBeforeFinish(dictionaryBytes, dictionaryCursor,
      dictionaryLen, cursor, dataType) - 2
  }

  override final def nextUTF8String(columnBytes: AnyRef, cursor: Long,
      mutated: Int): Long = cursor + 4

  override def absoluteUTF8String(columnBytes: AnyRef, position: Int): Long = {
    // TODO: PERF: optimize for local index access case by filling
    // in the dictionary array lazily on access
    baseCursor + ((position - numNullsUntilPosition(columnBytes, position) + 1) << 2)
  }

  override final def readUTF8String(columnBytes: AnyRef,
      cursor: Long, mutated: Int): UTF8String =
    stringDictionary(ColumnEncoding.readInt(columnBytes, cursor))

  override def readDictionaryIndex(columnBytes: AnyRef, cursor: Long, mutated: Int): Int =
    if (ColumnEncoding.littleEndian) {
      Platform.getInt(columnBytes, cursor)
    } else {
      java.lang.Integer.reverseBytes(Platform.getInt(columnBytes, cursor))
    }

  override final def nextInt(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    cursor + 4

  override def absoluteInt(columnBytes: AnyRef, position: Int): Long =
    absoluteUTF8String(columnBytes, position)

  override final def readInt(columnBytes: AnyRef, cursor: Long, mutated: Int): Int =
    intDictionary(ColumnEncoding.readInt(columnBytes, cursor))

  override final def nextLong(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    cursor + 4

  override def absoluteLong(columnBytes: AnyRef, position: Int): Long =
    absoluteUTF8String(columnBytes, position)

  override final def readLong(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
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
  private final var stringMap: ByteBufferHashMap = _

  private final var longMap: ObjectHashSet[LongIndexKey] = _
  private final var longArray: TLongArrayList = _
  private final var isIntMap: Boolean = _

  @transient private final var isShortDictionary: Boolean = _

  override def typeId: Int = if (isShortDictionary) ColumnEncoding.DICTIONARY_TYPE_ID
  else ColumnEncoding.BIG_DICTIONARY_TYPE_ID

  override def sizeInBytes(cursor: Long): Long = {
    if (stringMap ne null) {
      cursor - columnBeginPosition + stringMap.valueDataSize
    } else if (isIntMap) {
      cursor - columnBeginPosition + (longArray.size << 2)
    } else {
      cursor - columnBeginPosition + (longArray.size << 4)
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

  override def initialize(dataType: DataType, nullable: Boolean, initSize: Int,
      withHeader: Boolean, allocator: BufferAllocator): Long = {
    assert(withHeader, "DictionaryEncoding not supported without header")

    setAllocator(allocator)
    dataType match {
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

  override def writeInternals(columnBytes: AnyRef, cursor: Long): Long = {
    var numDictionaryElements: Int = 0
    val dictionarySize = if (stringMap ne null) {
      numDictionaryElements = stringMap.size
      stringMap.valueDataSize
    } else {
      numDictionaryElements = longArray.size
      if (isIntMap) numDictionaryElements << 2
      else numDictionaryElements << 4
    }
    // write dictionary and position cursor at start of index bytes
    writeDictionary(columnBytes, cursor, numDictionaryElements, dictionarySize)
  }

  override private[sql] def decoderBeforeFinish: ColumnDecoder = {
    // can depend on nullCount here because there is no additional space
    // pre-allocated for storing nulls by dictionary encoder
    val decoder = if (nullCount > 0) {
      if (isShortDictionary) new DictionaryDecoderNullable else new BigDictionaryDecoderNullable
    } else {
      if (isShortDictionary) new DictionaryDecoder else new BigDictionaryDecoder
    }
    initializeNullsBeforeFinish(decoder)
    // TODO: only supported for strings; use ByteBufferHashMap for longArray too
    if (stringMap ne null) {
      // TODO: SW: release of buffers?
      val dictionaryData = stringMap.valueData
      decoder.initializeBeforeFinish(dictionaryData.baseObject,
        dictionaryData.baseOffset, stringMap.size, columnBeginPosition, StringType)
      decoder
    } else throw new UnsupportedOperationException(
      "decoder on unfinished data for long/int dictionary")
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
    val str = if (value ne null) value else UTF8String.EMPTY_UTF8
    // add or get from dictionary
    var index = stringMap.addDictionaryString(str)
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
      index = longArray.size
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

  private def writeDictionary(columnBytes: AnyRef, startCursor: Long,
      numDictionaryElements: Int, dictionarySize: Long): Long = {
    var cursor = startCursor
    // write the size of dictionary
    ColumnEncoding.writeInt(columnBytes, cursor, numDictionaryElements)
    cursor += 4
    // write the dictionary elements
    if (stringMap ne null) {
      // dictionary is already in serialized form
      if (numDictionaryElements > 0) {
        Platform.copyMemory(stringMap.valueData.baseObject,
          stringMap.valueData.baseOffset, columnBytes, cursor, dictionarySize)
        cursor += dictionarySize
      }
    } else if (isIntMap) {
      var index = 0
      while (index < numDictionaryElements) {
        val l = longArray.getQuick(index)
        ColumnEncoding.writeInt(columnBytes, cursor, l.toInt)
        cursor += 4
        index += 1
      }
    } else {
      var index = 0
      while (index < numDictionaryElements) {
        val l = longArray.getQuick(index)
        ColumnEncoding.writeLong(columnBytes, cursor, l)
        cursor += 8
        index += 1
      }
    }
    cursor
  }

  override def finish(indexCursor: Long): ByteBuffer = {
    val numIndexBytes = (indexCursor - this.columnBeginPosition).toInt
    var numDictionaryElements: Int = 0
    val dictionarySize = if (stringMap ne null) {
      numDictionaryElements = stringMap.size
      stringMap.valueDataSize
    } else {
      numDictionaryElements = longArray.size
      if (isIntMap) numDictionaryElements << 2
      else numDictionaryElements << 4
    }
    // create the final data array of exact size that is known at this point
    val numNullWords = getNumNullWords
    val numNullBytes = numNullWords << 3
    val dataSize = 4L /* dictionary size */ + dictionarySize + numIndexBytes
    val storageAllocator = this.storageAllocator
    val columnData = storageAllocator.allocateForStorage(ColumnEncoding
        // typeId + number of nulls
        .checkBufferSize(8L + numNullBytes + dataSize))
    val columnBytes = storageAllocator.baseObject(columnData)
    val baseOffset = storageAllocator.baseOffset(columnData)
    var cursor = baseOffset
    // typeId
    ColumnEncoding.writeInt(columnBytes, cursor, typeId)
    cursor += 4
    // number of nulls
    ColumnEncoding.writeInt(columnBytes, cursor, numNullBytes)
    cursor += 4
    // write the null bytes
    cursor = writeNulls(columnBytes, cursor, numNullWords)

    // write the dictionary and then the indexes

    cursor = writeDictionary(columnBytes, cursor, numDictionaryElements, dictionarySize)
    // lastly copy the index bytes
    columnData.position((cursor - baseOffset).toInt)
    copyTo(columnData, srcOffset = 0, numIndexBytes)
    columnData.rewind()

    // reuse this index data in next round if possible
    releaseForReuse(numIndexBytes)

    columnData
  }

  override def encodedSize(indexCursor: Long, dataBeginPosition: Long): Long = {
    val numIndexBytes = indexCursor - dataBeginPosition
    val dictionarySize =
      if (stringMap ne null) stringMap.valueDataSize
      else if (isIntMap) longArray.size << 2
      else longArray.size << 4
    4L /* dictionary size */ + dictionarySize + numIndexBytes
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
