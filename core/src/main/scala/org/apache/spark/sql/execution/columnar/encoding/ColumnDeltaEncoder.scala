/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import org.apache.spark.sql.execution.columnar.impl.{ColumnDelta, ColumnFormatValue}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.array.LongArray
import org.apache.spark.unsafe.memory.MemoryBlock
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.collection.unsafe.sort.RadixSort

/**
 * Writes data to be stored in [[ColumnFormatValue]] deltas.
 */
final class ColumnDeltaEncoder(hierarchyDepth: Int) extends ColumnEncoder {

  private[this] var dataType: DataType = _
  /*
   * Sorted map by position and relative index into the update list.
   * Spark's RadixSort allows one to sort on partial bytes in longs
   * and thus can be used to sort on position and is way ahead of alternatives
   * in performance (including FastUtil's radix sort without
   *   partial sort support, red-black or AVL trees etc)
   * Below are some numbers comparing alternatives (last one is FastUtil's RadixSort
   * which is not really an alternative since it can't have associated non-sorted
   * integer value but listed here for comparison.

     numEntries = 100, iterations = 10000

     Java HotSpot(TM) 64-Bit Server VM 1.8.0_131-b11 on Linux 4.4.0-21-generic
     Intel(R) Core(TM) i7-5600U CPU @ 2.60GHz

     sort comparison     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
     -----------------------------------------------------------------------------
     AVL                       0 /    0         24.9          40.2       1.0X
     RB                        0 /    0         17.4          57.6       0.7X
     SparkRadixSort            0 /    0         40.6          24.6       1.6X
     RadixSort                 0 /    0         53.8          18.6       2.2X

     numEntries = 10000, iterations = 1000

     sort comparison     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
     ----------------------------------------------------------------------------
     AVL                       2 /    2          5.3         189.4       1.0X
     RB                        2 /    2          5.8         173.3       1.1X
     SparkRadixSort            0 /    0         82.3          12.2      15.6X
     RadixSort                 1 /    1         16.6          60.1       3.2X
   */
  private[this] var positionsArray: Array[Long] = _
  private[this] var positions: LongArray = _
  private[this] var positionIndex: Int = _
  private[this] var realEncoder: ColumnEncoder = _
  private[this] var maxSize: Int = _
  private[this] var previousDelta: ColumnFormatValue = _

  override def typeId: Int = realEncoder.typeId

  override def supports(dataType: DataType): Boolean = realEncoder.supports(dataType)

  override def nullCount: Int = realEncoder.nullCount

  override def isNullable: Boolean = realEncoder.isNullable

  override protected[encoding] def getNumNullWords: Int = realEncoder.getNumNullWords

  override protected[encoding] def writeNulls(columnBytes: AnyRef, cursor: Long,
      numWords: Int): Long = realEncoder.writeNulls(columnBytes, cursor, numWords)

  /**
   * Initialize this ColumnDeltaEncoder.
   *
   * @param dataType   DataType of the field to be written
   * @param nullable   True if the field can have nulls else false
   * @param batchSize  current size of the original column value
   * @param withHeader ignored
   * @param allocator  the [[BufferAllocator]] to use for the data
   * @return initial position of the cursor that caller must use to write
   */
  override def initialize(dataType: DataType, nullable: Boolean, batchSize: Int,
      withHeader: Boolean, allocator: BufferAllocator): Long = {
    if (batchSize < ColumnDelta.INIT_SIZE && hierarchyDepth > 0) {
      throw new IllegalStateException(
        s"Batch size = $batchSize too small for hierarchy depth = $hierarchyDepth")
    }
    // max expected hierarchy depth of 3
    val deltaRatio = batchSize.toDouble / ColumnDelta.INIT_SIZE.toDouble
    val hierarchyFactor = ColumnDelta.MAX_DEPTH match {
      case 1 => deltaRatio
      case 2 => math.sqrt(deltaRatio)
      case 3 => math.cbrt(deltaRatio)
      case d => throw new IllegalStateException(
        s"Configured hierarchy = $d too deep and can impact queries")
    }
    // calculate the maximum size at this hierarchy depth
    maxSize = hierarchyDepth match {
      case 0 => ColumnDelta.INIT_SIZE
      case 1 => (ColumnDelta.INIT_SIZE * hierarchyFactor).toInt
      case 2 => (ColumnDelta.INIT_SIZE * hierarchyFactor * hierarchyFactor).toInt
      case d => throw new IllegalStateException(
        s"Configured hierarchy depth (0 based) = $d exceeds max = ${ColumnDelta.MAX_DEPTH}")
    }
    this.dataType = dataType
    this.allocator = allocator
    // double the actual space is required by Spark's radix sort
    positionsArray = new Array[Long](maxSize * 2)
    positions = new LongArray(MemoryBlock.fromLongArray(positionsArray))
    realEncoder = ColumnEncoding.getColumnEncoder(dataType, nullable)
    realEncoder.initialize(dataType, nullable, maxSize, withHeader, allocator)
  }

  def setExistingDelta(delta: ColumnFormatValue): Unit = {
    if (delta ne null) {
      setSource(delta.getBufferRetain, releaseOld = true)
    }
    previousDelta = delta
  }

  override protected[encoding] def initializeNulls(initSize: Int): Int =
    realEncoder.initializeNulls(initSize)

  def setUpdatePosition(position: Int): Unit = {
    // sorted on LSB so position goes in LSB
    positionsArray(positionIndex) = position.toLong | (positionIndex.toLong << 32L)
    positionIndex += 1
  }

  override def writeIsNull(ordinal: Int): Unit = realEncoder.writeIsNull(ordinal)

  override def writeBoolean(cursor: Long, value: Boolean): Long =
    realEncoder.writeBoolean(cursor, value)

  override def writeByte(cursor: Long, value: Byte): Long =
    realEncoder.writeByte(cursor, value)

  override def writeShort(cursor: Long, value: Short): Long =
    realEncoder.writeShort(cursor, value)

  override def writeInt(cursor: Long, value: Int): Long =
    realEncoder.writeInt(cursor, value)

  override def writeLong(cursor: Long, value: Long): Long =
    realEncoder.writeLong(cursor, value)

  override def writeFloat(cursor: Long, value: Float): Long =
    realEncoder.writeFloat(cursor, value)

  override def writeDouble(cursor: Long, value: Double): Long =
    realEncoder.writeDouble(cursor, value)

  override def writeLongDecimal(cursor: Long, value: Decimal,
      ordinal: Int, precision: Int, scale: Int): Long =
    realEncoder.writeLongDecimal(cursor, value, ordinal, precision, scale)

  override def writeDecimal(cursor: Long, value: Decimal,
      ordinal: Int, precision: Int, scale: Int): Long =
    realEncoder.writeDecimal(cursor, value, ordinal, precision, scale)

  override def writeDate(cursor: Long, value: Int): Long =
    realEncoder.writeDate(cursor, value)

  override def writeTimestamp(cursor: Long, value: Long): Long =
    realEncoder.writeTimestamp(cursor, value)

  override def writeInterval(cursor: Long, value: CalendarInterval): Long =
    realEncoder.writeInterval(cursor, value)

  override def writeUTF8String(cursor: Long, value: UTF8String): Long =
    realEncoder.writeUTF8String(cursor, value)

  override def writeBinary(cursor: Long, value: Array[Byte]): Long =
    realEncoder.writeBinary(cursor, value)

  override def writeUnsafeData(cursor: Long, baseObject: AnyRef,
      baseOffset: Long, numBytes: Int): Long =
    realEncoder.writeUnsafeData(cursor, baseObject, baseOffset, numBytes)

  override def finish(cursor: Long): ByteBuffer = {
    // re-order the values in the full column value position order
    /*
    val decoder: ColumnDecoder = realEncoder.getRandomAccessDecoder
    // make space for the positions at the start
    val buffer = storageAllocator.allocate(4 + (positions.size() << 2) +
        realEncoder.finalSize)
    val columnBytes = realEncoder.columnBytes
    realEncoder.setSource(buffer, releaseOld = false)
    var cursor = realEncoder.columnBeginPosition
    val readAndEncode = dataType match {
      case StringType => position: Int =>
        val s = decoder.readUTF8String(columnBytes, position)
        cursor = realEncoder.writeUTF8String(cursor, s)
    }
    reorderValues(readAndEncode)
    */
    realEncoder.finish(cursor)
  }

  private def reorderAndMergeValues(readAndEncode: Long => Unit): Unit = {
    // sort on the last 4 bytes i.e. the position in full column batch;
    // check for special case of single delta
    val startIndex = if (positionIndex == 1) 0
    else RadixSort.sort(positions, positionIndex, 0, 3, false, false)
    val endIndex = startIndex + positionIndex
    var i = startIndex
    // check if there is an existing delta into which this has to be merged
    val prevDeltaBytes = columnBytes
    if (prevDeltaBytes ne null) {
      // simple two-way merge
      val deltaDecoder: ColumnDecoder = null // TODO: SW:
      val prevDeltaStart = columnBeginPosition
      val prevDeltaEnd = columnEndPosition
      var cursor = prevDeltaStart
      try {
        setSource(storageAllocator.allocateForStorage(ColumnEncoding.checkBufferSize(
          4 + (positions.size() << 2) + realEncoder.reuseUsedSize +
              (prevDeltaEnd - prevDeltaStart))), releaseOld = true) // TODO: SW: encodedSize)
        val numPositions = ColumnEncoding.readInt(columnBytes, cursor)
        cursor += 4

        var i1 = i
        var i2 = 0
        var position1 = positionsArray(i1)
        var position2 = ColumnEncoding.readInt(columnBytes, cursor)
        cursor += 4
        while (i1 < endIndex && i2 < numPositions) {
          if (position1 > position2) {
            // consume position2 and move it
            
          } else if (position1 < position2) {
            
          } else {
            // the two are equal; consume the more recent current delta
            // and increment both
          }
          i2 += 1
        }

      } finally {
        previousDelta.release()
      }
    } else {
      // make space for the positions at the start
      setSource(storageAllocator.allocateForStorage(4 + (positions.size().toInt << 2) +
          realEncoder.reuseUsedSize), releaseOld = true) // TODO: SW: encodedSize)
      // write the positions first
      var cursor = columnBeginPosition
      while (i < endIndex) {
        val position = (positionsArray(i) & 0xffffffffL).toInt
        ColumnEncoding.writeInt(columnBytes, cursor, position)
        cursor += 4
        i += 1
      }
      // now the values
      while (i < endIndex) {
        val position = positionsArray(i) & 0xffffffffL
        readAndEncode(position)
        i += 1
      }
    }
  }
}
