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

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.gemstone.gemfire.internal.shared.BufferAllocator
import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.codehaus.janino.CompilerFactory

import org.apache.spark.sql.catalyst.util.{SerializedArray, SerializedMap, SerializedRow}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.impl.{ColumnDelta, ColumnFormatValue}
import org.apache.spark.sql.store.CodeGeneration
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.LongArray
import org.apache.spark.unsafe.memory.MemoryBlock
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.collection.unsafe.sort.RadixSort

/**
 * Encodes a delta value for a [[ColumnFormatValue]] obtained after an
 * update operation that can change one or more values. This applies
 * the update in an optimized batch manner as far as possible.
 *
 * The format of delta encoding is straightforward and adds the positions
 * in the full column in addition to the normal column encoding. So the layout
 * looks like below:
 * {{{
 *    .----------------------- Base encoding scheme (4 bytes)
 *   |    .------------------- Null bitset size as number of longs N (4 bytes)
 *   |   |
 *   |   |   .---------------- Null bitset longs (8 x N bytes,
 *   |   |   |                                    empty if null count is zero)
 *   |   |   |  .------------- Positions in full column value
 *   |   |   |  |
 *   |   |   |  |    .-------- Encoded non-null elements
 *   |   |   |  |    |
 *   V   V   V  V    V
 *   +---+---+--+--- +--------------+
 *   |   |   |  |    |   ...   ...  |
 *   +---+---+--+----+--------------+
 *    \-----/ \--------------------/
 *     header           body
 * }}}
 *
 * Whether the value type is a delta or not is determined by the "deltaHierarchy"
 * field in [[ColumnFormatValue]] and the negative columnIndex in ColumnFormatKey.
 * Encoding typeId itself does not store anything for it separately.
 *
 * An alternative could be storing the position before each encoded element
 * but it will not work properly for schemes like run-length encoding that
 * will not write anything if elements are in that current run-length.
 *
 * A set of new updated column values results in the merge of those values
 * with the existing encoded values held in the current delta with smallest
 * hierarchy depth (i.e. one that has a maximum size of 100).
 * Each delta can grow to a limit after which it is subsumed in a larger delta
 * of bigger size thus creating a hierarchy of deltas. So base delta will go
 * till 100 entries or so, then the next higher level one will go till say 1000 entries
 * and so on till the full [[ColumnFormatValue]] size is attained. This design
 * attempts to minimize disk writes at the cost of some scan overhead for
 * columns that see a large number of updates. The hierarchy is expected to be
 * small not more than 3-4 levels to get a good balance between write overhead
 * and scan overhead.
 */
final class ColumnDeltaEncoder(val hierarchyDepth: Int) extends ColumnEncoder {

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
  /**
   * Relative index of the current delta i.e. 1st delta is 0, 2nd delta is 1 and so on.
   * so on. Initialized to -1 so that pre-increment in write initializes to 0 and the
   * current positionIndex can be used in writeIsNull.
   */
  private[this] var positionIndex: Int = -1
  private[this] var realEncoder: ColumnEncoder = _
  private[this] var dataBeginPosition: Long = _
  private[this] var maxSize: Int = _

  override def typeId: Int = realEncoder.typeId

  override def supports(dataType: DataType): Boolean = realEncoder.supports(dataType)

  override def nullCount: Int = realEncoder.nullCount

  override def isNullable: Boolean = realEncoder.isNullable

  override protected[sql] def getNumNullWords: Int = realEncoder.getNumNullWords

  override protected[sql] def writeNulls(columnBytes: AnyRef, cursor: Long,
      numWords: Int): Long = realEncoder.writeNulls(columnBytes, cursor, numWords)

  /**
   * Initialize this ColumnDeltaEncoder.
   *
   * @param dataType   DataType of the field to be written
   * @param nullable   True if the field can have nulls else false
   * @param initSize   initial number of columns to accomodate in the delta
   * @param withHeader ignored
   * @param allocator  the [[BufferAllocator]] to use for the data
   * @return initial position of the cursor that caller must use to write
   */
  override def initialize(dataType: DataType, nullable: Boolean, initSize: Int,
      withHeader: Boolean, allocator: BufferAllocator): Long = {
    if (initSize < 4 || (initSize < ColumnDelta.INIT_SIZE && hierarchyDepth > 0)) {
      throw new IllegalStateException(
        s"Batch size = $initSize too small for hierarchy depth = $hierarchyDepth")
    }
    this.dataType = dataType
    this.allocator = allocator
    this.maxSize = initSize
    // double the actual space is required by Spark's radix sort
    positionsArray = new Array[Long](initSize << 1)
    realEncoder = ColumnEncoding.getColumnEncoder(dataType, nullable)
    dataBeginPosition = realEncoder.initialize(dataType, nullable,
      initSize, withHeader, allocator)
    dataBeginPosition
  }

  override def writeInternals(columnBytes: AnyRef, cursor: Long): Long =
    realEncoder.writeInternals(columnBytes, cursor)

  def getMaxSizeForHierarchy(numColumnRows: Int): Int = {
    // max expected hierarchy depth of 3
    val deltaRatio = numColumnRows.toDouble / ColumnDelta.INIT_SIZE.toDouble
    lazy val hierarchyFactor = ColumnDelta.MAX_DEPTH match {
      case 1 => deltaRatio
      case 2 => math.sqrt(deltaRatio)
      case 3 => math.cbrt(deltaRatio)
      case d => throw new IllegalStateException(
        s"Configured hierarchy = $d too deep and can impact queries")
    }
    // calculate the maximum size at this hierarchy depth
    hierarchyDepth match {
      case 0 => ColumnDelta.INIT_SIZE
      case 1 => (ColumnDelta.INIT_SIZE * hierarchyFactor).toInt
      case 2 => (ColumnDelta.INIT_SIZE * hierarchyFactor * hierarchyFactor).toInt
      case -1 => Int.MaxValue
      case d => throw new IllegalStateException(
        s"Configured hierarchy depth (0 based) = $d exceeds max = ${ColumnDelta.MAX_DEPTH}")
    }
  }

  override protected[sql] def initializeNulls(initSize: Int): Int =
    realEncoder.initializeNulls(initSize)

  override private[sql] def decoderBeforeFinish: ColumnDecoder =
    throw new UnsupportedOperationException(s"decoderBeforeFinish for $toString")

  override protected def initializeNullsBeforeFinish(decoder: ColumnDecoder): Long =
    throw new UnsupportedOperationException(s"initializeNullsBeforeFinish for $toString")

  def setUpdatePosition(position: Int): Unit = {
    // sorted on LSB so position goes in LSB
    positionIndex += 1
    if (positionIndex == maxSize) {
      // double the size and array is twice that for Spark's RadixSort
      val newPositionsArray = new Array[Long](maxSize << 2)
      System.arraycopy(positionsArray, 0, newPositionsArray, 0, maxSize)
      maxSize <<= 1
      positionsArray = newPositionsArray
    }
    positionsArray(positionIndex) = position.toLong | (positionIndex.toLong << 32L)
  }

  override def writeIsNull(ordinal: Int): Unit = {
    // write the relative position in the delta rather than absolute position
    // to save quite a bit of space
    realEncoder.writeIsNull(positionIndex)
  }

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

  override def flushWithoutFinish(cursor: Long): Long = realEncoder.flushWithoutFinish(cursor)

  private def consumeDecoder(decoder: ColumnDecoder,
      decoderAbsolutePosition: Int /* for random access */ ,
      decoderOrdinal: Int /* for sequential access */ ,
      decoderBytes: AnyRef, writer: DeltaWriter, encoderCursor: Long,
      encoderOrdinal: Int, forMerge: Boolean, doWrite: Boolean = true): Long = {
    assert(doWrite || decoderAbsolutePosition < 0)
    val isNull = if (decoderAbsolutePosition < 0) {
      decoder.isNull(decoderBytes, decoderOrdinal, mutated = 0) == 0
    } else decoder.isNullAt(decoderBytes, decoderAbsolutePosition)
    if (isNull) {
      if (doWrite) realEncoder.writeIsNull(encoderOrdinal)
      encoderCursor
    } else {
      writer.readAndEncode(decoder, decoderBytes, decoderAbsolutePosition,
        realEncoder, encoderCursor, encoderOrdinal, forMerge, doWrite)
    }
  }

  private def writeHeader(columnBytes: AnyRef, cursor: Long, numNullWords: Int,
      positions: Array[Long], startIndex: Int, endIndex: Int): Long = {
    var deltaCursor = cursor
    // typeId
    ColumnEncoding.writeInt(columnBytes, deltaCursor, typeId)
    deltaCursor += 4
    // number of nulls
    ColumnEncoding.writeInt(columnBytes, deltaCursor, numNullWords << 3)
    deltaCursor += 4
    // write the null bytes
    deltaCursor = writeNulls(columnBytes, deltaCursor, numNullWords)

    // write the positions next
    if (positions ne null) {
      ColumnEncoding.writeInt(columnBytes, deltaCursor, endIndex - startIndex)
      deltaCursor += 4
      var i = startIndex
      while (i < endIndex) {
        ColumnEncoding.writeInt(columnBytes, deltaCursor, (positions(i) & 0xffffffffL).toInt)
        deltaCursor += 4
        i += 1
      }
      // pad to nearest word boundary before writing encoded data
      ((deltaCursor + 7) >> 3) << 3
    } else deltaCursor
  }

  def merge(newValue: ByteBuffer, existingValue: ByteBuffer,
      existingIsDelta: Boolean, field: StructField): ByteBuffer = {
    // TODO: PERF: delta encoder should create a "merged" dictionary i.e. having
    // only elements beyond the main dictionary so that the overall decoder can be
    // dictionary enabled. As of now delta decoder does not have an overall dictionary
    // so it cannot make use of existing dictionary optimizations (group by, hash join).
    // To do this the main dictionary will need to dump the ByteBufferHashMap.keyData
    // (also required for dictionary filter optimization), then this with index positions
    // can be explicitly compressed to reduce memory overhead, and then the
    // main dictionary can be loaded during updated encoding. Finally the dictionary
    // can also be sorted with positions rewritten accordingly for dictionary
    // to be useful for even range filters.
    // Also during delta merges, can merge dictionaries separately then rewrite only indexes.

    dataType = field.dataType
    allocator = GemFireCacheImpl.getCurrentBufferAllocator
    realEncoder = ColumnEncoding.getColumnEncoder(dataType, field.nullable)

    // Simple two-way merge with duplicate elimination. If current delta has small number
    // of values say a couple then it might seem that merging using insertion sort
    // will be faster, but for integers/longs even merging one element into 50 elements
    // turns out to be faster than lookup in micro-benchmarks due to sequential
    // reads vs random reads (merging into 50 elements requires ~7 random lookups
    //   per element for binary search vs merging requires ~50 sequential reads/compares).

    // new delta is on the "left" while the previous delta in the table is on "right"
    val (decoder1, columnBytes1, offset1) = ColumnEncoding.getColumnDecoderAndBuffer(
      newValue, field)
    var decoderCursor1 = decoder1.initializeNulls(columnBytes1, offset1, field)
    // read the positions
    val numPositions1 = ColumnEncoding.readInt(columnBytes1, decoderCursor1)
    decoderCursor1 += 4
    var positionCursor1 = decoderCursor1
    decoderCursor1 += (numPositions1 << 2)
    // align to word boundary then read internals etc
    decoderCursor1 = ((decoderCursor1 + 63) >> 6) << 6
    decoderCursor1 = decoder1.initializeCursor(columnBytes1, decoderCursor1, field)
    val newValueSize = newValue.remaining()
    val endOffset1 = offset1 + newValueSize

    val (decoder2, columnBytes2, offset2) = ColumnEncoding.getColumnDecoderAndBuffer(
      existingValue, field)
    var decoderCursor2 = 0L
    var numPositions2 = 0
    var positionCursor2 = 0L
    if (existingIsDelta) {
      decoderCursor2 = decoder2.initializeNulls(columnBytes2, offset2, field)
      // read the positions
      numPositions2 = ColumnEncoding.readInt(columnBytes2, decoderCursor2)
      decoderCursor2 += 4
      positionCursor2 = decoderCursor2
      decoderCursor2 += (numPositions2 << 2)
      // align to word boundary then read internals etc
      decoderCursor2 = ((decoderCursor2 + 63) >> 6) << 6
      decoderCursor2 = decoder2.initializeCursor(columnBytes2, decoderCursor2, field)
    } else {
      decoderCursor2 = decoder2.initialize(columnBytes2, offset2, field)
    }
    val existingValueSize = existingValue.remaining()
    val endOffset2 = offset2 + existingValueSize

    // Set the source of encoder with an upper limit for bytes that also avoids
    // checking buffer limit when writing position integers.
    // realEncoder will contain the intermediate and final merged and encoded delta.
    // The final exact size is not known yet due to possible duplicates,
    // so one copy of the encoded bytes has to be made. An alternative could be
    // to do one traversal to determine duplicates and get final size but that
    // would be overall slower than making one extra flat byte array copy.
    realEncoder.setSource(allocator.allocateForStorage(ColumnEncoding.checkBufferSize(
      newValueSize + existingValueSize)), releaseOld = false)

    val writer = DeltaWriter(dataType)
    // merge and write the sorted positions and corresponding values;
    var cursor = realEncoder.columnBeginPosition
    // the positions are written to this encoder if the end result is a delta
    if (existingIsDelta) {
      positionIndex = 0
      maxSize = numPositions1 + numPositions2
      positionsArray = new Array[Long](maxSize)
    } else {
      positionIndex = 0
      maxSize = 0
      positionsArray = null
    }

    var position1 = ColumnEncoding.readInt(columnBytes1, positionCursor1)
    positionCursor1 += 4
    decoder1.currentCursor = decoderCursor1
    var position2 = 0
    if (existingIsDelta) {
      position2 = ColumnEncoding.readInt(columnBytes2, positionCursor2)
      positionCursor2 += 4
    }
    decoder2.currentCursor = decoderCursor2
    var relativePosition1 = 0
    var relativePosition2 = 0
    var encoderOrdinal = -1

    var doProcess = decoderCursor1 < endOffset1 && decoderCursor2 < endOffset2
    while (doProcess) {
      encoderOrdinal += 1
      if (position1 > position2) {
        // set next update position to be from second
        if (existingIsDelta) positionsArray(encoderOrdinal) = position2
        // consume data at position2 and move it
        cursor = consumeDecoder(decoder2, decoderAbsolutePosition = -1, relativePosition2,
          columnBytes2, writer, cursor, encoderOrdinal, forMerge = true)
        relativePosition2 += 1
        if (decoder2.currentCursor < endOffset2) {
          if (existingIsDelta) {
            position2 = ColumnEncoding.readInt(columnBytes2, positionCursor2)
            positionCursor2 += 4
          } else {
            position2 += 1
          }
        } else {
          doProcess = false
        }
      } else {
        // set next update position to be from first
        if (existingIsDelta) positionsArray(encoderOrdinal) = position1
        // consume data at position1 and move it
        cursor = consumeDecoder(decoder1, decoderAbsolutePosition = -1, relativePosition1,
          columnBytes1, writer, cursor, encoderOrdinal, forMerge = true)
        // if the two are equal then keep the more recent delta from first
        // and skip the second
        if (position1 == position2) {
          positionCursor2 += 4
          cursor = consumeDecoder(decoder2, decoderAbsolutePosition = -1, relativePosition2,
            columnBytes2, writer, cursor, encoderOrdinal,
            forMerge = true, doWrite = false)
          relativePosition2 += 1
          if (decoder2.currentCursor < endOffset2) {
            if (existingIsDelta) {
              position2 = ColumnEncoding.readInt(columnBytes2, positionCursor2)
              positionCursor2 += 4
            } else {
              position2 += 1
            }
          } else {
            doProcess = false
          }
        }
        relativePosition1 += 1
        if (decoder1.currentCursor < endOffset1) {
          position1 = ColumnEncoding.readInt(columnBytes1, positionCursor1)
          positionCursor1 += 4
        } else {
          doProcess = false
        }
      }
    }
    // consume any remaining (slight inefficiency of reading first positions again
    //   but doing that for code clarity)
    while (decoder1.currentCursor < endOffset1) {
      encoderOrdinal += 1
      // set next update position to be from first
      if (existingIsDelta) {
        positionsArray(encoderOrdinal) = ColumnEncoding.readInt(columnBytes1, positionCursor1)
        positionCursor1 += 4
      }
      cursor = consumeDecoder(decoder1, decoderAbsolutePosition = -1, relativePosition1,
        columnBytes1, writer, cursor, encoderOrdinal, forMerge = true)
      relativePosition1 += 1
    }
    while (decoder2.currentCursor < endOffset2) {
      encoderOrdinal += 1
      // set next update position to be from second
      if (existingIsDelta) {
        positionsArray(encoderOrdinal) = ColumnEncoding.readInt(columnBytes2, positionCursor2)
        positionCursor2 += 4
      }
      cursor = consumeDecoder(decoder2, decoderAbsolutePosition = -1, relativePosition2,
        columnBytes2, writer, cursor, encoderOrdinal, forMerge = true)
      relativePosition2 += 1
    }

    // check that all positions have been consumed
    if (relativePosition1 != numPositions1) {
      throw new IllegalStateException("BUG: failed to consume required left-side deltas: " +
          s"consumed=$relativePosition1 total=$numPositions1")
    }
    if (existingIsDelta && relativePosition2 != numPositions2) {
      throw new IllegalStateException("BUG: failed to consume required right-side deltas: " +
          s"consumed=$relativePosition2 total=$numPositions2")
    }

    // now the final delta size and layout is known after duplicate
    // elimination above, so create the final buffer, add header and copy
    // the encoded data above
    val encodedData = realEncoder.columnData
    val encodedBytes = realEncoder.columnBytes
    val numNullWords = realEncoder.getNumNullWords
    val deltaStart = realEncoder.columnBeginPosition
    val deltaSize = cursor - deltaStart

    val numElements = encoderOrdinal + 1
    val positionsSize = if (existingIsDelta) {
      // round positions to nearest word as done by writeHeader
      ((4 /* numPositions */ + (numElements << 2) + 63) >> 6) << 6
    } else 0
    var buffer = allocator.allocateForStorage(ColumnEncoding.checkBufferSize(8L +
        (numNullWords << 3) /* header */ + positionsSize +
        realEncoder.encodedSize(cursor, deltaStart)))
    realEncoder.setSource(buffer, releaseOld = false)

    val columnBytes = allocator.baseObject(buffer)
    cursor = allocator.baseOffset(buffer)
    // write the header including positions
    cursor = writeHeader(columnBytes, cursor, numNullWords,
      positionsArray, 0, numElements)

    // write any internal structures (e.g. dictionary)
    cursor = writeInternals(columnBytes, cursor)

    // finally copy the entire encoded data i.e. list of (position + encoded value)
    Platform.copyMemory(encodedBytes, deltaStart, columnBytes, cursor, deltaSize)

    // release the intermediate buffer
    allocator.release(encodedData)
    buffer = realEncoder.columnData
    realEncoder.clearSource(0, releaseData = false)

    buffer
  }

  override def finish(encoderCursor: Long): ByteBuffer = {
    val writer = DeltaWriter(dataType)
    // TODO: PERF: this implementation can potentially be changed to use code
    // generation to embed the DeltaWriter code assuming it can have a good
    // performance improvement. The gains with that need to be proven conclusively
    // since its possible that JVM is able to inline the implementation itself
    // after a small number of calls. The major downside with code generation for the
    // whole method will be major loss of code readability, so needs very conclusive
    // evidence to go that route.

    // sort on the last 4 bytes i.e. the position in full column batch
    // check for special case of single delta
    val numDeltas = positionIndex + 1
    val positions = new LongArray(MemoryBlock.fromLongArray(positionsArray))
    val startIndex = if (numDeltas <= 1) 0
    else RadixSort.sort(positions, numDeltas, 0, 3, false, false)
    val endIndex = startIndex + numDeltas
    val decoder = realEncoder.decoderBeforeFinish
    val decoderBuffer = realEncoder.buffer
    val allocator = this.storageAllocator

    // make space for the positions at the start
    val numNullWords = realEncoder.getNumNullWords
    val buffer = allocator.allocateForStorage(ColumnEncoding.checkBufferSize((((8L +
        (numNullWords << 3) /* header */ +
        // round positions to nearest word as done by writeHeader
        4 /* numPositions */ + (numDeltas << 2) + 7) >> 3) << 3) +
        realEncoder.encodedSize(encoderCursor, dataBeginPosition)))
    realEncoder.setSource(buffer, releaseOld = false)
    val columnBytes = allocator.baseObject(buffer)
    var cursor = allocator.baseOffset(buffer)
    // write the header including positions
    cursor = writeHeader(columnBytes, cursor, numNullWords,
      positionsArray, startIndex, endIndex)

    // write any internal structures (e.g. dictionary)
    cursor = writeInternals(columnBytes, cursor)

    // write the encoded values
    var i = startIndex
    while (i < endIndex) {
      val position = positionsArray(i)
      val decoderPosition = (position >> 32L).toInt
      cursor = consumeDecoder(decoder, decoderPosition, decoderOrdinal = -1,
        decoderBuffer, writer, cursor, i - startIndex, forMerge = false)
      i += 1
    }
    // finally flush the encoder
    realEncoder.flushWithoutFinish(cursor)

    // clear the encoder
    realEncoder.clearSource(0, releaseData = false)
    buffer
  }
}

/**
 * Trait to read column values from delta encoded column and write to target
 * delta column. The reads may not be sequential and could be random-access reads
 * while writes will be sequential, sorted by position in the full column value.
 * Implementations should not do any null value handling.
 *
 * This uses a separate base class rather than a closure to avoid the overhead
 * of boxing/unboxing with multi-argument closures (>2 arguments).
 */
abstract class DeltaWriter {

  /**
   * Read a value (type will be determined by implementation) from given source
   * and write to the destination delta encoder.
   *
   * The "forMerge" flag will be true if two sorted delta values are being merged
   * (in which case the writer may decide to re-encode everything) or if a new
   * delta is just being re-ordered (in which case the writer may decide to change
   * minimally). For example in dictionary encoding it will create new dictionary
   * with "forMerge" else it will just re-order the dictionary indexes. When that
   * flag is set then reads will be sequential and srcPosition will be invalid.
   *
   * A negative value for "srcPosition" indicates that the reads are sequential
   * else a random access read from that position is required. The flag "doWrite"
   * will skip writing to the target encoder and should be false only for
   * the case of sequential reads (for random access there is nothing to be
   * "skipped" in any case).
   */
  def readAndEncode(srcDecoder: ColumnDecoder, srcColumnBytes: AnyRef,
      srcPosition: Int, destEncoder: ColumnEncoder, destCursor: Long,
      encoderOrdinal: Int, forMerge: Boolean, doWrite: Boolean): Long
}

/**
 * Factory for [[DeltaWriter]] used by code generation to enable using the
 * simpler Janino "createFastEvaluator" API.
 */
trait DeltaWriterFactory {
  def create(): DeltaWriter
}

object DeltaWriter {

  private[this] val defaultImports = Array(
    classOf[UTF8String].getName,
    classOf[Decimal].getName,
    classOf[CalendarInterval].getName,
    classOf[SerializedArray].getName,
    classOf[SerializedMap].getName,
    classOf[SerializedRow].getName,
    classOf[DeltaWriter].getName,
    classOf[ColumnDecoder].getName,
    classOf[ColumnEncoder].getName)

  /**
   * Code generated cache for ease of maintenance of similar DeltaWriter code
   * for most data types.
   */
  private val cache = CacheBuilder.newBuilder().maximumSize(100).build(
    new CacheLoader[DataType, DeltaWriterFactory]() {
      override def load(dataType: DataType): DeltaWriterFactory = {
        val evaluator = new CompilerFactory().newScriptEvaluator()
        evaluator.setClassName("io.snappydata.execute.GeneratedDeltaWriterFactory")
        evaluator.setParentClassLoader(getClass.getClassLoader)
        evaluator.setDefaultImports(defaultImports)

        val (name, complexType) = dataType match {
          case BooleanType => ("Boolean", "")
          case ByteType => ("Byte", "")
          case ShortType => ("Short", "")
          case IntegerType => ("Int", "")
          case LongType => ("Long", "")
          case FloatType => ("Float", "")
          case DoubleType => ("Double", "")
          case DateType => ("Date", "")
          case TimestampType => ("Timestamp", "")
          case CalendarIntervalType => ("Interval", "")
          case BinaryType => ("Binary", "")
          case _: ArrayType => ("Array", "SerializedArray")
          case _: MapType => ("Map", "SerializedMap")
          case _: StructType => ("Struct", "SerializedRow")
          case _ => throw new IllegalArgumentException(
            s"DeltaWriter of $dataType is not code generated")
        }
        val expression = if (complexType.isEmpty) {
          s"""
             |return new DeltaWriter() {
             |  @Override
             |  public long readAndEncode(ColumnDecoder srcDecoder, Object srcColumnBytes,
             |      int srcPosition, ColumnEncoder destEncoder, long destCursor,
             |      int encoderOrdinal, boolean forMerge, boolean doWrite) {
             |    if (srcPosition < 0) {
             |      srcDecoder.currentCursor_$$eq(srcDecoder.next$name(srcColumnBytes,
             |        srcDecoder.currentCursor(), 0));
             |      return doWrite ? destEncoder.write$name(destCursor, srcDecoder.read$name(
             |          srcColumnBytes, srcDecoder.currentCursor(), 0)) : destCursor;
             |    } else {
             |      return destEncoder.write$name(destCursor, srcDecoder.read$name(
             |          srcColumnBytes, srcDecoder.absolute$name(srcColumnBytes, srcPosition), 0));
             |    }
             |  }
             |};
          """.stripMargin
        } else {
          s"""
             |return new DeltaWriter() {
             |  @Override
             |  public long readAndEncode(ColumnDecoder srcDecoder, Object srcColumnBytes,
             |      int srcPosition, ColumnEncoder destEncoder, long destCursor,
             |      int encoderOrdinal, boolean forMerge, boolean doWrite) {
             |    if (srcPosition < 0) {
             |      srcDecoder.currentCursor_$$eq(srcDecoder.next$name(srcColumnBytes,
             |        srcDecoder.currentCursor(), 0));
             |      if (doWrite) {
             |        $complexType data = ($complexType)srcDecoder.read$name(srcColumnBytes,
             |           srcDecoder.currentCursor(), 0);
             |        return destEncoder.writeUnsafeData(destCursor, data.getBaseObject(),
             |           data.getBaseOffset(), data.getSizeInBytes());
             |      } else {
             |        return destCursor;
             |      }
             |    } else {
             |      $complexType data = ($complexType)srcDecoder.read$name(srcColumnBytes,
             |        srcDecoder.absolute$name(srcColumnBytes, srcPosition), 0);
             |      return destEncoder.writeUnsafeData(destCursor, data.getBaseObject(),
             |        data.getBaseOffset(), data.getSizeInBytes());
             |    }
             |  }
             |};
          """.stripMargin
        }
        CodeGeneration.logInfo(
          s"DEBUG: Generated DeltaWriter for type $dataType, code=$expression")
        evaluator.createFastEvaluator(expression, classOf[DeltaWriterFactory],
          Array.empty[String]).asInstanceOf[DeltaWriterFactory]
      }
    })

  def apply(dataType: DataType): DeltaWriter = Utils.getSQLDataType(dataType) match {
    case StringType => new DeltaWriter {
      override def readAndEncode(srcDecoder: ColumnDecoder, srcColumnBytes: AnyRef,
          srcPosition: Int, destEncoder: ColumnEncoder, destCursor: Long,
          encoderOrdinal: Int, forMerge: Boolean, doWrite: Boolean): Long = {
        if (forMerge) {
          if (srcPosition < 0) {
            srcDecoder.currentCursor = srcDecoder.nextUTF8String(srcColumnBytes,
              srcDecoder.currentCursor, mutated = 0)
            if (doWrite) {
              val str = srcDecoder.readUTF8String(srcColumnBytes, srcDecoder.currentCursor,
                mutated = 0)
              destEncoder.writeUTF8String(destCursor, str)
            } else destCursor
          } else {
            destEncoder.writeUTF8String(destCursor, srcDecoder.readUTF8String(
              srcColumnBytes, srcDecoder.absoluteUTF8String(srcColumnBytes, srcPosition),
              mutated = 0))
          }
        } else {
          // string types currently always use dictionary encoding so when
          // not merging (i.e. sorting single set of deltas), then simply
          // read and write dictionary indexes and leave the dictionary alone
          val index = srcDecoder.readDictionaryIndex(srcColumnBytes,
            srcDecoder.absoluteUTF8String(srcColumnBytes, srcPosition), mutated = 0)
          if (destEncoder.typeId == ColumnEncoding.DICTIONARY_TYPE_ID) {
            ColumnEncoding.writeShort(destEncoder.buffer, destCursor, index.toShort)
            destCursor + 2
          } else {
            ColumnEncoding.writeInt(destEncoder.buffer, destCursor, index)
            destCursor + 4
          }
        }
      }
    }
    case d: DecimalType if d.precision <= Decimal.MAX_LONG_DIGITS => new DeltaWriter {
      override def readAndEncode(srcDecoder: ColumnDecoder, srcColumnBytes: AnyRef,
          srcPosition: Int, destEncoder: ColumnEncoder, destCursor: Long,
          encoderOrdinal: Int, forMerge: Boolean, doWrite: Boolean): Long = {
        if (srcPosition < 0) {
          srcDecoder.currentCursor = srcDecoder.nextLongDecimal(srcColumnBytes,
            srcDecoder.currentCursor, mutated = 0)
          if (doWrite) {
            destEncoder.writeLongDecimal(destCursor, srcDecoder.readLongDecimal(
              srcColumnBytes, d.precision, d.scale, srcDecoder.currentCursor,
              mutated = 0), encoderOrdinal, d.precision, d.scale)
          } else destCursor
        } else {
          destEncoder.writeLongDecimal(destCursor, srcDecoder.readLongDecimal(
            srcColumnBytes, d.precision, d.scale, srcDecoder.absoluteLongDecimal(
              srcColumnBytes, srcPosition), mutated = 0), encoderOrdinal,
            d.precision, d.scale)
        }
      }
    }
    case d: DecimalType => new DeltaWriter {
      override def readAndEncode(srcDecoder: ColumnDecoder, srcColumnBytes: AnyRef,
          srcPosition: Int, destEncoder: ColumnEncoder, destCursor: Long,
          encoderOrdinal: Int, forMerge: Boolean, doWrite: Boolean): Long = {
        if (srcPosition < 0) {
          srcDecoder.currentCursor = srcDecoder.nextDecimal(srcColumnBytes,
            srcDecoder.currentCursor, mutated = 0)
          if (doWrite) {
            destEncoder.writeDecimal(destCursor, srcDecoder.readDecimal(
              srcColumnBytes, d.precision, d.scale, srcDecoder.currentCursor,
              mutated = 0), encoderOrdinal, d.precision, d.scale)
          } else destCursor
        } else {
          destEncoder.writeDecimal(destCursor, srcDecoder.readDecimal(
            srcColumnBytes, d.precision, d.scale, srcDecoder.absoluteDecimal(
              srcColumnBytes, srcPosition), mutated = 0), encoderOrdinal,
            d.precision, d.scale)
        }
      }
    }
    case _ => cache.get(dataType).create()
  }
}

/**
 * Currently just stores the deleted positions in a sorted way. This can be optimized
 * to use a more efficient storage when number of positions is large like
 * a boolean bitset, or use a more comprehensive compression scheme like
 * PFOR (https://github.com/lemire/JavaFastPFOR).
 */
final class ColumnDeleteEncoder extends ColumnEncoder {

  override def typeId: Int = -1

  override def supports(dataType: DataType): Boolean = dataType eq IntegerType

  override def nullCount: Int = 0

  override def isNullable: Boolean = false

  override protected[sql] def getNumNullWords: Int = 0

  override protected[sql] def initializeNulls(initSize: Int): Int =
    throw new UnsupportedOperationException(s"initializeNulls for $toString")

  override protected[sql] def writeNulls(columnBytes: AnyRef, cursor: Long, numWords: Int): Long =
    throw new UnsupportedOperationException(s"decoderBeforeFinish for $toString")

  override protected def initializeNullsBeforeFinish(decoder: ColumnDecoder): Long =
    throw new UnsupportedOperationException(s"initializeNullsBeforeFinish for $toString")

  override private[sql] def decoderBeforeFinish: ColumnDecoder =
    throw new UnsupportedOperationException(s"decoderBeforeFinish for $toString")

  override def writeIsNull(ordinal: Int): Unit =
    throw new UnsupportedOperationException(s"decoderBeforeFinish for $toString")

  private var deletedPositions: Array[Int] = _

  def initialize(initSize: Int): Long = {
    setAllocator(allocator)
    deletedPositions = new Array[Int](math.max(initSize << 2, 16))
    // cursor indicates index into deletedPositions array
    0L
  }

  override def initialize(dataType: DataType, nullable: Boolean, initSize: Int,
      withHeader: Boolean, allocator: BufferAllocator): Long = initialize(initSize)

  override def writeInt(cursor: Long, value: Int): Long = {
    if (cursor >= deletedPositions.length) {
      deletedPositions = java.util.Arrays.copyOf(deletedPositions,
        (deletedPositions.length * 3) >> 1)
    }
    deletedPositions(cursor.toInt) = value
    cursor + 1
  }

  private def createFinalBuffer(numPositions: Long): ByteBuffer = {
    val allocator = storageAllocator
    // add a header of 4 bytes for future use (e.g. format change)
    val bufferSize = (numPositions << 2) + 4
    val buffer = allocator.allocateForStorage(ColumnEncoding.checkBufferSize(bufferSize))
    val bufferBytes = allocator.baseObject(buffer)
    var bufferCursor = allocator.baseOffset(buffer)

    // header for future use
    ColumnEncoding.writeInt(bufferBytes, bufferCursor, 0)
    bufferCursor += 4
    if (ColumnEncoding.littleEndian) {
      // bulk copy if platform endian-ness matches the final format
      Platform.copyMemory(deletedPositions, Platform.INT_ARRAY_OFFSET,
        bufferBytes, bufferCursor, bufferSize)
    } else {
      var index = 0
      while (index < numPositions) {
        ColumnEncoding.writeInt(bufferBytes, bufferCursor, deletedPositions(index))
        bufferCursor += 4
        index += 1
      }
    }
    buffer
  }

  def merge(newValue: ByteBuffer, existingValue: ByteBuffer): ByteBuffer = {
    deletedPositions = new Array[Int](16)
    var position = 0L

    val allocator1 = ColumnEncoding.getAllocator(newValue)
    val columnBytes1 = allocator1.baseObject(newValue)
    var cursor1 = allocator1.baseOffset(newValue) + newValue.position()
    val endOffset1 = cursor1 + newValue.remaining()
    var position1 = ColumnEncoding.readInt(columnBytes1, cursor1)

    val allocator2 = ColumnEncoding.getAllocator(existingValue)
    val columnBytes2 = allocator2.baseObject(existingValue)
    var cursor2 = allocator2.baseOffset(existingValue) + existingValue.position()
    val endOffset2 = cursor2 + existingValue.remaining()
    var position2 = ColumnEncoding.readInt(columnBytes2, cursor2)

    // Simple two-way merge of deleted positions with duplicate elimination.
    var doProcess = cursor1 < endOffset1 && cursor2 < endOffset2
    while (doProcess) {
      if (position1 > position2) {
        // consume position2 and move
        position = writeInt(position, position2)
        cursor2 += 4
        if (cursor2 < endOffset2) {
          position2 = ColumnEncoding.readInt(columnBytes2, cursor2)
        } else {
          doProcess = false
        }
      } else {
        // consume position1 and move
        position = writeInt(position, position1)
        if (position1 == position2) {
          // move position2 without consuming
          cursor2 += 4
          if (cursor2 < endOffset2) {
            position2 = ColumnEncoding.readInt(columnBytes2, cursor2)
          } else {
            doProcess = false
          }
        }
        cursor1 += 4
        if (cursor1 < endOffset1) {
          position1 = ColumnEncoding.readInt(columnBytes1, cursor1)
        } else {
          doProcess = false
        }
      }
    }
    // consume any remaining (slight inefficiency of reading first positions again
    //   but doing that for code clarity)
    while (cursor1 < endOffset1) {
      position = writeInt(position, ColumnEncoding.readInt(columnBytes1, cursor1))
      cursor1 += 4
    }
    while (cursor2 < endOffset2) {
      position = writeInt(position, ColumnEncoding.readInt(columnBytes2, cursor2))
      cursor2 += 4
    }

    createFinalBuffer(position)
  }

  override def finish(cursor: Long): ByteBuffer = {
    // sort the deleted positions and create the final storage buffer

    // Spark's RadixSort is the fastest for larger sizes >= 1000. It requires
    // long values and sorting on partial bytes is a bit costly at small sizes.
    // The more common case is sorting of small number of elements where the
    // JDK's standard Arrays.sort is the fastest among those tested
    // (Fastutil's radixSort, quickSort, mergeSort, and Spark's RadixSort)
    if (cursor > 1) java.util.Arrays.sort(deletedPositions)

    createFinalBuffer(cursor)
  }
}
