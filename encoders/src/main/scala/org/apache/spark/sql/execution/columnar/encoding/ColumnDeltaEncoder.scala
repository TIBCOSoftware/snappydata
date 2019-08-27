/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

import java.nio.{ByteBuffer, ByteOrder}

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.gemstone.gemfire.internal.shared.BufferAllocator
import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.spark.Logging
import org.codehaus.janino.CompilerFactory
import org.apache.spark.sql.catalyst.util.{SerializedArray, SerializedMap, SerializedRow}
import org.apache.spark.sql.collection.SharedUtils
import org.apache.spark.sql.execution.columnar.impl.{ColumnDelta, ColumnFormatValue}
import org.apache.spark.sql.sources.JdbcExtendedUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

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
   * [sumedh] No longer explicitly sorting positions rather assuming
   * sorted at the SparkPlan level itself (using outputOrdering/requiredChildOrdering)
   * =================================================================================
   * ========================  old comments for reference  ===========================
   * =================================================================================
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
  /**
   * The position array is maintained as a raw ByteBuffer with native ByteOrder so
   * that ByteBuffer.get/put and Unsafe access via Platform class are equivalent.
   */
  private[this] var positionsArray: ByteBuffer = _

  /**
   * Relative index of the current delta i.e. 1st delta is 0, 2nd delta is 1 and so on.
   * so on. Initialized to -1 so that pre-increment in write initializes to 0 and the
   * current positionIndex can be used in writeIsNull.
   */
  private[this] var positionIndex: Int = -1
  private[this] var realEncoder: ColumnEncoder = _
  private[this] var dataOffset: Long = _
  private[this] var maxSize: Int = _

  private[this] def bufferOwner: String = "DELTA_ENCODER"

  override def typeId: Int = realEncoder.typeId

  override def supports(dataType: DataType): Boolean = realEncoder.supports(dataType)

  override def nullCount: Int = realEncoder.nullCount

  override def isNullable: Boolean = realEncoder.isNullable

  override protected[sql] def getNumNullWords: Int = realEncoder.getNumNullWords

  override protected[sql] def writeNulls(columnBytes: AnyRef, cursor: Long,
      numWords: Int): Long = realEncoder.writeNulls(columnBytes, cursor, numWords)

  private[this] def allocatePositions(size: Int): Unit = {
    positionsArray = allocator.allocate(size, bufferOwner).order(ByteOrder.nativeOrder())
    allocator.clearPostAllocate(positionsArray, 0)
  }

  override def initialize(dataType: DataType, nullable: Boolean, initSize: Int,
      withHeader: Boolean, allocator: BufferAllocator, minBufferSize: Int = -1): Long = {
    if (initSize < 4 || (initSize < ColumnDelta.INIT_SIZE && hierarchyDepth > 0)) {
      throw new IllegalStateException(
        s"Batch size = $initSize too small for hierarchy depth = $hierarchyDepth")
    }
    this.dataType = dataType
    this.allocator = allocator
    this.maxSize = initSize
    allocatePositions(initSize << 2)
    realEncoder = ColumnEncoding.getColumnEncoder(dataType, nullable)
    val cursor = realEncoder.initialize(dataType, nullable,
      initSize, withHeader, allocator, minBufferSize)
    dataOffset = realEncoder.offset(cursor)
    cursor
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

  private[this] def baseSetPosition(index: Int, value: Int): Unit = {
    positionsArray.putInt(index << 2, value)
  }

  def setUpdatePosition(position: Int): Unit = {
    // sorted on LSB so position goes in LSB
    positionIndex += 1
    if (positionIndex >= maxSize) {
      // expand by currentSize / 2
      val expandBy = maxSize >> 1
      val limit = positionsArray.limit()
      // expand will ensure ByteOrder to be same
      positionsArray = allocator.expand(positionsArray, expandBy << 2, bufferOwner)
      assert(positionsArray.order() == ByteOrder.nativeOrder())
      allocator.clearPostAllocate(positionsArray, limit)
      maxSize += expandBy
    }
    baseSetPosition(positionIndex, position)
  }

  def getRealEncoder: ColumnEncoder = realEncoder

  override def writeIsNull(position: Int): Unit = {
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
      position: Int, precision: Int, scale: Int): Long =
    realEncoder.writeLongDecimal(cursor, value, position, precision, scale)

  override def writeDecimal(cursor: Long, value: Decimal,
      position: Int, precision: Int, scale: Int): Long =
    realEncoder.writeDecimal(cursor, value, position, precision, scale)

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

  override private[sql] def close(releaseData: Boolean): Unit = {
    realEncoder.close(releaseData)
  }

  private def clearPositions(): Unit = {
    if (positionsArray ne null) {
      BufferAllocator.releaseBuffer(positionsArray)
      positionsArray = null
    }
  }

  private def consumeDecoder(decoder: ColumnDecoder, decoderNullPosition: Int,
      decoderBytes: AnyRef, writer: DeltaWriter, encoderCursor: Long,
      encoderPosition: Int, doWrite: Boolean = true): Long = {
    if (decoderNullPosition >= 0) {
      // nulls are always written as per relative position in decoder
      if (decoder.isNullAt(decoderBytes, decoderNullPosition)) {
        // null words are copied as is in initial creation so only write in merge
        if (doWrite) realEncoder.writeIsNull(encoderPosition)
        return encoderCursor
      }
    }
    writer.readAndEncode(decoder, decoderBytes, realEncoder, encoderCursor,
      encoderPosition, doWrite)
  }

  private def writeHeader(columnBytes: AnyRef, cursor: Long, numNullWords: Int,
      numBaseRows: Int, positions: ByteBuffer, numDeltas: Int): Long = {
    var deltaCursor = cursor
    // typeId
    ColumnEncoding.writeInt(columnBytes, deltaCursor, typeId)
    deltaCursor += 4
    // number of nulls
    ColumnEncoding.writeInt(columnBytes, deltaCursor, numNullWords << 3)
    deltaCursor += 4
    // write the null bytes
    deltaCursor = writeNulls(columnBytes, deltaCursor, numNullWords)

    // write the number of base column rows to help in merging and creating
    // next hierarchy deltas if required
    ColumnEncoding.writeInt(columnBytes, deltaCursor, numBaseRows)
    deltaCursor += 4

    // write the positions next
    ColumnEncoding.writeInt(columnBytes, deltaCursor, numDeltas)
    deltaCursor += 4
    positions.rewind()
    val positionsObj = allocator.baseObject(positions)
    var posCursor = allocator.baseOffset(positions)
    val posCursorEnd = posCursor + (numDeltas << 2)
    while (posCursor < posCursorEnd) {
      ColumnEncoding.writeInt(columnBytes, deltaCursor, Platform.getInt(positionsObj, posCursor))
      deltaCursor += 4
      posCursor += 4
    }
    // pad to nearest word boundary before writing encoded data
    ((deltaCursor + 7) >> 3) << 3
  }

  private[this] var tmpNumBaseRows: Int = _
  private[this] var tmpNumPositions: Int = _
  private[this] var tmpPositionCursor: Long = _

  private def initializeDecoder(columnBytes: AnyRef, cursor: Long): Long = {
    // read the number of base rows
    tmpNumBaseRows = ColumnEncoding.readInt(columnBytes, cursor)
    // read the positions
    tmpNumPositions = ColumnEncoding.readInt(columnBytes, cursor + 4)
    tmpPositionCursor = cursor + 8
    val positionEndCursor = tmpPositionCursor + (tmpNumPositions << 2)
    // round to nearest word to get data start position
    ((positionEndCursor + 7) >> 3) << 3
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

    dataType = JdbcExtendedUtils.getSQLDataType(field.dataType)
    setAllocator(GemFireCacheImpl.getCurrentBufferAllocator)

    // Simple two-way merge with duplicate elimination. If current delta has small number
    // of values say a couple then it might seem that merging using insertion sort
    // will be faster, but for integers/longs even merging one element into 50 elements
    // turns out to be faster than lookup in micro-benchmarks due to sequential
    // reads vs random reads (merging into 50 elements requires ~7 random lookups
    //   per element for binary search vs merging requires ~50 sequential reads/compares).

    // new delta is on the "left" while the previous delta in the table is on "right"
    val (decoder1, columnBytes1) = ColumnEncoding.getColumnDecoderAndBuffer(
      newValue, field, initializeDecoder)
    val nullable1 = decoder1.hasNulls
    val numBaseRows = tmpNumBaseRows
    val numPositions1 = tmpNumPositions
    var positionCursor1 = tmpPositionCursor
    val newValueSize = newValue.remaining()

    val (decoder2, columnBytes2) = if (existingIsDelta) {
      ColumnEncoding.getColumnDecoderAndBuffer(
        existingValue, field, initializeDecoder)
    } else {
      ColumnEncoding.getColumnDecoderAndBuffer(
        existingValue, field, ColumnEncoding.identityLong)
    }
    val nullable2 = decoder2.hasNulls
    var numPositions2 = 0
    var positionCursor2 = 0L
    val existingValueSize = existingValue.remaining()

    val nullable = field.nullable && (decoder1.hasNulls || decoder2.hasNulls)
    realEncoder = ColumnEncoding.getColumnEncoder(dataType, nullable)
    // Set the source of encoder with an upper limit for bytes that also avoids
    // checking buffer limit when writing position integers.
    // realEncoder will contain the intermediate and final merged and encoded delta.
    // The final exact size is not known yet due to possible duplicates,
    // so one copy of the encoded bytes has to be made. An alternative could be
    // to do one traversal to determine duplicates and get final size but that
    // would be overall slower than making one extra flat byte array copy.
    var cursor = realEncoder.initialize(dataType, nullable, ColumnDelta.INIT_SIZE,
      withHeader = false, allocator, newValueSize + existingValueSize)

    val writer = DeltaWriter(dataType)
    // merge and write the sorted positions and corresponding values
    // the positions are written to this encoder if the end result is a delta
    if (existingIsDelta) {
      positionIndex = 0
      numPositions2 = tmpNumPositions
      positionCursor2 = tmpPositionCursor
      maxSize = numPositions1 + numPositions2
      allocatePositions(maxSize << 2)
    } else {
      positionIndex = 0
      maxSize = 0
      clearPositions()
    }

    var position1 = ColumnEncoding.readInt(columnBytes1, positionCursor1)
    positionCursor1 += 4
    var position2 = 0
    if (existingIsDelta) {
      position2 = ColumnEncoding.readInt(columnBytes2, positionCursor2)
      positionCursor2 += 4
    }
    var relativePosition1 = 0
    var relativePosition2 = 0
    var encoderPosition = -1

    var doProcess = numPositions1 > 0 && numPositions2 > 0
    while (doProcess) {
      encoderPosition += 1
      val areEqual = position1 == position2
      val isGreater = position1 > position2
      if (isGreater || areEqual) {
        // set next update position to be from second
        if (existingIsDelta && !areEqual) baseSetPosition(encoderPosition, position2)
        // consume data at position2 and move it if position2 is smaller
        // else if they are equal then newValue gets precedence
        cursor = consumeDecoder(decoder2, if (nullable2) relativePosition2 else -1,
          columnBytes2, writer, cursor, encoderPosition, doWrite = !areEqual)
        relativePosition2 += 1
        if (relativePosition2 < numPositions2) {
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
      // if the two are equal then keep the more recent delta from first
      // write for the second was skipped in the first block above
      if (!isGreater) {
        // set next update position to be from first
        if (existingIsDelta) baseSetPosition(encoderPosition, position1)
        // consume data at position1 and move it
        cursor = consumeDecoder(decoder1, if (nullable1) relativePosition1 else -1,
          columnBytes1, writer, cursor, encoderPosition)
        relativePosition1 += 1
        if (relativePosition1 < numPositions1) {
          position1 = ColumnEncoding.readInt(columnBytes1, positionCursor1)
          positionCursor1 += 4
        } else {
          doProcess = false
        }
      }
    }
    // consume any remaining (slight inefficiency of reading first positions again
    //   but doing that for code clarity)
    positionCursor1 -= 4
    while (relativePosition1 < numPositions1) {
      encoderPosition += 1
      // set next update position to be from first
      if (existingIsDelta) {
        baseSetPosition(encoderPosition, ColumnEncoding.readInt(columnBytes1, positionCursor1))
        positionCursor1 += 4
      }
      cursor = consumeDecoder(decoder1, if (nullable1) relativePosition1 else -1,
        columnBytes1, writer, cursor, encoderPosition)
      relativePosition1 += 1
    }
    positionCursor2 -= 4
    while (relativePosition2 < numPositions2) {
      encoderPosition += 1
      // set next update position to be from second
      if (existingIsDelta) {
        baseSetPosition(encoderPosition, ColumnEncoding.readInt(columnBytes2, positionCursor2))
        positionCursor2 += 4
      }
      cursor = consumeDecoder(decoder2, if (nullable2) relativePosition2 else -1,
        columnBytes2, writer, cursor, encoderPosition)
      relativePosition2 += 1
    }

    // write any remaining bytes in encoder
    cursor = realEncoder.flushWithoutFinish(cursor)

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

    assert(cursor <= realEncoder.columnEndPosition)

    val numElements = encoderPosition + 1
    val positionsSize = if (existingIsDelta) {
      4 /* numBaseRows */ + 4 /* numPositions */ + (numElements << 2)
    } else 0
    val buffer = allocator.allocateForStorage(ColumnEncoding.checkBufferSize((((8L +
        // round positions to nearest word as done by writeHeader; for the non-delta case,
        // positionsSize is zero so total header is already rounded to word boundary
        (numNullWords << 3) /* header */ + positionsSize + 7) >> 3) << 3) +
        realEncoder.encodedSize(cursor, deltaStart)))
    realEncoder.setSource(buffer, releaseOld = false)

    val columnBytes = allocator.baseObject(buffer)
    cursor = allocator.baseOffset(buffer)
    // write the header including positions
    cursor = writeHeader(columnBytes, cursor, numNullWords, numBaseRows,
      positionsArray, numElements)

    // write any internal structures (e.g. dictionary)
    cursor = writeInternals(columnBytes, cursor)

    // cursor returned above may not be actual cursor into the data (SNAP-2054)
    cursor = realEncoder.columnBeginPosition + realEncoder.offset(cursor)
    assert(cursor + deltaSize == realEncoder.columnEndPosition)

    // finally copy the entire encoded data
    Platform.copyMemory(encodedBytes, deltaStart, columnBytes, cursor, deltaSize)

    // release the intermediate buffers
    allocator.release(encodedData)
    clearPositions()
    close(releaseData = false)

    buffer
  }

  override def finish(encoderCursor: Long): ByteBuffer = {
    throw new UnsupportedOperationException(
      "ColumnDeltaEncoder.finish(cursor) not expected to be called")
  }

  def finish(encoderCursor: Long, numBaseRows: Int): ByteBuffer = {
    val numDeltas = positionIndex + 1
    // write any remaining bytes in encoder
    val dataEndPosition = realEncoder.flushWithoutFinish(encoderCursor)
    val dataBeginPosition = realEncoder.columnBeginPosition + dataOffset
    val dataSize = dataEndPosition - dataBeginPosition
    val dataBuffer = realEncoder.buffer
    val dataColumnBytes = realEncoder.columnData
    val dataAllocator = realEncoder.allocator
    val allocator = this.storageAllocator

    // make space for the positions at the start
    val numNullWords = realEncoder.getNumNullWords
    val buffer = allocator.allocateForStorage(ColumnEncoding.checkBufferSize((((8L +
        (numNullWords << 3) /* header */ +
        // round positions to nearest word as done by writeHeader
        4 /* numBaseRows */ + 4 /* numPositions */ + (numDeltas << 2) + 7) >> 3) << 3) +
        realEncoder.encodedSize(encoderCursor, dataBeginPosition)))
    realEncoder.setSource(buffer, releaseOld = false)
    val columnBytes = allocator.baseObject(buffer)
    var cursor = allocator.baseOffset(buffer)
    // write the header including positions
    cursor = writeHeader(columnBytes, cursor, numNullWords, numBaseRows,
      positionsArray, numDeltas)

    // write any internal structures (e.g. dictionary)
    cursor = realEncoder.writeInternals(columnBytes, cursor)

    // cursor returned above may not be actual cursor into the data (SNAP-2054)
    val cursorOffset = realEncoder.offset(cursor)
    cursor = realEncoder.columnBeginPosition + cursorOffset

    // copy the encoded data
    if (cursor + dataSize != realEncoder.columnEndPosition) {
      throw new AssertionError("Unexpected data size mismatch." +
          s"Begin position = ${realEncoder.columnBeginPosition}, " +
          s"End position = ${realEncoder.columnEndPosition}, " +
          s"Data size = $dataSize, Cursor offset = $cursorOffset")
    }
    Platform.copyMemory(dataBuffer, dataBeginPosition, columnBytes, cursor, dataSize)

    // release the old buffers
    dataAllocator.release(dataColumnBytes)
    clearPositions()
    // clear the encoder
    close(releaseData = false)
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
      destEncoder: ColumnEncoder, destCursor: Long, encoderPosition: Int,
      doWrite: Boolean): Long
}

/**
 * Factory for [[DeltaWriter]] used by code generation to enable using the
 * simpler Janino "createFastEvaluator" API.
 */
trait DeltaWriterFactory {
  def create(): DeltaWriter
}

object DeltaWriter extends Logging {

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
             |      ColumnEncoder destEncoder, long destCursor, int encoderPosition,
             |      boolean doWrite) {
             |    srcDecoder.nonNullPosition_$$eq(srcDecoder.nonNullPosition() + 1);
             |    return doWrite ? destEncoder.write$name(destCursor, srcDecoder.read$name(
             |        srcColumnBytes, srcDecoder.nonNullPosition())) : destCursor;
             |  }
             |};
          """.stripMargin
        } else {
          s"""
             |return new DeltaWriter() {
             |  @Override
             |  public long readAndEncode(ColumnDecoder srcDecoder, Object srcColumnBytes,
             |      ColumnEncoder destEncoder, long destCursor, int encoderPosition,
             |      boolean doWrite) {
             |    srcDecoder.nonNullPosition_$$eq(srcDecoder.nonNullPosition() + 1);
             |    if (doWrite) {
             |      $complexType data = ($complexType)srcDecoder.read$name(srcColumnBytes,
             |         srcDecoder.nonNullPosition());
             |      return destEncoder.writeUnsafeData(destCursor, data.getBaseObject(),
             |         data.getBaseOffset(), data.getSizeInBytes());
             |    } else {
             |      return destCursor;
             |    }
             |  }
             |};
          """.stripMargin
        }

        logDebug(
          s"DEBUG: Generated DeltaWriter for type $dataType, code=$expression")
        evaluator.createFastEvaluator(expression, classOf[DeltaWriterFactory],
          SharedUtils.EMPTY_STRING_ARRAY).asInstanceOf[DeltaWriterFactory]
      }
    })

  def apply(dataType: DataType): DeltaWriter = JdbcExtendedUtils.getSQLDataType(dataType) match {
    case StringType => new DeltaWriter {
      override def readAndEncode(srcDecoder: ColumnDecoder, srcColumnBytes: AnyRef,
          destEncoder: ColumnEncoder, destCursor: Long, encoderPosition: Int,
          doWrite: Boolean): Long = {
        srcDecoder.nonNullPosition += 1
        if (doWrite) {
          destEncoder.writeUTF8String(destCursor, srcDecoder.readUTF8String(
            srcColumnBytes, srcDecoder.nonNullPosition))
        } else destCursor
      }
    }
    case d: DecimalType if d.precision <= Decimal.MAX_LONG_DIGITS => new DeltaWriter {
      override def readAndEncode(srcDecoder: ColumnDecoder, srcColumnBytes: AnyRef,
          destEncoder: ColumnEncoder, destCursor: Long, encoderPosition: Int,
          doWrite: Boolean): Long = {
        srcDecoder.nonNullPosition += 1
        if (doWrite) {
          destEncoder.writeLongDecimal(destCursor, srcDecoder.readLongDecimal(
            srcColumnBytes, d.precision, d.scale, srcDecoder.nonNullPosition),
            encoderPosition, d.precision, d.scale)
        } else destCursor
      }
    }
    case d: DecimalType => new DeltaWriter {
      override def readAndEncode(srcDecoder: ColumnDecoder, srcColumnBytes: AnyRef,
          destEncoder: ColumnEncoder, destCursor: Long, encoderPosition: Int,
          doWrite: Boolean): Long = {
        srcDecoder.nonNullPosition += 1
        if (doWrite) {
          destEncoder.writeDecimal(destCursor, srcDecoder.readDecimal(
            srcColumnBytes, d.precision, d.scale, srcDecoder.nonNullPosition),
            encoderPosition, d.precision, d.scale)
        } else destCursor
      }
    }
    case _ => cache.get(dataType).create()
  }
}