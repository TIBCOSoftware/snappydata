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
import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.codehaus.janino.CompilerFactory

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.util.{SerializedArray, SerializedMap, SerializedRow}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatValue
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
 *   |   |   |  .------------- Internal structures of encoding (like dictionary)
 *   |   |   |  |   .--------- Encoded non-null elements
 *   |   |   |  |  |
 *   |   |   |  |  |      .--- Position in full column value followed
 *   V   V   V  V  V      V    by encoded value
 *   +---+---+--+--+----------------+
 *   |   |   |  |  | v1 | v2 |  ... |
 *   +---+---+-----+----------------+
 *    \-----/ \--------------------/
 *     header           body
 * }}}
 *
 * Whether the value type is a delta or not is determined by the "deltaHierarchy"
 * field in [[ColumnFormatValue]] and the negative columnIndex in ColumnFormatKey.
 * Encoding typeId itself does not store anything for it separately.
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
  /**
   * Relative index of the current delta i.e. 1st delta is 0, 2nd delta is 1 and so on.
   * so on. Initialized to -1 so that pre-increment in write initializes to 0 and the
   * current positionIndex can be used in writeIsNull.
   */
  private[this] var positionIndex: Int = -1
  private[this] var realEncoder: ColumnEncoder = _
  private[this] var dataBeginPosition: Long = _
  private[this] var maxSize: Int = _
  private[this] var previousDelta: ColumnFormatValue = _

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
    positionsArray = new Array[Long](maxSize << 1)
    positions = new LongArray(MemoryBlock.fromLongArray(positionsArray))
    realEncoder = ColumnEncoding.getColumnEncoder(dataType, nullable)
    dataBeginPosition = realEncoder.initialize(dataType, nullable,
      maxSize, withHeader, allocator)
    dataBeginPosition
  }

  override def writeInternals(columnBytes: AnyRef, cursor: Long): Long =
    realEncoder.writeInternals(columnBytes, cursor)

  def setExistingDelta(delta: ColumnFormatValue): Unit = {
    if (delta ne null) {
      setSource(delta.getBufferRetain, releaseOld = true)
      previousDelta = delta
    } else {
      clearSource(0, releaseData = true)
      previousDelta = null
    }
  }

  override protected[sql] def initializeNulls(initSize: Int): Int =
    realEncoder.initializeNulls(initSize)

  override private[sql] def decoderBeforeFinish: ColumnDecoder =
    throw new UnsupportedOperationException(s"decoderBeforeFinish for $toString")

  override protected def initializeNullsBeforeFinish(decoder: ColumnDecoder): Long =
    throw new UnsupportedOperationException(s"initializeNullsBeforeFinish for $toString")

  override def finishedSize(cursor: Long, columnBeginPosition: Long): Long =
    throw new UnsupportedOperationException(s"finishedSize for $toString")

  def setUpdatePosition(position: Int): Unit = {
    // sorted on LSB so position goes in LSB
    positionIndex += 1
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

  private def consumeDecoder(decoder: ColumnDecoder,
      decoderAbsolutePosition: Int /* for random access */ ,
      decoderOrdinal: Int /* for sequential access */ ,
      decoderBytes: AnyRef, writer: DeltaWriter, encoderCursor: Long,
      encoderOrdinal: Int, forMerge: Boolean, doWrite: Boolean = true): Long = {
    assert(doWrite || decoderAbsolutePosition < 0)
    val isNull = if (decoderAbsolutePosition < 0) {
      decoder.notNull(decoderBytes, decoderOrdinal) == 0
    } else decoder.isNullAt(decoderBytes, decoderAbsolutePosition)
    if (isNull) {
      if (doWrite) realEncoder.writeIsNull(encoderOrdinal)
      encoderCursor
    } else {
      writer.readAndEncode(decoder, decoderBytes, decoderAbsolutePosition,
        realEncoder, encoderCursor, encoderOrdinal, forMerge, doWrite)
    }
  }

  private def writeHeader(columnBytes: AnyRef, cursor: Long,
      numNullWords: Int, numDeltas: Int): Long = {
    var deltaCursor = cursor
    // typeId
    ColumnEncoding.writeInt(columnBytes, deltaCursor, typeId)
    deltaCursor += 4
    // number of nulls
    ColumnEncoding.writeInt(columnBytes, deltaCursor, numNullWords << 3)
    deltaCursor += 4
    // write the null bytes
    deltaCursor = writeNulls(columnBytes, deltaCursor, numNullWords)
    // write any internal structures
    deltaCursor = writeInternals(columnBytes, deltaCursor)

    // write the number of elements
    ColumnEncoding.writeInt(columnBytes, deltaCursor, numDeltas)
    deltaCursor + 4
  }

  override def finish(encoderCursor: Long): ByteBuffer = {
    val writer = DeltaWriter(dataType)
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
    val startIndex = if (numDeltas <= 1) 0
    else RadixSort.sort(positions, numDeltas, 0, 3, false, false)
    val endIndex = startIndex + numDeltas
    val decoder = realEncoder.decoderBeforeFinish
    val decoderBuffer = realEncoder.buffer
    val allocator = this.storageAllocator
    // check if there is an existing delta into which this has to be merged
    if (previousDelta ne null) {
      // Simple two-way merge with duplicate elimination. If current delta has small number
      // of values say a couple then it might seem that merging using insertion sort
      // will be faster, but for integers/longs even merging one element into 50 elements
      // turns out to be faster than lookup in micro-benchmarks due to sequential
      // reads vs random reads (merging into 50 elements requires ~7 random lookups
      //   per element for binary search vs merging requires ~50 sequential reads/compares).
      try {
        // temporary StructField for the decoder interface
        val field = StructField("DELTA", dataType)
        // new delta (on the "left") is in realEncoder in "unfinished" form while
        // the previous delta in the table is in "finished" form and set into this
        // ColumnDeltaEncoder itself, so the decoder for the new delta is obtained
        // using "decoderBeforeFinish" above while a regular decoder is used for
        // the current delta in the table
        val columnBytes2 = this.columnBytes
        var decoderCursor2 = columnBeginPosition
        val decoder2 = ColumnEncoding.getColumnDecoder(columnBytes2, decoderCursor2, field)
        decoderCursor2 = decoder2.initialize(columnBytes2, decoderCursor2, field)
        // the numDeltas should be immediately after the header + internals
        val numDeltas2 = ColumnEncoding.readInt(columnBytes2, decoderCursor2)
        decoderCursor2 += 4

        // Set the source of encoder with an upper limit for bytes that also avoids
        // checking buffer limit when writing position integers.
        // realEncoder will now contain the intermediate and final encoded delta
        // while decoder will continue running on previous data so "releaseold = false".
        realEncoder.setSource(allocator.allocateForStorage(ColumnEncoding.checkBufferSize(
          realEncoder.finishedSize(encoderCursor, dataBeginPosition) +
              (columnEndPosition - decoderCursor2) +
              ((numDeltas + numDeltas2 + 1) << 2))), releaseOld = false)

        // merge and write the sorted positions and corresponding values
        var columnBytes = this.columnBytes
        var cursor = realEncoder.columnBeginPosition
        var index1 = startIndex
        var pos1 = positionsArray(index1)
        var position1 = (pos1 & 0xffffffffL).toInt
        var position2 = ColumnEncoding.readInt(columnBytes2, decoderCursor2)
        var relativePosition2 = 0
        var encoderOrdinal = -1
        decoderCursor2 += 4
        writer.decoderCursor = decoderCursor2

        var doProcess = index1 < endIndex && relativePosition2 < numDeltas2
        while (doProcess) {
          encoderOrdinal += 1
          if (position1 > position2) {
            // consume data at position2 and move it
            ColumnEncoding.writeInt(realEncoder.columnBytes, cursor, position2)
            cursor = consumeDecoder(decoder2, decoderAbsolutePosition = -1, relativePosition2,
              columnBytes2, writer, cursor + 4, encoderOrdinal, forMerge = true)
            relativePosition2 += 1
            if (relativePosition2 < numDeltas2) {
              position2 = ColumnEncoding.readInt(columnBytes, writer.decoderCursor)
              writer.decoderCursor += 4
            } else {
              doProcess = false
            }
          } else {
            // consume data at position1 and move it
            ColumnEncoding.writeInt(realEncoder.columnBytes, cursor, position1)
            // the left side "unfinished" delta needs random access reads since it has
            // been RadixSorted and not written to another buffer in order yet
            cursor = consumeDecoder(decoder, (pos1 >> 32).toInt, decoderOrdinal = -1,
              decoderBuffer, writer, cursor + 4, encoderOrdinal, forMerge = true)
            // if the two are equal then keep the more recent delta from first
            // and skip the second
            if (position1 == position2) {
              cursor = consumeDecoder(decoder2, decoderAbsolutePosition = -1, relativePosition2,
                columnBytes2, writer, cursor + 4, encoderOrdinal,
                forMerge = true, doWrite = false)
              relativePosition2 += 1
              if (relativePosition2 < numDeltas2) {
                position2 = ColumnEncoding.readInt(columnBytes, writer.decoderCursor)
                writer.decoderCursor += 4
              } else {
                doProcess = false
              }
            }
            index1 += 1
            if (index1 < endIndex) {
              pos1 = positionsArray(index1)
              position1 = (pos1 & 0xffffffffL).toInt
            } else {
              doProcess = false
            }
          }
        }
        // check remaining from second
        if (relativePosition2 < numDeltas2) {
          doProcess = true
          while (doProcess) {
            encoderOrdinal += 1
            ColumnEncoding.writeInt(realEncoder.columnBytes, cursor, position2)
            cursor = consumeDecoder(decoder2, decoderAbsolutePosition = -1, relativePosition2,
              columnBytes2, writer, cursor + 4, encoderOrdinal, forMerge = true)
            relativePosition2 += 1
            if (relativePosition2 < numDeltas2) {
              position2 = ColumnEncoding.readInt(columnBytes, writer.decoderCursor)
              writer.decoderCursor += 4
            } else {
              doProcess = false
            }
          }
        }
        // check remaining from first
        else if (index1 < endIndex) {
          doProcess = true
          while (doProcess) {
            encoderOrdinal += 1
            ColumnEncoding.writeInt(realEncoder.columnBytes, cursor, position1)
            cursor = consumeDecoder(decoder, (pos1 >> 32).toInt, decoderOrdinal = -1,
              decoderBuffer, writer, cursor + 4, encoderOrdinal, forMerge = true)
            index1 += 1
            if (index1 < endIndex) {
              pos1 = positionsArray(index1)
              position1 = (pos1 & 0xffffffffL).toInt
            } else {
              doProcess = false
            }
          }
        }

        // now the final delta size and layout is known after duplicate
        // elimination above, so create the final buffer, add header and copy
        // the encoded data above
        val encodedData = realEncoder.columnData
        val encodedBytes = realEncoder.columnBytes
        val numNullWords = realEncoder.getNumNullWords
        val deltaStart = realEncoder.columnBeginPosition
        val deltaSize = cursor - deltaStart

        var buffer = allocator.allocateForStorage(ColumnEncoding.checkBufferSize(
          realEncoder.finishedSize(encoderCursor, dataBeginPosition) +
              4 /* numPositions */))
        realEncoder.setSource(buffer, releaseOld = false)

        columnBytes = allocator.baseObject(buffer)
        cursor = allocator.baseOffset(buffer)
        // write header and internal structures (e.g. dictionary)
        cursor = writeHeader(columnBytes, cursor, numNullWords, encoderOrdinal + 1)

        // finally copy the entire encoded data i.e. list of (position + encoded value)
        Platform.copyMemory(encodedBytes, deltaStart, columnBytes, cursor, deltaSize)

        // release the intermediate buffer
        allocator.release(encodedData)
        buffer = realEncoder.columnData
        realEncoder.clearSource(0, releaseData = false)

        buffer
      } finally {
        previousDelta.release()
      }
    } else {
      // make space for the positions at the start
      val numUpdates = positions.size().toInt
      val numNullWords = realEncoder.getNumNullWords
      val buffer = allocator.allocateForStorage(ColumnEncoding.checkBufferSize(
        realEncoder.finishedSize(encoderCursor, dataBeginPosition) +
            4 /* numPositions */ + (numUpdates << 2)))
      realEncoder.setSource(buffer, releaseOld = false)
      val columnBytes = allocator.baseObject(buffer)
      var cursor = allocator.baseOffset(buffer)
      // write header and internal structures (e.g. dictionary)
      cursor = writeHeader(columnBytes, cursor, numNullWords, numUpdates)

      // write the position in full column, followed by the value
      var i = startIndex
      while (i < endIndex) {
        val position = positionsArray(i)
        ColumnEncoding.writeInt(columnBytes, cursor, (position & 0xffffffffL).toInt)
        cursor += 4
        val decoderPosition = (position >> 32L).toInt
        cursor = consumeDecoder(decoder, decoderPosition, decoderOrdinal = -1,
          decoderBuffer, writer, cursor, i - startIndex, forMerge = false)
        i += 1
      }
      // clear the encoder
      realEncoder.clearSource(0, releaseData = false)
      buffer
    }
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

  final var decoderCursor: Long = _

  /**
   * Read a value (type will be determined by implementation) from given source
   * and write to the destination delta encoder.
   *
   * The "forMerge" flag will be true if two sorted delta values are being merged
   * (in which case the writer may decide to re-encode everything) or if a new
   * delta is just being re-ordered (in which case the writer may decide to change
   * minimally. For example in dictionary encoding it will create new dictionary
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
  private val cache = CacheBuilder.newBuilder().build(
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
             |      decoderCursor = srcDecoder.next$name(srcColumnBytes, decoderCursor);
             |      return doWrite ? destEncoder.write$name(destCursor,
             |        srcDecoder.read$name(srcColumnBytes, decoderCursor)) : destCursor;
             |    } else {
             |      return destEncoder.write$name(destCursor, srcDecoder.read$name(
             |        srcColumnBytes, srcDecoder.absolute$name(srcColumnBytes, srcPosition)));
             |    }
             |  }
             |}
          """.stripMargin
        } else {
          s"""
             |return new DeltaWriter() {
             |  @Override
             |  public long readAndEncode(ColumnDecoder srcDecoder, Object srcColumnBytes,
             |      int srcPosition, ColumnEncoder destEncoder, long destCursor,
             |      int encoderOrdinal, boolean forMerge, boolean doWrite) {
             |    if (srcPosition < 0) {
             |      decoderCursor = srcDecoder.next$name(srcColumnBytes, decoderCursor);
             |      if (doWrite) {
             |        $complexType data = ($complexType)srcDecoder.read$name(srcColumnBytes,
             |           decoderCursor);
             |        return destEncoder.writeUnsafeData(destCursor, data.getBaseObject(),
             |           data.getBaseOffset(), data.getSizeInBytes());
             |      } else {
             |        return destCursor;
             |      }
             |    } else {
             |      $complexType data = ($complexType)srcDecoder.read$name(srcColumnBytes,
             |        srcDecoder.absolute$name(srcColumnBytes, srcPosition));
             |      return destEncoder.writeUnsafeData(destCursor, data.getBaseObject(),
             |        data.getBaseOffset(), data.getSizeInBytes());
             |    }
             |  }
             |}
          """.stripMargin
        }
        logDebug(s"DEBUG: Generated DeltaWriter for type $dataType, code=$expression")
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
            decoderCursor = srcDecoder.nextUTF8String(srcColumnBytes, decoderCursor)
            if (doWrite) {
              val str = srcDecoder.readUTF8String(srcColumnBytes, decoderCursor)
              destEncoder.writeUTF8String(destCursor, str)
            } else destCursor
          } else {
            destEncoder.writeUTF8String(destCursor, srcDecoder.readUTF8String(
              srcColumnBytes, srcDecoder.absoluteUTF8String(srcColumnBytes, srcPosition)))
          }
        } else {
          // string types currently always use dictionary encoding so when
          // not merging (i.e. sorting single set of deltas), then simply
          // read and write dictionary indexes and leave the dictionary alone
          val index = srcDecoder.readDictionaryIndex(srcColumnBytes,
            srcDecoder.absoluteUTF8String(srcColumnBytes, srcPosition))
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
          decoderCursor = srcDecoder.nextLongDecimal(srcColumnBytes, decoderCursor)
          if (doWrite) {
            destEncoder.writeLongDecimal(destCursor, srcDecoder.readLongDecimal(
              srcColumnBytes, d.precision, d.scale, decoderCursor),
              encoderOrdinal, d.precision, d.scale)
          } else destCursor
        } else {
          destEncoder.writeLongDecimal(destCursor, srcDecoder.readLongDecimal(
            srcColumnBytes, d.precision, d.scale, srcDecoder.absoluteLongDecimal(
              srcColumnBytes, srcPosition)), encoderOrdinal, d.precision, d.scale)
        }
      }
    }
    case d: DecimalType => new DeltaWriter {
      override def readAndEncode(srcDecoder: ColumnDecoder, srcColumnBytes: AnyRef,
          srcPosition: Int, destEncoder: ColumnEncoder, destCursor: Long,
          encoderOrdinal: Int, forMerge: Boolean, doWrite: Boolean): Long = {
        if (srcPosition < 0) {
          decoderCursor = srcDecoder.nextDecimal(srcColumnBytes, decoderCursor)
          if (doWrite) {
            destEncoder.writeDecimal(destCursor, srcDecoder.readDecimal(
              srcColumnBytes, d.precision, d.scale, decoderCursor),
              encoderOrdinal, d.precision, d.scale)
          } else destCursor
        } else {
          destEncoder.writeDecimal(destCursor, srcDecoder.readDecimal(
            srcColumnBytes, d.precision, d.scale, srcDecoder.absoluteDecimal(
              srcColumnBytes, srcPosition)), encoderOrdinal, d.precision, d.scale)
        }
      }
    }
    case _ => cache.get(dataType).create()
  }
}

object ColumnDelta {

  /**
   * The initial size of delta column (the smallest delta in the hierarchy).
   */
  val INIT_SIZE = 100

  /**
   * The maximum depth of the hierarchy of deltas for column starting with
   * smallest delta, which is merged with larger delta, then larger, ...
   * till the full column value.
   */
  val MAX_DEPTH = 3
}
