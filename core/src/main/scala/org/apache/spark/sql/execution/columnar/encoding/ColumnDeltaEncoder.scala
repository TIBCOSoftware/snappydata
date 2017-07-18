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
import org.apache.spark.sql.execution.columnar.impl.{ColumnDelta, ColumnFormatValue}
import org.apache.spark.sql.types._
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
  /**
   * Relative index of the current delta i.e. 1st delta is 0, 2nd delta is 1 and so on.
   * so on. Initialized to -1 so that pre-increment in write initializes to 0 and the
   * current positionIndex can be used in writeIsNull.
   */
  private[this] var positionIndex: Int = -1
  private[this] var realEncoder: ColumnEncoder = _
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
    realEncoder.initialize(dataType, nullable, maxSize, withHeader, allocator)
  }

  override def writeInternals(columnBytes: AnyRef, cursor: Long): Long =
    realEncoder.writeInternals(columnBytes, cursor)

  def setExistingDelta(delta: ColumnFormatValue): Unit = {
    if (delta ne null) {
      setSource(delta.getBufferRetain, releaseOld = true)
    }
    previousDelta = delta
  }

  override protected[sql] def initializeNulls(initSize: Int): Int =
    realEncoder.initializeNulls(initSize)

  override private[sql] def decoderBeforeFinish: ColumnDecoder =
    throw new UnsupportedOperationException(s"decoderBeforeFinish for $toString")

  override protected def initializeNullsBeforeFinish(decoder: ColumnDecoder): Long =
    throw new UnsupportedOperationException(s"initializeNullsBeforeFinish for $toString")

  override def finishedSize(cursor: Long): Long =
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

  private def consumeDecoder(decoder: ColumnDecoder, decoderPosition: Int,
      decoderBuffer: AnyRef, writer: DeltaWriter, cursor: Long, ordinal: Int,
      forMerge: Boolean): Long = {
    // TODO: SW: decoderPosition is passed -1 for decoderCursor case
    if (decoder.isNullAt(decoderBuffer, decoderPosition)) {
      realEncoder.writeIsNull(ordinal)
      cursor
    } else {
      writer.readAndEncode(decoder, decoderBuffer,
        decoderPosition, realEncoder, cursor, ordinal, forMerge)
    }
  }

  override def finish(encoderCursor: Long): ByteBuffer = {
    val writer = DeltaWriter(dataType)
    // TODO: SW: PERF: delta encoder should create a "merged" dictionary i.e. having
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
    // TODO: PERF: reorderAndMergeValues implementation can potentially be changed
    // to use code generation to embed the DeltaWriter code assuming it can have a good
    // performance improvement. The gains with that need to be proven conclusively
    // since its possible that JVM is able to inline the implementation itself
    // after a small number of calls. The major downside will be loss of code readability.

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
    val columnBytes2 = this.columnBytes
    if (columnBytes2 ne null) {
      // Simple two-way merge with duplicate elimination. If current delta has small number
      // of values say a couple then it might seem that merging using insertion sort
      // will be faster, but for integers/longs even merging one element into 50 elements
      // turns out to be faster than lookup in micro-benchmarks due to sequential
      // reads vs random reads (merging into 50 elements requires ~7 random lookups
      //   per element for binary search vs merging requires ~50 sequential reads/compares).
      try {
        val field = StructField("DELTA", dataType)
        var decoderCursor2 = columnBeginPosition
        val decoder2 = ColumnEncoding.getColumnDecoder(columnBytes2, decoderCursor2, field)
        decoderCursor2 = decoder2.initialize(columnBytes2, decoderCursor2, field)
        // the numDeltas should be immediately after the header + internals
        val numDeltas2 = ColumnEncoding.readInt(columnBytes2, decoderCursor2)
        decoderCursor2 += 4

        // set the source of encoder with an upper limit for bytes
        realEncoder.setSource(allocator.allocateForStorage(ColumnEncoding.checkBufferSize(
          realEncoder.finishedSize(encoderCursor) + (columnEndPosition - decoderCursor2) +
              ((numDeltas + numDeltas2) << 2))), releaseOld = false)

        // merge and write the sorted positions and corresponding values
        var i1 = startIndex
        var cursor = realEncoder.columnBeginPosition
        var pos1 = positionsArray(i1)
        var position1 = (pos1 & 0xffffffffL).toInt
        var position2 = ColumnEncoding.readInt(columnBytes2, decoderCursor2)
        var relativePosition2 = 0
        decoderCursor2 += 4
        writer.decoderCursor = decoderCursor2

        var process = i1 < endIndex && relativePosition2 < numDeltas2
        while (process) {
          if (position1 > position2) {
            // consume data at position2 and move it
            ColumnEncoding.writeInt(realEncoder.columnBytes, cursor, position2)
            cursor = consumeDecoder(decoder2, decoderPosition = -1, columnBytes2,
              writer, cursor + 4, relativePosition2, forMerge = true)
            relativePosition2 += 1
            if (relativePosition2 < numDeltas2) {
              position2 = ColumnEncoding.readInt(columnBytes, writer.decoderCursor)
              writer.decoderCursor += 4
            } else {
              process = false
            }
          } else {
            // consume data at position1 and move it
            ColumnEncoding.writeInt(realEncoder.columnBytes, cursor, position1)
            cursor = consumeDecoder(decoder, (pos1 >> 32).toInt, decoderBuffer,
              writer, cursor + 4, i1 - startIndex, forMerge = true)
            // if the two are equal then keep the more recent delta from first
            // and skip the second
            if (position1 == position2) {
              relativePosition2 += 1
              if (relativePosition2 < numDeltas2) {
                position2 = ColumnEncoding.readInt(columnBytes, writer.decoderCursor)
                writer.decoderCursor += 4
              } else {
                process = false
              }
            }
            i1 += 1
            if (i1 < endIndex) {
              pos1 = positionsArray(i1)
              position1 = (pos1 & 0xffffffffL).toInt
            } else {
              process = false
            }
          }
        }
        // check remaining from second
        if (relativePosition2 < numDeltas2) {
          process = true
          while (process) {
            ColumnEncoding.writeInt(realEncoder.columnBytes, cursor, position2)
            cursor = consumeDecoder(decoder2, decoderPosition = -1, columnBytes2,
              writer, cursor + 4, relativePosition2, forMerge = true)
            relativePosition2 += 1
            if (relativePosition2 < numDeltas2) {
              position2 = ColumnEncoding.readInt(columnBytes, writer.decoderCursor)
              writer.decoderCursor += 4
            } else {
              process = false
            }
          }
        }
        // check remaining from first
        else if (i1 < endIndex) {
          process = true
          while (process) {
            ColumnEncoding.writeInt(realEncoder.columnBytes, cursor, position1)
            cursor = consumeDecoder(decoder, (pos1 >> 32).toInt, decoderBuffer,
              writer, cursor + 4, i1 - startIndex, forMerge = true)
            i1 += 1
            if (i1 < endIndex) {
              pos1 = positionsArray(i1)
              position1 = (pos1 & 0xffffffffL).toInt
            } else {
              process = false
            }
          }
        }
        realEncoder.columnData
      } finally {
        previousDelta.release()
      }
    } else {
      // make space for the positions at the start
      val numUpdates = positions.size().toInt
      val buffer = allocator.allocateForStorage(ColumnEncoding.checkBufferSize(
        realEncoder.finishedSize(encoderCursor) +
            4 /* numPositions */ + (numUpdates << 2)))
      realEncoder.setSource(buffer, releaseOld = false)
      val numNullWords = realEncoder.getNumNullWords
      val columnBytes = allocator.baseObject(buffer)
      var cursor = allocator.baseOffset(buffer)
      // typeId
      ColumnEncoding.writeInt(columnBytes, cursor, typeId)
      cursor += 4
      // number of nulls
      ColumnEncoding.writeInt(columnBytes, cursor, numNullWords << 3)
      cursor += 4
      // write the null bytes
      cursor = writeNulls(columnBytes, cursor, numNullWords)
      // write any internal structures
      cursor = writeInternals(columnBytes, cursor)

      // write the number of elements
      ColumnEncoding.writeInt(columnBytes, cursor, numUpdates)
      cursor += 4
      // write the position in full column, followed by the value
      var i = startIndex
      while (i < endIndex) {
        val position = positionsArray(i)
        ColumnEncoding.writeInt(columnBytes, cursor, (position & 0xffffffffL).toInt)
        cursor += 4
        val decoderPosition = (position >> 32L).toInt
        cursor = consumeDecoder(decoder, decoderPosition, decoderBuffer, writer,
          cursor, i - startIndex, forMerge = false)
        i += 1
      }
      buffer
    }
  }
}

/**
 * Trait to read column values from delta encoded column and write to target
 * delta column. The reads will not be sequential rather random-access reads
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
   */
  def readAndEncode(srcDecoder: ColumnDecoder, srcColumnBytes: AnyRef,
      srcPosition: Int, destEncoder: ColumnEncoder, destCursor: Long,
      ordinal: Int, forMerge: Boolean): Long
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
             |      int ordinal, boolean forMerge) {
             |    if (srcPosition < 0) {
             |      decoderCursor = srcDecoder.next$name(srcColumnBytes, decoderCursor);
             |      return destEncoder.write$name(destCursor,
             |        srcDecoder.read$name(srcColumnBytes, decoderCursor));
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
             |      int ordinal, boolean forMerge) {
             |    if (srcPosition < 0) {
             |      decoderCursor = srcDecoder.next$name(srcColumnBytes, decoderCursor);
             |      $complexType data = ($complexType)srcDecoder.read$name(srcColumnBytes,
             |         decoderCursor);
             |      destEncoder.writeUnsafeData(destCursor, data.getBaseObject(),
             |         data.getBaseOffset(), data.getSizeInBytes());
             |    } else {
             |      $complexType data = ($complexType)srcDecoder.read$name(srcColumnBytes,
             |        srcDecoder.absolute$name(srcColumnBytes, srcPosition));
             |      destEncoder.writeUnsafeData(destCursor, data.getBaseObject(),
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
          ordinal: Int, forMerge: Boolean): Long = {
        if (forMerge) {
          if (srcPosition >= 0) {
            destEncoder.writeUTF8String(destCursor, srcDecoder.readUTF8String(
              srcColumnBytes, srcDecoder.absoluteUTF8String(srcColumnBytes, srcPosition)))
          } else {
            decoderCursor = srcDecoder.nextUTF8String(srcColumnBytes, decoderCursor)
            val str = srcDecoder.readUTF8String(srcColumnBytes, decoderCursor)
            destEncoder.writeUTF8String(destCursor, str)
          }
        } else {
          // string types currently always use dictionary encoding
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
          ordinal: Int, forMerge: Boolean): Long = {
        if (srcPosition < 0) {
          decoderCursor = srcDecoder.nextLongDecimal(srcColumnBytes, decoderCursor)
          destEncoder.writeLongDecimal(destCursor, srcDecoder.readLongDecimal(
            srcColumnBytes, d.precision, d.scale, decoderCursor), ordinal, d.precision, d.scale)
        } else {
          destEncoder.writeLongDecimal(destCursor, srcDecoder.readLongDecimal(
            srcColumnBytes, d.precision, d.scale, srcDecoder.absoluteLongDecimal(
              srcColumnBytes, srcPosition)), ordinal, d.precision, d.scale)
        }
      }
    }
    case d: DecimalType => new DeltaWriter {
      override def readAndEncode(srcDecoder: ColumnDecoder, srcColumnBytes: AnyRef,
          srcPosition: Int, destEncoder: ColumnEncoder, destCursor: Long,
          ordinal: Int, forMerge: Boolean): Long = {
        if (srcPosition < 0) {
          decoderCursor = srcDecoder.nextDecimal(srcColumnBytes, decoderCursor)
          destEncoder.writeDecimal(destCursor, srcDecoder.readDecimal(
            srcColumnBytes, d.precision, d.scale, decoderCursor), ordinal, d.precision, d.scale)
        } else {
          destEncoder.writeDecimal(destCursor, srcDecoder.readDecimal(
            srcColumnBytes, d.precision, d.scale, srcDecoder.absoluteDecimal(
              srcColumnBytes, srcPosition)), ordinal, d.precision, d.scale)
        }
      }
    }
    case _ => cache.get(dataType).create()
  }
}
