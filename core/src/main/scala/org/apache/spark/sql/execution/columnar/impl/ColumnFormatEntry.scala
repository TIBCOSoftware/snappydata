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
package org.apache.spark.sql.execution.columnar.impl

import java.io.{DataInput, DataOutput}
import java.nio.ByteBuffer
import java.util.concurrent.locks.LockSupport

import scala.collection.JavaConverters._

import com.gemstone.gemfire.cache.{EntryOperation, Region}
import com.gemstone.gemfire.internal.cache.lru.Sizeable
import com.gemstone.gemfire.internal.cache.partitioned.PREntriesIterator
import com.gemstone.gemfire.internal.cache.store.SerializedBufferData
import com.gemstone.gemfire.internal.cache.{AbstractRegionEntry, GemFireCacheImpl, InternalPartitionResolver}
import com.gemstone.gemfire.internal.shared.{ClientSharedUtils, InputStreamChannel, OutputStreamChannel}
import com.gemstone.gemfire.internal.size.ReflectionSingleObjectSizer.REFERENCE_SIZE
import com.gemstone.gemfire.internal.{DSCODE, DirectByteBufferDataInput, HeapDataOutputStream}
import com.gemstone.gemfire.{DataSerializable, DataSerializer}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.snappy.ColumnBatchKey
import org.slf4j.Logger

import org.apache.spark.Logging
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.encoding.ColumnStatsSchema
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatEntry.alignedSize
import org.apache.spark.sql.execution.columnar.{ColumnBatchIterator, JDBCAppendableRelation}
import org.apache.spark.unsafe.hash.Murmur3_x86_32

/**
 * Utility methods for column format storage keys and values.
 */
object ColumnFormatEntry extends Logging {

  private[columnar] def logger: Logger = log

  /** ClassId for [[ColumnFormatKey]] used by DataSerializable instantiator. */
  val COLUMN_KEY_CLASSID: Byte = 1

  /** ClassId for [[ColumnFormatValue]] used by DataSerializable instantiator. */
  val COLUMN_VALUE_CLASSID: Byte = 2

  /**
   * Size of the serialization type/class IDs of [[ColumnFormatValue]].
   */
  private[columnar] val VALUE_HEADER_CLASSID_SIZE = 2

  /**
   * Size of the serialization header of [[ColumnFormatValue]] including the
   * serialization type/class IDs (2 bytes), padding (2 bytes), and size (4).
   * Padding is added for 8 byte alignment.
   */
  private[columnar] val VALUE_HEADER_SIZE = VALUE_HEADER_CLASSID_SIZE + 2 + 4

  private[columnar] val VALUE_EMPTY_BUFFER = {
    val buffer = ByteBuffer.wrap(new Array[Byte](VALUE_HEADER_SIZE))
    writeValueSerializationHeader(buffer, 0)
    buffer
  }

  private[columnar] def alignedSize(size: Int) = ((size + 7) >>> 3) << 3

  private[columnar] def writeValueSerializationHeader(buffer: ByteBuffer,
      size: Int): Unit = {
    // write the typeId + classId and size
    buffer.put(DSCODE.USER_DATA_SERIALIZABLE)
    buffer.put(ColumnFormatEntry.COLUMN_VALUE_CLASSID)
    buffer.putShort(0) // padding
    buffer.putInt(size) // assume big-endian buffer
  }
}

/**
 * Key object in the column store.
 */
final class ColumnFormatKey(private[columnar] var partitionId: Int,
    private[columnar] var columnIndex: Int,
    private[columnar] var uuid: String)
    extends DataSerializable with ColumnBatchKey {

  // to be used only by deserialization
  def this() = this(-1, -1, "")

  override def getNumColumnsInTable(columnTableName: String): Int = {
    val bufferTable = JDBCAppendableRelation.getTableName(columnTableName)
    Misc.getMemStore.getAllContainers.asScala.find(_.getQualifiedTableName
        .equalsIgnoreCase(bufferTable)).get.getNumColumns - 1
  }

  override def getColumnBatchRowCount(itr: PREntriesIterator[_],
      re: AbstractRegionEntry, numColumnsInTable: Int): Int = {
    val numColumns = numColumnsInTable * ColumnStatsSchema.NUM_STATS_PER_COLUMN
    val currentBucketRegion = itr.getHostedBucketRegion
    if (columnIndex == ColumnBatchIterator.STATROW_COL_INDEX) {
      val value = re.getValue(currentBucketRegion)
          .asInstanceOf[ColumnFormatValue]
      val unsafeRow = Utils.toUnsafeRow(value.getBuffer, numColumns)
      unsafeRow.getInt(ColumnStatsSchema.COUNT_INDEX_IN_SCHEMA)
    } else 0
  }

  def getColumnIndex: Int = columnIndex

  override def hashCode(): Int = Murmur3_x86_32.hashLong(
    (columnIndex.toLong << 32L) | (uuid.hashCode & 0xffffffffL), partitionId)

  override def equals(obj: Any): Boolean = obj match {
    case k: ColumnFormatKey => partitionId == k.partitionId &&
        columnIndex == k.columnIndex && uuid.equals(k.uuid)
    case _ => false
  }

  override def toData(out: DataOutput): Unit = {
    out.writeInt(partitionId)
    out.writeInt(columnIndex)
    DataSerializer.writeString(uuid, out)
  }

  override def fromData(in: DataInput): Unit = {
    partitionId = in.readInt()
    columnIndex = in.readInt()
    uuid = DataSerializer.readString(in)
  }

  override def getSizeInBytes: Int = {
    // UUID is shared among all columns so count its overhead only once
    // in stats row (but the reference overhead will be counted in all)
    if (columnIndex == ColumnBatchIterator.STATROW_COL_INDEX) {
      // first the char[] size inside String
      val charSize = Sizeable.PER_OBJECT_OVERHEAD + 4 /* length */ +
          uuid.length * 2
      val strSize = Sizeable.PER_OBJECT_OVERHEAD + 4 /* hash */ +
          REFERENCE_SIZE /* char[] reference */
      val size = Sizeable.PER_OBJECT_OVERHEAD +
          REFERENCE_SIZE /* String reference */ +
          4 /* columnIndex */ + 4 /* partitionId */
      // align all to 8 bytes assuming 64-bit JVMs
      alignedSize(size) + alignedSize(charSize) + alignedSize(strSize)
    } else {
      // only String reference counted for others
      alignedSize(Sizeable.PER_OBJECT_OVERHEAD +
          REFERENCE_SIZE /* String reference */ +
          4 /* columnIndex */ + 4 /* partitionId */)
    }
  }
}

/**
 * Partition resolver for the column store.
 */
final class ColumnPartitionResolver
    extends InternalPartitionResolver[ColumnFormatKey, ColumnFormatValue] {

  override def getName: String = "ColumnPartitionResolver"

  override def getRoutingObject(opDetails: EntryOperation[ColumnFormatKey,
      ColumnFormatValue]): AnyRef = Int.box(opDetails.getKey.partitionId)

  override def getRoutingObject(key: AnyRef, value: AnyRef,
      callbackArg: AnyRef, region: Region[_, _]): AnyRef = {
    Int.box(key.asInstanceOf[ColumnFormatKey].partitionId)
  }

  override def getDDLString: String =
    s"PARTITIONER '${classOf[ColumnPartitionResolver].getName}'"

  override def close(): Unit = {}
}

/**
 * Value object in the column store simply encapsulates binary data as
 * a ByteBuffer. This can be either a direct buffer (the default) or a
 * heap buffer. The reason for a separate type is to easily store data
 * off-heap without any major changes to engine otherwise as well as
 * efficiently serialize/deserialize them directly to Oplog/socket channels.
 *
 * This class extends [[SerializedBufferData]] to avoid a copy when
 * reading/writing from Oplog. Consequently it has a pre-serialized buffer
 * that has the serialization header at the start (typeID + classID + size).
 */
final class ColumnFormatValue
    extends SerializedBufferData with DataSerializable with Sizeable {

  serializedBuffer = ColumnFormatEntry.VALUE_EMPTY_BUFFER

  def this(buffer: ByteBuffer) = {
    this()
    setBuffer(buffer)
  }

  def setBuffer(buffer: ByteBuffer): Unit = {
    // write the serialization header and move ahead to start of data
    ColumnFormatEntry.writeValueSerializationHeader(buffer,
      buffer.remaining() - ColumnFormatEntry.VALUE_HEADER_SIZE)
    serializedBuffer = buffer
  }

  def getBuffer: ByteBuffer = serializedBuffer

  override def toData(out: DataOutput): Unit = {
    val buffer = serializedBuffer
    val numBytes = buffer.remaining()
    out.writeShort(0) // padding for 8-byte alignment
    out.writeInt(numBytes)
    if (numBytes > 0) {
      out match {
        case channel: OutputStreamChannel =>
          val position = buffer.position()
          do {
            if (channel.write(buffer) == 0) {
              // wait for a bit before retrying
              LockSupport.parkNanos(100L)
            }
          } while (buffer.hasRemaining)
          // rewind to original position
          buffer.position(position)

        case hdos: HeapDataOutputStream =>
          // ColumnFormatEntry.logger.info("SW: toData on hdos", new Throwable)
          val position = buffer.position()
          hdos.write(buffer)
          // rewind to original position
          buffer.position(position)

        case _ =>
          ColumnFormatEntry.logger.info("SW: toData on non-channel, non-hdos", new Throwable)
          out.write(ClientSharedUtils.toBytes(buffer, numBytes, numBytes),
            0, numBytes)
      }
    }
  }

  override def fromData(in: DataInput): Unit = {
    // skip padding
    in.readShort()
    val numBytes = in.readInt()
    if (numBytes > 0) {
      val allocator = GemFireCacheImpl.getCurrentBufferAllocator
      in match {
        case din: DirectByteBufferDataInput =>
          // just transfer the internal buffer; higher layer will take care
          // not to release this buffer (if direct);
          // buffer is already positioned at start of data
          serializedBuffer = din.getInternalBuffer

        case channel: InputStreamChannel =>
          // order is BIG_ENDIAN by default
          val buffer = allocator.allocate(numBytes +
              ColumnFormatEntry.VALUE_HEADER_SIZE)
          ColumnFormatEntry.writeValueSerializationHeader(buffer,
            numBytes)
          val position = buffer.position()
          do {
            if (channel.read(buffer) == 0) {
              // wait for a bit before retrying
              LockSupport.parkNanos(100L)
            }
          } while (buffer.hasRemaining)
          // move to the start of data
          buffer.position(position)
          serializedBuffer = buffer

        case _ =>
          ColumnFormatEntry.logger.info("SW: fromData on a non-channel", new Throwable)
          // order is BIG_ENDIAN by default
          serializedBuffer = allocator.allocate(numBytes +
              ColumnFormatEntry.VALUE_HEADER_SIZE)
          ColumnFormatEntry.writeValueSerializationHeader(serializedBuffer,
            numBytes)
          val bytes = new Array[Byte](numBytes)
          in.readFully(bytes, 0, numBytes)
          val position = serializedBuffer.position()
          serializedBuffer.put(bytes, 0, numBytes)
          // move to the start of data
          serializedBuffer.position(position)
      }
    } else {
      serializedBuffer = ColumnFormatEntry.VALUE_EMPTY_BUFFER
    }
  }

  override def getSizeInBytes: Int = {
    // cannot use ReflectionObjectSizer to get estimate especially for direct
    // buffer which has a reference queue all of which gets counted incorrectly
    if (serializedBuffer.isDirect) {
      val freeMemorySize = Sizeable.PER_OBJECT_OVERHEAD + 8 /* address */
      val cleanerSize = Sizeable.PER_OBJECT_OVERHEAD +
          REFERENCE_SIZE * 7 /* next, prev, thunk in Cleaner, 4 in Reference */
      val bbSize = Sizeable.PER_OBJECT_OVERHEAD +
          REFERENCE_SIZE * 4 /* hb, att, cleaner, fd */ +
          5 * 4 /* 5 ints */ + 3 /* 3 bools */ + 8 /* address */
      val size = Sizeable.PER_OBJECT_OVERHEAD + REFERENCE_SIZE /* BB */
      val dataSize = serializedBuffer.capacity + 8 /* off-heap overhead */
      alignedSize(size) + alignedSize(dataSize) + alignedSize(bbSize) +
          alignedSize(cleanerSize) + alignedSize(freeMemorySize)
    } else {
      val hbSize = Sizeable.PER_OBJECT_OVERHEAD + 4 /* length */ +
          serializedBuffer.capacity()
      val bbSize = Sizeable.PER_OBJECT_OVERHEAD + REFERENCE_SIZE /* hb */ +
          5 * 4 /* 5 ints */ + 3 /* 3 bools */ + 8 /* unused address */
      val size = Sizeable.PER_OBJECT_OVERHEAD + REFERENCE_SIZE /* BB */
      alignedSize(size) + alignedSize(bbSize) + alignedSize(hbSize)
    }
  }
}
