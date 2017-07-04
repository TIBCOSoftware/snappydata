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
import java.sql.Blob
import java.util.concurrent.locks.LockSupport

import scala.collection.JavaConverters._

import com.gemstone.gemfire.DataSerializer
import com.gemstone.gemfire.cache.{DiskAccessException, EntryDestroyedException, EntryOperation, Region, RegionDestroyedException}
import com.gemstone.gemfire.internal.cache._
import com.gemstone.gemfire.internal.cache.lru.Sizeable
import com.gemstone.gemfire.internal.cache.partitioned.PREntriesIterator
import com.gemstone.gemfire.internal.cache.persistence.DiskRegionView
import com.gemstone.gemfire.internal.cache.store.{ManagedDirectBufferAllocator, SerializedDiskBuffer}
import com.gemstone.gemfire.internal.shared.{ClientSharedUtils, InputStreamChannel, OutputStreamChannel, Version}
import com.gemstone.gemfire.internal.size.ReflectionSingleObjectSizer.REFERENCE_SIZE
import com.gemstone.gemfire.internal.{ByteBufferDataInput, DSCODE, DataSerializableFixedID, HeapDataOutputStream}
import com.pivotal.gemfirexd.internal.engine.store.{GemFireContainer, RegionKey, RowEncoder}
import com.pivotal.gemfirexd.internal.engine.{GfxdDataSerializable, GfxdSerializable, Misc}
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow
import com.pivotal.gemfirexd.internal.iapi.types.{DataValueDescriptor, SQLBlob, SQLInteger, SQLVarchar}
import com.pivotal.gemfirexd.internal.impl.sql.compile.TableName
import com.pivotal.gemfirexd.internal.impl.sql.execute.ValueRow
import com.pivotal.gemfirexd.internal.snappy.ColumnBatchKey
import io.snappydata.thrift.common.BufferedBlob
import io.snappydata.thrift.internal.ClientBlob
import org.slf4j.Logger

import org.apache.spark.Logging
import org.apache.spark.memory.MemoryManagerCallback
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ColumnBatchIterator
import org.apache.spark.sql.execution.columnar.encoding.ColumnStatsSchema
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatEntry.alignedSize
import org.apache.spark.unsafe.hash.Murmur3_x86_32

/**
 * Utility methods for column format storage keys and values.
 */
object ColumnFormatEntry extends Logging {

  private[columnar] def logger: Logger = log

  def registerTypes(): Unit = {
    // register the column key and value types
    GfxdDataSerializable.registerSqlSerializable(classOf[ColumnFormatKey])
    GfxdDataSerializable.registerSqlSerializable(classOf[ColumnFormatValue])
  }

  /**
   * Size of the serialization type/class IDs of [[ColumnFormatValue]].
   */
  private[columnar] val VALUE_HEADER_CLASSID_SIZE = 3

  /**
   * Size of the serialization header of [[ColumnFormatValue]] including the
   * serialization type/class IDs (3 bytes), padding (1 byte), and size (4).
   * Padding is added for 8 byte alignment.
   */
  private[columnar] val VALUE_HEADER_SIZE = VALUE_HEADER_CLASSID_SIZE + 1 + 4

  private[columnar] val VALUE_EMPTY_BUFFER = {
    val buffer = ByteBuffer.wrap(new Array[Byte](VALUE_HEADER_SIZE))
    writeValueSerializationHeader(buffer, 0)
    buffer
  }

  private[columnar] def alignedSize(size: Int) = ((size + 7) >>> 3) << 3

  private[columnar] def writeValueSerializationHeader(buffer: ByteBuffer,
      size: Int): Unit = {
    // write the typeId + classId and size
    buffer.put(DSCODE.DS_FIXED_ID_BYTE)
    buffer.put(DataSerializableFixedID.GFXD_TYPE)
    buffer.put(GfxdSerializable.COLUMN_FORMAT_VALUE)
    buffer.put(0.toByte) // padding
    buffer.putInt(size) // assume big-endian buffer
  }
}

/**
 * Key object in the column store.
 */
final class ColumnFormatKey(private[columnar] var partitionId: Int,
    private[columnar] var columnIndex: Int,
    private[columnar] var uuid: String)
    extends GfxdDataSerializable with ColumnBatchKey with RegionKey with Serializable {

  // to be used only by deserialization
  def this() = this(-1, -1, "")

  override def getNumColumnsInTable(columnTableName: String): Int = {
    val bufferTable = ColumnFormatRelation.getTableName(columnTableName)
    Misc.getMemStore.getAllContainers.asScala.find(_.getQualifiedTableName
        .equalsIgnoreCase(bufferTable)).get.getNumColumns - 1
  }

  override def getColumnBatchRowCount(itr: PREntriesIterator[_],
      re: AbstractRegionEntry, numColumnsInTable: Int): Int = {
    val numColumns = numColumnsInTable * ColumnStatsSchema.NUM_STATS_PER_COLUMN + 1
    val currentBucketRegion = itr.getHostedBucketRegion
    if (columnIndex == ColumnBatchIterator.STATROW_COL_INDEX &&
        !re.isDestroyedOrRemoved) {
      val value = re.getValue(currentBucketRegion)
          .asInstanceOf[ColumnFormatValue]
      if (value ne null) {
        val buffer = value.getBufferRetain
        try {
          if (buffer.remaining() > 0) {
            val unsafeRow = Utils.toUnsafeRow(buffer, numColumns)
            unsafeRow.getInt(ColumnStatsSchema.COUNT_INDEX_IN_SCHEMA)
          } else 0
        } finally {
          value.release()
        }
      } else 0
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

  override def getGfxdID: Byte = GfxdSerializable.COLUMN_FORMAT_KEY

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
          REFERENCE_SIZE
      /* char[] reference */
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

  override def nCols(): Int = 3

  override def getKeyColumn(index: Int): DataValueDescriptor = index match {
    case 0 => new SQLVarchar(uuid)
    case 1 => new SQLInteger(partitionId)
    case 2 => new SQLInteger(columnIndex)
  }

  override def getKeyColumns(keys: Array[DataValueDescriptor]): Unit = {
    keys(0) = new SQLVarchar(uuid)
    keys(1) = new SQLInteger(partitionId)
    keys(2) = new SQLInteger(columnIndex)
  }

  override def getKeyColumns(keys: Array[AnyRef]): Unit = {
    keys(0) = uuid
    keys(1) = Int.box(partitionId)
    keys(2) = Int.box(columnIndex)
  }

  override def setRegionContext(region: LocalRegion): Unit = {}

  override def beforeSerializationWithValue(
      valueIsToken: Boolean): KeyWithRegionContext = this

  override def afterDeserializationWithValue(v: AnyRef): Unit = {}

  override def toString: String =
    s"ColumnKey(columnIndex=$columnIndex,partitionId=$partitionId,uuid=$uuid)"
}

/**
 * Partition resolver for the column store.
 */
final class ColumnPartitionResolver(tableName: TableName)
    extends InternalPartitionResolver[ColumnFormatKey, ColumnFormatValue] {

  private val regionPath = tableName.getFullTableNameAsRegionPath

  private lazy val region = Misc.getRegionByPath(regionPath)
      .asInstanceOf[PartitionedRegion]
  private lazy val rootMasterRegion = ColocationHelper.getLeaderRegionName(region)

  override def getName: String = "ColumnPartitionResolver"

  override def getRoutingObject(opDetails: EntryOperation[ColumnFormatKey,
      ColumnFormatValue]): AnyRef = Int.box(opDetails.getKey.partitionId)

  override def getRoutingObject(key: AnyRef, value: AnyRef,
      callbackArg: AnyRef, region: Region[_, _]): AnyRef = {
    Int.box(key.asInstanceOf[ColumnFormatKey].partitionId)
  }

  override def getPartitioningColumnsCount: Int = 1

  override def getMasterTable(rootMaster: Boolean): String = {
    val master = if (rootMaster) rootMasterRegion else region.getColocatedWithRegion
    if (master ne null) master.getFullPath else null
  }

  override def getDDLString: String =
    s"PARTITIONER '${classOf[ColumnPartitionResolver].getName}'"

  override def close(): Unit = {}
}

/**
 * Value object in the column store simply encapsulates binary data as a
 * ByteBuffer. This can be either a direct buffer or a heap buffer depending
 * on the system off-heap configuration. The reason for a separate type is to
 * easily store data off-heap without any major changes to engine otherwise as
 * well as efficiently serialize/deserialize them directly to Oplog/socket.
 *
 * This class extends [[SerializedDiskBuffer]] to avoid a copy when
 * reading/writing from Oplog. Consequently it has a pre-serialized buffer
 * that has the serialization header at the start (typeID + classID + size).
 * This helps it avoid additional byte writes when transferring data to the
 * channels as well as avoiding trimming of buffer when reading from it.
 */
final class ColumnFormatValue
    extends SerializedDiskBuffer with GfxdSerializable with Sizeable {

  @volatile
  @transient private var columnBuffer = ColumnFormatEntry.VALUE_EMPTY_BUFFER
  @transient private var diskId: DiskId = _
  @transient private var diskRegion: DiskRegionView = _

  def this(buffer: ByteBuffer) = {
    this()
    setBuffer(buffer)
  }

  def setBuffer(buffer: ByteBuffer,
      changeOwnerToStorage: Boolean = true,
      writeSerializationHeader: Boolean = true): Unit = synchronized {
    val columnBuffer = GemFireCacheImpl.getCurrentBufferAllocator
        .transfer(buffer, ManagedDirectBufferAllocator.DIRECT_STORE_OBJECT_OWNER)
    if (changeOwnerToStorage && columnBuffer.isDirect) {
      MemoryManagerCallback.memoryManager.changeOffHeapOwnerToStorage(
        columnBuffer, allowNonAllocator = true)
    }
    if (writeSerializationHeader) {
      // write the serialization header and move ahead to start of data
      ColumnFormatEntry.writeValueSerializationHeader(columnBuffer,
        buffer.remaining() - ColumnFormatEntry.VALUE_HEADER_SIZE)
    }
    this.columnBuffer = columnBuffer
    // reference count is required to be 1 at this point
    val refCount = this.refCount
    assert(refCount == 1, s"Unexpected refCount=$refCount")
  }

  def isDirect: Boolean = columnBuffer.isDirect

  /**
   * Callers of this method should have a corresponding release method
   * for eager release to work else off-heap object may keep around
   * occupying system RAM until the next GC cycle. Callers may decide
   * whether to keep the [[release]] method in a finally block to ensure
   * its invocation, or do it only in normal paths because JVM reference
   * collector will eventually clean it in any case.
   */
  override def getBufferRetain: ByteBuffer = {
    if (retain()) {
      columnBuffer.duplicate()
    } else synchronized {
      // check if already read from disk by another thread and still valid
      val buffer = this.columnBuffer
      if ((buffer ne ColumnFormatEntry.VALUE_EMPTY_BUFFER) && retain()) {
        return buffer.duplicate()
      }

      // try to read using DiskId
      val diskId = this.diskId
      if (diskId ne null) {
        try {
          DiskEntry.Helper.getValueOnDisk(diskId, diskRegion) match {
            case v: ColumnFormatValue =>
              // transfer the buffer from the temporary ColumnFormatValue
              columnBuffer = v.columnBuffer
              // restart reference count from 1
              while (true) {
                val refCount = SerializedDiskBuffer.refCountUpdate.get(this)
                val updatedRefCount = if (refCount <= 0) 1 else refCount + 1
                if (SerializedDiskBuffer.refCountUpdate.compareAndSet(
                  this, refCount, updatedRefCount)) {
                  return columnBuffer.duplicate()
                }
              }
            case null | _: Token => // return empty buffer
            case o => throw new IllegalStateException(
              s"unexpected value in column store $o")
          }
        } catch {
          case _: EntryDestroyedException | _: DiskAccessException |
               _: RegionDestroyedException =>
            // These exception types mean that value has disappeared from disk
            // due to compaction or bucket has moved so return empty value.
            // RegionDestroyedException is also ignored since background
            // processors like gateway event processors will not expect it.
        }
      }
      ColumnFormatEntry.VALUE_EMPTY_BUFFER.duplicate()
    }
  }

  override def needsRelease: Boolean = columnBuffer.isDirect

  override protected def releaseBuffer(): Unit = synchronized {
    // Remove the buffer at this point. Any further reads will need to be
    // done either using DiskId, or will return empty if no DiskId is available
    val buffer = this.columnBuffer
    if (buffer.isDirect) {
      this.columnBuffer = ColumnFormatEntry.VALUE_EMPTY_BUFFER
      ManagedDirectBufferAllocator.instance().release(buffer)
    }
  }

  override def setDiskId(id: DiskId, dr: DiskRegionView): Unit = synchronized {
    if (id ne null) {
      this.diskId = id
      // set/update diskRegion only if incoming value has been provided
      if (dr ne null) {
        this.diskRegion = dr
      }
    } else {
      this.diskId = null
      this.diskRegion = null
    }
  }

  override def write(channel: OutputStreamChannel): Unit = {
    // write the pre-serialized buffer as is
    val buffer = getBufferRetain
    try {
      // rewind buffer to the start for the write;
      // no need to change position back since this is a duplicate ByteBuffer
      buffer.rewind()
      write(channel, buffer)
    } finally {
      release()
    }
  }

  override def size(): Int = columnBuffer.limit()

  override def getGfxdID: Byte = GfxdSerializable.COLUMN_FORMAT_VALUE

  override def getDSFID: Int = DataSerializableFixedID.GFXD_TYPE

  override def getSerializationVersions: Array[Version] = null

  override def toData(out: DataOutput): Unit = {
    val buffer = getBufferRetain
    try {
      val numBytes = buffer.remaining()
      out.writeByte(0) // padding for 8-byte alignment
      out.writeInt(numBytes)
      if (numBytes > 0) {
        out match {
          case channel: OutputStreamChannel =>
            write(channel, buffer)

          case hdos: HeapDataOutputStream =>
            hdos.write(buffer)

          case _ =>
            val allocator = GemFireCacheImpl.getCurrentBufferAllocator
            out.write(allocator.toBytes(buffer))
        }
      }
    } finally {
      release()
    }
  }

  override def fromData(in: DataInput): Unit = {
    // skip padding
    in.readByte()
    val numBytes = in.readInt()
    if (numBytes > 0) {
      val allocator = GemFireCacheImpl.getCurrentBufferAllocator
      in match {
        case din: ByteBufferDataInput =>
          // just transfer the internal buffer; higher layer (e.g. BytesAndBits)
          // will take care not to release this buffer (if direct);
          // buffer is already positioned at start of data
          val buffer = allocator.transfer(din.getInternalBuffer,
            ManagedDirectBufferAllocator.DIRECT_STORE_OBJECT_OWNER)
          if (buffer.isDirect) {
            MemoryManagerCallback.memoryManager.changeOffHeapOwnerToStorage(
              buffer, allowNonAllocator = true)
          }
          columnBuffer = buffer

        case channel: InputStreamChannel =>
          // order is BIG_ENDIAN by default
          val buffer = allocator.allocateForStorage(numBytes +
              ColumnFormatEntry.VALUE_HEADER_SIZE)
          ColumnFormatEntry.writeValueSerializationHeader(buffer,
            numBytes)
          val position = buffer.position()
          do {
            if (channel.read(buffer) == 0) {
              // wait for a bit before retrying
              LockSupport.parkNanos(ClientSharedUtils.PARK_NANOS_FOR_READ_WRITE)
            }
          } while (buffer.hasRemaining)
          // move to the start of data
          buffer.position(position)
          columnBuffer = buffer

        case _ =>
          // order is BIG_ENDIAN by default
          val bytes = new Array[Byte](numBytes +
              ColumnFormatEntry.VALUE_HEADER_SIZE)
          in.readFully(bytes, ColumnFormatEntry.VALUE_HEADER_SIZE, numBytes)
          // extra copy of empty 8-byte header too
          val buffer = allocator.fromBytesToStorage(bytes, 0,
            numBytes + ColumnFormatEntry.VALUE_HEADER_SIZE)
          // owner is already marked for storage
          setBuffer(buffer, changeOwnerToStorage = false)
      }
    } else {
      columnBuffer = ColumnFormatEntry.VALUE_EMPTY_BUFFER
    }
  }

  override def getSizeInBytes: Int = {
    // Cannot use ReflectionObjectSizer to get estimate especially for direct
    // buffer which has a reference queue all of which gets counted incorrectly.
    // Returns instantaneous size by design and not synchronized
    // (or retain/release) with capacity being valid even after releaseBuffer.
    val buffer = columnBuffer
    if (buffer.isDirect) {
      val freeMemorySize = Sizeable.PER_OBJECT_OVERHEAD + 8
      /* address */
      val cleanerSize = Sizeable.PER_OBJECT_OVERHEAD +
          REFERENCE_SIZE * 7
      /* next, prev, thunk in Cleaner, 4 in Reference */
      val bbSize = Sizeable.PER_OBJECT_OVERHEAD +
          REFERENCE_SIZE * 4 /* hb, att, cleaner, fd */ +
          5 * 4 /* 5 ints */ + 3 /* 3 bools */ + 8
      /* address */
      val size = Sizeable.PER_OBJECT_OVERHEAD + REFERENCE_SIZE /* BB */
      alignedSize(size) + alignedSize(bbSize) +
          alignedSize(cleanerSize) + alignedSize(freeMemorySize)
    } else {
      val hbSize = Sizeable.PER_OBJECT_OVERHEAD + 4 /* length */ +
          buffer.capacity()
      val bbSize = Sizeable.PER_OBJECT_OVERHEAD + REFERENCE_SIZE /* hb */ +
          5 * 4 /* 5 ints */ + 3 /* 3 bools */ + 8
      /* unused address */
      val size = Sizeable.PER_OBJECT_OVERHEAD + REFERENCE_SIZE /* BB */
      alignedSize(size) + alignedSize(bbSize) + alignedSize(hbSize)
    }
  }

  override def getOffHeapSizeInBytes: Int = {
    // Returns instantaneous size by design and not synchronized
    // (or retain/release) with capacity being valid even after releaseBuffer.
    val buffer = columnBuffer
    if (buffer.isDirect) {
      buffer.capacity() + ManagedDirectBufferAllocator.DIRECT_OBJECT_OVERHEAD
    } else 0
  }

  override def toString: String = {
    val buffer = columnBuffer.duplicate()
    s"ColumnValue[size=${buffer.remaining()} $buffer diskId=$diskId diskRegion=$diskRegion]"
  }
}

final class ColumnFormatEncoder extends RowEncoder {

  override def toRow(entry: RegionEntry, value: AnyRef,
      container: GemFireContainer): ExecRow = {
    val batchKey = entry.getRawKey.asInstanceOf[ColumnFormatKey]
    val batchValue = value.asInstanceOf[ColumnFormatValue]
    // layout the same way as declared in ColumnFormatRelation
    val row = new ValueRow(4)
    row.setColumn(1, new SQLVarchar(batchKey.uuid))
    row.setColumn(2, new SQLInteger(batchKey.partitionId))
    row.setColumn(3, new SQLInteger(batchKey.columnIndex))
    // set value reference which will be released after thrift write;
    // this written blob does not include the serialization header
    // unlike in fromRow which does include it
    row.setColumn(4, new SQLBlob(new ClientBlob(batchValue)))
    row
  }

  override def fromRow(row: Array[DataValueDescriptor],
      container: GemFireContainer): java.util.Map.Entry[RegionKey, AnyRef] = {
    val batchKey = new ColumnFormatKey(uuid = row(0).getString,
      partitionId = row(1).getInt, columnIndex = row(2).getInt)
    // transfer buffer from BufferedBlob as is, or copy for others
    val columnBuffer = row(3).getObject match {
      case blob: BufferedBlob => blob.getAsLastChunk.chunk
      case blob: Blob => ByteBuffer.wrap(blob.getBytes(1, blob.length().toInt))
    }
    // the incoming blob includes the space for serialization header to avoid
    // a copy when transferring to ColumnFormatValue, so just move to
    // the start of data and write the serialization header
    columnBuffer.rewind()
    // set the buffer into ColumnFormatValue
    val batchValue = new ColumnFormatValue(columnBuffer)
    new java.util.AbstractMap.SimpleEntry[RegionKey, AnyRef](batchKey, batchValue)
  }

  override def fromRowToKey(key: Array[DataValueDescriptor],
      container: GemFireContainer): RegionKey =
    new ColumnFormatKey(uuid = key(0).getString,
      partitionId = key(1).getInt, columnIndex = key(2).getInt)
}
