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
import java.nio.{ByteBuffer, ByteOrder}
import java.util.concurrent.locks.LockSupport

import com.gemstone.gemfire.cache.{DiskAccessException, EntryDestroyedException, EntryOperation, Region, RegionDestroyedException}
import com.gemstone.gemfire.internal.cache._
import com.gemstone.gemfire.internal.cache.lru.Sizeable
import com.gemstone.gemfire.internal.cache.partitioned.PREntriesIterator
import com.gemstone.gemfire.internal.cache.persistence.DiskRegionView
import com.gemstone.gemfire.internal.cache.store.SerializedDiskBuffer
import com.gemstone.gemfire.internal.shared.unsafe.DirectBufferAllocator
import com.gemstone.gemfire.internal.shared.{ClientResolverUtils, ClientSharedUtils, HeapBufferAllocator, InputStreamChannel, OutputStreamChannel, Version}
import com.gemstone.gemfire.internal.size.ReflectionSingleObjectSizer.REFERENCE_SIZE
import com.gemstone.gemfire.internal.{ByteBufferDataInput, DSCODE, DataSerializableFixedID, HeapDataOutputStream}
import com.pivotal.gemfirexd.internal.engine.store.{GemFireContainer, RegionKey}
import com.pivotal.gemfirexd.internal.engine.{GfxdDataSerializable, GfxdSerializable, Misc}
import com.pivotal.gemfirexd.internal.iapi.types.{DataValueDescriptor, SQLInteger, SQLLongint}
import com.pivotal.gemfirexd.internal.impl.sql.compile.TableName
import com.pivotal.gemfirexd.internal.snappy.ColumnBatchKey
import org.slf4j.Logger

import org.apache.spark.Logging
import org.apache.spark.memory.MemoryManagerCallback
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.encoding.{ColumnDeleteDelta, ColumnStatsSchema}
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
    GfxdDataSerializable.registerSqlSerializable(classOf[ColumnDelta])
    GfxdDataSerializable.registerSqlSerializable(classOf[ColumnDeleteDelta])
  }

  private[columnar] def alignedSize(size: Int) = ((size + 7) >>> 3) << 3

  val STATROW_COL_INDEX: Int = -1

  val DELTA_STATROW_COL_INDEX: Int = -2

  val DELETE_MASK_COL_INDEX: Int = -3
}

/**
 * Key object in the column store.
 */
final class ColumnFormatKey(private[columnar] var uuid: Long,
    private[columnar] var partitionId: Int,
    private[columnar] var columnIndex: Int)
    extends GfxdDataSerializable with ColumnBatchKey with RegionKey
        with Serializable with Logging {

  // to be used only by deserialization
  def this() = this(-1L, -1, -1)

  override def getNumColumnsInTable(columnTableName: String): Int = {
    val bufferTable = ColumnFormatRelation.getTableName(columnTableName)
    val bufferRegion = Misc.getRegionForTable(bufferTable, true)
    bufferRegion.getUserAttribute.asInstanceOf[GemFireContainer].getNumColumns - 1
  }

  override def getColumnBatchRowCount(itr: PREntriesIterator[_],
      re: AbstractRegionEntry, numColumnsInTable: Int): Int = {
    val numColumns = numColumnsInTable * ColumnStatsSchema.NUM_STATS_PER_COLUMN + 1
    val currentBucketRegion = itr.getHostedBucketRegion
    if (columnIndex == ColumnFormatEntry.STATROW_COL_INDEX &&
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

  override def hashCode(): Int = Murmur3_x86_32.hashInt(
    ClientResolverUtils.addLongToHashOpt(uuid, columnIndex), partitionId)

  override def equals(obj: Any): Boolean = obj match {
    case k: ColumnFormatKey => uuid == k.uuid &&
        partitionId == k.partitionId && columnIndex == k.columnIndex
    case _ => false
  }

  override def getGfxdID: Byte = GfxdSerializable.COLUMN_FORMAT_KEY

  override def toData(out: DataOutput): Unit = {
    out.writeLong(uuid)
    out.writeInt(partitionId)
    out.writeInt(columnIndex)
  }

  override def fromData(in: DataInput): Unit = {
    uuid = in.readLong()
    partitionId = in.readInt()
    columnIndex = in.readInt()
  }

  override def getSizeInBytes: Int = {
    alignedSize(Sizeable.PER_OBJECT_OVERHEAD +
        8 /* uuid */ + 4 /* columnIndex */ + 4 /* partitionId */)
  }

  override def nCols(): Int = 3

  override def getKeyColumn(index: Int): DataValueDescriptor = index match {
    case 0 => new SQLLongint(uuid)
    case 1 => new SQLInteger(partitionId)
    case 2 => new SQLInteger(columnIndex)
  }

  override def getKeyColumns(keys: Array[DataValueDescriptor]): Unit = {
    keys(0) = new SQLLongint(uuid)
    keys(1) = new SQLInteger(partitionId)
    keys(2) = new SQLInteger(columnIndex)
  }

  override def getKeyColumns(keys: Array[AnyRef]): Unit = {
    keys(0) = Long.box(uuid)
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
 * reading/writing from Oplog. Consequently it writes the serialization header
 * itself (typeID + classID + size) into stream as would be written by
 * DataSerializer.writeObject. This helps it avoid additional byte writes when
 * transferring data to the channels.
 */
class ColumnFormatValue extends SerializedDiskBuffer
    with GfxdSerializable with Sizeable {

  @volatile
  @transient protected var columnBuffer = DiskEntry.Helper.NULL_BUFFER
  @transient protected var diskId: DiskId = _
  @transient protected var diskRegion: DiskRegionView = _

  def this(buffer: ByteBuffer) = {
    this()
    setBuffer(buffer)
  }

  def setBuffer(buffer: ByteBuffer,
      changeOwnerToStorage: Boolean = true): Unit = synchronized {
    val columnBuffer = GemFireCacheImpl.getCurrentBufferAllocator
        .transfer(buffer, DirectBufferAllocator.DIRECT_STORE_OBJECT_OWNER)
    if (changeOwnerToStorage && columnBuffer.isDirect) {
      MemoryManagerCallback.memoryManager.changeOffHeapOwnerToStorage(
        columnBuffer, allowNonAllocator = true)
    }
    this.columnBuffer = columnBuffer
    // reference count is required to be 1 at this point
    val refCount = this.refCount
    assert(refCount == 1, s"Unexpected refCount=$refCount")
  }

  final def isDirect: Boolean = columnBuffer.isDirect

  override final def copyToHeap(owner: String): Unit = {
    if (isDirect) {
      columnBuffer = HeapBufferAllocator.instance().transfer(columnBuffer, owner)
    }
  }

  @inline protected def duplicateBuffer(buffer: ByteBuffer): ByteBuffer = {
    // slice buffer for non-zero position so callers don't have to deal with it
    if (buffer.position() == 0) buffer.duplicate() else buffer.slice()
  }

  /**
   * Callers of this method should have a corresponding release method
   * for eager release to work else off-heap object may keep around
   * occupying system RAM until the next GC cycle. Callers may decide
   * whether to keep the [[release]] method in a finally block to ensure
   * its invocation, or do it only in normal paths because JVM reference
   * collector will eventually clean it in any case.
   *
   * Calls to this specific class are guaranteed to always return buffers
   * which have position as zero so callers can make simplifying assumptions
   * about the same.
   */
  override final def getBufferRetain: ByteBuffer = {
    if (retain()) {
      duplicateBuffer(columnBuffer)
    } else {
      var diskId: DiskId = null
      synchronized {
        // check if already read from disk by another thread and still valid
        val buffer = this.columnBuffer
        if ((buffer ne DiskEntry.Helper.NULL_BUFFER) && retain()) {
          return duplicateBuffer(buffer)
        }
        diskId = this.diskId
      }

      // try to read using DiskId
      if (diskId ne null) {
        try {
          DiskEntry.Helper.getValueOnDisk(diskId, diskRegion) match {
            case v: ColumnFormatValue => synchronized {
              // transfer the buffer from the temporary ColumnFormatValue
              columnBuffer = v.columnBuffer
              // restart reference count from 1
              while (true) {
                val refCount = SerializedDiskBuffer.refCountUpdate.get(this)
                val updatedRefCount = if (refCount <= 0) 1 else refCount + 1
                if (SerializedDiskBuffer.refCountUpdate.compareAndSet(
                  this, refCount, updatedRefCount)) {
                  return duplicateBuffer(columnBuffer)
                }
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
      DiskEntry.Helper.NULL_BUFFER.duplicate()
    }
  }

  override final def needsRelease: Boolean = columnBuffer.isDirect

  override protected def releaseBuffer(): Unit = synchronized {
    // Remove the buffer at this point. Any further reads will need to be
    // done either using DiskId, or will return empty if no DiskId is available
    val buffer = this.columnBuffer
    if (buffer.isDirect) {
      this.columnBuffer = DiskEntry.Helper.NULL_BUFFER
      DirectBufferAllocator.instance().release(buffer)
    }
  }

  override final def setDiskId(id: DiskId, dr: DiskRegionView): Unit = synchronized {
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

  override final def write(channel: OutputStreamChannel): Unit = {
    // write the pre-serialized buffer as is
    val buffer = getBufferRetain
    try {
      // first write the serialization header
      // write the typeId + classId and size
      channel.write(DSCODE.DS_FIXED_ID_BYTE)
      channel.write(DataSerializableFixedID.GFXD_TYPE)
      channel.write(getGfxdID)
      channel.write(0.toByte) // padding
      channel.writeInt(buffer.limit())

      // no need to change position back since this is a duplicate ByteBuffer
      write(channel, buffer)
    } finally {
      release()
    }
  }

  override final def writeSerializationHeader(src: ByteBuffer,
      writeBuf: ByteBuffer): Boolean = {
    if (writeBuf.remaining() >= 8) {
      writeBuf.put(DSCODE.DS_FIXED_ID_BYTE)
      writeBuf.put(DataSerializableFixedID.GFXD_TYPE)
      writeBuf.put(getGfxdID)
      writeBuf.put(0.toByte) // padding
      if (writeBuf.order() eq ByteOrder.BIG_ENDIAN) {
        writeBuf.putInt(src.remaining())
      } else {
        writeBuf.putInt(Integer.reverseBytes(src.remaining()))
      }
      true
    } else false
  }

  override final def channelSize(): Int = 8 /* header */ + columnBuffer.remaining()

  override final def size(): Int = columnBuffer.remaining()

  override final def getDSFID: Int = DataSerializableFixedID.GFXD_TYPE

  override def getGfxdID: Byte = GfxdSerializable.COLUMN_FORMAT_VALUE

  override def getSerializationVersions: Array[Version] = null

  override def toData(out: DataOutput): Unit = {
    val buffer = getBufferRetain
    try {
      val numBytes = buffer.limit()
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
            DirectBufferAllocator.DIRECT_STORE_OBJECT_OWNER)
          if (buffer.isDirect) {
            MemoryManagerCallback.memoryManager.changeOffHeapOwnerToStorage(
              buffer, allowNonAllocator = true)
          }
          columnBuffer = buffer

        case channel: InputStreamChannel =>
          // order is BIG_ENDIAN by default
          val buffer = allocator.allocateForStorage(numBytes)
          do {
            if (channel.read(buffer) == 0) {
              // wait for a bit before retrying
              LockSupport.parkNanos(ClientSharedUtils.PARK_NANOS_FOR_READ_WRITE)
            }
          } while (buffer.hasRemaining)
          // move to the start of data
          buffer.rewind()
          columnBuffer = buffer

        case _ =>
          // order is BIG_ENDIAN by default
          val bytes = new Array[Byte](numBytes)
          in.readFully(bytes, 0, numBytes)
          val buffer = allocator.fromBytesToStorage(bytes, 0, numBytes)
          // owner is already marked for storage
          setBuffer(buffer, changeOwnerToStorage = false)
      }
    } else {
      columnBuffer = DiskEntry.Helper.NULL_BUFFER
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
      val size = Sizeable.PER_OBJECT_OVERHEAD +
          REFERENCE_SIZE * 3 /* BB, DiskId, DiskRegion */
      alignedSize(size) + alignedSize(bbSize) +
          alignedSize(cleanerSize) + alignedSize(freeMemorySize)
    } else {
      val hbSize = Sizeable.PER_OBJECT_OVERHEAD + 4 /* length */ +
          buffer.capacity()
      val bbSize = Sizeable.PER_OBJECT_OVERHEAD + REFERENCE_SIZE /* hb */ +
          5 * 4 /* 5 ints */ + 3 /* 3 bools */ + 8
      /* unused address */
      val size = Sizeable.PER_OBJECT_OVERHEAD +
          REFERENCE_SIZE * 3 /* BB, DiskId, DiskRegion */
      alignedSize(size) + alignedSize(bbSize) + alignedSize(hbSize)
    }
  }

  override def getOffHeapSizeInBytes: Int = {
    // Returns instantaneous size by design and not synchronized
    // (or retain/release) with capacity being valid even after releaseBuffer.
    val buffer = columnBuffer
    if (buffer.isDirect) {
      buffer.capacity() + DirectBufferAllocator.DIRECT_OBJECT_OVERHEAD
    } else 0
  }

  override def toString: String = {
    val buffer = columnBuffer.duplicate()
    s"ColumnValue[size=${buffer.remaining()} $buffer " +
        s"diskId=$diskId diskRegion=$diskRegion]"
  }
}
