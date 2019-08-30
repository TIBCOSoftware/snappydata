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

package org.apache.spark.sql.execution.columnar.impl

import java.io.{DataInput, DataOutput}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.function.Supplier
import javax.annotation.concurrent.GuardedBy

import com.gemstone.gemfire.cache.{DiskAccessException, EntryDestroyedException, EntryOperation, Region, RegionDestroyedException}
import com.gemstone.gemfire.internal.DSFIDFactory.GfxdDSFID
import com.gemstone.gemfire.internal.cache._
import com.gemstone.gemfire.internal.cache.lru.Sizeable
import com.gemstone.gemfire.internal.cache.persistence.DiskRegionView
import com.gemstone.gemfire.internal.cache.store.SerializedDiskBuffer
import com.gemstone.gemfire.internal.shared._
import com.gemstone.gemfire.internal.shared.unsafe.DirectBufferAllocator
import com.gemstone.gemfire.internal.size.ReflectionSingleObjectSizer.REFERENCE_SIZE
import com.gemstone.gemfire.internal.{ByteBufferDataInput, DSCODE, DSFIDFactory, DataSerializableFixedID, HeapDataOutputStream}
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.store.{GemFireContainer, RegionKey}
import com.pivotal.gemfirexd.internal.engine.{GfxdDataSerializable, GfxdSerializable, Misc}
import com.pivotal.gemfirexd.internal.iapi.types.{DataValueDescriptor, SQLInteger, SQLLongint}
import com.pivotal.gemfirexd.internal.impl.sql.compile.TableName
import com.pivotal.gemfirexd.internal.snappy.ColumnBatchKey

import org.apache.spark.memory.MemoryManagerCallback.{allocateExecutionMemory, memoryManager, releaseExecutionMemory}
import org.apache.spark.sql.collection.SharedUtils
import org.apache.spark.sql.execution.columnar.encoding.{ColumnDeleteDelta, ColumnEncoding, ColumnStatsSchema}
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatEntry.alignedSize
import org.apache.spark.sql.store.{CompressionCodecId, CompressionUtils}

/**
 * Utility methods for column format storage keys and values.
 */
object ColumnFormatEntry {

  def registerTypes(): Unit = {
    // register the column key and value types
    DSFIDFactory.registerGemFireXDClass(GfxdSerializable.COLUMN_FORMAT_KEY,
      new Supplier[GfxdDSFID] {
        override def get(): GfxdDSFID = new ColumnFormatKey()
      })
    DSFIDFactory.registerGemFireXDClass(GfxdSerializable.COLUMN_FORMAT_VALUE,
      new Supplier[GfxdDSFID] {
        override def get(): GfxdDSFID = new ColumnFormatValue()
      })
    DSFIDFactory.registerGemFireXDClass(GfxdSerializable.COLUMN_FORMAT_DELTA,
      new Supplier[GfxdDSFID] {
        override def get(): GfxdDSFID = new ColumnDelta()
      })
    DSFIDFactory.registerGemFireXDClass(GfxdSerializable.COLUMN_DELETE_DELTA,
      new Supplier[GfxdDSFID] {
        override def get(): GfxdDSFID = new ColumnDeleteDelta()
      })
  }

  /**
   * max number of consecutive compressions after which buffer will be
   * replaced with compressed one in memory
   */
  private[columnar] val MAX_CONSECUTIVE_COMPRESSIONS = 2

  private[columnar] def alignedSize(size: Int) = ((size + 7) >>> 3) << 3

  val STATROW_COL_INDEX: Int = -1

  val DELTA_STATROW_COL_INDEX: Int = -2

  // table index mapping code depends on this being the smallest meta-column
  // (see ColumnDelta.tableColumnIndex and similar methods)
  val DELETE_MASK_COL_INDEX: Int = -3

  private[columnar] val dummyStats = new DummyCachePerfStats
}

/**
 * Key object in the column store.
 *
 * @param uuid        an ID for the key which should be unique in the cluster for a region
 * @param partitionId the bucket ID of the key; must be same as ID of bucket where key is put
 * @param columnIndex 1-based column index for the key (negative for meta-data and delta columns)
 */
final class ColumnFormatKey(private[columnar] var uuid: Long,
    private[columnar] var partitionId: Int,
    private[columnar] var columnIndex: Int)
    extends GfxdDataSerializable with ColumnBatchKey with RegionKey with Serializable {

  // to be used only by deserialization
  def this() = this(-1L, -1, -1)

  override def getNumColumnsInTable(columnTableName: String): Int = {
    val bufferTable = GemFireContainer.getRowBufferTableName(columnTableName)
    GemFireXDUtils.getGemFireContainer(bufferTable, true).getNumColumns - 1
  }

  override def getColumnBatchRowCount(bucketRegion: BucketRegion,
      re: AbstractRegionEntry, numColumnsInTable: Int): Int = {
    val currentBucketRegion = bucketRegion.getHostedBucketRegion
    if ((columnIndex == ColumnFormatEntry.STATROW_COL_INDEX ||
        columnIndex == ColumnFormatEntry.DELTA_STATROW_COL_INDEX ||
        columnIndex == ColumnFormatEntry.DELETE_MASK_COL_INDEX) &&
        !re.isDestroyedOrRemoved) {
      val statsOrDeleteVal = re.getValue(currentBucketRegion)
      if (statsOrDeleteVal ne null) {
        val statsOrDelete = statsOrDeleteVal.asInstanceOf[ColumnFormatValue]
            .getValueRetain(FetchRequest.DECOMPRESS)
        val buffer = statsOrDelete.getBuffer
        try {
          if (buffer.remaining() > 0) {
            if (columnIndex == ColumnFormatEntry.STATROW_COL_INDEX ||
                columnIndex == ColumnFormatEntry.DELTA_STATROW_COL_INDEX) {
              val numColumns = ColumnStatsSchema.numStatsColumns(numColumnsInTable)
              val unsafeRow = SharedUtils.toUnsafeRow(buffer, numColumns)
              unsafeRow.getInt(ColumnStatsSchema.COUNT_INDEX_IN_SCHEMA)
            } else {
              val allocator = ColumnEncoding.getAllocator(buffer)
              // decrement by deleted row count
              -ColumnEncoding.readInt(allocator.baseObject(buffer),
                allocator.baseOffset(buffer) + buffer.position() + 8)
            }
          } else 0
        } finally {
          statsOrDelete.release()
        }
      } else 0
    } else 0
  }

  def getColumnIndex: Int = columnIndex

  private[columnar] def withColumnIndex(columnIndex: Int): ColumnFormatKey = {
    if (columnIndex != this.columnIndex) new ColumnFormatKey(uuid, partitionId, columnIndex)
    else this
  }

  // use the same hash code for all the columns in the same batch so that they
  // are gotten together by the iterator
  override def hashCode(): Int = ClientResolverUtils.addLongToHashOpt(uuid, partitionId)

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

  override val getPartitioningColumns: Array[String] = Array("PARTITIONID")

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
  @transient protected final var columnBuffer: ByteBuffer = DiskEntry.Helper.NULL_BUFFER

  @volatile
  @transient protected[columnar] final var compressionCodecId: Byte =
    CompressionCodecId.DEFAULT.id.toByte

  /**
   * This keeps track of whether the buffer is compressed or not.
   * In addition it keeps a count of how many times compression was done on
   * the buffer without intervening decompression, and if it exceeds
   * [[ColumnFormatEntry.MAX_CONSECUTIVE_COMPRESSIONS]] and no one is using
   * the decompressed buffer, then replace columnBuffer with compressed version.
   *
   * A negative value indicates that the buffer is not compressible (too small
   * or not enough compression can be achieved), a zero indicates a compressed
   * buffer while a positive count indicates a decompressed buffer and number
   * of times compression was done.
   */
  @GuardedBy("this")
  @transient protected final var decompressionState: Byte = -1
  @GuardedBy("this")
  @transient protected final var fromDisk: Boolean = false
  @GuardedBy("this")
  @transient protected final var entry: AbstractOplogDiskRegionEntry = _
  @GuardedBy("this")
  @transient protected final var regionContext: RegionEntryContext = _

  def this(buffer: ByteBuffer, codecId: Int, isCompressed: Boolean,
      changeOwnerToStorage: Boolean = true) = {
    this()
    setBuffer(buffer, codecId, isCompressed, changeOwnerToStorage)
  }

  def setBuffer(buffer: ByteBuffer, codecId: Int,
      isCompressed: Boolean, changeOwnerToStorage: Boolean = true): Unit = {
    val columnBuffer = if (changeOwnerToStorage) {
      transferToStorage(buffer, GemFireCacheImpl.getCurrentBufferAllocator)
    } else buffer
    // reference count is required to be 1 at this point
    synchronized {
      if (refCount != 1) {
        throw new IllegalStateException(s"Unexpected refCount=$refCount")
      }
      this.columnBuffer = columnBuffer.order(ByteOrder.LITTLE_ENDIAN)
      this.compressionCodecId = codecId.toByte
      this.decompressionState = if (isCompressed) 0 else 1
    }
  }

  private def transferToStorage(buffer: ByteBuffer, allocator: BufferAllocator): ByteBuffer = {
    val newBuffer = allocator.transfer(buffer, DirectBufferAllocator.DIRECT_STORE_OBJECT_OWNER)
    changeOwnerToStorage(newBuffer, allocator)
    newBuffer
  }

  private def changeOwnerToStorage(buffer: ByteBuffer, allocator: BufferAllocator): Unit = {
    if (allocator.isManagedDirect) {
      memoryManager.changeOffHeapOwnerToStorage(buffer, allowNonAllocator = true)
    }
  }

  @GuardedBy("this")
  protected final def isCompressed: Boolean = decompressionState == 0

  /**
   * @inheritdoc
   */
  override final def copyToHeap(owner: String): Unit = {
    var oldBuffer: ByteBuffer = null
    var newBuffer: ByteBuffer = null
    var state: Byte = 0
    var context: RegionEntryContext = null
    synchronized {
      oldBuffer = this.columnBuffer
      newBuffer = HeapBufferAllocator.instance().transfer(oldBuffer, owner)
      state = this.decompressionState
      context = this.regionContext
      replaceStoredBuffer(newBuffer, state, state == 0, context)
    }
    if (context ne null) {
      handleBufferReplace(newBuffer, oldBuffer, state, state == 0, context,
        releaseOldBuffer = false)
    }
  }

  @inline protected final def duplicateBuffer(buffer: ByteBuffer): ByteBuffer = {
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
  override final def getBufferRetain: ByteBuffer =
    getValueRetain(FetchRequest.ORIGINAL).getBuffer

  /**
   * Return the data as a ByteBuffer. Should be invoked only after a [[retain]]
   * or [[getValueRetain]] call.
   */
  override final def getBuffer: ByteBuffer = duplicateBuffer(columnBuffer)

  override def getValueRetain(fetchRequest: FetchRequest): ColumnFormatValue = {
    var entry: AbstractOplogDiskRegionEntry = null
    var regionContext: RegionEntryContext = null

    val resultValue = transformValueRetain(incReference = true, fetchRequest)
    if (resultValue ne null) return resultValue
    else if (fetchRequest eq FetchRequest.DECOMPRESS_IF_IN_MEMORY) return null
    else synchronized {
      entry = this.entry
      regionContext = this.regionContext
    }
    var transformValue = false
    // try to read using DiskId
    val diskId = if (entry ne null) entry.getDiskId else null
    if (diskId ne null) {
      val dr = regionContext match {
        case r: LocalRegion => r.getDiskRegionView
        case _ => regionContext.asInstanceOf[DiskRegionView]
      }
      dr.acquireReadLock()
      try diskId.synchronized(synchronized {
        if ((this.columnBuffer ne DiskEntry.Helper.NULL_BUFFER) && incrementReference()) {
          // transform outside any locks (else UMM lock may cause deadlock like in SNAP-2349)
          transformValue = true
        } else {
          DiskEntry.Helper.getValueOnDiskNoLock(diskId, dr) match {
            case v: ColumnFormatValue =>
              // transfer the buffer from the temporary ColumnFormatValue
              columnBuffer = v.columnBuffer
              decompressionState = v.decompressionState
              fromDisk = true
              // restart reference count from 1
              refCount = 1
              transformValue = true

            case null | _: Token => // return empty buffer
            case o => throw new IllegalStateException(
              s"unexpected value in column store $o")
          }
        }
      }) catch {
        case _: EntryDestroyedException | _: DiskAccessException |
             _: RegionDestroyedException =>
        // These exception types mean that value has disappeared from disk
        // due to compaction or bucket has moved so return empty value.
        // RegionDestroyedException is also ignored since background
        // processors like gateway event processors will not expect it.
      } finally {
        dr.releaseReadLock()
        if (transformValue) return transformValueRetain(incReference = false, fetchRequest)
      }
    }
    this
  }

  private def transformValueRetain(incReference: Boolean,
      fetchRequest: FetchRequest): ColumnFormatValue = {
    fetchRequest match {
      case FetchRequest.COMPRESS => compressValue(incReference)
      case FetchRequest.ORIGINAL => if (retain()) this else null
      case _ => decompressValue(incReference, fetchRequest eq FetchRequest.DECOMPRESS_IF_IN_MEMORY)
    }
  }

  private def getCachePerfStats(context: RegionEntryContext): CachePerfStats = {
    if (context ne null) context.getCachePerfStats
    else {
      val cache = GemFireCacheImpl.getInstance()
      if (cache ne null) cache.getCachePerfStats else ColumnFormatEntry.dummyStats
    }
  }

  private def determineHeapSizeChange(newBuffer: ByteBuffer, oldBuffer: ByteBuffer): Int = {
    (if (newBuffer.hasArray) newBuffer.capacity() else 0) -
        (if (oldBuffer.hasArray) oldBuffer.capacity() else 0)
  }

  private def handleBufferReplace(newBuffer: ByteBuffer, oldBuffer: ByteBuffer,
      oldBufferState: Byte, oldBufferIsCompressed: Boolean, context: RegionEntryContext,
      releaseOldBuffer: Boolean = true): Unit = {
    var success = false
    try {
      val heapSizeChange = determineHeapSizeChange(newBuffer, oldBuffer)
      if (heapSizeChange > 0) {
        if (!SharedUtils.acquireStorageMemory(context.getFullPath,
          heapSizeChange, buffer = null, offHeap = false, shouldEvict = true)) {
          throw LocalRegion.lowMemoryException(null, heapSizeChange)
        }
      }
      // release if there has been a reduction in size
      // (due to heap/off-heap transition or compression)
      else if (heapSizeChange < 0) {
        SharedUtils.releaseStorageMemory(context.getFullPath,
          -heapSizeChange, offHeap = false)
      }
      success = true
      if (releaseOldBuffer) BufferAllocator.releaseBuffer(oldBuffer)
    } finally if (!success) {
      // revert the old buffer in case of a LowMemoryException
      synchronized(replaceStoredBuffer(oldBuffer, oldBufferState, oldBufferIsCompressed, context))
    }
  }

  @GuardedBy("this")
  private def replaceStoredBuffer(newBuffer: ByteBuffer, state: Byte,
      isCompressed: Boolean, context: RegionEntryContext): Unit = {
    if (this.refCount > 1 && isInRegion(context)) {
      // update the statistics before changing self
      val newVal = copy(newBuffer, isCompressed, changeOwnerToStorage = false)
      context.updateMemoryStats(this, newVal)
    }
    this.columnBuffer = newBuffer
    this.decompressionState = state
  }

  private def isInRegion(context: RegionEntryContext): Boolean = {
    // (entry eq null) means no disk persistence
    (context ne null) && !fromDisk && ((entry eq null) || !entry.isValueNull)
  }

  private def decompressValue(incReference: Boolean, onlyIfStored: Boolean): ColumnFormatValue = {
    var context: RegionEntryContext = null
    var position = 0
    var typeId = 0
    var outputLen = 0
    var refCountDecremented = false
    var doReplace = false

    // First sync block to check if decompression is required and whether underlying
    // buffer can be replaced. The second sync block is required to be separate
    // because buffer allocation for decompression has to be outside sync.
    synchronized {
      if (incReference && !incrementReference()) return null
      else if (this.decompressionState != 0) {
        if (this.decompressionState > 1) {
          this.decompressionState = 1
        }
        return this
      } else {
        val buffer = this.columnBuffer
        // check if decompression is required
        assert(buffer.order() eq ByteOrder.LITTLE_ENDIAN)
        position = buffer.position()
        typeId = buffer.getInt(position)
        outputLen = buffer.getInt(position + 4)
        if (typeId >= 0) {
          this.decompressionState = 1
          return this
        } else {
          context = this.regionContext
          // replace buffer only if it is stored in region and no other thread
          // is reading it; last condition can be relaxed for heap buffers because
          // the buffer remains valid even after release (which is a no-op)

          // allow some concurrency at this point; proper refCount check will be just before replace
          doReplace = (buffer.hasArray || this.refCount <= 4) && isInRegion(context)
          // first check if decompression should be skipped
          // (when onlyIfStored is true and underlying buffer cannot be replaced)
          if (!onlyIfStored || doReplace) {
            // decrement the reference count here because the returned value might be a copy
            if (incReference && this.refCount > 2) {
              assert(decrementReference())
              refCountDecremented = true
            }
          } else {
            return this
          }
        }
      }
    }

    // replace underlying buffer if either no other thread is holding a reference
    // or if this is a heap buffer
    val allocator = GemFireCacheImpl.getCurrentBufferAllocator
    val perfStats = getCachePerfStats(context)
    // all memory acquire/release/change operations should be done outside of sync block
    val decompressed = allocateExecutionMemory(outputLen,
      CompressionUtils.DECOMPRESSION_OWNER, allocator)
    var buffer: ByteBuffer = null
    var state: Byte = 0
    try {
      synchronized {
        // increment the reference count in the main sync block if decremented earlier
        // to allow for replacing underlying buffer with higher concurrency
        if (refCountDecremented && !incrementReference()) return null
        // check if another thread already decompressed and changed the underlying buffer
        if (this.decompressionState != 0) {
          if (this.decompressionState > 1) this.decompressionState = 1
          BufferAllocator.releaseBuffer(decompressed)
          return this
        }
        buffer = this.columnBuffer
        state = this.decompressionState
        val startDecompression = perfStats.startDecompression()
        CompressionUtils.codecDecompress(buffer, decompressed, outputLen, position, -typeId)
        // update decompression stats
        perfStats.endDecompression(startDecompression)
        // proper refCount check at this point to ensure no other thread is holding reference
        // to this buffer before releasing it
        doReplace &&= (buffer.hasArray || this.refCount <= 2)
        if (doReplace) {
          replaceStoredBuffer(decompressed, 1.toByte, isCompressed = false, context)
        } else {
          // decrement reference since a new value will be returned
          if (incReference) assert(decrementReference())
        }
      }
      if (doReplace) {
        // acquire the increased storage memory after replacing decompressed buffer
        handleBufferReplace(decompressed, buffer, state, oldBufferIsCompressed = true, context)
        changeOwnerToStorage(decompressed, allocator)
        perfStats.incDecompressedReplaced()
        this
      } else {
        perfStats.incDecompressedReplaceSkipped()
        copy(decompressed, isCompressed = false, changeOwnerToStorage = false)
      }
    } finally {
      // release the memory acquired for decompression
      // (any on-the-fly returned buffer will be part of runtime overhead)
      releaseExecutionMemory(decompressed, CompressionUtils.DECOMPRESSION_OWNER)
    }
  }

  private def compressValue(incReference: Boolean): ColumnFormatValue = {
    var buffer: ByteBuffer = null
    var state: Byte = 0
    var maxCompressionsExceeded = false
    var context: RegionEntryContext = null
    var codecId = 0
    var doReplace = false

    // First sync block to check if compression is required and whether underlying
    // buffer can be replaced. The second sync block is required to be separate
    // because buffer allocation for compression has to be outside sync.
    synchronized {
      if (incReference && !incrementReference()) return null
      // a negative value indicates that the buffer is not compressible (either too
      // small or minimum compression ratio is not achieved)
      else if (this.decompressionState <= 0) return this
      else {
        // compress buffer if required
        if (compressionCodecId != CompressionCodecId.None.id) {
          buffer = this.columnBuffer
          state = this.decompressionState
          maxCompressionsExceeded = state > ColumnFormatEntry.MAX_CONSECUTIVE_COMPRESSIONS
          context = this.regionContext
          codecId = this.compressionCodecId
          // check if buffer is stored in region and should also be replaced (if multiple
          // consecutive compressions done and no other thread is reading or this is a heap buffer)
          doReplace = maxCompressionsExceeded && (buffer.hasArray || this.refCount <= 2) &&
              isInRegion(context)
        } else {
          return this
        }
      }
    }

    val allocator = GemFireCacheImpl.getCurrentBufferAllocator
    val perfStats = getCachePerfStats(context)
    // all memory acquire/release/change operations should be done outside of sync block
    var compressed = CompressionUtils.acquireBufferForCompress(codecId, buffer,
      buffer.remaining(), allocator)
    // check the case when compression is to be skipped due to small size
    if (compressed eq buffer) return this
    try {
      synchronized {
        // check if another thread already compressed and changed the underlying buffer
        if (this.decompressionState <= 0) {
          allocator.release(compressed)
          return this
        }

        buffer = this.columnBuffer
        state = this.decompressionState
        val bufferLen = buffer.remaining()
        val startCompression = perfStats.startCompression()
        compressed = CompressionUtils.codecCompress(codecId, buffer, bufferLen,
          compressed, allocator)
        // update compression stats
        perfStats.endCompression(startCompression, bufferLen, compressed.limit())
        if (compressed ne buffer) {
          if (doReplace) {
            replaceStoredBuffer(compressed, 0, isCompressed = true, context)
          } else {
            if (!maxCompressionsExceeded) {
              this.decompressionState = (this.decompressionState + 1).toByte
            }
            // decrement reference since a new value will be returned
            if (incReference) assert(decrementReference())
          }
        } else {
          doReplace = false
          // update skipped compression stats
          perfStats.endCompressionSkipped(startCompression, bufferLen)
          // mark that buffer is not compressible to avoid more attempts
          this.decompressionState = -1
        }
      }
      if (doReplace) {
        // trim to size if there is wasted space
        val size = compressed.limit()
        val newBuffer = if (compressed.capacity() >= size + 32) {
          val trimmed = allocator.allocateForStorage(size).order(ByteOrder.LITTLE_ENDIAN)
          trimmed.put(compressed)
          allocator.release(compressed)
          trimmed.rewind()
          trimmed
        } else compressed
        // for the case of off-heap to heap transition, acquire increased storage
        // memory to fail with LME if there is no available memory
        handleBufferReplace(newBuffer, buffer, state, oldBufferIsCompressed = false, context)
        // replace underlying storage with trimmed buffer if different
        if (newBuffer ne compressed) synchronized {
          replaceStoredBuffer(newBuffer, 0, isCompressed = true, context)
        } else {
          changeOwnerToStorage(compressed, allocator)
        }
        perfStats.incCompressedReplaced()
        this
      } else if (compressed ne buffer) {
        perfStats.incCompressedReplaceSkipped()
        copy(compressed, isCompressed = true, changeOwnerToStorage = false)
      } else this
    } finally {
      // release the memory acquired for compression
      // (any on-the-fly returned buffer will be part of runtime overhead)
      releaseExecutionMemory(compressed, CompressionUtils.COMPRESSION_OWNER)
    }
  }

  // always true because compressed/uncompressed transitions need proper locking
  override final def needsRelease: Boolean = true

  override protected def releaseBuffer(): Unit = {
    // Remove the buffer at this point. Any further reads will need to be
    // done either using DiskId, or will return empty if no DiskId is available
    val buffer = this.columnBuffer
    if (buffer.isDirect) {
      this.columnBuffer = DiskEntry.Helper.NULL_BUFFER
      this.decompressionState = -1
      this.fromDisk = false
      DirectBufferAllocator.instance().release(buffer)
    }
  }

  protected def copy(buffer: ByteBuffer, isCompressed: Boolean,
      changeOwnerToStorage: Boolean): ColumnFormatValue = {
    new ColumnFormatValue(buffer, compressionCodecId, isCompressed, changeOwnerToStorage)
  }

  override final def setDiskEntry(entry: AbstractOplogDiskRegionEntry,
      context: RegionEntryContext): Unit = synchronized {
    this.entry = entry
    // set/update diskRegion only if incoming value has been provided
    if (context ne null) {
      this.regionContext = context
      val codec = context.getColumnCompressionCodec
      if (codec ne null) {
        this.compressionCodecId = CompressionCodecId.fromName(codec).id.toByte
      }
    }
  }

  override final def write(channel: OutputStreamChannel): Unit = {
    // write the pre-serialized buffer as is
    // Oplog layer will get compressed form by calling getValueRetain
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
    // avoid forced compression for localhost connection but still send
    // compressed if already compressed to avoid potentially unnecessary work
    var outputStreamChannel: OutputStreamChannel = null
    val writeValue = out match {
      case channel: OutputStreamChannel =>
        outputStreamChannel = channel
        val v = getValueRetain(
          if (channel.isSocketToSameHost) FetchRequest.DECOMPRESS_IF_IN_MEMORY
          else FetchRequest.COMPRESS)
        if (v ne null) v else getValueRetain(FetchRequest.ORIGINAL)
      case _ => getValueRetain(FetchRequest.COMPRESS)
    }
    val buffer = writeValue.getBuffer
    try {
      val numBytes = buffer.limit()
      out.writeByte(0) // padding for 8-byte alignment
      out.writeInt(numBytes)
      if (numBytes > 0) {
        if (outputStreamChannel ne null) {
          write(outputStreamChannel, buffer)
        } else out match {
          case hdos: HeapDataOutputStream =>
            hdos.write(buffer)

          case _ =>
            val allocator = ColumnEncoding.getAllocator(buffer)
            out.write(allocator.toBytes(buffer))
        }
      }
    } finally {
      writeValue.release()
    }
  }

  override def fromData(in: DataInput): Unit = {
    // skip padding
    in.readByte()
    val numBytes = in.readInt()
    if (numBytes > 0) {
      val allocator = GemFireCacheImpl.getCurrentBufferAllocator
      var buffer = in match {
        case din: ByteBufferDataInput =>
          // just transfer the internal buffer; higher layer (e.g. BytesAndBits)
          // will take care not to release this buffer (if direct);
          // buffer is already positioned at start of data
          val buffer = allocator.transfer(din.getInternalBuffer,
            DirectBufferAllocator.DIRECT_STORE_OBJECT_OWNER)
          if (buffer.isDirect) {
            memoryManager.changeOffHeapOwnerToStorage(buffer, allowNonAllocator = true)
          }
          buffer

        case channel: InputStreamChannel =>
          val buffer = allocator.allocateForStorage(numBytes)
          var numTries = 0
          do {
            if (channel.read(buffer) == 0) {
              // wait for a bit after some retries (no timeout)
              numTries += 1
              ClientSharedUtils.parkThreadForAsyncOperationIfRequired(channel, 0L, numTries)
            }
          } while (buffer.hasRemaining)
          // move to the start of data
          buffer.rewind()
          buffer

        case _ =>
          // order is BIG_ENDIAN by default
          val bytes = new Array[Byte](numBytes)
          in.readFully(bytes, 0, numBytes)
          allocator.fromBytesToStorage(bytes, 0, numBytes)
      }
      buffer = buffer.order(ByteOrder.LITTLE_ENDIAN)
      val codecId = -buffer.getInt(buffer.position())
      val isCompressed = CompressionCodecId.isCompressed(codecId)
      // owner is already marked for storage
      // if not compressed set the default codecId while the actual one will be
      // set when the value is placed in region (in setDiskLocation) that will
      // be used in further toData calls if required
      setBuffer(buffer, if (isCompressed) codecId else CompressionCodecId.DEFAULT.id,
        isCompressed, changeOwnerToStorage = false)
    } else {
      this.columnBuffer = DiskEntry.Helper.NULL_BUFFER
      this.decompressionState = -1
      this.fromDisk = false
    }
  }

  override def getSizeInBytes: Int = {
    // Cannot use ReflectionObjectSizer to get estimate especially for direct
    // buffer which has a reference queue all of which gets counted incorrectly.
    // Returns instantaneous size by design and not synchronized
    // (or retain/release) with capacity being valid even after releaseBuffer.
    val buffer = columnBuffer
    if (buffer eq DiskEntry.Helper.NULL_BUFFER) 0
    else if (buffer.isDirect) {
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

  protected def className: String = "ColumnValue"

  override def toString: String = {
    var buffer: ByteBuffer = null
    var diskId: DiskId = null
    var state: Byte = 0
    val contextName = synchronized {
      buffer = getBuffer
      diskId = if (entry ne null) entry.getDiskId else null
      state = this.decompressionState
      this.regionContext match {
        case null => ""
        case context => context.getFullPath
      }
    }
    // refCount access is deliberately not synchronized
    s"$className@${System.identityHashCode(this)}[size=${buffer.remaining()} $buffer " +
        s"diskId=$diskId context=$contextName state=$state refCount=$refCount]"
  }
}
