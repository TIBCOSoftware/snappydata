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

import java.nio.ByteBuffer

import com.gemstone.gemfire.cache.{EntryEvent, EntryNotFoundException, Region}
import com.gemstone.gemfire.internal.cache.delta.Delta
import com.gemstone.gemfire.internal.cache.versions.{VersionSource, VersionTag}
import com.gemstone.gemfire.internal.cache.{DiskEntry, EntryEventImpl, GemFireCacheImpl, PartitionedRegion, TXManagerImpl}
import com.gemstone.gemfire.internal.concurrent.QueryKeyedObjectPool
import com.gemstone.gemfire.internal.shared.FetchRequest
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer
import com.pivotal.gemfirexd.internal.engine.{GfxdSerializable, Misc}
import org.apache.commons.pool2.impl.DefaultPooledObject
import org.apache.commons.pool2.{BaseKeyedPooledObjectFactory, PooledObject}

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Expression, GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.collection.SharedUtils
import org.apache.spark.sql.execution.columnar.encoding.{ColumnDeltaEncoder, ColumnStatsSchema}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StructField, StructType}

/**
 * Encapsulates a delta for update to be applied to column table and also
 * is stored in the region. The key for a delta is a negative columnIndex
 * evaluated as (`ColumnFormatEntry.DELTA_STATROW_COL_INDEX - 1 + MAX_DEPTH * -columnIndex`)
 * where `columnIndex` is the 0-based index of the underlying table column.
 *
 * Note that this delta is for carrying the delta update and applying on existing delta,
 * if any, while the actual value that is stored in the region is a [[ColumnFormatValue]].
 * This is to ensure clean working of the delta mechanism where store-layer
 * code checks the type of object for Delta and makes assumptions about it
 * (like it being a temporary value that should not go into region etc).
 *
 * For a description of column delta format see the class comments in
 * [[org.apache.spark.sql.execution.columnar.encoding.ColumnDeltaEncoder]].
 */
final class ColumnDelta extends ColumnFormatValue with Delta {

  def this(buffer: ByteBuffer, codecId: Int, isCompressed: Boolean,
      changeOwnerToStorage: Boolean = true) = {
    this()
    setBuffer(buffer, codecId, isCompressed, changeOwnerToStorage)
  }

  override protected def copy(buffer: ByteBuffer, isCompressed: Boolean,
      changeOwnerToStorage: Boolean): ColumnDelta = {
    new ColumnDelta(buffer, compressionCodecId, isCompressed, changeOwnerToStorage)
  }

  override def apply(putEvent: EntryEvent[_, _]): AnyRef = {
    val event = putEvent.asInstanceOf[EntryEventImpl]
    apply(event.getRegion, event.getKey, event.getOldValueAsOffHeapDeserializedOrRaw,
      event.getTransactionId == null)
  }

  override def apply(region: Region[_, _], key: AnyRef, oldValue: AnyRef,
      prepareForOffHeap: Boolean): AnyRef = {
    if (oldValue eq null) synchronized {
      // first delta, so put as is
      val result = new ColumnFormatValue(columnBuffer, compressionCodecId, isCompressed)
      // buffer has been transferred and should be removed from delta
      // which would no longer be usable after this point
      columnBuffer = DiskEntry.Helper.NULL_BUFFER
      decompressionState = -1
      result
    } else {
      // merge with existing delta
      val oldColValue = oldValue.asInstanceOf[ColumnFormatValue].getValueRetain(
        FetchRequest.DECOMPRESS)
      val existingBuffer = oldColValue.getBuffer
      val newValue = getValueRetain(FetchRequest.DECOMPRESS)
      val newBuffer = newValue.getBuffer
      try {
        val schema = region.getUserAttribute.asInstanceOf[GemFireContainer]
            .fetchHiveMetaData(false) match {
          case null => throw new IllegalStateException(
            s"Table for region ${region.getFullPath} not found in hive metadata")
          case m => m.schema.asInstanceOf[StructType]
        }
        val columnIndex = key.asInstanceOf[ColumnFormatKey].columnIndex
        // TODO: SW: if old value itself is returned, then avoid any put at GemFire layer
        // (perhaps throw some exception that can be caught and ignored in virtualPut)
        if (columnIndex == ColumnFormatEntry.STATROW_COL_INDEX) {
          // ignore if either of the buffers is empty (old placeholder of 4 bytes
          // while UnsafeRow based data can never be less than 8 bytes)
          if (!existingBuffer.hasRemaining) {
            // entry has been removed from the region
            ColumnDelta.logWarning(s"Statistics entry for $key to apply ColumnDelta removed!")
            null
          } else if (existingBuffer.limit() <= 4 || newBuffer.limit() <= 4) {
            // returned value should be the original and not oldColValue which may be a temp copy
            oldValue
          } else {
            val merged = mergeStats(existingBuffer, newBuffer, schema)
            if (merged ne null) {
              new ColumnFormatValue(merged, oldColValue.compressionCodecId, isCompressed = false)
            } else {
              // returned value should be the original and not oldColValue which may be a temp copy
              oldValue
            }
          }
        } else if (columnIndex == ColumnFormatEntry.DELTA_STATROW_COL_INDEX) {
          // ignore delta stats row puts from old smart connector
          oldValue
        } else if (!existingBuffer.hasRemaining) {
          // entry has been removed from the region
          ColumnDelta.logWarning(s"Entry for $key to apply ColumnDelta removed!")
          null
        } else {
          // currently only delta to delta merges are supported
          assert(columnIndex < ColumnFormatEntry.DELETE_MASK_COL_INDEX)
          val tableColumnIndex = ColumnDelta.tableColumnIndex(columnIndex) - 1
          val encoder = new ColumnDeltaEncoder(ColumnDelta.deltaHierarchyDepth(columnIndex))
          new ColumnFormatValue(encoder.merge(newBuffer, existingBuffer,
            columnIndex < ColumnFormatEntry.DELETE_MASK_COL_INDEX, schema(tableColumnIndex)),
            oldColValue.compressionCodecId, isCompressed = false)
        }
      } finally {
        oldColValue.release()
        newValue.release()
        // release own buffer too and delta should be unusable now
        release()
      }
    }
  }

  /**
   * Merge original column batch statistics with those of the delta put.
   */
  private def mergeStats(oldBuffer: ByteBuffer, newBuffer: ByteBuffer,
      schema: StructType): ByteBuffer = {
    val numColumnsInStats = ColumnStatsSchema.numStatsColumns(schema.length)
    val oldStatsRow = SharedUtils.toUnsafeRow(oldBuffer, numColumnsInStats)
    val newStatsRow = SharedUtils.toUnsafeRow(newBuffer, numColumnsInStats)

    val countIndex = ColumnStatsSchema.COUNT_INDEX_IN_SCHEMA
    val values = new Array[Any](numColumnsInStats)
    val statsExprs = new Array[BoundReference](numColumnsInStats)
    val oldCount = oldStatsRow.getInt(countIndex)
    // this tells us the number of updated values
    val newCount = newStatsRow.getInt(countIndex)
    // write batchCount as negative to indicate the presence of delta updates
    values(countIndex) = -math.abs(oldCount)
    statsExprs(countIndex) = BoundReference(countIndex, IntegerType, nullable = false)
    var hasChange = oldCount > 0 // positive value means this is the first delta for this batch
    // non-generated code for evaluation since this is only for one row
    // (besides binding to two separate rows will need custom code)
    for (i <- schema.indices) {
      val statsIndex = i * ColumnStatsSchema.NUM_STATS_PER_COLUMN + 1
      val dataType = schema(i).dataType
      val lowerExpr = BoundReference(statsIndex, dataType, nullable = true)
      val upperExpr = BoundReference(statsIndex + 1, dataType, nullable = true)
      val nullCountExpr = BoundReference(statsIndex + 2, IntegerType, nullable = false)
      val ordering = TypeUtils.getInterpretedOrdering(dataType)

      val oldLower = lowerExpr.eval(oldStatsRow)
      val newLower = lowerExpr.eval(newStatsRow)
      val oldUpper = upperExpr.eval(oldStatsRow)
      val newUpper = upperExpr.eval(newStatsRow)
      val oldNullCount = nullCountExpr.eval(oldStatsRow).asInstanceOf[Int]
      val newNullCount = nullCountExpr.eval(newStatsRow).asInstanceOf[Int]

      // Unlike normal < or > semantics, comparison against null on either
      // side should return the non-null value or null if both are null.
      // This is like Spark's Greatest/Least semantics. Likewise nullCount
      // should return null only if both are null else skip the null one, if any,
      // like the "sum" aggregate semantics (and unlike the SQL add semantics that
      // returns null if either of the sides is null). The "AddStats" extension
      // to Add operator is to encapsulate that behaviour.
      val lower =
        if (newLower == null) oldLower
        else if (oldLower == null) {
          if (!hasChange && newLower != null) hasChange = true
          newLower
        }
        else if (ordering.lt(newLower, oldLower)) {
          if (!hasChange) hasChange = true
          newLower
        }
        else oldLower
      val upper =
        if (newUpper == null) oldUpper
        else if (oldUpper == null) {
          if (!hasChange && newUpper != null) hasChange = true
          newUpper
        }
        else if (ordering.lt(oldUpper, newUpper)) {
          if (!hasChange) hasChange = true
          newUpper
        }
        else oldUpper
      // Determine the new nullCount conservatively by assuming that all the new non-null
      // values have overridden values that were previously null; this works since this is just
      // a stat not required to be absolutely accurate so such a value ensures that IsNull
      // and IsNotNull predicate filters will be conservatively correct (though latter might
      //   return true even when in the best case it should have been false). In a nutshell,
      // the nullCount stat is only a tri-state (= 0, = batchCount, something in middle) which
      // is also not accurate without doing a full scan of base data + delta.
      assert(newCount >= newNullCount)
      var nullCount = math.max(oldNullCount - (newCount - newNullCount), newNullCount)
      if (nullCount <= 0 && oldNullCount > 0) {
        nullCount = 1
      }
      if (!hasChange && nullCount != oldNullCount) hasChange = true

      values(statsIndex) = lower
      statsExprs(statsIndex) = lowerExpr
      values(statsIndex + 1) = upper
      statsExprs(statsIndex + 1) = upperExpr
      values(statsIndex + 2) = nullCount
      statsExprs(statsIndex + 2) = nullCountExpr
    }
    if (!hasChange) return null // indicates caller to return old column value
    // generate InternalRow to UnsafeRow projection
    val projection = ColumnDelta.unsafeProjectionPool.borrowObject(statsExprs)
    try {
      val statsRow = projection.apply(new GenericInternalRow(values))
      SharedUtils.createStatsBuffer(statsRow.getBytes, GemFireCacheImpl.getCurrentBufferAllocator)
    } finally {
      ColumnDelta.unsafeProjectionPool.returnObject(statsExprs, projection)
    }
  }

  /** first delta update for a column will be put as is into the region */
  override def allowCreate(): Boolean = true

  override def merge(region: Region[_, _], toMerge: Delta): Delta =
    throw new UnsupportedOperationException("Unexpected call to ColumnDelta.merge")

  override def cloneDelta(): Delta =
    throw new UnsupportedOperationException("Unexpected call to ColumnDelta.cloneDelta")

  override def setVersionTag(versionTag: VersionTag[_ <: VersionSource[_]]): Unit =
    throw new UnsupportedOperationException("Unexpected call to ColumnDelta.setVersionTag")

  override def getVersionTag: VersionTag[_ <: VersionSource[_]] =
    throw new UnsupportedOperationException("Unexpected call to ColumnDelta.getVersionTag")

  override def getGfxdID: Byte = GfxdSerializable.COLUMN_FORMAT_DELTA

  override protected def className: String = "ColumnDelta"
}

object ColumnDelta extends Enumeration with Logging {

  /**
   * The initial size of delta column (the smallest delta in the hierarchy).
   */
  val INIT_SIZE = 100

  /**
   * The maximum depth of the hierarchy of deltas for column starting with
   * smallest delta, which is merged with larger delta, then larger, ...
   * till the full column value.
   *
   * As of now only one-level of depth is used and deltas always merged immediately.
   */
  val MAX_DEPTH = 3

  /**
   * This is the currently used maximum depth which must be <= [[MAX_DEPTH]].
   * It should only be used by transient execution-time structures and never in storage.
   */
  val USED_MAX_DEPTH = 2

  val mutableKeyNamePrefix = "snappydata_internal_column_"

  /**
   * Extension to standard enumeration value to hold the StructField.
   * Also exposes the name as "name" field rather than having to use toString.
   */
  final class MutableColumn(i: Int, val name: String, dataType: DataType, nullable: Boolean)
      extends ColumnDelta.Val(i, name) {
    val field: StructField = StructField(name, dataType, nullable)
  }

  type Type = MutableColumn

  private def Value(i: Int, name: String, dataType: DataType,
      nullable: Boolean = false): MutableColumn = new MutableColumn(i, name, dataType, nullable)
  /**
   * These enumeration values are the virtual columns that are injected in the select plan for
   * update/delete so that those operations can actually apply the changes.
   */
  val RowOrdinal: Type = Value(0, mutableKeyNamePrefix + "row_ordinal", LongType)
  val BatchId: Type = Value(1, mutableKeyNamePrefix + "batch_id", LongType)
  val BucketId: Type = Value(2, mutableKeyNamePrefix + "bucket_ordinal", IntegerType)
  val BatchNumRows: Type = Value(3, mutableKeyNamePrefix + "batch_numrows", IntegerType)

  private[this] val mutableKeyList: IndexedSeq[Type] =
    this.values.toIndexedSeq.sortBy(_.id).asInstanceOf[IndexedSeq[Type]]

  val mutableKeyMap: Map[String, Type] = mutableKeyList.map(v => v.toString -> v).toMap
  val mutableKeyNames: IndexedSeq[String] = mutableKeyList.map(_.name)
  val mutableKeyFields: IndexedSeq[StructField] = mutableKeyList.map(_.field)

  private lazy val unsafeProjectionPool = {
    val keyPooledFactory = new BaseKeyedPooledObjectFactory[Seq[Expression], UnsafeProjection] {
      override def create(exprs: Seq[Expression]): UnsafeProjection = {
        val start = System.nanoTime()
        val result = GenerateUnsafeProjection.generate(exprs)
        val elapsed = (System.nanoTime() - start).toDouble / 1000000.0
        logDebug(s"ColumnDelta: UnsafeProjection code generated in $elapsed ms")
        result
      }

      override def wrap(value: UnsafeProjection): PooledObject[UnsafeProjection] =
        new DefaultPooledObject[UnsafeProjection](value)
    }
    val pool = new QueryKeyedObjectPool[Seq[Expression], UnsafeProjection](keyPooledFactory,
      Misc.getGemFireCache.getCancelCriterion)
    // basic configuration for the pool including max per-key limits, idle time etc
    val maxPerKey = math.max(8, Runtime.getRuntime.availableProcessors() * 2)
    pool.setMaxTotalPerKey(maxPerKey)
    pool.setMaxIdlePerKey(maxPerKey)
    pool.setMaxTotal(1000)
    pool.setTimeBetweenEvictionRunsMillis(60000L)
    pool.setMinEvictableIdleTimeMillis(10L * 60000L) // 10 minutes
    pool
  }

  def mutableKeyAttributes: Seq[AttributeReference] = StructType(mutableKeyFields).toAttributes

  def deltaHierarchyDepth(deltaColumnIndex: Int): Int = if (deltaColumnIndex < 0) {
    (-deltaColumnIndex + ColumnFormatEntry.DELETE_MASK_COL_INDEX - 1) % MAX_DEPTH
  } else -1

  /**
   * Returns 1 based table column index for given delta or table column index
   * (table column index stored in region key is 1 based).
   */
  def tableColumnIndex(deltaColumnIndex: Int): Int = if (deltaColumnIndex < 0) {
    (-deltaColumnIndex + ColumnFormatEntry.DELETE_MASK_COL_INDEX + MAX_DEPTH - 1) / MAX_DEPTH
  } else deltaColumnIndex

  /**
   * Returns the delta column index as store in region key given the 0 based
   * table column index (table column index stored in region key is 1 based).
   */
  def deltaColumnIndex(tableColumnIndex: Int, hierarchyDepth: Int): Int =
    -tableColumnIndex * MAX_DEPTH + ColumnFormatEntry.DELETE_MASK_COL_INDEX - 1 - hierarchyDepth

  /**
   * Delete entire batch from column store for the batchId and partitionId
   * matching those of given key.
   */
  private[columnar] def deleteBatch(key: ColumnFormatKey, columnRegion: PartitionedRegion,
      numTableColumns: Int): Unit = {

    // delete all the rows with matching batchId
    def destroyKey(key: ColumnFormatKey): Unit = {
      try {
        columnRegion.destroy(key, null)
        logDebug(s"ColumnStore for ${columnRegion.getFullPath}: destroyed key $key")
      } catch {
        case _: EntryNotFoundException => // ignore
      }
    }

    val regionPath = columnRegion.getColocatedWithRegion.getFullPath
    val statsKey = key.toStatsRowKey
    logDebug(s"ColumnStore for $regionPath: destroying batch with key = $statsKey " +
        s"(transaction = ${TXManagerImpl.getCurrentTXState})")
    // delete the stats rows first
    destroyKey(statsKey)
    destroyKey(key.withColumnIndex(ColumnFormatEntry.DELTA_STATROW_COL_INDEX))
    // column values and deltas next
    for (columnIndex <- 1 to numTableColumns) {
      destroyKey(key.withColumnIndex(columnIndex))
      for (depth <- 0 until MAX_DEPTH) {
        destroyKey(key.withColumnIndex(deltaColumnIndex(
          columnIndex - 1 /* zero based */ , depth)))
      }
    }
    // lastly the delete delta row itself
    destroyKey(key.withColumnIndex(ColumnFormatEntry.DELETE_MASK_COL_INDEX))
  }
}
