/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
import com.gemstone.gemfire.internal.cache.{DiskEntry, EntryEventImpl, GemFireCacheImpl}
import com.gemstone.gemfire.internal.shared.FetchRequest
import com.pivotal.gemfirexd.internal.engine.GfxdSerializable
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer

import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, BoundReference, GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.collection.SharedUtils
import org.apache.spark.sql.execution.columnar.encoding.{ColumnDeltaEncoder, ColumnEncoding, ColumnStatsSchema}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

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
        if (columnIndex == ColumnFormatEntry.DELTA_STATROW_COL_INDEX) {
          // ignore if either of the buffers is empty (old placeholder of 4 bytes
          // while UnsafeRow based data can never be less than 8 bytes)
          if (existingBuffer.limit() <= 4 || newBuffer.limit() <= 4) {
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
        } else {
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

  private def mergeStats(oldBuffer: ByteBuffer, newBuffer: ByteBuffer,
      schema: StructType): ByteBuffer = {
    val numColumnsInStats = ColumnStatsSchema.numStatsColumns(schema.length)
    val oldStatsRow = SharedUtils.toUnsafeRow(oldBuffer, numColumnsInStats)
    val newStatsRow = SharedUtils.toUnsafeRow(newBuffer, numColumnsInStats)
    val oldCount = oldStatsRow.getInt(ColumnStatsSchema.COUNT_INDEX_IN_SCHEMA)
    val newCount = newStatsRow.getInt(ColumnStatsSchema.COUNT_INDEX_IN_SCHEMA)

    val values = new Array[Any](numColumnsInStats)
    val statsSchema = new Array[StructField](numColumnsInStats)
    val nullCountField = StructField("nullCount", IntegerType)
    values(ColumnStatsSchema.COUNT_INDEX_IN_SCHEMA) = oldCount + newCount
    statsSchema(ColumnStatsSchema.COUNT_INDEX_IN_SCHEMA) =
        StructField("count", IntegerType, nullable = false)
    var hasChange = false
    // non-generated code for evaluation since this is only for one row
    // (besides binding to two separate rows will need custom code)
    for (i <- schema.indices) {
      val statsIndex = i * ColumnStatsSchema.NUM_STATS_PER_COLUMN + 1
      val dataType = schema(i).dataType
      val lowerExpr = BoundReference(statsIndex, dataType, nullable = true)
      val upperExpr = BoundReference(statsIndex + 1, dataType, nullable = true)
      val nullCountExpr = BoundReference(statsIndex + 2, IntegerType, nullable = true)
      val ordering = TypeUtils.getInterpretedOrdering(dataType)

      val oldLower = lowerExpr.eval(oldStatsRow)
      val newLower = lowerExpr.eval(newStatsRow)
      val oldUpper = upperExpr.eval(oldStatsRow)
      val newUpper = upperExpr.eval(newStatsRow)
      val oldNullCount = nullCountExpr.eval(oldStatsRow)
      val newNullCount = nullCountExpr.eval(newStatsRow)

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
      val nullCount = new AddStats(nullCountExpr).eval(newNullCount, oldNullCount)
      if (!hasChange && nullCount != oldNullCount) hasChange = true

      values(statsIndex) = lower
      // shared name in StructField for all columns is fine because UnsafeProjection
      // code generation uses only the dataType and doesn't care for the name here
      statsSchema(statsIndex) = StructField("lowerBound", dataType, nullable = true)
      values(statsIndex + 1) = upper
      statsSchema(statsIndex + 1) = StructField("upperBound", dataType, nullable = true)
      values(statsIndex + 2) = nullCount
      statsSchema(statsIndex + 2) = nullCountField
    }
    if (!hasChange) return null // indicates caller to return old column value
    // generate InternalRow to UnsafeRow projection
    val projection = UnsafeProjection.create(statsSchema.map(_.dataType))
    val statsRow = projection.apply(new GenericInternalRow(values))
    SharedUtils.createStatsBuffer(statsRow.getBytes, GemFireCacheImpl.getCurrentBufferAllocator)
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

  /**
   * This is the currently used maximum depth which must be <= [[MAX_DEPTH]].
   * It should only be used by transient execution-time structures and never in storage.
   */
  val USED_MAX_DEPTH = 2

  val mutableKeyNamePrefix = "snappydata_internal_column_"
  /**
   * These are the virtual columns that are injected in the select plan for
   * update/delete so that those operations can actually apply the changes.
   */
  val mutableKeyNames: Seq[String] = Seq(
    mutableKeyNamePrefix + "row_ordinal",
    mutableKeyNamePrefix + "batch_id",
    mutableKeyNamePrefix + "bucket_ordinal",
    mutableKeyNamePrefix + "batch_numrows"
  )
  val mutableKeyFields: Seq[StructField] = Seq(
    StructField(mutableKeyNames.head, LongType, nullable = false),
    StructField(mutableKeyNames(1), LongType, nullable = false),
    StructField(mutableKeyNames(2), IntegerType, nullable = false),
    StructField(mutableKeyNames(3), IntegerType, nullable = false)
  )

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
   * Check if all the rows in a batch have been deleted.
   */
  private[columnar] def checkBatchDeleted(deleteBuffer: ByteBuffer): Boolean = {
    val allocator = ColumnEncoding.getAllocator(deleteBuffer)
    val bufferBytes = allocator.baseObject(deleteBuffer)
    val bufferCursor = allocator.baseOffset(deleteBuffer) + 4
    val numBaseRows = ColumnEncoding.readInt(bufferBytes, bufferCursor)
    val numDeletes = ColumnEncoding.readInt(bufferBytes, bufferCursor + 4)
    numDeletes >= numBaseRows
  }

  /**
   * Delete entire batch from column store for the batchId and partitionId
   * matching those of given key.
   */
  private[columnar] def deleteBatch(key: ColumnFormatKey, columnRegion: Region[_, _],
      columnTableName: String): Unit = {

    // delete all the rows with matching batchId
    def destroyKey(key: ColumnFormatKey): Unit = {
      try {
        columnRegion.destroy(key)
      } catch {
        case _: EntryNotFoundException => // ignore
      }
    }

    val numColumns = key.getNumColumnsInTable(columnTableName)
    // delete the stats rows first
    destroyKey(key.withColumnIndex(ColumnFormatEntry.STATROW_COL_INDEX))
    destroyKey(key.withColumnIndex(ColumnFormatEntry.DELTA_STATROW_COL_INDEX))
    // column values and deltas next
    for (columnIndex <- 1 to numColumns) {
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

/**
 * Unlike the "Add" operator that follows SQL semantics of returning null
 * if either of the expressions is, this will return the non-null value
 * if either is null or add if both are non-null (like the "sum" aggregate).
 */
final class AddStats(attr: BoundReference) extends Add(attr, attr) {
  def eval(leftVal: Any, rightVal: Any): Any = {
    if (leftVal == null) rightVal
    else if (rightVal == null) leftVal
    else nullSafeEval(leftVal, rightVal)
  }
}
