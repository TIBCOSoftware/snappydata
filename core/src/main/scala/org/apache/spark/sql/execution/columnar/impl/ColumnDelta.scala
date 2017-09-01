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

import java.nio.ByteBuffer

import com.gemstone.gemfire.cache.{EntryEvent, Region}
import com.gemstone.gemfire.internal.cache.delta.Delta
import com.gemstone.gemfire.internal.cache.versions.{VersionSource, VersionTag}
import com.gemstone.gemfire.internal.cache.{DiskEntry, EntryEventImpl}
import com.pivotal.gemfirexd.internal.engine.GfxdSerializable
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.columnar.encoding.ColumnDeltaEncoder
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

  def this(buffer: ByteBuffer) = {
    this()
    setBuffer(buffer)
  }

  override def apply(putEvent: EntryEvent[_, _]): AnyRef = {
    val event = putEvent.asInstanceOf[EntryEventImpl]
    apply(event.getRegion, event.getKey, event.getOldValueAsOffHeapDeserializedOrRaw,
      event.getTransactionId == null)
  }

  override def apply(region: Region[_, _], key: AnyRef, oldValue: AnyRef,
      prepareForOffHeap: Boolean): AnyRef = {
    if (oldValue eq null) {
      // first delta, so put as is
      val result = new ColumnFormatValue(columnBuffer)
      // buffer has been transferred and should be removed from delta
      // which would no longer be usable after this point
      columnBuffer = DiskEntry.Helper.NULL_BUFFER
      result
    } else {
      // merge with existing delta
      val columnIndex = key.asInstanceOf[ColumnFormatKey].columnIndex
      if (columnIndex == ColumnFormatEntry.DELTA_STATROW_COL_INDEX) {
        // TODO: SW: merge stats
        oldValue
      } else {
        val tableColumnIndex = ColumnDelta.tableColumnIndex(columnIndex)
        val encoder = new ColumnDeltaEncoder(ColumnDelta.deltaHierarchyDepth(columnIndex))
        val schema = region.getUserAttribute.asInstanceOf[GemFireContainer]
            .fetchHiveMetaData(false).schema.asInstanceOf[StructType]
        val oldColumnValue = oldValue.asInstanceOf[ColumnFormatValue]
        val existingBuffer = oldColumnValue.getBufferRetain
        try {
          new ColumnFormatValue(encoder.merge(columnBuffer, existingBuffer,
            columnIndex < ColumnFormatEntry.DELETE_MASK_COL_INDEX, schema(tableColumnIndex)))
        } finally {
          oldColumnValue.release()
          // release own buffer too and delta should be unusable now
          release()
        }
      }
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

  override def toString: String = {
    val buffer = columnBuffer.duplicate()
    s"ColumnDelta[size=${buffer.remaining()} $buffer"
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

  val mutableKeyNamePrefix = "SNAPPYDATA_INTERNAL_COLUMN_"
  /**
   * These are the virtual columns that are injected in the select plan for
   * update/delete so that those operations can actually apply the changes.
   */
  val mutableKeyNames: Seq[String] = Seq(
    mutableKeyNamePrefix + "ROW_ORDINAL",
    mutableKeyNamePrefix + "BATCH_ID",
    mutableKeyNamePrefix + "BUCKET_ORDINAL"
  )
  val mutableKeyFields: Seq[StructField] = Seq(
    StructField(mutableKeyNames.head, LongType, nullable = false),
    StructField(mutableKeyNames(1), LongType, nullable = false),
    StructField(mutableKeyNames(2), IntegerType, nullable = false)
  )
  def mutableKeyAttributes: Seq[AttributeReference] = StructType(mutableKeyFields).toAttributes

  def deltaHierarchyDepth(deltaColumnIndex: Int): Int = if (deltaColumnIndex < 0) {
    (-deltaColumnIndex + ColumnFormatEntry.DELETE_MASK_COL_INDEX - 1) % MAX_DEPTH
  } else -1

  /**
   * Returns 0 based table column index (while that stored in region is 1 based).
   */
  def tableColumnIndex(deltaColumnIndex: Int): Int = if (deltaColumnIndex < 0) {
    (-deltaColumnIndex + ColumnFormatEntry.DELETE_MASK_COL_INDEX - 1) / MAX_DEPTH
  } else deltaColumnIndex

  /**
   * Returns the delta column index as store in region key given the 0 based
   * table column index (table column index stored in region key is 1 based).
   */
  def deltaColumnIndex(tableColumnIndex: Int, hierarchyDepth: Int): Int =
    -tableColumnIndex * MAX_DEPTH + ColumnFormatEntry.DELETE_MASK_COL_INDEX - 1 - hierarchyDepth
}
