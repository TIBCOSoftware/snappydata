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

import com.gemstone.gemfire.cache.{EntryEvent, EntryNotFoundException, Region}
import com.gemstone.gemfire.internal.cache.delta.Delta
import com.gemstone.gemfire.internal.cache.versions.{VersionSource, VersionTag}
import com.gemstone.gemfire.internal.cache.{DiskEntry, EntryEventImpl, GemFireCacheImpl}
import com.gemstone.gemfire.internal.shared.FetchRequest
import com.pivotal.gemfirexd.internal.engine.GfxdSerializable
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer

import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, BoundReference, GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

object ColumnDeltaConstant {

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

  val mutableKeyNamePrefix = "SNAPPYDATA_INTERNAL_COLUMN_"
  /**
   * These are the virtual columns that are injected in the select plan for
   * update/delete so that those operations can actually apply the changes.
   */
  val mutableKeyNames: Seq[String] = Seq(
    mutableKeyNamePrefix + "ROW_ORDINAL",
    mutableKeyNamePrefix + "BATCH_ID",
    mutableKeyNamePrefix + "BUCKET_ORDINAL",
    mutableKeyNamePrefix + "BATCH_NUMROWS"
  )
  val mutableKeyFields: Seq[StructField] = Seq(
    StructField(mutableKeyNames.head, LongType, nullable = false),
    StructField(mutableKeyNames(1), LongType, nullable = false),
    StructField(mutableKeyNames(2), IntegerType, nullable = false),
    StructField(mutableKeyNames(3), IntegerType, nullable = false)
  )

  def mutableKeyAttributes: Seq[AttributeReference] = StructType(mutableKeyFields).toAttributes

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

}