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

import scala.collection.JavaConverters._

import com.gemstone.gemfire.internal.cache.{NonLocalRegionEntry, PartitionedRegion, RegionEntry, TXState, Token}
import com.pivotal.gemfirexd.internal.engine.distributed.message.GetAllExecutorMessage
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireResultSet
import io.snappydata.collection.LongObjectHashMap

import org.apache.spark.sql.execution.columnar.impl.ColumnFormatEntry.{DELETE_MASK_COL_INDEX, STATROW_COL_INDEX}

/**
 * A [[ClusteredColumnIterator]] that fetches entries from a remote bucket.
 */
final class RemoteEntriesIterator(bucketId: Int, projection: Array[Int],
    pr: PartitionedRegion, txState: TXState) extends ClusteredColumnIterator {

  private val statsRows = {
    val statsKeys = pr.getBucketKeys(bucketId).iterator().asScala.filter {
      case k: ColumnFormatKey => k.columnIndex == STATROW_COL_INDEX
      case _ => false
    }
    // get the full entries for all stats rows in one getAll
    val msg = new GetAllExecutorMessage(pr, statsKeys.asInstanceOf[Iterator[AnyRef]].toArray,
      null, null, null, null, null, null, txState, null, false, false)
    statsKeys.zip(GemFireResultSet.callGetAllExecutorMessage(msg).asScala.toIterator)
  }

  /**
   * Full projection including all of delta and meta-data columns (except base stats entry)
   */
  private val fullProjection = {
    val projectionArray = new Array[Int](projection.length *
        (ColumnDelta.USED_MAX_DEPTH + 1) - DELETE_MASK_COL_INDEX + STATROW_COL_INDEX)
    for (i <- DELETE_MASK_COL_INDEX until STATROW_COL_INDEX) {
      projectionArray(i - DELETE_MASK_COL_INDEX) = i
    }
    var i = STATROW_COL_INDEX - DELETE_MASK_COL_INDEX
    for (p <- projection) {
      projectionArray(i) = p
      i += 1
      for (d <- 0 until ColumnDelta.USED_MAX_DEPTH) {
        projectionArray(i) = ColumnDelta.deltaColumnIndex(p - 1, d)
        i += 1
      }
    }
    projectionArray
  }

  private var currentStatsKey: ColumnFormatKey = _
  private val currentValueMap = LongObjectHashMap.withExpectedSize[AnyRef](8)

  override def hasNext: Boolean = statsRows.hasNext

  override def next(): RegionEntry = {
    currentValueMap.clear()
    val p = statsRows.next()
    currentStatsKey = p._1.asInstanceOf[ColumnFormatKey]
    NonLocalRegionEntry.newEntry(currentStatsKey, p._2, pr, null)
  }

  override def getColumnValue(column: Int): AnyRef = {
    if (currentValueMap.size() == 0) {
      // fetch all the projected columns for current batch
      val fetchKeys = fullProjection.map(c =>
        new ColumnFormatKey(currentStatsKey.uuid, currentStatsKey.partitionId, c))
      val msg = new GetAllExecutorMessage(pr, fetchKeys.asInstanceOf[Array[AnyRef]],
        null, null, null, null, null, null, txState, null, false, false)
      fetchKeys.zip(GemFireResultSet.callGetAllExecutorMessage(msg).asScala).foreach {
        case (_, null) | (_, _: Token) =>
        case (k, v) => currentValueMap.justPut(k.columnIndex, v.asInstanceOf[AnyRef])
      }
    }
    currentValueMap.get(column)
  }

  override def close(): Unit = {
    currentStatsKey = null
    currentValueMap.clear()
  }
}
