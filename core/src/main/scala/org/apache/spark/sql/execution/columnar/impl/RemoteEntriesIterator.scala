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

import java.util.function.Predicate

import scala.collection.JavaConverters._

import com.gemstone.gemfire.internal.cache.{NonLocalRegionEntry, PartitionedRegion, RegionEntry, TXStateInterface}
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdListResultCollector.ListResultCollectorValue
import com.pivotal.gemfirexd.internal.engine.distributed.message.GetAllExecutorMessage
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireResultSet
import io.snappydata.collection.LongObjectHashMap

import org.apache.spark.sql.execution.columnar.impl.ColumnFormatEntry.{DELETE_MASK_COL_INDEX, STATROW_COL_INDEX}

/**
 * A [[ClusteredColumnIterator]] that fetches entries from a remote bucket.
 */
final class RemoteEntriesIterator(bucketId: Int, projection: Array[Int],
    pr: PartitionedRegion, tx: TXStateInterface) extends ClusteredColumnIterator {

  private val statsRows = {
    val statsKeys = pr.getBucketKeys(bucketId, StatsFilter, false, tx).toArray
    // get the stats rows using getAll (max 1000 at a time)
    statsKeys.grouped(1000).flatMap(fetchUsingGetAll)
  }

  /**
   * Full projection including all of delta and meta-data columns (except base stats entry)
   */
  private val fullProjection = {
    // (STATROW_COL_INDEX - DELETE_MASK_COL_INDEX) gives the number of meta-data
    // columns which are always fetched. This excludes STATROW_COL_INDEX itself
    // that has already been fetched separately and includes the delete bitmask.
    // And for each projected column there is a base column and up-to USED_MAX_DEPTH deltas.
    val numMetaColumns = STATROW_COL_INDEX - DELETE_MASK_COL_INDEX
    val projectionArray = new Array[Int](projection.length *
        (ColumnDelta.USED_MAX_DEPTH + 1) + numMetaColumns)
    for (i <- DELETE_MASK_COL_INDEX until STATROW_COL_INDEX) {
      projectionArray(i - DELETE_MASK_COL_INDEX) = i
    }
    var i = numMetaColumns
    for (p <- projection) {
      projectionArray(i) = p
      i += 1
      for (d <- 0 until ColumnDelta.USED_MAX_DEPTH) {
        projectionArray(i) = ColumnDelta.deltaColumnIndex(p - 1 /* method expects 0-based */, d)
        i += 1
      }
    }
    projectionArray
  }

  private var currentStatsKey: ColumnFormatKey = _
  private val currentValueMap = LongObjectHashMap.withExpectedSize[AnyRef](8)

  private def fetchUsingGetAll(keys: Array[AnyRef]): Seq[(AnyRef, AnyRef)] = {
    val msg = new GetAllExecutorMessage(pr, keys, null, null, null, null,
      null, null, tx, null, false, false)
    val allMemberResults = GemFireResultSet.callGetAllExecutorMessage(msg).asScala
    allMemberResults.flatMap { case v: ListResultCollectorValue =>
      msg.getKeysPerMember(v.memberID).asScala.zip(
        v.resultOfSingleExecution.asInstanceOf[java.util.List[AnyRef]].asScala)

    }
  }

  override def hasNext: Boolean = statsRows.hasNext

  override def next(): RegionEntry = {
    currentValueMap.clear()
    val p = statsRows.next()
    currentStatsKey = p._1.asInstanceOf[ColumnFormatKey]
    NonLocalRegionEntry.newEntry(currentStatsKey, p._2, null, null)
  }

  override def getColumnValue(column: Int): AnyRef = {
    if (currentValueMap.size() == 0) {
      // fetch all the projected columns for current batch
      val fetchKeys = fullProjection.map(c =>
        new ColumnFormatKey(currentStatsKey.uuid, currentStatsKey.partitionId, c): AnyRef)
      fetchUsingGetAll(fetchKeys).foreach {
        case (k: ColumnFormatKey, v) => currentValueMap.justPut(k.columnIndex, v)
      }
    }
    currentValueMap.get(column)
  }

  override def close(): Unit = {
    currentStatsKey = null
    currentValueMap.clear()
  }
}

object StatsFilter extends Predicate[AnyRef] with Serializable {
  override def test(key: AnyRef): Boolean = key match {
    case k: ColumnFormatKey => k.columnIndex == STATROW_COL_INDEX
    case _ => false
  }
}
