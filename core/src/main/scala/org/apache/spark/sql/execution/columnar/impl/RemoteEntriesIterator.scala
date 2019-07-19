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

import java.util.Comparator
import java.util.function.Predicate

import scala.collection.AbstractIterator
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.internal.cache.store.SerializedDiskBuffer
import com.gemstone.gemfire.internal.cache.{NonLocalRegionEntry, PartitionedRegion, RegionEntry, TXStateInterface}
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdListResultCollector.ListResultCollectorValue
import com.pivotal.gemfirexd.internal.engine.distributed.message.GetAllExecutorMessage
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireResultSet
import org.eclipse.collections.api.block.procedure.Procedure
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap

import org.apache.spark.sql.execution.columnar.impl.ColumnFormatEntry._

/**
 * A [[ClusteredColumnIterator]] that fetches entries from a remote bucket.
 */
final class RemoteEntriesIterator(bucketId: Int, projection: Array[Int],
    pr: PartitionedRegion, tx: TXStateInterface) extends ClusteredColumnIterator {

  private type BatchStatsRows = (ColumnFormatKey, AnyRef, AnyRef)

  private val statsRows: Iterator[BatchStatsRows] = {
    val statsKeys = pr.getBucketKeys(bucketId, StatsFilter, false, tx).toArray
    // bring both the stats rows together for all batches
    val comparator = new Comparator[AnyRef] {
      override def compare(o1: AnyRef, o2: AnyRef): Int = {
        var k1: ColumnFormatKey = null
        var k2: ColumnFormatKey = null
        o1 match {
          case k: ColumnFormatKey => k1 = k; k2 = o2.asInstanceOf[ColumnFormatKey]
          case (k: ColumnFormatKey, _) => k1 = k; k2 = o2.asInstanceOf[(ColumnFormatKey, _)]._1
        }
        assert(k1.partitionId == bucketId)
        assert(k2.partitionId == bucketId)
        if (k1.uuid < k2.uuid) -1
        else if (k1.uuid > k2.uuid) 1
        // keep full stats before delta stats
        else java.lang.Long.signum(k2.columnIndex.toLong - k1.columnIndex.toLong)
      }
    }
    java.util.Arrays.sort(statsKeys, comparator)
    // get the stats rows using getAll (max 1000 at a time)
    new AbstractIterator[BatchStatsRows] {

      private var absoluteIndex: Int = _
      private var currentBatch: ArrayBuffer[BatchStatsRows] = _
      private var currentBatchIter: Iterator[BatchStatsRows] = Iterator.empty

      fetchNextBatch()

      private def fetchNextBatch(): Boolean = {
        if (absoluteIndex >= statsKeys.length) {
          currentBatch = null
          currentBatchIter = Iterator.empty
          return false
        }
        // check if 1000th entry marks a boundary (i.e. either both stats row
        // of same batch are included or neither are)
        var batchLastIndex = math.min(absoluteIndex + 1000, statsKeys.length)
        // if previous to lastKey is same UUID then can safely include both
        // else include only till previous to be on the safe side, but need
        // to do this only if: a) at least two keys in batch, b) batch has not reached end
        if (batchLastIndex > absoluteIndex + 1 && batchLastIndex < statsKeys.length) {
          val lastKey = statsKeys(batchLastIndex - 1).asInstanceOf[ColumnFormatKey]
          val lastButOneKey = statsKeys(batchLastIndex - 2).asInstanceOf[ColumnFormatKey]
          if (lastButOneKey.uuid != lastKey.uuid) batchLastIndex -= 1
        }
        val results = fetchUsingGetAll(
          java.util.Arrays.copyOfRange(statsKeys, absoluteIndex, batchLastIndex)).toArray
        absoluteIndex = batchLastIndex
        java.util.Arrays.sort(results.asInstanceOf[Array[AnyRef]], comparator)
        currentBatch = new ArrayBuffer[BatchStatsRows](1000)
        var i = 0
        while (i < results.length) {
          // check for two stats rows or only one by comparing UUIDs
          val (k1: ColumnFormatKey, v1) = results(i)
          var v2: AnyRef = null
          i += 1
          if (i < results.length) {
            val (k2: ColumnFormatKey, v) = results(i)
            if (k1.uuid == k2.uuid) {
              v2 = v
              i += 1
            }
          }
          currentBatch += ((k1, v1, v2))
        }
        currentBatchIter = currentBatch.iterator
        currentBatchIter.hasNext
      }

      override def hasNext: Boolean = currentBatchIter.hasNext || fetchNextBatch()

      override def next(): BatchStatsRows = {
        val result = currentBatchIter.next()
        if (!currentBatchIter.hasNext) fetchNextBatch()
        result
      }
    }
  }

  /**
   * Full projection including all of delta and meta-data columns (except base stats entry)
   */
  private val fullProjection = {
    // (DELTA_STATROW_COL_INDEX - DELETE_MASK_COL_INDEX) gives the number of meta-data
    // columns which are always fetched. This excludes stats rows (full and delta)
    // that have already been fetched separately, while includes the delete bitmask.
    // And for each projected column there is a base column and up-to USED_MAX_DEPTH deltas.
    val numMetaColumns = DELTA_STATROW_COL_INDEX - DELETE_MASK_COL_INDEX
    val projectionArray = new Array[Int](projection.length *
        (ColumnDelta.USED_MAX_DEPTH + 1) + numMetaColumns)
    for (i <- DELETE_MASK_COL_INDEX until DELTA_STATROW_COL_INDEX) {
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
  private var currentStatsValue: AnyRef = _
  private var currentDeltaStats: AnyRef = _
  private val currentValueMap = new IntObjectHashMap[AnyRef](8)

  private def fetchUsingGetAll(keys: Array[AnyRef]): Seq[(AnyRef, AnyRef)] = {
    val msg = new GetAllExecutorMessage(pr, keys, null, null, null, null,
      null, null, tx, null, false, false)
    val allMemberResults = GemFireResultSet.callGetAllExecutorMessage(msg).asScala
    allMemberResults.flatMap { case v: ListResultCollectorValue =>
      msg.getKeysPerMember(v.memberID).asScala.zip(
        v.resultOfSingleExecution.asInstanceOf[java.util.List[AnyRef]].asScala)
    }
  }

  private def releaseBuffer(v: AnyRef): Unit = v match {
    case s: SerializedDiskBuffer => s.release()
    case _ =>
  }

  private def releaseValues(): Unit = {
    if (!currentValueMap.isEmpty) {
      currentValueMap.forEachValue(new Procedure[AnyRef] {
        override def value(v: AnyRef): Unit = {
          releaseBuffer(v)
        }
      })
      currentValueMap.clear()
    }
    releaseBuffer(currentStatsValue)
    releaseBuffer(currentDeltaStats)
  }

  override def hasNext: Boolean = statsRows.hasNext

  override def next(): RegionEntry = {
    releaseValues()
    val p = statsRows.next()
    currentStatsKey = p._1
    currentStatsValue = p._2
    currentDeltaStats = p._3
    NonLocalRegionEntry.newEntry(currentStatsKey, currentStatsValue, null, null)
  }

  override def getColumnValue(column: Int): AnyRef = {
    if (column == DELTA_STATROW_COL_INDEX) return currentDeltaStats
    if (currentValueMap.isEmpty) {
      // fetch all the projected columns for current batch
      val fetchKeys = fullProjection.map(c =>
        new ColumnFormatKey(currentStatsKey.uuid, currentStatsKey.partitionId, c): AnyRef)
      fetchUsingGetAll(fetchKeys).foreach {
        case (k: ColumnFormatKey, v) => currentValueMap.put(k.columnIndex, v)
      }
    }
    currentValueMap.get(column)
  }

  override def close(): Unit = {
    currentStatsKey = null
    releaseValues()
  }
}

object StatsFilter extends Predicate[AnyRef] with Serializable {
  override def test(key: AnyRef): Boolean = key match {
    case k: ColumnFormatKey =>
      k.columnIndex == STATROW_COL_INDEX || k.columnIndex == DELTA_STATROW_COL_INDEX
    case _ => false
  }
}
