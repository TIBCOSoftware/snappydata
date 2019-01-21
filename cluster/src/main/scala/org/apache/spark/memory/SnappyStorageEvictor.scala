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

package org.apache.spark.memory


import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import com.gemstone.gemfire.cache.RegionDestroyedException
import com.gemstone.gemfire.internal.cache._
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceType
import com.gemstone.gemfire.internal.i18n.LocalizedStrings
import com.pivotal.gemfirexd.internal.engine.Misc

import org.apache.spark.Logging
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation


class SnappyStorageEvictor extends Logging {

  private def getAllRegionList(offHeap: Boolean,
      hasOffHeap: Boolean): ArrayBuffer[LocalRegion] = {
    val cache = GemFireCacheImpl.getExisting
    val allRegionList = new ArrayBuffer[LocalRegion]()
    val irm: InternalResourceManager = cache.getResourceManager
    for (listener <- irm.getResourceListeners(
      SnappyStorageEvictor.resourceType).asScala) listener match {
      case pr: PartitionedRegion =>
        if (includePartitionedRegion(pr, offHeap, hasOffHeap)) {
          allRegionList ++= pr.getDataStore.getAllLocalBucketRegions.asScala
        }
      // no off-heap local regions yet in SnappyData
      case lr: LocalRegion =>
        if (!offHeap && includeLocalRegion(lr)) {
          allRegionList += lr
        }
      case _ =>
    }
    if (SnappyStorageEvictor.MINIMUM_ENTRIES_PER_BUCKET > 0) {
      for (i <- (allRegionList.length - 1) to 0 by -1) allRegionList(i) match {
        case br: BucketRegion if br.getNumEntriesInVM <= SnappyStorageEvictor
            .MINIMUM_ENTRIES_PER_BUCKET => allRegionList.remove(i)
        case _ =>
      }
    }
    allRegionList
  }

  @throws(classOf[Exception])
  def evictRegionData(bytesRequired: Long, offHeap: Boolean): Long = {
    val cache = GemFireCacheImpl.getInstance()
    if (cache eq null) return 0L

    // check if offHeap has been configured
    val hasOffHeap = cache.getMemorySize > 0
    // nothing to be done for off-heap when no storage off-heap is present
    if (!hasOffHeap && offHeap) return 0L

    val stats = cache.getCachePerfStats
    stats.incEvictorJobsStarted()
    var totalBytesEvicted: Long = 0
    val regionSet = Random.shuffle(getAllRegionList(offHeap, hasOffHeap))
    val start = CachePerfStats.getStatTime
    try {
      while (regionSet.nonEmpty) {
        for (i <- (regionSet.length - 1) to 0 by -1) {
          val region = regionSet(i)
          try {
            val bytesEvicted = region.entries.asInstanceOf[AbstractLRURegionMap]
                .centralizedLruUpdateCallback(offHeap, true)
            if (bytesEvicted == 0) {
              regionSet.remove(i)
            } else {
              // for off-heap don't change on-heap pool sizes assuming
              // the on-heap eviction to be small (actual accounting of
              //   the reduction of on-heap data would already have been
              //   taken care of in the centralizedLruUpdateCallback)
              if (offHeap) {
                // off-heap is returned in MSB
                totalBytesEvicted += (bytesEvicted >>> 32L) & 0xffffffffL
              } else {
                totalBytesEvicted += bytesEvicted
              }
              if (totalBytesEvicted >= bytesRequired) {
                return totalBytesEvicted
              }
            }
          } catch {
            case rd: RegionDestroyedException =>
              cache.getCancelCriterion.checkCancelInProgress(rd)
            case e: Exception =>
              cache.getCancelCriterion.checkCancelInProgress(e)
              cache.getLoggerI18n.warning(LocalizedStrings.Eviction_EVICTOR_TASK_EXCEPTION,
                Array[AnyRef](e.getMessage), e)
          }
        }
      }
    } finally {
      if (start != 0L) {
        val end = CachePerfStats.getStatTime
        stats.incEvictWorkTime(end - start)
      }
      stats.incEvictorJobsCompleted()
    }
    totalBytesEvicted
  }

  protected def includePartitionedRegion(region: PartitionedRegion,
      offHeap: Boolean, hasOffHeap: Boolean): Boolean = {
    val hasLRU = (region.getEvictionAttributes.getAlgorithm.isLRUHeap
      && (region.getDataStore != null)
      && !region.getAttributes.getEnableOffHeapMemory && !region.isRowBuffer())
    if (hasOffHeap) {
      // when off-heap is enabled then all column tables use off-heap
      val regionPath = Misc.getFullTableNameFromRegionPath(region.getFullPath)
      if (offHeap) hasLRU && ColumnFormatRelation.isColumnTable(regionPath)
      else hasLRU && !ColumnFormatRelation.isColumnTable(regionPath)
    } else {
      assert(!offHeap,
        "unexpected invocation for hasOffHeap=false and offHeap=true")
      hasLRU
    }
  }

  protected def includeLocalRegion(region: LocalRegion): Boolean = {
    (region.getEvictionAttributes.getAlgorithm.isLRUHeap
      && !region.getAttributes.getEnableOffHeapMemory)
  }
}

object SnappyStorageEvictor {
  val MINIMUM_ENTRIES_PER_BUCKET: Int =
    Integer.getInteger("gemfire.HeapLRUCapacityController.inlineEvictionThreshold", 0)
  val resourceType = ResourceType.HEAP_MEMORY
}
