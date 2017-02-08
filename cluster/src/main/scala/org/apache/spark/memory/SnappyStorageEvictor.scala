/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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


import com.gemstone.gemfire.cache.RegionDestroyedException
import com.gemstone.gemfire.internal.i18n.LocalizedStrings

import scala.collection.JavaConversions._
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceType
import com.gemstone.gemfire.internal.cache._
import org.apache.spark.Logging


class SnappyStorageEvictor extends Logging{

  private def getAllRegionList: Seq[LocalRegion] = {
    val cache = GemFireCacheImpl.getExisting
    val allRegionList = new java.util.ArrayList[LocalRegion]()
    val irm: InternalResourceManager = cache.getResourceManager
    import scala.collection.JavaConversions._
    for (listener <- irm.getResourceListeners(SnappyStorageEvictor.resourceType)) {
      if (listener.isInstanceOf[PartitionedRegion]) {
        val pr: PartitionedRegion = listener.asInstanceOf[PartitionedRegion]
        if (includePartitionedRegion(pr)) {
          allRegionList.addAll(pr.getDataStore.getAllLocalBucketRegions)
        }
      }
      else if (listener.isInstanceOf[LocalRegion]) {
        val lr: LocalRegion = listener.asInstanceOf[LocalRegion]
        if (includeLocalRegion(lr)) {
          allRegionList.add(lr)
        }
      }
    }
    if (SnappyStorageEvictor.MINIMUM_ENTRIES_PER_BUCKET > 0) {
      val iter: Iterator[LocalRegion] = allRegionList.iterator
      while (iter.hasNext) {
        val lr: LocalRegion = iter.next
        if (lr.isInstanceOf[BucketRegion]) {
          if ((lr.asInstanceOf[BucketRegion]).getNumEntriesInVM <= SnappyStorageEvictor.MINIMUM_ENTRIES_PER_BUCKET) {
            iter.remove
          }
        }
      }
    }
    return allRegionList
  }

  @throws(classOf[Exception])
  def evictRegionData(bytesRequired: Long): Long = {
    val cache = GemFireCacheImpl.getExisting
    cache.getCachePerfStats.incEvictorJobsStarted
    var bytesEvicted: Long = 0
    var totalBytesEvicted: Long = 0
    val regionSet = getAllRegionList
    try {
      while (true) {
        cache.getCachePerfStats
        val start: Long = CachePerfStats.getStatTime
        if (regionSet.isEmpty) {
          return 0;
        }
        val iter: Iterator[LocalRegion] = regionSet.iterator
        while (iter.hasNext) {
          val region: LocalRegion = iter.next
          try {
            bytesEvicted = (region.entries.asInstanceOf[AbstractLRURegionMap]).centralizedLruUpdateCallback
            if (bytesEvicted == 0) {
              iter.remove
            }
            totalBytesEvicted += bytesEvicted
            if (totalBytesEvicted >= bytesRequired) {
              return totalBytesEvicted
            }
          }
          catch {
            case rd: RegionDestroyedException => {
              cache.getCancelCriterion.checkCancelInProgress(rd)
            }
            case e: Exception => {
              cache.getCancelCriterion.checkCancelInProgress(e)
              cache.getLoggerI18n.warning(LocalizedStrings.Eviction_EVICTOR_TASK_EXCEPTION, Array[AnyRef](e.getMessage), e)
            }
          } finally {
            cache.getCachePerfStats
            val end: Long = CachePerfStats.getStatTime
            cache.getCachePerfStats.incEvictWorkTime(end - start)
          }
        }
      }
    } finally {
      cache.getCachePerfStats.incEvictorJobsCompleted
    }
    return totalBytesEvicted
  }

  protected def includePartitionedRegion(region: PartitionedRegion): Boolean = {
    return (region.getEvictionAttributes.getAlgorithm.isLRUHeap
      && (region.getDataStore != null)
      && !region.getAttributes.getEnableOffHeapMemory)
  }

  protected def includeLocalRegion(region: LocalRegion): Boolean = {
    return (region.getEvictionAttributes.getAlgorithm.isLRUHeap
      && !region.getAttributes.getEnableOffHeapMemory)
  }


}

object SnappyStorageEvictor{
  val MINIMUM_ENTRIES_PER_BUCKET: Int = Integer.getInteger("gemfire.HeapLRUCapacityController.inlineEvictionThreshold", 0)
  val resourceType = ResourceType.HEAP_MEMORY
}
