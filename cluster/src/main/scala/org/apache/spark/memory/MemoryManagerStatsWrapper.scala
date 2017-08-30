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

import com.gemstone.gemfire.internal.snappy.memory.{MemoryManagerStats, MemoryManagerStatsOps}


class MemoryManagerStatsWrapper extends MemoryManagerStatsOps {

  private[memory] var stats: MemoryManagerStats = _

  def setMemoryManagerStats(mStats: MemoryManagerStats): Unit = {
    stats = mStats
  }

  override def incStoragePoolSize(offHeap: Boolean, delta: Long): Unit =
    if (stats ne null) stats.incStoragePoolSize(offHeap, delta)

  override def getStoragePoolSize(offHeap: Boolean): Long =
    if (stats ne null) stats.getStoragePoolSize(offHeap) else 0L

  override def decStoragePoolSize(offHeap: Boolean, delta: Long): Unit =
    if (stats ne null) stats.decStoragePoolSize(offHeap, delta)

  override def incExecutionPoolSize(offHeap: Boolean, delta: Long): Unit =
    if (stats ne null) stats.incExecutionPoolSize(offHeap, delta)

  override def decExecutionPoolSize(offHeap: Boolean, delta: Long): Unit =
    if (stats ne null) stats.decExecutionPoolSize(offHeap, delta)

  override def incStorageMemoryUsed(offHeap: Boolean, delta: Long): Unit =
    if (stats ne null) stats.incStorageMemoryUsed(offHeap, delta)

  override def decStorageMemoryUsed(offHeap: Boolean, delta: Long): Unit =
    if (stats ne null) stats.decStorageMemoryUsed(offHeap, delta)

  override def incExecutionMemoryUsed(offHeap: Boolean, delta: Long): Unit =
    if (stats ne null) stats.incExecutionMemoryUsed(offHeap, delta)

  override def decExecutionMemoryUsed(offHeap: Boolean, delta: Long): Unit =
    if (stats ne null) stats.decExecutionMemoryUsed(offHeap, delta)

  override def incNumFailedStorageRequest(offHeap: Boolean): Unit =
    if (stats ne null) stats.incNumFailedStorageRequest(offHeap)

  override def incNumFailedExecutionRequest(offHeap: Boolean): Unit =
    if (stats ne null) stats.incNumFailedExecutionRequest(offHeap)

  override def incFailedEvictionRequest(offHeap: Boolean): Unit =
    if (stats ne null) stats.incFailedEvictionRequest(offHeap)

  override def incMaxStorageSize(offHeap: Boolean, delta: Long): Unit =
    if (stats ne null) stats.incMaxStorageSize(offHeap, delta)

  override def getMaxStorageSize(offHeap: Boolean): Long =
    if (stats ne null) stats.getMaxStorageSize(offHeap) else 0L
}
