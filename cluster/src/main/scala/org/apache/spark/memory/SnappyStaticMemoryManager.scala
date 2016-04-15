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

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.storage.{BlockStatus, BlockId}

private[spark] class SnappyStaticMemoryManager(
    override val conf: SparkConf,
    val maxExecutionMemory: Long,
    override val maxStorageMemory: Long,
    numCores: Int)
    extends StaticMemoryManager(conf, maxExecutionMemory,
      maxStorageMemory, numCores) {

  def this(conf: SparkConf, numCores: Int) {
    this(conf,
      StaticMemoryManager.getMaxExecutionMemory(conf),
      StaticMemoryManager.getMaxStorageMemory(conf),
      numCores)
  }

  override def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long = synchronized {
    if (SnappyMemoryUtils.isCriticalUp || SnappyMemoryUtils.isEvictionUp) {
      0
    } else {
      super.acquireExecutionMemory(numBytes, taskAttemptId , memoryMode)
    }
  }

  override def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean = synchronized {
    if (SnappyMemoryUtils.isCriticalUp || SnappyMemoryUtils.isEvictionUp) {
      false
    } else {
      super.acquireStorageMemory(blockId, numBytes, evictedBlocks)
    }
  }
}
