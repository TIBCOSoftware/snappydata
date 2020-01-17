/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

final class DirectStringConsumer(memoryManager: TaskMemoryManager, pageSize: Int)
    extends MemoryConsumer(memoryManager, pageSize, MemoryMode.OFF_HEAP) {

  def this(memoryManager: TaskMemoryManager) = this(memoryManager, 8)

  override def spill(size: Long, trigger: MemoryConsumer): Long = 0L

  def copyUTF8String(s: UTF8String): UTF8String = {
    if ((s ne null) && (s.getBaseObject ne null)) {
      val size = s.numBytes()
      val page = taskMemoryManager.allocatePage(Math.max(pageSize, size), this)
      if ((page ne null) && page.size >= size) {
        used += page.size
        val ds = UTF8String.fromAddress(null, page.getBaseOffset, size)
        Platform.copyMemory(s.getBaseObject, s.getBaseOffset, null, ds.getBaseOffset, size)
        return ds
      } else if (page ne null) {
        taskMemoryManager.freePage(page, this)
      }
    }
    s
  }
}
