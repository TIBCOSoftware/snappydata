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
package io.snappydata.collection

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.gemstone.gemfire.internal.shared.BufferAllocator

import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String
final class SHAMap(valueSize: Int) extends ByteBufferHashMap(128, 0.6, 0, valueSize,
  GemFireCacheImpl.getCurrentBufferAllocator, null, null, 0L) {

  override protected def handleExisting(mapKeyObject: AnyRef, mapKeyOffset: Long): Int = {
    // Get the valueOffSet
    Platform.getInt(mapKeyObject, mapKeyOffset)
  }

  override protected def handleNew(mapKeyObject: AnyRef, mapKeyOffset: Long): Int = {
    handleNewInsert()
    0
  }
}
