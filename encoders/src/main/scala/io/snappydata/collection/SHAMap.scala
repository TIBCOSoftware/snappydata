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
package io.snappydata.collection

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl

final class SHAMap(initialCapacity: Int, valueSize: Int,
  maxCapacity: Int) extends ByteBufferHashMap(initialCapacity,0.75,
  0, valueSize, GemFireCacheImpl.getCurrentBufferAllocator,
  null,null,0L, maxCapacity) {

  override protected def handleExisting(mapKeyObject: AnyRef, mapKeyOffset: Long,
    valueOffset: Int): Int = {
    // Get the valueOffSet
   // (Platform.getLong(mapKeyObject, mapKeyOffset) >>> 32L).toInt
    valueOffset
  }

  override protected def handleNew(mapKeyObject: AnyRef, mapKeyOffset: Long,
    valueOffset: Int): Int = {
    // Read the value start offset before invoking handleNewInsert which may cause rehash
    // & make the mayKeyObject & mapKeyOffset invalid
   // val valueOffset = (Platform.getLong(mapKeyObject, mapKeyOffset) >>> 32L).toInt
    handleNewInsert()
    -1 * valueOffset
  }
}
