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
package org.apache.spark.serializer

import io.snappydata.SnappyFunSuite


class PooledKryoSerializerSuite extends SnappyFunSuite {

  test("Test borrow-reset-release") {
    // Borrow from one thread
    val serzr = KryoSerializerPool.borrow()
    // Other thread might reset the pool by resetting the setDefaultClassLoader
    KryoSerializerPool.clear()
    // The 1st thread should not see any problem
    KryoSerializerPool.release(serzr)
  }
}
