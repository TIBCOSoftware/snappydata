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

import io.snappydata.test.dunit.DistributedTestBase.InitializeRun

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SnappySession


class MemoryManagerStatsSuite extends MemoryFunSuite {

  InitializeRun.setUp()

  test("Init stats") {
    val sparkSession = createSparkSession(1, 1, 2000000L)
    val snappySession = new SnappySession(sparkSession.sparkContext)

    val beforeTableSize = SparkEnv.get.memoryManager.storageMemoryUsed
    val memoryManager = SparkEnv.get.memoryManager
        .asInstanceOf[SnappyUnifiedMemoryManager]
    val stats = memoryManager.wrapperStats
    println(stats.getMaxStorageSize(false))
    println(stats.getStoragePoolSize(false))
  }

}
