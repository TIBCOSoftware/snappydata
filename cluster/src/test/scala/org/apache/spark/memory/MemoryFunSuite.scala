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

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

import org.apache.spark.sql.{SnappyContext, SnappySession, SparkSession}

class MemoryFunSuite extends FunSuite with BeforeAndAfter with BeforeAndAfterAll {

  after {
    if (SnappyContext.globalSparkContext != null) {
      val snappySession = new SnappySession(SnappyContext.globalSparkContext)
      snappySession.dropTable("t1", true)
      SnappyContext.globalSparkContext.stop()
    }
  }

  //Only use if sure of the problem
  def assertApproximate(value1: Long, value2: Long, error :Int = 2): Unit = {
    if (value1 == value2) return
    if (Math.abs(value1 - value2) > (value2 * error) / 100) {
      throw new java.lang.AssertionError(s"assertion " +
          s"failed $value1 & $value2 are not within permissable limit")
    }
  }

  private[memory] def createSparkSession(memoryFraction: Double,
      storageFraction: Double,
      sparkMemory: Long = 1000,
      cachedBatchSize: Int = 5): SparkSession = {
    SparkSession
        .builder
        .appName(getClass.getName)
        .master("local[*]")
        .config(io.snappydata.Property.CachedBatchSize.name, cachedBatchSize)
        .config("spark.memory.fraction", memoryFraction)
        .config("spark.memory.storageFraction", storageFraction)
        .config("spark.testing.memory", sparkMemory)
        .config("spark.testing.reservedMemory", "0")
        .config("snappydata.store.critical-heap-percentage", "90")
        .config("spark.memory.manager", "org.apache.spark.memory.SnappyUnifiedMemoryManager")
        .getOrCreate
  }
}
