/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql.store

import scala.concurrent.duration.{FiniteDuration, MINUTES}

import io.snappydata.cluster.ClusterManagerTestBase

import org.apache.spark.sql.SnappyContext

/**
 * SortedColumnTests and SortedColumnPerformanceTests in DUnit.
 */
class SortedColumnDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  def disabled_testBasicInsert(): Unit = {
    val snc = SnappyContext(sc).snappySession
    val colTableName = "colDeltaTable"
    val numElements = 551
    val numBuckets = 2

    SortedColumnTests.verfiyInsertDataExists(snc, numElements)
    SortedColumnTests.verfiyUpdateDataExists(snc, numElements)
    SortedColumnTests.testBasicInsert(snc, colTableName, numBuckets, numElements)
  }

  def disabled_testInsertPerformance() {
    val snc = SnappyContext(sc).snappySession
    val colTableName = "colDeltaTable"
    val numElements = 9999551
    val numBuckets = SortedColumnPerformanceTests.cores
    val numIters = 2

    SortedColumnPerformanceTests.benchmarkInsert(snc, colTableName, numBuckets, numElements,
      numIters, "insert")
  }

  def disabled_testPointQueryPerformance() {
    val snc = SnappyContext(sc).snappySession
    val colTableName = "colDeltaTable"
    val numElements = 999551
    val numBuckets = SortedColumnPerformanceTests.cores
    val numIters = 100
    SortedColumnPerformanceTests.benchmarkQuery(snc, colTableName, numBuckets, numElements,
      numIters, "PointQuery", numTimesInsert = 10,
      doVerifyFullSize = true)(SortedColumnPerformanceTests.executeQuery_PointQuery_mt)
    // while (true) {}
  }

  def testPointQueryPerformanceMultithreaded() {
    val snc = SnappyContext(sc).snappySession
    val colTableName = "colDeltaTable"
    val numElements = 999551
    val numBuckets = SortedColumnPerformanceTests.cores
    val numIters = 100
    val totalNumThreads = SortedColumnPerformanceTests.cores
    val totalTime: FiniteDuration = new FiniteDuration(5, MINUTES)
    SortedColumnPerformanceTests.benchmarkQuery(snc, colTableName, numBuckets, numElements,
      numIters, "PointQuery multithreaded", numTimesInsert = 10, isMultithreaded = true,
      doVerifyFullSize = false, totalThreads = totalNumThreads,
      runTime = totalTime)(SortedColumnPerformanceTests.executeQuery_PointQuery_mt)
    // while (true) {}
  }

  def disabled_testRangeQueryPerformance() {
    val snc = SnappyContext(sc).snappySession
    val colTableName = "colDeltaTable"
    val numElements = 999551
    val numBuckets = SortedColumnPerformanceTests.cores
    val numIters = 21
    SortedColumnPerformanceTests.benchmarkQuery(snc, colTableName, numBuckets, numElements,
      numIters, "RangeQuery", numTimesInsert = 10,
      doVerifyFullSize = true)(SortedColumnPerformanceTests.executeQuery_RangeQuery_mt)
    // while (true) {}
  }
}
