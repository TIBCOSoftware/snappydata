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
package io.snappydata.cluster

import scala.Predef._

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.gemstone.gemfire.internal.cache.control.{HeapMemoryMonitor, InternalResourceManager}
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.ServiceManager

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SnappyContext
import org.apache.spark.storage.{RDDInfo, StorageLevel}

class SnappyResourceEventsDUnitTest (s: String) extends ClusterManagerTestBase(s) {

  import SnappyResourceEventsDUnitTest._

  override def tearDown2(): Unit = {
    resetGFResourceManager()
    Array(vm3, vm2, vm1, vm0).foreach(_.invoke(this.getClass,
      "resetGFResourceManager"))
    super.tearDown2()
  }

  def _testEvictionUp(): Unit = {
    // Execute the job
    runSparkJob()
    vm0.invoke(this.getClass, "raiseEvictionUpMemoryEvent")
    vm1.invoke(this.getClass, "raiseEvictionUpMemoryEvent")
    vm2.invoke(this.getClass, "raiseEvictionUpMemoryEvent")
    runSparkJobAfterThresholdBreach()
  }

  def _testCriticalUp(): Unit = {
    // Execute the job
    runSparkJob()
    vm0.invoke(this.getClass, "raiseCriticalUpMemoryEvent")
    vm1.invoke(this.getClass, "raiseCriticalUpMemoryEvent")
    vm2.invoke(this.getClass, "raiseCriticalUpMemoryEvent")
    runSparkJobAfterThresholdBreach()
  }

  def testDummy(): Unit = {
    // Dummy test. Does not want to delete this class as some test codes can be reused later.
  }

 }

object SnappyResourceEventsDUnitTest {

  private def sc = SnappyContext.globalSparkContext

  def runSparkJob(): Unit = {
    val rdd1 = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)).cache()
    println(rdd1.count())
    assert(!sc.getRDDStorageInfo.isEmpty)
  }

  def getInMemorySizeForCachedRDDs: Long = {
    val rddInfo: Array[RDDInfo] = sc.getRDDStorageInfo
    var sum = 0L
    for (i <- rddInfo.indices) {
      sum = sum + rddInfo(i).memSize
    }
    sum
  }

  def runSparkJobAfterThresholdBreach(): Unit = {
    val sum1: Long = getInMemorySizeForCachedRDDs
    println("1. cached rdd mem size before caching rdd when critical or eviction up = " + sum1)

    val rdd2 = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)).cache()
    println(rdd2.count())
    val sum2: Long = getInMemorySizeForCachedRDDs
    println("2. cached rdd mem size after caching first rdd when critical or eviction up = " + sum2)
    // make sure that after eviction up new rdd being cached does not result in
    // increased memory usage
    assert(!(sum2 > sum1), s"sum1 = $sum1, sum2 = $sum2")

    val rdd3 = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)).cache()
    println(rdd3.count())
    val sum3: Long = getInMemorySizeForCachedRDDs
    println("3. cached rdd mem size after caching second rdd when critical or eviction up = " + sum3)
    // make sure that after eviction up new rdd being cached does not result in
    // increased memory usage
    assert(!(sum3 > sum2), s"sum2 = $sum2, sum3 = $sum3")
  }

  def raiseCriticalUpMemoryEvent(): Unit = {
    println("About to raise CRITICAL UP event")
    val gfCache: GemFireCacheImpl = Misc.getGemFireCache
    val resMgr: InternalResourceManager = gfCache.getResourceManager
    HeapMemoryMonitor.setTestDisableMemoryUpdates(true)
    resMgr.getHeapMonitor.setTestMaxMemoryBytes(100)
    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(92)
    resMgr.setCriticalHeapPercentage(90F)

    resMgr.getHeapMonitor.updateStateAndSendEvent(92)
    println("CRITICAL UP event sent")
  }

  def raiseEvictionUpMemoryEvent(): Unit = {
    println("About to raise EVICTION UP event")
    val gfCache: GemFireCacheImpl = Misc.getGemFireCache
    val resMgr: InternalResourceManager = gfCache.getResourceManager
    HeapMemoryMonitor.setTestDisableMemoryUpdates(true)
    resMgr.getHeapMonitor.setTestMaxMemoryBytes(100)
    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(90)
    resMgr.setEvictionHeapPercentage(40F)
    resMgr.getHeapMonitor.updateStateAndSendEvent(85)
    println("EVICTION UP event sent")
  }

  def resetGFResourceManager(): Unit = {
    val service = ServiceManager.currentFabricServiceInstance
    if (service != null) {
      val gfCache: GemFireCacheImpl = Misc.getGemFireCacheNoThrow
      if (gfCache != null) {
        val resMgr: InternalResourceManager = gfCache.getResourceManager
        resMgr.getHeapMonitor.setTestMaxMemoryBytes(0)
        resMgr.getHeapMonitor.updateStateAndSendEvent(10)
      }
    }
  }
}
