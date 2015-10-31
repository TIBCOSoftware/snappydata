package io.snappydata.dunit.cluster

import scala.Predef._

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.gemstone.gemfire.internal.cache.control.{HeapMemoryMonitor, InternalResourceManager}
import com.pivotal.gemfirexd.internal.engine.Misc

import org.apache.spark.SparkEnv
import org.apache.spark.storage.StorageLevel

/**
 * Created by shirishd on 19/10/15.
 */
class SnappyCriticalUpDUnitTest (s: String) extends ClusterManagerTestBase(s) {

  def testCriticalUp(): Unit = {
    vm1.invoke(this.getClass, "startSnappyServer")
    vm0.invoke(this.getClass, "startSnappyLead")
    vm2.invoke(this.getClass, "startSnappyServer")

    // Execute the job
    vm0.invoke(this.getClass, "runSparkJob")
    vm1.invoke(this.getClass, "raiseCriticalUpMemoryEvent")
    vm2.invoke(this.getClass, "raiseCriticalUpMemoryEvent")
    vm0.invoke(this.getClass, "runSparkJobWithCriticalUp")

    vm1.invoke(this.getClass, "assertShuffleMemoryManagerBehavior")
    vm2.invoke(this.getClass, "assertShuffleMemoryManagerBehavior")

    vm0.invoke(this.getClass, "stopSnappyLead")
    vm2.invoke(this.getClass, "stopSnappyServer")
    vm1.invoke(this.getClass, "stopSnappyServer")
  }

  def testEvictionUp(): Unit = {
    vm1.invoke(this.getClass, "startSnappyServer")
    vm0.invoke(this.getClass, "startSnappyLead")

    // Execute the job
    vm0.invoke(this.getClass, "runSparkJob2")
    vm1.invoke(this.getClass, "raiseEvictionUpMemoryEvent")
    vm0.invoke(this.getClass, "runSparkJobWithEvictionUp")

    vm0.invoke(this.getClass, "stopSnappyLead")
    vm1.invoke(this.getClass, "stopSnappyServer")

  }

}

object SnappyCriticalUpDUnitTest extends ClusterManagerTestUtils {

  def runSparkJob(): Unit = {
    val rdd1 = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8)).cache()
    rdd1.count
    assert(!sc.getRDDStorageInfo.isEmpty)
    rdd1.unpersist(true)
    assert(sc.getRDDStorageInfo.isEmpty)
  }

  def runSparkJobWithCriticalUp(): Unit = {
    val rdd2 = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8)).cache()
    rdd2.count
    assert(sc.getRDDStorageInfo.isEmpty)
    println(sc.getRDDStorageInfo.length)
  }

  def assertShuffleMemoryManagerBehavior(): Unit = {
    assert(SparkEnv.get.shuffleMemoryManager.tryToAcquire(1000) == 0)
  }

  def runSparkJob2(): Unit = {
    val rdd1 = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8)).persist(StorageLevel.MEMORY_ONLY)
    rdd1.count
    assert(!sc.getRDDStorageInfo.isEmpty)
    println("rdd1 " + rdd1.id)
  }

  def runSparkJobWithEvictionUp(): Unit = {
    val rdd2 = sc.makeRDD(Array(1)).persist(StorageLevel.MEMORY_ONLY)
    rdd2.count
    assert(!sc.getRDDStorageInfo.isEmpty)
    assert(sc.getRDDStorageInfo.size == 1)

    val rdd3 = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8)).persist(StorageLevel.MEMORY_ONLY)
    rdd3.count
    assert(!sc.getRDDStorageInfo.isEmpty)
    assert(sc.getRDDStorageInfo.size == 1)
  }

  def raiseCriticalUpMemoryEvent(): Unit = {
    println("About to raise CRITICAL UP event")
    val gfCache: GemFireCacheImpl = Misc.getGemFireCache
    val resMgr: InternalResourceManager = gfCache.getResourceManager
    resMgr.getHeapMonitor.setTestMaxMemoryBytes(100)
    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(90)
    resMgr.setCriticalHeapPercentage(90F)

    resMgr.getHeapMonitor().updateStateAndSendEvent(92);
    println("CRITICAL UP event sent")
  }

  def raiseEvictionUpMemoryEvent(): Unit = {
    println("About to raise EVICTION UP event")
    val gfCache: GemFireCacheImpl = Misc.getGemFireCache
    val resMgr: InternalResourceManager = gfCache.getResourceManager
    resMgr.getHeapMonitor.setTestMaxMemoryBytes(100)
    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(90)
    resMgr.setEvictionHeapPercentage(60)
    resMgr.getHeapMonitor().updateStateAndSendEvent(70);
    println("EVICTION UP event sent")

  }
}