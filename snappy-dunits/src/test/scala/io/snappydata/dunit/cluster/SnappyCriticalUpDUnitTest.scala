package io.snappydata.dunit.cluster

import scala.Predef._

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.gemstone.gemfire.internal.cache.control.{HeapMemoryMonitor, InternalResourceManager}
import com.pivotal.gemfirexd.internal.engine.Misc

import org.apache.spark.SparkEnv

/**
 * Created by shirishd on 19/10/15.
 */
class SnappyCriticalUpDUnitTest (s: String) extends ClusterManagerTestBase(s) {

  def testCriticalUp(): Unit = {
    // Lead is started before other servers are started.
    vm0.invoke(this.getClass, "startSnappyLead")
    vm1.invoke(this.getClass, "startSnappyServer")
    vm2.invoke(this.getClass, "startSnappyServer")

    // Execute the job
    vm0.invoke(this.getClass, "runSparkJob")
    vm1.invoke(this.getClass, "raiseMemoryEvent")
    vm2.invoke(this.getClass, "raiseMemoryEvent")
    vm0.invoke(this.getClass, "runSparkJobWithCriticalUp")

    vm1.invoke(this.getClass, "assertShuffleMemoryManagerBehavior")
    vm2.invoke(this.getClass, "assertShuffleMemoryManagerBehavior")

    vm0.invoke(this.getClass, "stopSnappyLead")
    vm1.invoke(this.getClass, "stopSnappyServer")
    vm2.invoke(this.getClass, "stopSnappyServer")
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
    //val snContext = org.apache.spark.sql.SnappyContext(sc)
    //raiseMemoryEvent(true)
    val rdd2 = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8)).cache()
    rdd2.count
    assert(sc.getRDDStorageInfo.isEmpty)
    println(sc.getRDDStorageInfo.length)
  }

  def assertShuffleMemoryManagerBehavior(): Unit = {
    assert(SparkEnv.get.shuffleMemoryManager.tryToAcquire(1000) == 0)
  }

  def raiseMemoryEvent(): Unit = {
    println("About to raise CRITICAL UP event")
    val gfCache: GemFireCacheImpl = Misc.getGemFireCache
    val resMgr: InternalResourceManager = gfCache.getResourceManager
    resMgr.getHeapMonitor.setTestMaxMemoryBytes(100)
    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(90)
    resMgr.setCriticalHeapPercentage(90F)

    resMgr.getHeapMonitor().updateStateAndSendEvent(92);
    println("CRITICAL UP event sent")

  }
}