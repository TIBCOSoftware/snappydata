package io.snappydata.dunit

import java.util.Properties

import scala.Predef._

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.gemstone.gemfire.internal.cache.control.{HeapMemoryMonitor, InternalResourceManager}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore
import dunit.{DistributedTestBase, Host}
import io.snappydata.ServiceManager

/**
 * Created by shirishd on 19/10/15.
 */
class SnappyCriticalUpDUnitTest (s: String) extends DistributedTestBase(s) {

  val host = Host.getHost(0);

  val vm0 = host.getVM(0);
  val vm1 = host.getVM(1);
  val vm2 = host.getVM(2);
  val vm3 = host.getVM(3);

  override
  def setUp(): Unit = {
    //super.setUp()
  }

  override
  def tearDown2(): Unit = {
  }
  
  def testCriticalUp(): Unit = {
    val locatorNetPort = 9999 //AvailablePortHelper.getRandomAvailableTCPPort
    val serverNetPort = 8888 //AvailablePortHelper.getRandomAvailableTCPPort
    val peerDiscoveryPort = 7777 //AvailablePortHelper.getRandomAvailableTCPPort

    val locatorArgs = new Array[AnyRef](3)
    locatorArgs(0) = "localhost"
    locatorArgs(1) = new Integer(locatorNetPort)
    locatorArgs(2) = new Integer(peerDiscoveryPort)

    vm1.invoke(this.getClass, "startLocator", locatorArgs)

    val driverArgs = new Array[AnyRef](1)
    //val locStr = InetAddress.getLocalHost.getHostAddress+"["+peerDiscoveryPort+"]"
    val locStr = "localhost[" + peerDiscoveryPort + "]"
    driverArgs(0) = locStr

    vm2.invoke(this.getClass, "startDriverApp", driverArgs)
  }

}

object SnappyCriticalUpDUnitTest {

  def helloWorld(): Unit = {
    hello("Hello World! " + this.getClass);
  }

  def hello(s: String): Unit = {
    println(s);
  }

  def startLocator(bindAddress: String, netport: Int, peerDiscoveryPort: Int): Unit = {
    val locatorService = ServiceManager.getLocatorInstance
    val bootProps = new Properties()
    bootProps.setProperty("persist-dd", "false")
    locatorService.start("localhost", peerDiscoveryPort, bootProps)
    locatorService.startNetworkServer("localhost", netport, bootProps)
    println("Loc vm type = " + GemFireStore.getBootedInstance.getMyVMKind)
    println("locator prop in loc = " + InternalDistributedSystem.getConnectedInstance.getConfig.getLocators)
  }

  def startDriverApp(locatorStr: String): Unit = {
    startSnappyLocalModeAndTestCiritcalUp(locatorStr)
    val dsys = InternalDistributedSystem.getConnectedInstance
    assert(dsys != null)
    println("Driver vm type = " + GemFireStore.getBootedInstance.getMyVMKind)
    println("locator prop in driver app = " + InternalDistributedSystem.getConnectedInstance.getConfig.getLocators)
  }

  private def startSnappyLocalModeAndTestCiritcalUp(locStr: String): Unit = {
    val setMaster: String = "local[6]"

    val conf = new org.apache.spark.SparkConf().setAppName("SnappyCriticalUpDUnitTest")
        .set("spark.logConf", "true")

    if (setMaster != null) {
      conf.setMaster(setMaster)
    }

    // Set the url from the locator
    val snappydataurl = "jdbc:snappydata:;locators=" + locStr + ";persist-dd=false;"
    conf.set("gemfirexd.db.url", snappydataurl)
    conf.set("gemfirexd.db.driver", "com.pivotal.gemfirexd.jdbc.EmbeddedDriver")


    val sc = new org.apache.spark.SparkContext(conf)
    val snContext = org.apache.spark.sql.SnappyContext(sc)
    snContext.sql("set spark.sql.shuffle.partitions=6")

    val props = Map(
      "url" -> snappydataurl,
      "poolImpl" -> "tomcat",
      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
      "user" -> "app",
      "password" -> "app"
    )

    val rdd1 = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8)).cache()
    rdd1.count
    assert(!sc.getRDDStorageInfo.isEmpty)
    rdd1.unpersist(true)
    assert(sc.getRDDStorageInfo.isEmpty)


    raiseMemoryEvent(true)
    val rdd2 = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8)).cache()
    rdd2.count
    assert(sc.getRDDStorageInfo.isEmpty)
    println(sc.getRDDStorageInfo.length)
  }

  private def raiseMemoryEvent(criticalUp: Boolean): Unit = {
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
