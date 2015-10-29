package io.snappydata.dunit.cluster

import dunit.AvailablePortHelper

/**
 * Created by kneeraj on 29/10/15.
 */
class QueryRoutingDUnitTest(val s: String) extends ClusterManagerTestBase(s) {

  override def setUp(): Unit = {
    ClusterManagerTestBase.locatorNetPort = AvailablePortHelper.getRandomAvailableTCPPort
    super.setUp()
  }

  private val servPort1 = AvailablePortHelper.getRandomAvailableTCPPort
  private val servPort2 = AvailablePortHelper.getRandomAvailableTCPPort

  def testQueryRouting(): Unit = {
    // Lead is started before other servers are started.
    vm0.invoke(this.getClass, "startSnappyLead")
    vm1.invoke(this.getClass, "startSnappyServer", Array.fill[AnyRef](1)(Integer.valueOf(servPort1)))
    vm2.invoke(this.getClass, "startSnappyServer", Array.fill[AnyRef](1)(Integer.valueOf(servPort2)))

    // Execute the job
    vm0.invoke(this.getClass, "createTablesAndInsertData")
    Thread.sleep(10000)

    // Stop the lead node
    vm0.invoke(this.getClass, "stopSnappyLead")
    Thread.sleep(5000)

    // Start the lead node in another JVM. The executors should
    // connect with this new lead.
    // In this case servers are already running and a lead comes
    // and join
    vm3.invoke(this.getClass, "startSnappyLead")
    vm3.invoke(this.getClass, "startSparkJob")
    Thread.sleep(10000)

    // Stop everything.
    vm3.invoke(this.getClass, "stopSnappyLead")
    vm2.invoke(this.getClass, "stopSnappyServer")
    vm1.invoke(this.getClass, "stopSnappyServer")
  }
}

/**
 * Since this object derives from ClusterManagerTestUtils
 */
object QueryRoutingDUnitTest extends ClusterManagerTestUtils {
  def createTablesAndInsertData(): Unit = {
    val slices = 5
    val n = math.min(1000000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = sc.parallelize(1 until n, slices).map { i =>
        val x = 2 - 1
        val y =  2 - 1
        if (x*x + y*y < 1) 1 else 0
      }.reduce(_ + _)
    val pi = 4.0 * count / n
    assert(3.14 <= pi)
    assert(3.15 > pi)
  }
}

