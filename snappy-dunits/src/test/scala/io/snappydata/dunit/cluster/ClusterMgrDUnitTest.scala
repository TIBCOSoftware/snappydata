package io.snappydata.dunit.cluster

import scala.math._


/**
 * Created by hemant on 16/10/15.
 */
class ClusterMgrDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  /**
   * This test starts a lead node and two server nodes. Executes a job.
   * Then stops the lead node and starts lead in another node and then executes
   * the same job.
   */
  def testMultipleDriver(): Unit = {
    // Lead is started before other servers are started.
    vm0.invoke(this.getClass, "startSnappyLead")
    vm1.invoke(this.getClass, "startSnappyServer")
    vm2.invoke(this.getClass, "startSnappyServer")

    // Execute the job
    vm0.invoke(this.getClass, "startSparkJob")
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
object ClusterMgrDUnitTest extends ClusterManagerTestUtils{

  def startSparkJob(): Unit = {
    val slices = 5
    val n = math.min(1000000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = sc.parallelize(1 until n, slices).map { i =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x*x + y*y < 1) 1 else 0
      }.reduce(_ + _)
    val pi = 4.0 * count / n
    assert(3.14 <= pi)
    assert(3.15 > pi)
  }
}
