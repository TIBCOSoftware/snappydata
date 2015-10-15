package io.snappydata.dunit

import java.util.Properties

import com.pivotal.gemfirexd.{FabricService, TestUtil}
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import dunit.{Host, AvailablePortHelper, DistributedTestBase, VM}
import io.snappydata.{Locator, Lead, ServiceManager, Server}

/**
 * Created by amogh on 15/10/15.
 */
class SnappyBaseDUnitTest(s: String) extends DistributedTestBase(s) {

  var props: Properties = null

  val host = Host.getHost(0);

  val vm0 = host.getVM(0);
  val vm1 = host.getVM(1);
  val vm2 = host.getVM(2);

  override
  def setUp(): Unit = {
    props = TestUtil.doCommonSetup(null)
    GemFireXDUtils.IS_TEST_MODE = true
  }

  override
  def tearDown2(): Unit = {
  }

  /**
   * This test is not really making use of distributed capability of the framework here.
   * It just does what tests in ServerStartSuite.scala do, but in a single test. It has been added here as a starting point.
   *
   * These entities (lead, server, locator) do not really talk to each other in this test.
   * Each is started with mcast-port = 0.
   */
  def testSnappyEntitiesStartStop(): Unit = {
    val arg: Array[AnyRef] = Array(props)

    vm0.invoke(this.getClass, "startSnappyLead", arg)
    vm1.invoke(this.getClass, "startSnappyServer", arg)
    vm2.invoke(this.getClass, "startSnappyLocator", arg)

    vm0.invoke(this.getClass, "stopSnappyLead")
    vm1.invoke(this.getClass, "stopSnappyServer")
    vm2.invoke(this.getClass, "stopSnappyLocator")
  }
}

/**
 * New utility methods would need to be added as and when corresponding snappy code gets added.
 */
object SnappyBaseDUnitTest {

  def startSnappyLead(props: Properties): Unit = {
    val lead: Lead = ServiceManager.getLeadInstance

    lead.start(props)

    assert(ServiceManager.getLeadInstance.status == FabricService.State.RUNNING)
  }

  def stopSnappyLead(): Unit = {
    val lead = ServiceManager.getLeadInstance

    if (lead != null) {
      lead.stop(null)
    }
  }

  def startSnappyServer(props: Properties): Unit = {
    val server: Server = ServiceManager.getServerInstance

    server.start(props)

    assert(ServiceManager.getServerInstance.status == FabricService.State.RUNNING)
  }

  def stopSnappyServer(): Unit = {
    val server = ServiceManager.getServerInstance

    if (server != null) {
      server.stop(null)
    }
  }

  def startSnappyLocator(props: Properties): Unit = {
    val loc: Locator = ServiceManager.getLocatorInstance

    loc.start("localhost", AvailablePortHelper.getRandomAvailableTCPPort, props)

    assert(ServiceManager.getLocatorInstance.status == FabricService.State.RUNNING)
  }

  def stopSnappyLocator(): Unit = {
    val loc = ServiceManager.getLocatorInstance

    if (loc != null) {
      loc.stop(null)
    }
  }

}