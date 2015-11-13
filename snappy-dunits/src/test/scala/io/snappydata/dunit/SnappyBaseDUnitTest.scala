package io.snappydata.dunit

import java.sql.DriverManager
import java.util.Properties

import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore
import com.pivotal.gemfirexd.{Attribute, FabricService}
import dunit.{AvailablePortHelper, DistributedTestBase, Host}
import io.snappydata.{Lead, Locator, Server, ServiceManager}
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup

/**
 * Created by amogh on 15/10/15.
 */
class SnappyBaseDUnitTest(s: String) extends DistributedTestBase(s) {

  import SnappyBaseDUnitTest._

  val props: Properties = new Properties()

  val host = Host.getHost(0)
  val vm0 = host.getVM(0)
  val vm1 = host.getVM(1)
  val vm2 = host.getVM(2)

  override
  def setUp(): Unit = {
    super.setUp()
    props.setProperty(Attribute.SYS_PERSISTENT_DIR, "basetest")
    props.setProperty("mcast-port", "0")
    GemFireXDUtils.IS_TEST_MODE = true
  }

  override
  def tearDown2(): Unit = {
    super.tearDown2()
    props.clear()
    Array(vm0, vm1, vm2).foreach(_.invoke(this.getClass, "stopAny"))
    GemFireXDUtils.IS_TEST_MODE = false
  }


  def testHelloWorld(): Unit = {
    helloWorld()
  }

  /**
   * This test is not really making use of distributed capability of the framework here.
   * It just does what tests in ServerStartSuite.scala do, but in a single test. It has been added here as a starting point.
   *
   * These entities (lead, server, locator) do not really talk to each other in this test.
   * Each is started with mcast-port = 0.
   */
  def _testSnappyEntitiesStartStop(): Unit = {
    val arg: Array[AnyRef] = Array(props)

    vm2.invoke(this.getClass, "startSnappyLocator", arg)
    vm0.invoke(this.getClass, "startSnappyLead", arg)
    vm1.invoke(this.getClass, "startSnappyServer", arg)
  }
}

/**
 * New utility methods would need to be added as and when corresponding snappy code gets added.
 */
object SnappyBaseDUnitTest {

  def helloWorld(): Unit = {
    println("Hello World! " + this.getClass)
  }

  def startSnappyLead(props: Properties): Unit = {
    val lead: Lead = ServiceManager.getLeadInstance

    lead.start(props)

    assert(ServiceManager.getLeadInstance.status == FabricService.State.RUNNING)
  }

  def startSnappyServer(props: Properties): Unit = {
    val server: Server = ServiceManager.getServerInstance

    server.start(props)

    assert(ServiceManager.getServerInstance.status == FabricService.State.RUNNING)
  }

  def startSnappyLocator(props: Properties): Unit = {
    val loc: Locator = ServiceManager.getLocatorInstance

    loc.start("localhost", AvailablePortHelper.getRandomAvailableTCPPort, props)

    assert(ServiceManager.getLocatorInstance.status == FabricService.State.RUNNING)
  }

  def stopAny(): Unit = {
    val service = ServiceManager.currentFabricServiceInstance
    if (service != null) {
      // cleanup the database objects first
      val store: GemFireStore = GemFireStore.getBootedInstance
      if (store != null && Misc.getGemFireCacheNoThrow != null
          && GemFireXDUtils.getMyVMKind.isAccessorOrStore) {
        val conn = DriverManager.getConnection("jdbc:snappydata:;")
        CleanDatabaseTestSetup.cleanDatabase(conn, false)
        conn.close()
      }
      service.stop(null)
    }
  }
}
