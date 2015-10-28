package io.snappydata.dunit.cluster

import java.io.File
import java.net.InetAddress
import java.util.Properties
import com.gemstone.gemfire.internal.{DistributionLocator, SocketCreator}
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore
import com.pivotal.gemfirexd.{FabricService, TestUtil}
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import dunit.{SerializableRunnable, Host, DistributedTestBase}
import io.snappydata.{Locator, Server, ServiceManager}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler.cluster.SnappyEmbeddedModeClusterManager

/**
 * Created by hemant on 19/10/15.
 */
class ClusterManagerTestBase(s: String) extends DistributedTestBase(s) {
  var props: Properties = null

  val host = Host.getHost(0);

  val vm0 = host.getVM(0);
  val vm1 = host.getVM(1);
  val vm2 = host.getVM(2);
  val vm3 = host.getVM(3);


  override
  def setUp(): Unit = {
    props = TestUtil.doCommonSetup(null)
    GemFireXDUtils.IS_TEST_MODE = true
    DistributedTestBase.invokeInLocator(new SerializableRunnable() {
      override def run(): Unit = {
        val loc: Locator = ServiceManager.getLocatorInstance

        loc.start("localhost", 0, new Properties())
        assert(ServiceManager.getLocatorInstance.status == FabricService.State.RUNNING)
      }
    })

  }

  override
  def tearDown2(): Unit = {
    GemFireXDUtils.IS_TEST_MODE = false
    DistributedTestBase.invokeInLocator(new SerializableRunnable() {
      override def run(): Unit = {
        val loc = ServiceManager.getLocatorInstance

        if (loc != null) {
          loc.stop(null)
        }
      }
    })
  }


}

/**
 * New utility methods would need to be added as and when corresponding snappy code gets added.
 */
class ClusterManagerTestUtils {

  /* SparkContext is initialized on the lead node and hence,
  this can be used only by jobs running on Lead node */
  var sc: SparkContext = null

  /**
   * Start a snappy lead. This code starts a Spark server and at the same time
   * also starts a SparkContext and hence it kind of becomes lead. We will use
   * LeadImpl once the code for that is ready.
   *
   * Only a single instance of SnappyLead should be started.
   */
  def startSnappyLead(): Unit = {
    assert(sc == null)
    val props = new Properties
    props.setProperty("host-data", "false")
    SparkContext.registerClusterManager(SnappyEmbeddedModeClusterManager)
    val conf: SparkConf = new SparkConf().setMaster("external:snappy").setAppName("myapp")
    new File("./" + "driver").mkdir()
    new File("./" + "driver/events").mkdir()

    val dataDirForDriver = new File("./" + "driver/data").getAbsolutePath
    val eventDirForDriver = new File("./" + "driver/events").getAbsolutePath
    conf.set("spark.local.dir", dataDirForDriver)
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", eventDirForDriver)
    sc = new SparkContext(conf)
    val localHost: InetAddress = SocketCreator.getLocalHost
    props.setProperty("locators", "localhost" + '[' + DistributionLocator.DEFAULT_LOCATOR_PORT
        + ']');
    val lead: Server = ServiceManager.getServerInstance
    lead.start(props)
    assert(ServiceManager.getServerInstance.status == FabricService.State.RUNNING)
  }

  /**
   * Stops sparkcontext and the server instance.
   */
  def stopSnappyLead(): Unit = {
    sc.stop
    sc = null
    val lead = ServiceManager.getServerInstance
    if (lead != null) {
      lead.stop(null)
    }
  }


  /**
   * Start a snappy server. Any number of snappy servers can be started.
   */
  def startSnappyServer(): Unit = {
    val props = new Properties
    val localHost: InetAddress = SocketCreator.getLocalHost
    props.setProperty("locators", "localhost" + '[' + DistributionLocator.DEFAULT_LOCATOR_PORT
        + ']');
    val server: Server = ServiceManager.getServerInstance

    server.start(props)

    val advisee = GemFireStore.getBootedInstance.getDistributionAdvisor.getAdvisee
    assert(ServiceManager.getServerInstance.status == FabricService.State.RUNNING)
  }

  def stopSnappyServer(): Unit = {
    val server = ServiceManager.getServerInstance

    if (server != null) {
      server.stop(null)
    }
  }

}

