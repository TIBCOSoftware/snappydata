package io.snappydata.dunit.cluster

import java.io.File
import java.sql.DriverManager
import java.util.Properties

import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore
import com.pivotal.gemfirexd.{Attribute, DistributedSQLTestBase, FabricService, TestUtil}
import dunit.{AvailablePortHelper, DistributedTestBase, Host, SerializableRunnable}
import io.snappydata.{Locator, Server, ServiceManager}
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup
import org.apache.spark.scheduler.cluster.SnappyEmbeddedModeClusterManager
import org.apache.spark.sql.SnappyContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * Base class for tests using Snappy ClusterManager. New utility methods
 * would need to be added as and when corresponding snappy code gets added.
 *
 * @author hemant
 */
class ClusterManagerTestBase(s: String) extends DistributedTestBase(s) {

  val props: Properties = new Properties()

  val host = Host.getHost(0)
  val vm0 = host.getVM(0)
  val vm1 = host.getVM(1)
  val vm2 = host.getVM(2)
  val vm3 = host.getVM(3)

  final def locatorPort: Int = DistributedTestBase.getDUnitLocatorPort
  protected final def startArgs =
    Array(locatorPort, props).asInstanceOf[Array[AnyRef]]

  val locatorNetPort: Int = 0
  val locatorNetProps = new Properties()

  override def setUp(): Unit = {
    props.setProperty(Attribute.SYS_PERSISTENT_DIR, s)
    TestUtil.currentTest = getName
    TestUtil.currentTestClass = getTestClass
    TestUtil.skipDefaultPartitioned = true
    TestUtil.doCommonSetup(props)
    GemFireXDUtils.IS_TEST_MODE = true

    val locPort = locatorPort
    val locNetPort = locatorNetPort
    val locNetProps = locatorNetProps
    DistributedTestBase.invokeInLocator(new SerializableRunnable() {
      override def run(): Unit = {
        val loc: Locator = ServiceManager.getLocatorInstance

        if (loc.status != FabricService.State.RUNNING) {
          loc.start("localhost", locPort, locNetProps)
        }
        if (locNetPort > 0) {
          loc.startNetworkServer("localhost", locNetPort, locNetProps)
        }
        assert(loc.status == FabricService.State.RUNNING)
      }
    })
  }

  override def tearDown2(): Unit = {
    GemFireXDUtils.IS_TEST_MODE = false
    Array(vm3, vm2, vm1, vm0).foreach(_.invoke(this.getClass, "stopSpark"))
    Array(vm3, vm2, vm1, vm0).foreach(_.invoke(this.getClass, "stopAny"))
    props.clear()
    val locNetPort = locatorNetPort
    DistributedTestBase.invokeInLocator(new SerializableRunnable() {
      override def run(): Unit = {
        if (locNetPort > 0) {
          val loc = ServiceManager.getLocatorInstance
          if (loc != null) {
            loc.stopAllNetworkServers()
          }
        }
      }
    })
  }
}

object ClusterManagerTestBase {

  /** The fixed port on which Snappy locator is running for all dunit tests. */
  final val locatorPort = AvailablePortHelper.getRandomAvailableTCPPort
}

/**
 * New utility methods would need to be added as and when corresponding snappy code gets added.
 */
class ClusterManagerTestUtils {
  val logger = LoggerFactory.getLogger(getClass)

  /* SparkContext is initialized on the lead node and hence,
  this can be used only by jobs running on Lead node */
  var sc: SparkContext = _

  var snc: SnappyContext = _

  /**
   * Start a snappy lead. This code starts a Spark server and at the same time
   * also starts a SparkContext and hence it kind of becomes lead. We will use
   * LeadImpl once the code for that is ready.
   *
   * Only a single instance of SnappyLead should be started.
   */
  def startSnappyLead(locatorPort: Int, props: Properties): Unit = {
    assert(sc == null)
    props.setProperty("host-data", "false")
    SparkContext.registerClusterManager(SnappyEmbeddedModeClusterManager)
    val conf: SparkConf = new SparkConf().setMaster(s"snappydata://localhost[$locatorPort]")
        .setAppName("myapp")


    new File("./" + "driver").mkdir()
    new File("./" + "driver/events").mkdir()

    val dataDirForDriver = new File("./" + "driver/data").getAbsolutePath
    val eventDirForDriver = new File("./" + "driver/events").getAbsolutePath
    conf.set("spark.local.dir", dataDirForDriver)
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", eventDirForDriver)
    props.asScala.foreach({ case (k, v) =>
      conf.set(io.snappydata.Prop.propPrefix + k, v)
    })
    logger.info(s"About to create SparkContext with conf ${conf}")
    sc = new SparkContext(conf)
    logger.info("SparkContext CREATED, about to create SnappyContext.")
    snc = SnappyContext(sc)
    logger.info("SnappyContext CREATED successfully.")
//    props.setProperty("locators", "localhost[" + locatorPort + ']')
//    val lead: Server = ServiceManager.getServerInstance
//    lead.start(props)
//    assert(lead.status == FabricService.State.RUNNING)
  }

  /**
   * Start a snappy server. Any number of snappy servers can be started.
   */
  def startSnappyServer(locatorPort: Int, props: Properties): Unit = {
    props.setProperty("locators", "localhost[" + locatorPort + ']')
    val server: Server = ServiceManager.getServerInstance

    server.start(props)

    assert(server.status == FabricService.State.RUNNING)
  }

  def stopSpark(): Unit = {
    SnappyContext.stop()
    if (snc != null) {
      snc = null
    }
    if (sc != null) {
      if (!sc.isStopped) sc.stop()
      sc = null
    }
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
