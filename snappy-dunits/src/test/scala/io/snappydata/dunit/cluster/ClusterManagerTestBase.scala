package io.snappydata.dunit.cluster

import java.io.File
import java.util.Properties

import com.gemstone.gemfire.internal.tools.gfsh.app.commands.value

import scala.collection.JavaConverters._

import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.{FabricService, TestUtil}
import dunit.{AvailablePortHelper, DistributedTestBase, Host, SerializableRunnable}
import io.snappydata.{Locator, Server, ServiceManager}
import org.slf4j.LoggerFactory

import org.apache.spark.scheduler.cluster.SnappyEmbeddedModeClusterManager
import org.apache.spark.sql.SnappyContext
import org.apache.spark.{SparkConf, SparkContext}

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
    // props.setProperty(Attribute.SYS_PERSISTENT_DIR, s)
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
    getClass.getMethod("stopSpark").invoke(null)
    Array(vm3, vm2, vm1, vm0).foreach(_.invoke(this.getClass, "stopAny"))
    getClass.getMethod("stopAny").invoke(null)
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

  def startSnappyLead(locatorPort: Int, props: Properties): Unit = {
    startSnappyLead(locatorPort, props, addUrlForHiveMetaStore = false)
  }

  /**
    * Start a snappy lead. This code starts a Spark server and at the same time
    * also starts a SparkContext and hence it kind of becomes lead. We will use
    * LeadImpl once the code for that is ready.
    *
    * Only a single instance of SnappyLead should be started.
    */
  def startSnappyLead(locatorPort: Int,
      props: Properties,
      addUrlForHiveMetaStore: Boolean): Unit = {
    assert(sc == null)
    props.setProperty("host-data", "false")
    props.setProperty("log-level", "fine")
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
      if (k.indexOf(".") < 0) {
        conf.set(io.snappydata.Constant.STORE_PROPERTY_PREFIX + k, v)
      }
      else {
        conf.set(k, v)
      }
    })
    if (addUrlForHiveMetaStore) {
      val snappydataurl = "jdbc:snappydata:;locators=localhost[" +
          locatorPort + "];route-query=false;user=HIVE_METASTORE;default-persistent=true"
      conf.set("gemfirexd.db.url", snappydataurl)
      conf.set("gemfirexd.db.driver", "com.pivotal.gemfirexd.jdbc.EmbeddedDriver")
    }
    logger.info(s"About to create SparkContext with conf \n" + conf.toDebugString)
    sc = new SparkContext(conf)
    logger.info("SparkContext CREATED, about to create SnappyContext.")
    snc = SnappyContext(sc)
    // Setting the default column batch size for tests to 3
    snc.setConf("spark.sql.inMemoryColumnarStorage.batchSize", "3")

    assert(ServiceManager.getServerInstance.status == FabricService.State.RUNNING)
    logger.info("SnappyContext CREATED successfully.")
    val lead: Server = ServiceManager.getServerInstance
    assert(lead.status == FabricService.State.RUNNING)
  }

  /**
    * Start a snappy server. Any number of snappy servers can be started.
    */
  def startSnappyServer(locatorPort: Int, props: Properties): Unit = {
    props.setProperty("locators", "localhost[" + locatorPort + ']')
    props.setProperty("log-level", "fine")
    val server: Server = ServiceManager.getServerInstance
    server.start(props)
    assert(server.status == FabricService.State.RUNNING)
  }

  def startNetServer(netPort: Int): Unit = {
    ServiceManager.getServerInstance.startNetworkServer("localhost", netPort, null)
    // ServiceManager.getServerInstance.startDRDAServer("localhost", netPort, null)
  }

  def stopSpark(): Unit = {
    // cleanup metastore
    val snc = SnappyContext()
    if (snc != null) {
      snc.catalog.getTables(None).foreach {
        case (tableName, false) =>
          snc.dropExternalTable(tableName, ifExists = true)
        case _ =>
      }
      SnappyContext.stop()
    }
    if (sc != null) {
      if (!sc.isStopped) sc.stop()
      sc = null
    }
  }

  def stopAny(): Unit = {
    val service = ServiceManager.currentFabricServiceInstance
    if (service != null) {
      service.stop(null)
    }
  }
}
