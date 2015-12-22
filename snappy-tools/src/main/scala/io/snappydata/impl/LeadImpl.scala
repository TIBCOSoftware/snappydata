package io.snappydata.impl

import java.sql.SQLException
import java.util.Properties
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._

import akka.actor.ActorSystem
import com.gemstone.gemfire.distributed.internal.DistributionConfig
import com.gemstone.gemfire.distributed.internal.locks.{DLockService, DistributedMemberLock}
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.pivotal.gemfirexd.FabricService.State
import com.pivotal.gemfirexd.NetworkInterface
import com.pivotal.gemfirexd.internal.engine.store.ServerGroupUtils
import com.typesafe.config.{Config, ConfigFactory}
import io.snappydata._
import spark.jobserver.JobServer

import org.apache.spark.scheduler.cluster.SnappyEmbeddedModeClusterManager
import org.apache.spark.sql.SnappyContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

class LeadImpl extends ServerImpl with Lead with Logging {

  self =>

  private val LOCK_SERVICE_NAME = "__PRIMARY_LEADER_LS"

  private val bootProperties = new Properties()

  private lazy val dls = {

    val gfCache = GemFireCacheImpl.getInstance

    if (gfCache == null || gfCache.isClosed) {
      throw new Exception("GemFire Cache not initialized")
    }

    val dSys = gfCache.getDistributedSystem

    DLockService.create(LOCK_SERVICE_NAME, dSys, true, true, true)
  }

  private var sparkContext: SparkContext = _

  private val latch = new CountDownLatch(1)
  private var notificationCallback: (() => Unit) = _
  private lazy val primaryLeadNodeWaiter = scheduleWaitForPrimaryDeparture()

  private lazy val primaryLeaderLock = new DistributedMemberLock(dls,
    LOCK_SERVICE_NAME, DistributedMemberLock.NON_EXPIRING_LEASE,
    DistributedMemberLock.LockReentryPolicy.PREVENT_SILENTLY)

  private[snappydata] val snappyProperties = Utils.getFields(Property).
      map({
        case (_, propValue) if propValue.isInstanceOf[String] =>
          val propName = propValue.asInstanceOf[String]
          if (propName.startsWith(Constant.PROPERTY_PREFIX) &&
              !propName.startsWith(Constant.STORE_PROPERTY_PREFIX)) {
            propName.substring(Constant.PROPERTY_PREFIX.length)
          } else {
            ""
          }
        case (propField, _) => s"Property Field=${propField} non string"
      }).toSet


  var _directApiInvoked: Boolean = false

  def directApiInvoked: Boolean = _directApiInvoked

  @throws(classOf[SQLException])
  override def start(bootProperties: Properties, ignoreIfStarted: Boolean): Unit = {

    _directApiInvoked = true
    val locator = {
      bootProperties.getProperty(DistributionConfig.LOCATORS_NAME) match {
        case v if v != null => v
        case _ =>
          bootProperties.getProperty(Property.locators)
      }
    }

    val conf = new SparkConf()
    conf.setMaster(Constant.SNAPPY_URL_PREFIX + s"$locator").
      setAppName("leaderLauncher").
      set(Property.jobserverEnabled, "true")

    // inspect user input and add appropriate prefixes
    // if property doesn't contain '.'
    // if input prop key is found in io.snappydata.Property,
    // its prefixed with 'snappydata.' otherwise its assumed
    // to be snappydata.store.
    bootProperties.asScala.foreach({ case (k, v) =>
      val key = if (k.indexOf(".") < 0) {
        if (snappyProperties(k)) {
          Constant.PROPERTY_PREFIX + k
        } else {
          Constant.STORE_PROPERTY_PREFIX + k
        }
      }
      else {
        k
      }
      conf.set(key, v)
    })

    SnappyEmbeddedModeClusterManager.register()

    sparkContext = new SparkContext(conf)
  }

  private[snappydata] def internalStart(sc: SparkContext): Unit = {

    val conf = sc.getConf // this will get you a cloned copy
    initStartupArgs(conf, sc)

    logInfo("cluster configuration after overriding certain properties \n"
      + conf.toDebugString)

    val confProps = conf.getAll
    val storeProps = new Properties()
    
    val filteredProp = confProps.filter {
      case (k, _) => k.startsWith(Constant.STORE_PROPERTY_PREFIX)
    }.map {
      case (k, v) => (k.replaceFirst(Constant.STORE_PROPERTY_PREFIX, ""), v)
    }
    storeProps.putAll(filteredProp.toMap.asJava)

    logInfo("passing store properties as " + storeProps)
    super.start(storeProps, ignoreIfStarted = false)

    status() match {
      case State.RUNNING =>
        bootProperties.putAll(confProps.toMap.asJava)
        logInfo("ds connected. About to check for primary lead lock.")
        // check for leader's primary election
        val startStatus = directApiInvoked match {
          case true =>
            primaryLeaderLock.tryLock()
          case _ =>
            primaryLeaderLock.lockInterruptibly()
            true
        }

        startStatus match {
          case true =>
            logInfo("Primary lead lock acquired.")
          // let go.
          case false =>
            serverstatus = State.STANDBY
            primaryLeadNodeWaiter.start()
        }
      case _ =>
        logWarning(LocalizedMessages.res.getTextMessage("SD_LEADER_NOT_READY", status()))
    }
  }

  @throws(classOf[SQLException])
  override def stop(shutdownCredentials: Properties): Unit = {
    assert(sparkContext != null, "Mix and match of LeadService api " +
      "and SparkContext is unsupported.")
    if (!sparkContext.isStopped) {
      sparkContext.stop()
      sparkContext = null
    }
  }

  private[snappydata] def internalStop(shutdownCredentials: Properties): Unit = {
    primaryLeadNodeWaiter.interrupt()
    bootProperties.clear()
    SnappyContext.stop()
    // TODO: [soubhik] find a way to stop jobserver.
    sparkContext = null
    super.stop(shutdownCredentials)
  }

  override def waitUntilPrimary(): Unit = synchronized {
    status() match {
      case State.STANDBY => latch.await()
      case State.RUNNING => ; // no-op
      case _ => logWarning("not waiting because server not in standby mode. status is "
        + status())
    }
  }

  private[snappydata] def initStartupArgs(conf: SparkConf, sc: SparkContext = null) = {

    def changeOrAppend(attr: String, value: String,
                       overwrite: Boolean = false,
                       ignoreIfPresent: Boolean = false) = {
      val x = conf.getOption(attr).orNull
      x match {
        case null =>
          conf.set(attr, value)
        case v if ignoreIfPresent => ; // skip setting property.
        case v if overwrite => conf.set(attr, value)
        case v => conf.set(attr, x ++ s""",$value""")
      }
    }

    changeOrAppend(Constant.STORE_PROPERTY_PREFIX +
        com.pivotal.gemfirexd.Attribute.SERVER_GROUPS, LeadImpl.LEADER_SERVERGROUP)

    assert(conf.getOption(Property.locators).isDefined ||
        conf.getOption(Property.mcastPort).isDefined,
      s"Either ${Property.locators} or ${Property.mcastPort} " +
          s"must be defined for SnappyData cluster to start")
    import org.apache.spark.sql.collection.Utils
    // skip overriding host-data if loner VM.
    if(sc != null && Utils.isLoner(sc)) {
      changeOrAppend(Constant.STORE_PROPERTY_PREFIX +
          com.pivotal.gemfirexd.Attribute.GFXD_HOST_DATA,
        "true", overwrite = true)
    } else {
      changeOrAppend(Constant.STORE_PROPERTY_PREFIX +
          com.pivotal.gemfirexd.Attribute.GFXD_HOST_DATA,
        "false", overwrite = true)
      changeOrAppend(Constant.STORE_PROPERTY_PREFIX +
          com.pivotal.gemfirexd.Attribute.GFXD_PERSIST_DD,
        "false", overwrite = true)
    }
    changeOrAppend(Property.jobserverEnabled, "false", ignoreIfPresent = true)

    conf
  }

  protected[snappydata] def notifyWhenPrimary(f: () => Unit): Unit =
    this.notificationCallback = f

  private[snappydata] def scheduleWaitForPrimaryDeparture() = {

    val r = new Runnable() {
      override def run(): Unit = {
        try {
          logInfo("About to wait for member lock")
          primaryLeaderLock.lockInterruptibly()
          latch.countDown()
          logInfo("Notifying status ...")
          val callback = notificationCallback
          if (callback != null) {
            callback()
          }
        } catch {
          case ie: InterruptedException =>
            logInfo("Thread interrupted. Shutting down primary lead node lock waiter.")
            Thread.currentThread().interrupt()
          case e: Throwable =>
            logWarning("Exception while becoming primary lead node after standby mode", e)
            throw e
        }
      }
    }

    val t = new Thread(Utils.SnappyDataThreadGroup, r, "Waiter To Become Primary Lead Node")
    t.setDaemon(true)
    t.setContextClassLoader(this.getClass.getClassLoader)
    t
  }

  @throws(classOf[Exception])
  private[snappydata] def startAddOnServices(sc: SparkContext): Unit = this.synchronized {
    LeadImpl.setInitializingSparkContext(sc)

    if (status() == State.UNINITIALIZED || status() == State.STOPPED) {
      // for SparkContext.setMaster("local[xx]"), ds.connect won't happen
      // until now.
      logInfo("Connecting to snappydata cluster now...")
      internalStart(sc)
    }

    val jobServerEnabled = bootProperties.getProperty(Property.jobserverEnabled).toBoolean
    if (_directApiInvoked) {
      assert(jobServerEnabled,
        "JobServer must have been enabled with lead.start(..) invocation")
    }
    if (jobServerEnabled) {
      logInfo("Starting job server...")

      val confFile = bootProperties.getProperty("jobserver.configFile") match {
        case null => Array[String]()
        case c => Array(c)
      }

      JobServer.start(confFile, getConfig, createActorSystem)
    }

    // This will use GfxdDistributionAdvisor#distributeProfileUpdate
    // which inturn will create a new profile object via #instantiateProfile
    // whereby ClusterCallbacks#getDriverURL should be now returning
    // the correct URL given SparkContext is fully initialized.
    logInfo("About to send profile update after initialization completed.")
    ServerGroupUtils.sendUpdateProfile()

    LeadImpl.clearInitializingSparkContext()
  }

  def getConfig(args: Array[String]): Config = {

    System.setProperty("config.trace", "loads")

    val notConfigurable = ConfigFactory.parseResources("jobserver-overrides.conf")

    val bootConfig = notConfigurable.withFallback(ConfigFactory.parseProperties(bootProperties))

    val snappyDefaults = bootConfig.withFallback(
      ConfigFactory.parseResources("jobserver-defaults.conf"))

    val builtIn = ConfigFactory.load()

    val finalConf = snappyDefaults.withFallback(builtIn).resolve()

    logInfo("Passing JobServer with config " + finalConf.root.render())

    finalConf
  }


  def createActorSystem(conf: Config): ActorSystem = {
    ActorSystem("SnappyLeadJobServer", conf)
  }

  @throws(classOf[SQLException])
  override def startNetworkServer(bindAddress: String,
                                  port: Int,
                                  networkProperties: Properties): NetworkInterface = {
    throw new SQLException("Network server cannot be started on lead node.")
  }

  @throws(classOf[SQLException])
  override def startThriftServer(bindAddress: String,
                                 port: Int,
                                 networkProperties: Properties): NetworkInterface = {
    throw new SQLException("Thrift server cannot be started on lead node.")
  }

  @throws(classOf[SQLException])
  override def startDRDAServer(bindAddress: String,
                               port: Int,
                               networkProperties: Properties): NetworkInterface = {
    throw new SQLException("DRDA server cannot be started on lead node.")
  }

  override def stopAllNetworkServers(): Unit = {
    // nothing to do as none of the net servers are allowed to start.
  }
}

object LeadImpl {

  val LEADER_SERVERGROUP = "IMPLICIT_LEADER_SERVERGROUP"
  private[this] val startingContext: AtomicReference[SparkContext] =
    new AtomicReference[SparkContext](null)

  def invokeLeadStart(sc: SparkContext): Unit = {
    val lead = ServiceManager.getLeadInstance.asInstanceOf[LeadImpl]
    lead.internalStart(sc)
  }

  def invokeLeadStartAddonService(sc: SparkContext): Unit = {
    val lead = ServiceManager.getLeadInstance.asInstanceOf[LeadImpl]
    lead.startAddOnServices(sc)
  }

  def invokeLeadStop(shutdownCredentials: Properties): Unit = {
    val lead = ServiceManager.getLeadInstance.asInstanceOf[LeadImpl]
    lead.internalStop(shutdownCredentials)
  }

  def setInitializingSparkContext(sc: SparkContext): Unit = {
    assert(sc != null)
    startingContext.set(sc)
  }

  def getInitializingSparkContext(): SparkContext = {
    val sc = SnappyContext.globalSparkContext
    if (sc != null) {
      return sc
    }

    val initSC = startingContext.get()
    assert(initSC != null)

    initSC
  }

  def clearInitializingSparkContext(): Unit = {
    startingContext.set(null)
  }


}
