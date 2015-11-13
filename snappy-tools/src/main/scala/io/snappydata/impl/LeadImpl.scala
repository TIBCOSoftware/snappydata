package io.snappydata.impl

import java.sql.SQLException
import java.util.Properties
import java.util.concurrent.CountDownLatch

import scala.collection.JavaConverters._

import akka.actor.ActorSystem
import com.gemstone.gemfire.distributed.internal.{DistributionConfig, InternalDistributedSystem}
import com.gemstone.gemfire.distributed.internal.locks.{DLockService, DistributedMemberLock}
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.pivotal.gemfirexd.FabricService.State
import com.pivotal.gemfirexd.internal.engine.store.ServerGroupUtils
import com.pivotal.gemfirexd.{Attribute, NetworkInterface}
import com.typesafe.config.{Config, ConfigFactory}
import io.snappydata.{Const, Prop, Lead, LocalizedMessages, Utils}
import org.slf4j.LoggerFactory
import spark.jobserver.JobServer

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SnappyContextFactory, SnappyContext}

class LeadImpl extends ServerImpl with Lead {

  self =>

  val logger = LoggerFactory.getLogger(getClass)

  private val LOCK_SERVICE_NAME = "__PRIMARY_LEADER_LS"

  private val bootProperties = new Properties()

  private lazy val dls = {

    val gfCache = GemFireCacheImpl.getInstance

    if (gfCache == null || gfCache.isClosed) {
      throw new Exception("GemFire Cache not initialized")
    }

    val dSys = gfCache.getDistributedSystem.asInstanceOf[InternalDistributedSystem]

    DLockService.create(LOCK_SERVICE_NAME, dSys, true, true, true)
  }

  private val latch = new CountDownLatch(1)
  private var notificationCallback: (() => Unit) = _
  private lazy val primaryLeadNodeWaiter = scheduleWaitForPrimaryDeparture

  private lazy val primaryLeaderLock = new DistributedMemberLock(dls,
    LOCK_SERVICE_NAME, DistributedMemberLock.NON_EXPIRING_LEASE,
    DistributedMemberLock.LockReentryPolicy.PREVENT_SILENTLY)

  var _blockingStart: Boolean = true
  def blockingStart: Boolean = _blockingStart

  @throws(classOf[SQLException])
  override def start(bootProperties: Properties, ignoreIfStarted: Boolean): Unit = {

    _blockingStart = false
    val locator = bootProperties.getProperty(DistributionConfig.LOCATORS_NAME)
    val conf = new SparkConf()
    conf.setMaster(Const.jdbcUrlPrefix + s"$locator").setAppName("leaderLauncher")

    bootProperties.asScala.foreach({ case (k, v) => conf.set(k, v) })

    val sc = new SparkContext(conf)

    SnappyContext(sc)

  }

  def internalStart(conf: SparkConf): Unit = {

    val overriddenConf = initStartupArgs(conf)

    val confProps = overriddenConf.getAll
    bootProperties.putAll(confProps.toMap.asJava)

    val storeProps = new Properties()

    val filteredProp = confProps.filter {
      case (k, _) => k.startsWith(Prop.propPrefix)
    }.map {
      case (k, v) => (k.replaceFirst(Prop.propPrefix, ""), v)
    }
    storeProps.putAll(filteredProp.toMap.asJava)

    super.start(storeProps, false)

    status() match {
      case State.RUNNING =>
        logger.info("ds connected. About to check for primary lead lock.")
        // check for leader's primary election
        val startStatus = blockingStart match {
          case true =>
            primaryLeaderLock.lockInterruptibly()
            true
          case _ => primaryLeaderLock.tryLock()
        }
        startStatus match {
          case true =>
            logger.info("Primary lead lock acquired.")
            // let go.
          case false =>
            serverstatus = State.STANDBY
            primaryLeadNodeWaiter.start()
            return
        }
      case _ =>
        logger.warn(LocalizedMessages.res.getTextMessage("SD_LEADER_NOT_READY", status()))
        return
    }
  }

  @throws(classOf[SQLException])
  override def stop(shutdownCredentials: Properties): Unit = {
    primaryLeadNodeWaiter.interrupt()
    bootProperties.clear()
    super.stop(shutdownCredentials)
  }

  override def waitUntilPrimary(): Unit = synchronized {
    status() match {
      case State.STANDBY => latch.await()
      case State.RUNNING => ; // no-op
      case _ => logger.warn("not waiting because server not in standby mode. status is "
          + status())
    }
  }

  private[snappydata] def initStartupArgs(conf: SparkConf) = {

    def changeOrAppend(attr: String, value: String,
        overwrite: Boolean = false,
        ignoreIfPresent: Boolean = false) = {
      val x = conf.getOption(attr).getOrElse {null}
      x match {
        case null =>
          conf.set(attr, value)
        case v if ignoreIfPresent => ; // skip setting property.
        case v if overwrite => conf.set(attr, value)
        case v => conf.set(attr, x ++ s""",${value}""")
      }
    }

    changeOrAppend(com.pivotal.gemfirexd.Attribute.SERVER_GROUPS, LEADER_SERVERGROUP)
    changeOrAppend(com.pivotal.gemfirexd.Attribute.GFXD_HOST_DATA, "false", overwrite = true)
    changeOrAppend(Prop.jobserverEnabled, "false", ignoreIfPresent = true)

    conf
  }

  protected[snappydata] def notifyWhenPrimary(f: () => Unit): Unit = this.notificationCallback = f

  private[snappydata] def scheduleWaitForPrimaryDeparture() = {

    val r = new Runnable() {
      override def run(): Unit = {
        try {
          logger.info("About to wait for member lock")
          primaryLeaderLock.lockInterruptibly()
          latch.countDown()
          logger.info("Notifying status ...")
          notificationCallback()
        } catch {
          case ie: InterruptedException =>
            logger.info("Thread interrupted. Shutting down primary lead node lock waiter.")
            Thread.currentThread().interrupt()
          case e: Throwable =>
            logger.warn("Exception while becoming primary lead node after standby mode", e)
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
  private[snappydata] def startAddOnServices(snc: SnappyContext): Unit = this.synchronized {

    if (status() == State.UNINITIALIZED || status() == State.STOPPED) {
      // for SparkContext.setMaster("local[xx]"), ds.connect won't happen
      // until now.
      logger.info("Connecting to snappydata cluster now...")
      internalStart(snc.sparkContext.getConf)
    }

    if (bootProperties.getProperty(Prop.jobserverEnabled).toBoolean) {
      SnappyContextFactory.setSnappyContext(snc)
      logger.info("Starting job server...")

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
    logger.info("About to send profile update after initialization completed.")
    ServerGroupUtils.sendUpdateProfile()
  }

  def getConfig(args: Array[String]): Config = {

    System.setProperty("config.trace", "loads")

    val notConfigurable = ConfigFactory.parseResources("jobserver-overrides.conf")

    val bootConfig = notConfigurable.withFallback(ConfigFactory.parseProperties(bootProperties))

    val snappyDefaults = bootConfig.withFallback(
      ConfigFactory.parseResources("jobserver-defaults.conf"))

    val builtIn = ConfigFactory.load()

    val finalConf = snappyDefaults.withFallback(builtIn).resolve()

    logger.info("Passing JobServer with config ", finalConf.root.render())

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