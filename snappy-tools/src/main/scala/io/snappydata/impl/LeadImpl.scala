package io.snappydata.impl

import java.io.File
import java.sql.SQLException
import java.util.Properties
import java.util.concurrent.CountDownLatch

import akka.actor.ActorSystem
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem
import com.gemstone.gemfire.distributed.internal.locks.{DLockService, DistributedMemberLock}
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.pivotal.gemfirexd.FabricService.State
import com.pivotal.gemfirexd.NetworkInterface
import com.typesafe.config.{ConfigFactory, Config}
import io.snappydata.{Lead, LocalizedMessages, Utils}
import org.slf4j.LoggerFactory
import spark.jobserver.JobServer

class LeadImpl extends ServerImpl with Lead {

  self =>

  val genericLogger = LoggerFactory.getLogger(getClass)

  private val LOCK_SERVICE_NAME = "__PRIMARY_LEADER_LS"

  @volatile var bootProperties = new Properties()

  private lazy val dls = {

    val gfCache = GemFireCacheImpl.getInstance

    if (gfCache == null || gfCache.isClosed)
      throw new Exception("GemFire Cache not initialized")

    val dSys = gfCache.getDistributedSystem.asInstanceOf[InternalDistributedSystem]

    DLockService.create(LOCK_SERVICE_NAME, dSys, true, true, true)
  }

  private val latch = new CountDownLatch(1)
  private var notificationCallback: (() => Unit) = _
  private lazy val primaryLeadNodeWaiter = scheduleWaitForPrimaryDeparture

  private lazy val primaryLeaderLock = new DistributedMemberLock(dls,
    LOCK_SERVICE_NAME, DistributedMemberLock.NON_EXPIRING_LEASE,
    DistributedMemberLock.LockReentryPolicy.PREVENT_SILENTLY)

  @throws(classOf[SQLException])
  override def start(bootProperties: Properties, ignoreIfStarted: Boolean) = this.synchronized {
    this.bootProperties = initStartupArgs(bootProperties)
    super.start(this.bootProperties, ignoreIfStarted)

    status() match {
      case State.RUNNING =>
        genericLogger.info("Initiating startup of additional lead node services...")
        // check for leader's primary election
        primaryLeaderLock.tryLock() match {
          case true =>
            startAddOnServices(bootProperties)
          case false =>
            serverstatus = State.STANDBY
            primaryLeadNodeWaiter.start()
        }
      case _ =>
        genericLogger.warn(LocalizedMessages.res.getTextMessage("SD_LEADER_NOT_READY", status()))
    }
  }

  @throws(classOf[SQLException])
  override def stop(shutdownCredentials: Properties) = {
    primaryLeadNodeWaiter.interrupt()
    super.stop(shutdownCredentials)
  }

  override def waitUntilPrimary(): Unit = synchronized {
    status() match {
      case State.STANDBY => latch.await()
      case _ => genericLogger.warn("not waiting because server not in standby mode. status is " + status())
    }
  }

  private[snappydata] def initStartupArgs(args: Properties): Properties = {

    def changeOrAppend(attr: String, value: String, overwrite: Boolean = false) = {
      val x = args.getProperty(attr)
      x match {
        case null =>
          args.setProperty(attr, value)
        case v if overwrite => args.setProperty(attr, value)
        case v => args.setProperty(attr, x ++ s""",${value}""")
      }
    }

    changeOrAppend(com.pivotal.gemfirexd.Attribute.SERVER_GROUPS, LEADER_SERVERGROUP)
    changeOrAppend(com.pivotal.gemfirexd.Attribute.GFXD_HOST_DATA, "false", true)

    args
  }

  protected[snappydata] def notifyWhenPrimary(f: () => Unit): Unit = this.notificationCallback = f

  private[snappydata] def scheduleWaitForPrimaryDeparture() = {

    val r = new Runnable() {
      override def run(): Unit = {
        try {
          genericLogger.info("About to wait for member lock")
          primaryLeaderLock.lockInterruptibly()
          latch.countDown()
          genericLogger.info("Notifying status ...")
          notificationCallback()
        } catch {
          case ie: InterruptedException =>
            genericLogger.info("Thread interrupted. Shutting down primary lead node lock waiter.")
            Thread.currentThread().interrupt()
          case e: Throwable =>
            genericLogger.warn("Exception while becoming primary lead node after standby mode", e)
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
  private[snappydata] def startAddOnServices(bootProperties: Properties) = {

    genericLogger.info("Starting job server...")

    def getConfig(args: Array[String]) : Config = {

//      System.setProperty("config.trace", "loads")

      val notConfigurable = ConfigFactory.parseResources("jobserver-overrides.conf")

      val bootConfig = notConfigurable.withFallback(ConfigFactory.parseProperties(bootProperties))

      val snappyDefaults = bootConfig.withFallback(
          ConfigFactory.parseResources("jobserver-defaults.conf"))

      val builtIn = ConfigFactory.load()

      val finalConf = snappyDefaults.withFallback(builtIn).resolve()

//      System.out.println("SB: Passing JobServer with config ", finalConf.root.render())

      finalConf
    }

    val confFile = bootProperties.getProperty("jobserver.config") match {
      case null => Array[String]()
      case c => Array(c)
    }

    JobServer.start(confFile, getConfig, createActorSystem)
  }

  def createActorSystem(conf: Config): ActorSystem = {
    ActorSystem("SnappyLeadJobServer", conf)
  }

  @throws(classOf[SQLException])
  override def startNetworkServer(bindAddress: String, port: Int, networkProperties: Properties): NetworkInterface = {
    throw new SQLException("Network server cannot be started on lead node.")
  }

  @throws(classOf[SQLException])
  override def startThriftServer(bindAddress: String, port: Int, networkProperties: Properties): NetworkInterface = {
    throw new SQLException("Thrift server cannot be started on lead node.")
  }

  @throws(classOf[SQLException])
  override def startDRDAServer(bindAddress: String, port: Int, networkProperties: Properties): NetworkInterface = {
    throw new SQLException("DRDA server cannot be started on lead node.")
  }

  override def stopAllNetworkServers(): Unit = {
    // nothing to do as none of the net servers are allowed to start.
  }
}