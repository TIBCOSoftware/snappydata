package io.snappydata.impl

import java.sql.SQLException
import java.util.Properties

import com.gemstone.gemfire.cache.Cache
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem
import com.gemstone.gemfire.distributed.internal.locks.{DLockService, DistributedMemberLock}
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.pivotal.gemfirexd.FabricService.State
import com.pivotal.gemfirexd.{FabricService, NetworkInterface}
import com.pivotal.gemfirexd.tools.internal.GfxdServerLauncher
import io.snappydata.{Lead, LocalizedMessages}
import org.slf4j.LoggerFactory
import spark.jobserver.JobServer

class LeadImpl extends ServerImpl with Lead {
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

  private lazy val primaryLeaderLock = new DistributedMemberLock(dls,
    LOCK_SERVICE_NAME, DistributedMemberLock.NON_EXPIRING_LEASE,
    DistributedMemberLock.LockReentryPolicy.PREVENT_SILENTLY)


  def initStartupArgs(args: Properties): Properties = {

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

  @throws(classOf[SQLException])
  override def start(bootProperties: Properties, ignoreIfStarted: Boolean) = this.synchronized {
    super.start(initStartupArgs(bootProperties), ignoreIfStarted)
    this.bootProperties = bootProperties
  }

  @throws(classOf[Exception])
  override def startPrimaryServices(): Unit = synchronized {

    assert(status() == State.RUNNING, LocalizedMessages.res.getTextMessage("SD_LEADER_NOT_READY", status()))

    // check for leader's primary election
    primaryLeaderLock.tryLock() match {
      case true =>
        primaryServiceStatus = State.STARTING
        startAddOnServices()
      case false =>
        primaryServiceStatus = State.STANDBY
    }

  }

  @throws(classOf[Exception])
  override def waitForPrimaryDeparture(): Unit = synchronized {

    primaryLeaderLock.lockInterruptibly()

    if (GemFireCacheImpl.getInstance == null || GemFireCacheImpl.getInstance.isClosed) {
      return
    }

  }

  @throws(classOf[Exception])
  private[snappydata] def startAddOnServices() = {

    genericLogger.info("Starting job server...")
    JobServer.main(getJobServerArgs())

    primaryServiceStatus = State.RUNNING
  }

  def getJobServerArgs(): Array[String] = {

    val conf = bootProperties.getProperty("jobserver.config")

    conf match {
      case null => Array()
      case conf => Array(conf)
    }

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