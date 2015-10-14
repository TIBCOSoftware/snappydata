package io.snappydata.tools

import java.util
import java.util.Properties

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.cache.Cache
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem
import com.gemstone.gemfire.distributed.internal.locks.{DLockService, DistributedMemberLock}
import com.gemstone.gemfire.internal.cache.CacheServerLauncher
import com.pivotal.gemfirexd.tools.internal.GfxdServerLauncher
import io.snappydata.LocalizedMessages
import org.slf4j.LoggerFactory
import spark.jobserver.JobServer

/**
 * Extending server launcher to init Jobserver as part of lead
 * node startup. This node won't start DRDA network server.
 *
 * Created by soubhikc on 30/09/15.
 */
class LeaderLauncher(baseName: String) extends GfxdServerLauncher(baseName) {

  val genericLogger = LoggerFactory.getLogger(getClass)

  private val LOCK_SERVICE_NAME = "__PRIMARY_LEADER_LS"

  val LEADER_SERVERGROUP = "IMPL_LEADER_SERVERGROUP"

  private var gfCache: Option[Cache] = None

  private lazy val dls = initPrimaryLeaderLockService()

  private lazy val primaryLeaderLock = new DistributedMemberLock(dls,
    LOCK_SERVICE_NAME, DistributedMemberLock.NON_EXPIRING_LEASE,
    DistributedMemberLock.LockReentryPolicy.PREVENT_SILENTLY)


  def initStartupArgs(args: ArrayBuffer[String]) = {

    assert(args.length > 0, LocalizedMessages.res.getTextMessage("SD_ZERO_ARGS"))

    def changeOrAppend(attr: String, value: String, overwrite: Boolean = false) = {
      args.indexWhere(_.indexOf(attr) > 0) match {
        case -1 => args += s"""-${attr}=${value}"""
        case idx if overwrite => args(idx) =  args(idx).takeWhile(_ != '=') + s"""=${value}"""
        case idx => args(idx) = args(idx) ++ s""",${value}"""
      }
    }


    args(0).equalsIgnoreCase("start") match {
      case true =>
        changeOrAppend(com.pivotal.gemfirexd.Attribute.SERVER_GROUPS, LEADER_SERVERGROUP)
        changeOrAppend(com.pivotal.gemfirexd.Attribute.GFXD_HOST_DATA, "false", true)
        changeOrAppend(GfxdServerLauncher.RUN_NETSERVER, "false", true)
      case _ =>
    }

    args.toArray[String]
  }

  override protected def run(args: Array[String]): Unit = {
    super.run(initStartupArgs(ArrayBuffer(args: _*)))
  }

  @throws(classOf[Exception])
  override protected def startAdditionalServices(cache: Cache, options: java.util.Map[String, Object], props: Properties): Unit = {
    // don't call super.startAdditionalServices.
    // We don't want to init net-server in leader.

    gfCache = Some(cache)

    super.writeStatus(CacheServerLauncher.createStatus(this.baseName, CacheServerLauncher.STANDBY, getProcessId))

    // wait for Leader's primary DLock
    primaryLeaderLock.lockInterruptibly()

    if (!gfCache.get.getDistributedSystem.isConnected) {
      return
    }

    genericLogger.info("Starting job server...")

    super.writeStatus(CacheServerLauncher.createStatus(this.baseName, CacheServerLauncher.STARTING, getProcessId))

    startJobServer(options.toMap, props)
  }


  override protected def getBaseName (name: String) = "snappyleader"

  def initPrimaryLeaderLockService() = {
    val dSys = gfCache.map(_.getDistributedSystem.asInstanceOf[InternalDistributedSystem]).getOrElse {
      throw new Exception("GemFire Cache not initialized")
    }

    DLockService.create(LOCK_SERVICE_NAME, dSys, true, true, true)
  }

  def startJobServer(options: Map[String, AnyRef], props: Properties): Unit = {
    val args = options.filter({ case (k, _) => k.equalsIgnoreCase("jobserver.config") })
        .map({ case (_, v) => v.asInstanceOf[String] }).toArray

    JobServer.main(args)
  }

}

object LeaderLauncher {

  def main(args: Array[String]): Unit = {
    val launcher = new LeaderLauncher("SnappyData Leader")
    launcher.run(args)
  }

}