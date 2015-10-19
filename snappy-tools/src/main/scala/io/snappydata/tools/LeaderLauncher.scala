package io.snappydata.tools

import java.util.Properties

import com.gemstone.gemfire.cache.Cache
import com.gemstone.gemfire.internal.cache.CacheServerLauncher
import com.pivotal.gemfirexd.FabricService
import com.pivotal.gemfirexd.FabricService.State
import com.pivotal.gemfirexd.tools.internal.GfxdServerLauncher
import io.snappydata.{Lead, LocalizedMessages, ServiceManager}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
 * Extending server launcher to init Jobserver as part of lead
 * node startup. This node won't start DRDA network server.
 *
 * Created by soubhikc on 30/09/15.
 */
class LeaderLauncher(baseName: String) extends GfxdServerLauncher(baseName) {

  val genericLogger = LoggerFactory.getLogger(getClass)

  @throws(classOf[Exception])
  override protected def getFabricServiceInstance: FabricService = ServiceManager.getLeadInstance

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
        changeOrAppend(GfxdServerLauncher.RUN_NETSERVER, "false", true)
      case _ =>
    }

    args.toArray[String]
  }

  override protected def run(args: Array[String]): Unit = {
    super.run(initStartupArgs(ArrayBuffer(args: _*)))
  }

  @throws(classOf[Exception])
  override protected def startAdditionalServices(cache: Cache,
                                                 options: java.util.Map[String, Object], props: Properties): Unit = {
    // don't call super.startAdditionalServices.
    // We don't want to init net-server in leader.
    val leadInst = getFabricServiceInstance.asInstanceOf[Lead];

    do {
      leadInst.startPrimaryServices()

      leadInst.getPrimaryServiceStatus() match {
        case State.STARTING =>
          Thread.sleep(1000)
        case State.STANDBY =>
          super.writeStatus(CacheServerLauncher.createStatus(this.baseName, CacheServerLauncher.STANDBY, getProcessId))
          leadInst.waitForPrimaryDeparture()
        case State.RUNNING =>
          super.writeStatus(CacheServerLauncher.createStatus(this.baseName, CacheServerLauncher.RUNNING, getProcessId))
          return
        case v =>
          super.writeStatus(CacheServerLauncher.createStatus(this.baseName, getStatus.state, getProcessId, "Unknown Leader Primary Service Status " + v, null))
      }
    } while (true)
  }

  override protected def getBaseName (name: String) = "snappyleader"
}

object LeaderLauncher {

  def main(args: Array[String]): Unit = {
    val launcher = new LeaderLauncher("SnappyData Leader")
    launcher.run(args)
  }

}