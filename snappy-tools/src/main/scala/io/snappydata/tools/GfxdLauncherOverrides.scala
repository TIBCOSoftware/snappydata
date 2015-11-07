package io.snappydata.tools

import com.pivotal.gemfirexd.FabricService
import com.pivotal.gemfirexd.tools.GfxdDistributionLocator
import com.pivotal.gemfirexd.tools.internal.GfxdServerLauncher
import io.snappydata.ServiceManager

/**
 * Launcher extension for GFXD server launcher to use Snappy service manager.
 *
 * @author soubhik
 */
class ServerLauncher(baseName: String) extends GfxdServerLauncher(baseName) {

  @throws(classOf[Exception])
  override protected def getFabricServiceInstance: FabricService =
    ServiceManager.getServerInstance

  override protected def run(args: Array[String]): Unit = {
    super.run(args)
  }
}

object ServerLauncher {

  def main(args: Array[String]): Unit = {
    val launcher = new ServerLauncher("SnappyData Server")
    launcher.run(args)
  }
}

/**
 * Launcher extension for GFXD locator launcher to use Snappy service manager.
 *
 * @author soubhik
 */
class LocatorLauncher(baseName: String) extends GfxdDistributionLocator(baseName) {

  @throws(classOf[Exception])
  override protected def getFabricServiceInstance: FabricService =
    ServiceManager.getLocatorInstance

  override protected def run(args: Array[String]): Unit = {
    super.run(args)
  }
}

object LocatorLauncher {

  def main(args: Array[String]): Unit = {
    val launcher = new LocatorLauncher("SnappyData Locator")
    launcher.run(args)
  }
}
