package io.snappydata.tools

import com.pivotal.gemfirexd.FabricService
import com.pivotal.gemfirexd.internal.iapi.tools.i18n.LocalizedResource
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager
import com.pivotal.gemfirexd.tools.GfxdDistributionLocator
import com.pivotal.gemfirexd.tools.internal.GfxdServerLauncher
import io.snappydata.{LocalizedMessages, ServiceManager}

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

  override protected def usage(): Unit = {
    val script: String = LocalizedMessages.res.getTextMessage("SD_SERVER_SCRIPT")
    val name: String = LocalizedMessages.res.getTextMessage("SD_SERVER_NAME")
    val extraHelp = LocalizedResource.getMessage("FS_EXTRA_HELP", LocalizedMessages.res.getTextMessage("FS_PRODUCT"))
    val usageOutput: String = LocalizedResource.getMessage("SERVER_HELP", script, name, LocalizedResource.getMessage("FS_ADDRESS_ARG"), extraHelp)

    printUsage(usageOutput, SanityManager.DEFAULT_MAX_OUT_LINES)
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

  override protected def usage(): Unit = {
    val script: String = LocalizedMessages.res.getTextMessage("SD_LOC_SCRIPT")
    val name: String = LocalizedMessages.res.getTextMessage("SD_LOC_NAME")
    printUsage(LocalizedResource.getMessage("SERVER_HELP", script, name, LocalizedResource.getMessage("LOC_ADDRESS_ARG"), LocalizedResource.getMessage("LOC_EXTRA_HELP")), SanityManager.DEFAULT_MAX_OUT_LINES)
  }
}

object LocatorLauncher {

  def main(args: Array[String]): Unit = {
    val launcher = new LocatorLauncher("SnappyData Locator")
    launcher.run(args)
  }
}
