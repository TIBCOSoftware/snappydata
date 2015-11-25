package io.snappydata

import java.util.Properties

import scala.collection.JavaConversions._

import com.gemstone.gemfire.distributed.DistributedMember
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.impl.LeadImpl

import org.apache.spark.SparkContext

/**
  * Created by soubhikc on 11/11/15.
  */
object ToolsCallbackImpl extends ToolsCallback {

  override def invokeLeadStartAddonService(sc: SparkContext): Unit = {
    LeadImpl.invokeLeadStartAddonService(sc)
  }

  override def invokeStartFabricServer(sc: SparkContext): Unit = {
    val locator = sc.getConf.get(Property.locators)
    if (!Utils.LocatorURLPattern.matcher(locator).matches())
      throw new Exception(s"locator info should be provided in the format host[port]")
    val properties = new Properties()
    properties.setProperty("locators", locator)
    properties.setProperty("host-data", "false")
    ServiceManager.getServerInstance.start(properties)
  }

  override def invokeStopFabricServer(sc: SparkContext): Unit = {
    ServiceManager.getServerInstance.stop(null)
  }

  def getAllLocators(sc: SparkContext): collection.Map[DistributedMember, String] = {
    val advisor = GemFireXDUtils.getGfxdAdvisor
    val locators= advisor.adviseLocators(null)
    val locatorServers = collection.mutable.HashMap[DistributedMember , String]()
    locators.foreach(locator => locatorServers.put(locator, advisor.getDRDAServers(locator)))
    locatorServers
  }

  override def getLocatorJDBCURL(sc: SparkContext): String = {

    val locatorUrl = getAllLocators(sc).filter(x => x._2 != null && !x._2.isEmpty)
      .map(locator => {
        val url = locator._2
        val host = url.substring(0, url.indexOf("/"))
        val port = url.substring(url.indexOf("["))
        host + port
      }).mkString(",")

    "jdbc:" + Constant.JDBC_URL_PREFIX + (if (locatorUrl.contains(",")) {
      locatorUrl.substring(0, locatorUrl.indexOf(",")) +
          ";secondary-locators=" +
          locatorUrl.substring(locatorUrl.indexOf(",") + 1)
    } else locatorUrl)
  }
}
