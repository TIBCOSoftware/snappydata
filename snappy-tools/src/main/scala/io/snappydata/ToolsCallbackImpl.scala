/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package io.snappydata


import java.util.Properties

import scala.collection.JavaConverters._

import com.gemstone.gemfire.distributed.DistributedMember
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.impl.LeadImpl

import org.apache.spark.SparkContext
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.columntable.StoreCallbacksImpl

object ToolsCallbackImpl extends ToolsCallback {

  override def invokeLeadStartAddonService(sc: SparkContext): Unit = {
    LeadImpl.invokeLeadStartAddonService(sc)
  }

  override def invokeStartFabricServer(sc: SparkContext,
      hostData: Boolean): Unit = {
    val properties = new Properties()
    sc.getConf.getAll.filter(
      confProperty => confProperty._1.startsWith(Constant.STORE_PROPERTY_PREFIX)).
        foreach(storeProperty => storeProperty._1 match {
          case Property.locators =>
            if (!Utils.LocatorURLPattern.matcher(storeProperty._2).matches()) {
              throw new AnalysisException(s"locators property " + storeProperty._2 + " should " +
                  "be provided in the format host[port] or host:port", null)
            }
            properties.setProperty("locators", storeProperty._2)

          case _ =>
            properties.setProperty(storeProperty._1.trim.replaceFirst
            (Constant.STORE_PROPERTY_PREFIX, ""), storeProperty._2)
        }
        )
    // overriding the host-data property based on the provided flag
    if (!hostData) {
      properties.setProperty("host-data", "false")
      // no DataDictionary persistence for non-embedded mode
      properties.setProperty("persist-dd", "false")
    }
    ServiceManager.getServerInstance.start(properties)
  }

  override def invokeStopFabricServer(sc: SparkContext): Unit = {
    ServiceManager.getServerInstance.stop(null)
  }

  def getAllLocators(sc: SparkContext): collection.Map[DistributedMember, String] = {
    val advisor = GemFireXDUtils.getGfxdAdvisor
    val locators = advisor.adviseLocators(null)
    val locatorServers = collection.mutable.HashMap[DistributedMember , String]()
    locators.asScala.foreach(locator =>
      locatorServers.put(locator, advisor.getDRDAServers(locator)))
    locatorServers
  }

  override def getLocatorJDBCURL(sc: SparkContext): String = {

    val locatorUrl = getAllLocators(sc).filter(x => x._2 != null && !x._2.isEmpty)
        .map(locator => {
          org.apache.spark.sql.collection.Utils.getClientHostPort(locator._2)
        }).mkString(",")

    "jdbc:" + Constant.SNAPPY_URL_PREFIX + (if (locatorUrl.contains(",")) {
      locatorUrl.substring(0, locatorUrl.indexOf(",")) +
          ";secondary-locators=" + locatorUrl.substring(locatorUrl.indexOf(",") + 1)
    } else locatorUrl)
  }

  override def clearStaticArtifacts(): Unit = {
    StoreCallbacksImpl.stores.clear()
  }
}
