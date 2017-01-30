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
package io.snappydata.util

import java.util.Properties
import java.util.regex.Pattern

import scala.collection.JavaConverters._

import _root_.com.gemstone.gemfire.distributed.DistributedMember
import _root_.com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.{Constant, Property, ServerManager}

import org.apache.spark.SparkContext
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.impl.StoreCallbacksImpl

/**
 * Common utility methods for store services.
 */
object ServiceUtils {

  val LOCATOR_URL_PATTERN = Pattern.compile("(.+:[0-9]+)|(.+\\[[0-9]+\\])")

  private[snappydata] def getStoreProperties(
      confProps: Array[(String, String)]): Properties = {
    val storeProps = new Properties()
    confProps.foreach {
      case (Property.Locators(), v) =>
        if (!LOCATOR_URL_PATTERN.matcher(v).matches()) {
          throw Utils.analysisException(s"locators property $v should " +
              "be provided in the format host[port] or host:port")
        }
        storeProps.setProperty("locators", v)
      case (k, v) if k.startsWith(Constant.STORE_PROPERTY_PREFIX) =>
        storeProps.setProperty(k.trim.replaceFirst(
          Constant.STORE_PROPERTY_PREFIX, ""), v)
      case (k, v) if k.startsWith(Constant.SPARK_STORE_PREFIX) =>
        storeProps.setProperty(k.trim.replaceFirst(
          Constant.SPARK_STORE_PREFIX, ""), v)
      case _ => // ignore rest
    }
    storeProps
  }

  def invokeStartFabricServer(sc: SparkContext,
      hostData: Boolean): Unit = {
    val properties = getStoreProperties(sc.getConf.getAll)
    // overriding the host-data property based on the provided flag
    if (!hostData) {
      properties.setProperty("host-data", "false")
      // no DataDictionary persistence for non-embedded mode
      properties.setProperty("persist-dd", "false")
    }
    ServerManager.getServerInstance.start(properties)

    // initialize cluster callbacks if possible by reflection
    try {
      Utils.classForName("io.snappydata.gemxd.ClusterCallbacksImpl$")
    } catch {
      case _: ClassNotFoundException => // ignore if failed to load
    }
  }

  def invokeStopFabricServer(sc: SparkContext): Unit = {
    ServerManager.getServerInstance.stop(null)
  }

  def getAllLocators(sc: SparkContext): scala.collection.Map[DistributedMember, String] = {
    val advisor = GemFireXDUtils.getGfxdAdvisor
    val locators = advisor.adviseLocators(null)
    val locatorServers = scala.collection.mutable.HashMap[DistributedMember , String]()
    locators.asScala.foreach(locator =>
      locatorServers.put(locator, advisor.getDRDAServers(locator)))
    locatorServers
  }

  def getLocatorJDBCURL(sc: SparkContext): String = {
    val locatorUrl = getAllLocators(sc).filter(x => x._2 != null && !x._2.isEmpty)
        .map(locator => {
          org.apache.spark.sql.collection.Utils.getClientHostPort(locator._2)
        }).mkString(",")

    "jdbc:" + Constant.SNAPPY_URL_PREFIX + (if (locatorUrl.contains(",")) {
      locatorUrl.substring(0, locatorUrl.indexOf(",")) +
          "/;secondary-locators=" + locatorUrl.substring(locatorUrl.indexOf(",") + 1 ) + "/"
    } else locatorUrl +"/")
  }

  def clearStaticArtifacts(): Unit = {

  }
}
