/*
 * Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
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

import java.nio.file.{Files, Paths}
import java.util.Properties
import java.util.regex.Pattern

import scala.collection.JavaConverters._

import _root_.com.gemstone.gemfire.distributed.DistributedMember
import _root_.com.gemstone.gemfire.distributed.internal.DistributionConfig
import _root_.com.gemstone.gemfire.distributed.internal.DistributionConfig.ENABLE_NETWORK_PARTITION_DETECTION_NAME
import _root_.com.gemstone.gemfire.internal.shared.ClientSharedUtils
import _root_.com.pivotal.gemfirexd.internal.engine.GfxdConstants
import _root_.com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.{Constant, Property, ServerManager, SnappyTableStatsProviderService}
import org.slf4j.{Logger, LoggerFactory}

import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.hive.HiveClientUtil
import org.apache.spark.sql.{SnappyContext, SparkSession, ThinClientConnectorMode}
import org.apache.spark.{SparkContext, SparkEnv}

/**
 * Common utility methods for store services.
 */
object ServiceUtils {

  val LOCATOR_URL_PATTERN: Pattern = Pattern.compile("(.+:[0-9]+)|(.+\\[[0-9]+\\])")

  private[snappydata] def getStoreProperties(
      confProps: Seq[(String, String)], forInit: Boolean = false): Properties = {
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
      case (k, v) if k.startsWith(Constant.SPARK_PREFIX) ||
          k.startsWith(Constant.PROPERTY_PREFIX) ||
          k.startsWith(Constant.JOBSERVER_PROPERTY_PREFIX) => storeProps.setProperty(k, v)
      case _ => // ignore rest
    }
    setCommonBootDefaults(storeProps, forLocator = false, forInit)
  }

  private[snappydata] def setCommonBootDefaults(props: Properties,
      forLocator: Boolean, forInit: Boolean = true): Properties = {
    val storeProps = if (props ne null) props else new Properties()
    if (!forLocator) {
      // set default recovery delay to 2 minutes (SNAP-1541)
      storeProps.putIfAbsent(GfxdConstants.DEFAULT_STARTUP_RECOVERY_DELAY_PROP, "120000")
      val isRecoveryMode = props.getProperty(GfxdConstants.SNAPPY_PREFIX + "recover")
      if(isRecoveryMode != null) {
        // It is crucial to enforce process locality in case of recovery mode
        storeProps.putIfAbsent("spark.locality.wait.process", "1800s")
      } else {
        // try hard to maintain executor and node locality
        storeProps.putIfAbsent("spark.locality.wait.process", "20s")
      }
      storeProps.putIfAbsent("spark.locality.wait", "10s")
      // default value for spark.sql.files.maxPartitionBytes in snappy is 32mb
      storeProps.putIfAbsent("spark.sql.files.maxPartitionBytes", "33554432")

      // change hive temporary files location to be inside working directory
      // to fix issues with concurrent queries trying to access/write same directories
      if (forInit) {
        ClientSharedUtils.deletePath(Paths.get(HiveClientUtil.HIVE_TMPDIR), false, false)
        Files.createDirectories(Paths.get(HiveClientUtil.HIVE_TMPDIR))
      }
      // set as system properties so that these can be overridden by
      // hive-site.xml if required
      val sysProps = System.getProperties
      for ((hiveVar, dirName) <- HiveClientUtil.HIVE_DEFAULT_SETTINGS) {
        sysProps.putIfAbsent(hiveVar.varname, dirName)
      }
      // increase call stack depth
      sysProps.putIfAbsent("spark.callstack.depth", "50")
    }
    // set default member-timeout higher for GC pauses (SNAP-1777)
    storeProps.putIfAbsent(DistributionConfig.MEMBER_TIMEOUT_NAME, "30000")
    // set network partition detection by default
    storeProps.putIfAbsent(ENABLE_NETWORK_PARTITION_DETECTION_NAME, "true")
    storeProps
  }

  def invokeStartFabricServer(sc: SparkContext,
      hostData: Boolean): Unit = {
    val properties = getStoreProperties(Utils.getInternalSparkConf(sc).getAll, forInit = true)
    // overriding the host-data property based on the provided flag
    if (!hostData) {
      properties.setProperty("host-data", "false")
      // no DataDictionary persistence for non-embedded mode
      properties.setProperty("persist-dd", "false")
    }
    // set the log-level from initialized SparkContext's level if set to higher level than default
    if (!properties.containsKey("log-level")) {
      // preserve existing sl4j level if it is higher
      val rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)
      if (rootLogger.isWarnEnabled()) {
        properties.setProperty("log-level",
          ClientSharedUtils.getJavaLogLevel(rootLogger).getName.toLowerCase)
      }
    }
    ServerManager.getServerInstance.start(properties)

    // initialize cluster callbacks if possible by reflection
    try {
      Utils.classForName("io.snappydata.gemxd.ClusterCallbacksImpl$")
    } catch {
      case _: ClassNotFoundException => // ignore if failed to load
    }
  }

  def invokeStopFabricServer(sc: SparkContext, shutDownCreds: Properties = null): Unit = {
    ServerManager.getServerInstance.stop(shutDownCreds)
  }

  def getAllLocators(sc: SparkContext): scala.collection.Map[DistributedMember, String] = {
    val advisor = GemFireXDUtils.getGfxdAdvisor
    val locators = advisor.adviseLocators(null)
    val locatorServers = scala.collection.mutable.HashMap[DistributedMember, String]()
    locators.asScala.foreach(locator =>
      locatorServers.put(locator,
        if (ClientSharedUtils.isThriftDefault) advisor.getThriftServers(locator)
        else advisor.getDRDAServers(locator)))
    locatorServers
  }

  def getLocatorJDBCURL(sc: SparkContext): String = {
    val locatorUrl = getAllLocators(sc).filter(x => x._2 != null && !x._2.isEmpty)
        .map(locator => {
          org.apache.spark.sql.collection.Utils.getClientHostPort(locator._2)
        }).mkString(",")

    Constant.DEFAULT_THIN_CLIENT_URL + (if (locatorUrl.contains(",")) {
      locatorUrl.substring(0, locatorUrl.indexOf(",")) +
          "/;secondary-locators=" + locatorUrl.substring(locatorUrl.indexOf(",") + 1)
    } else locatorUrl + "/")
  }

  def clearStaticArtifacts(): Unit = {
  }

  def isOffHeapStorageAvailable(sparkSession: SparkSession): Boolean = {
    SnappyContext.getClusterMode(sparkSession.sparkContext) match {
      case _: ThinClientConnectorMode =>
        SparkEnv.get.memoryManager.tungstenMemoryMode == MemoryMode.OFF_HEAP
      case _ =>
        try {
          SnappyTableStatsProviderService.getService.getMembersStatsFromService.
              values.forall(member => !member.isDataServer ||
              (member.getOffHeapMemorySize > 0))
        }
        catch {
          case _: Throwable => false
        }
    }
  }

  /**
   * We capture table ddl string and add it to table properties before adding to catalog.
   * This will also contain passwords in the string such as jdbc connection string or
   * s3 location etc. This method masks passwords
   * @param str DDL string that might contain jdbc/s3 passwords
   * @return similar string with masked passwords
   */
  def maskPasswordsInString(str: String): String = {
    val jdbcPattern1 = ".*jdbc.*(?i)\\bpassword\\b=([^);&']*).*".r
    val jdbcPattern2 = ".*jdbc.*:(.*)@.*".r
    val s3Pattern1 = "s3[an]?://([^:]*):([^@]*)@.*".r
    val optionPattern1 = ".*(?i)\\bpassword\\b '([^']*)'.*".r
    var maskedStr = str.replace("\n", " ")
    val mask = "xxxxx"

    maskedStr match {
      case jdbcPattern1(passwd) => maskedStr = maskedStr.replace(passwd, mask)
      case _ =>
    }
    maskedStr match {
      case jdbcPattern2(passwd) => maskedStr = maskedStr.replace(passwd, mask)
      case _ =>
    }
    maskedStr match {
      case s3Pattern1(passwd3, passwd4) => maskedStr = maskedStr.replace(passwd3, mask)
          .replace(passwd4, mask)
      case _ =>
    }
    maskedStr match {
      case optionPattern1(passwd) => maskedStr = maskedStr.replace(passwd, mask)
      case _ =>
    }
    maskedStr
  }
}
