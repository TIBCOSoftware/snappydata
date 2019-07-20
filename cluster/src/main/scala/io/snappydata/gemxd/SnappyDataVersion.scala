/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
package io.snappydata.gemxd

import java.io.{InputStream, PrintStream, PrintWriter}

import scala.collection.mutable

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.gemstone.gemfire.internal.shared.NativeCalls
import com.gemstone.gemfire.internal.{ClassPathLoader, GemFireVersion, SharedLibrary}
import com.pivotal.gemfirexd.internal.GemFireXDVersion
import com.pivotal.gemfirexd.internal.shared.common.SharedUtils

class SnappyDataVersion {
}

object SnappyDataVersion {

  // currently version in SNAPPYDATA_VERSION_PROPERTIES is used for column store version and
  // SnappyData platform version. If these 2 are to be given different versions separate
  // properties file can be created for column store
  private val SNAPPYDATA_VERSION_PROPERTIES = "io/snappydata/SnappyDataVersion.properties"

  val AQP_VERSION_PROPERTIES = "io/snappydata/SnappyAQPVersion.properties"

  private val isNativeLibLoaded: Boolean = {
    GemFireCacheImpl.setGFXDSystem(true)
    val isNativeLibLoaded = if (NativeCalls.getInstance.loadNativeLibrary) {
      SharedLibrary.register("gemfirexd")
    } else false
    val instance: GemFireVersion = GemFireVersion.getInstance(classOf[SnappyDataVersion],
      SNAPPYDATA_VERSION_PROPERTIES)
    if (isNativeLibLoaded) {
      // try to load _getNativeVersion by reflection
      try {
        val m = classOf[GemFireXDVersion].getDeclaredMethod("_getNativeVersion")
        instance.setNativeVersion(m.invoke(null).asInstanceOf[String])
      } catch {
        case _: Exception => // ignore
      }
    } else {
      instance.setNativeVersion("gemfirexd " + instance.getNativeVersion)
    }
    isNativeLibLoaded
  }

  def loadProperties(): Unit = {
    GemFireCacheImpl.setGFXDSystem(true)
    GemFireVersion.getInstance(classOf[SnappyDataVersion], SNAPPYDATA_VERSION_PROPERTIES)
  }

  // scalastyle:off println
  def print(ps: PrintStream): Unit = {
    val pw: PrintWriter = new PrintWriter(ps)

    // platform version
    loadProperties()
    val platform = s" Platform Version ${GemFireVersion.getProductVersion} " +
        s"${GemFireVersion.getProductReleaseStage}"

    // rowstore version
    GemFireVersion.getInstance(classOf[GemFireXDVersion], SharedUtils.GFXD_VERSION_PROPERTIES)
    val product = if (GemFireVersion.isEnterpriseEdition) "TIBCO ComputeDB" else "SnappyData"
    pw.println(product + platform)
    pw.printf("%4s%s\n", " ", GemFireVersion.getProductName + " " +
        GemFireVersion.getProductVersion + " " + GemFireVersion.getProductReleaseStage)

    // column store version
    GemFireVersion.getInstance(classOf[SnappyDataVersion], SNAPPYDATA_VERSION_PROPERTIES)
    pw.printf("%4s%s\n", " ", GemFireVersion.getProductName + " Column Store " +
        GemFireVersion.getProductVersion + " " + GemFireVersion.getProductReleaseStage)

    // AQP version if available
    val is: InputStream = ClassPathLoader.getLatest.getResourceAsStream(
      classOf[SnappyDataVersion], AQP_VERSION_PROPERTIES)
    if (is ne null) try {
      GemFireVersion.getInstance(classOf[SnappyDataVersion], AQP_VERSION_PROPERTIES)
      pw.printf("%4s%s\n", " ", GemFireVersion.getProductName + " " +
          GemFireVersion.getProductVersion + " " + GemFireVersion.getProductReleaseStage)
    } finally {
      is.close()
    }
    pw.flush()
  }

  def print(ps: PrintStream, printSourceInfo: Boolean): Unit = {
    if (!isNativeLibLoaded) {
      System.err.println("Native library not loaded")
    }

    val pw: PrintWriter = new PrintWriter(ps)

    GemFireVersion.getInstance(classOf[GemFireXDVersion], SharedUtils.GFXD_VERSION_PROPERTIES)
    pw.println(GemFireVersion.getProductName)
    GemFireVersion.print(pw, printSourceInfo)

    GemFireVersion.getInstance(classOf[SnappyDataVersion], SNAPPYDATA_VERSION_PROPERTIES)
    pw.println(GemFireVersion.getProductName)
    GemFireVersion.print(pw, printSourceInfo)

    // AQP version if available
    val is: InputStream = ClassPathLoader.getLatest.getResourceAsStream(
      classOf[SnappyDataVersion], AQP_VERSION_PROPERTIES)
    if (is ne null) try {
      GemFireVersion.getInstance(classOf[SnappyDataVersion], AQP_VERSION_PROPERTIES)
      pw.println(GemFireVersion.getProductName)
      GemFireVersion.print(pw, printSourceInfo)
    } finally {
      is.close()
    }

    pw.flush()
  }

  // scalastyle:on println

  def createVersionFile(): Unit = {
    loadProperties()
    GemFireVersion.createVersionFile()
  }

  def getSnappyDataProductVersion: mutable.HashMap[String, String] = {
    GemFireVersion.getInstance(classOf[GemFireXDVersion], SNAPPYDATA_VERSION_PROPERTIES)

    val versionDetails = mutable.HashMap.empty[String, String]
    versionDetails.put("productName", GemFireVersion.getProductName)
    versionDetails.put("productVersion", GemFireVersion.getProductVersion)
    versionDetails.put("buildId", GemFireVersion.getBuildId)
    versionDetails.put("buildDate", GemFireVersion.getBuildDate)
    versionDetails.put("buildPlatform", GemFireVersion.getBuildPlatform)
    versionDetails.put("nativeCodeVersion", GemFireVersion.getNativeCodeVersion)
    versionDetails.put("sourceRevision", GemFireVersion.getSourceRevision)

    GemFireVersion.getInstance(classOf[GemFireXDVersion], SharedUtils.GFXD_VERSION_PROPERTIES)
    val productEditionType = if (GemFireVersion.isEnterpriseEdition) "Enterprise" else "Community"

    versionDetails.put("editionType", productEditionType)

    versionDetails
  }
}
