/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import java.io.File
import java.net.URLClassLoader
import java.util.UUID

import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.cluster.ExecutorInitiator
import io.snappydata.impl.LeadImpl
import org.apache.spark.executor.SnappyExecutor
import org.apache.spark.{SparkContext, SparkFiles}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.ui.SnappyDashboardTab
import org.apache.spark.util.{SnappyUtils, Utils}

object ToolsCallbackImpl extends ToolsCallback {

  override def updateUI(sc: SparkContext): Unit = {
    SnappyUtils.getSparkUI(sc).foreach(new SnappyDashboardTab(_))
  }

  override def removeAddedJar(sc: SparkContext, jarName: String): Unit =
    sc.removeAddedJar(jarName)

  /**
    * Callback to spark Utils to fetch file
    */
  override def doFetchFile(
      url: String,
      targetDir: File,
      filename: String): File = {
    SnappyUtils.doFetchFile(url, targetDir, filename)
  }

  override def setSessionDependencies(sparkContext: SparkContext, appName: String,
      classLoader: ClassLoader): Unit = {
    SnappyUtils.setSessionDependencies(sparkContext, appName, classLoader)
  }

  override def addURIs(alias: String, jars: Array[String],
    deploySql: String, isPackage: Boolean = true): Unit = {
    if (alias != null) {
      Misc.getMemStore.getGlobalCmdRgn.put(alias, deploySql)
    }
    val lead = ServiceManager.getLeadInstance.asInstanceOf[LeadImpl]
    val loader = lead.urlclassloader
    jars.foreach(j => {
      val url = new File(j).toURI.toURL
      loader.addURL(url)
    })
    // Close and reopen interpreter
    if (alias != null) {
      lead.closeAndReopenInterpreterServer();
    }
  }

  override def addURIsToExecutorClassLoader(jars: Array[String]): Unit = {
    if (ExecutorInitiator.snappyExecBackend != null) {
      val snappyexecutor = ExecutorInitiator.snappyExecBackend.executor.asInstanceOf[SnappyExecutor]
      snappyexecutor.updateMainLoader(jars)
    }
  }

  override def getAllGlobalCmnds(): Array[String] = {
    GemFireXDUtils.waitForNodeInitialization()
    Misc.getMemStore.getGlobalCmdRgn.values().toArray.map(_.asInstanceOf[String])
  }

  override def getGlobalCmndsSet(): java.util.Set[java.util.Map.Entry[String, String]] = {
    GemFireXDUtils.waitForNodeInitialization()
    Misc.getMemStore.getGlobalCmdRgn.entrySet()
  }

  override def removePackage(alias: String): Unit = {
    GemFireXDUtils.waitForNodeInitialization()
    val packageRegion = Misc.getMemStore.getGlobalCmdRgn()
    packageRegion.destroy(alias)
  }

  override def setLeadClassLoader(): Unit = {
    val instance = ServiceManager.currentFabricServiceInstance
    instance match {
      case li: LeadImpl => {
        val loader = li.urlclassloader
        if (loader != null) {
          Thread.currentThread().setContextClassLoader(loader)
        }
      }
      case _ =>
    }
  }

  override def getLeadClassLoader(): URLClassLoader = {
    var ret: URLClassLoader = null
    val instance = ServiceManager.currentFabricServiceInstance
    instance match {
      case li: LeadImpl => {
        val loader = li.urlclassloader
        if (loader != null) {
          ret = loader
        }
      }
      case _ =>
    }
    ret
  }
}
