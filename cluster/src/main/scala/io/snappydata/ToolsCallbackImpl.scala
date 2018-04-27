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

  override def addURIs(alias: String, jars: Array[String], deploySql: String): Unit = {
    Misc.getMemStore.getGlobalCmdRgn.put(alias, deploySql)
    val logger = Misc.getCacheLogWriter
    logger.info(s"KN: addURIs key = $alias and val = $deploySql")
    getAllGlobalCmnds.foreach(x => logger.info(s"KN: retrieved value = $x"))
    val lead = ServiceManager.getLeadInstance.asInstanceOf[LeadImpl]
    val loader = lead.urlclassloader
    jars.foreach(j => {
      val url = new File(j).toURI.toURL
      println(s"KN: addURIs add url called with url: $url" )
      loader.addURL(url)
    })
  }

  override def addURIsToExecutorClassLoader(jars: Array[String]): Unit = {
    println(s"KN: addURIsToExecutorClassLoader called with jars: $jars" )
    if (ExecutorInitiator.snappyExecBackend != null) {
      val snappyexecutor = ExecutorInitiator.snappyExecBackend.executor.asInstanceOf[SnappyExecutor]
      snappyexecutor.updateMainLoader(jars)
    }
  }

  override def getAllGlobalCmnds(): Array[String] = {
    GemFireXDUtils.waitForNodeInitialization()
    val values = Misc.getMemStore.getGlobalCmdRgn.values()
    val logger = Misc.getCacheLogWriter
    logger.info(s"KN: getAllGlobalCmnds size of the region = ${Misc.getMemStore.getGlobalCmdRgn.size()}")
    logger.info(s"KN: getAllGlobalCmnds values = ${values}", new Exception)
    values.toArray.foreach(x => logger.info(s"KN: getAllGlobalCmnds Inside foreach - $x"))
    Misc.getMemStore.getGlobalCmdRgn.values().toArray.map(_.asInstanceOf[String])
  }

  override def removePackage(alias: String): Unit = {
    GemFireXDUtils.waitForNodeInitialization()
    val packageRegion = Misc.getMemStore.getGlobalCmdRgn()
    packageRegion.destroy(alias)
  }
}
