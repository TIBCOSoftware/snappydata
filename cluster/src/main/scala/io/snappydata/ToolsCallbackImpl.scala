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
package io.snappydata

import java.io.{File, RandomAccessFile}
import java.lang.reflect.InvocationTargetException
import java.net.URLClassLoader

import com.gemstone.gemfire.cache.EntryExistsException
import scala.collection.JavaConverters._
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.iapi.error.StandardException
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import io.snappydata.cluster.ExecutorInitiator
import io.snappydata.impl.LeadImpl

import org.apache.spark.executor.SnappyExecutor
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.columnar.impl.StoreCallbacksImpl
import org.apache.spark.sql.execution.ui.SQLTab
import org.apache.spark.sql.hive.thriftserver.SnappyHiveThriftServer2
import org.apache.spark.sql.internal.ContextJarUtils
import org.apache.spark.ui.{JettyUtils, SnappyDashboardTab}
import org.apache.spark.util.SnappyUtils
import org.apache.spark.{Logging, SparkCallbacks, SparkContext, SparkFiles}

object ToolsCallbackImpl extends ToolsCallback with Logging {

  override def updateUI(sc: SparkContext): Unit = {

    SnappyUtils.getSparkUI(sc).foreach(ui => {
      // Create Snappy Dashboard and SQL tabs.
      // Set SnappyData authenticator SecurityHandler.
      SparkCallbacks.getAuthenticatorForJettyServer() match {
        case Some(_) =>
          logInfo("Setting auth handler")
          // Set JettyUtils.skipHandlerStart for adding dashboard and sql security handlers
          JettyUtils.skipHandlerStart.set(true)
          // Creating SQL and Dashboard UI tabs
          if (!sc.isLocal) {
            new SQLTab(ExternalStoreUtils.getSQLListener.get(), ui)
          }
          SnappyHiveThriftServer2.attachUI()
          new SnappyDashboardTab(ui)
          // Set security handlers
          ui.getHandlers.foreach { h =>
            if (!h.isStarted) {
              h.setSecurityHandler(JettyUtils.basicAuthenticationHandler())
              h.start()
            }
          }
          // Unset JettyUtils.skipHandlerStart
          JettyUtils.skipHandlerStart.set(false)
        case None => logDebug("Not setting auth handler")
          // Creating SQL and Dashboard UI tabs
          if (!sc.isLocal) {
            new SQLTab(ExternalStoreUtils.getSQLListener.get(), ui)
          }
          SnappyHiveThriftServer2.attachUI()
          new SnappyDashboardTab(ui)
      }
    })
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
      classLoader: ClassLoader, addAllJars: Boolean): Unit = {
    SnappyUtils.setSessionDependencies(sparkContext, appName, classLoader, addAllJars)
  }

  override def addURIs(alias: String, jars: Array[String],
      deploySql: String, isPackage: Boolean = true): Unit = {
    if (alias != null) {
      try {
        Misc.getMemStore.getGlobalCmdRgn.create(alias, deploySql)
      } catch {
        case eee: EntryExistsException => throw StandardException.newException(
          SQLState.LANG_DB2_DUPLICATE_NAMES, alias , "of deploying jars/packages")
      }
    }
    val lead = ServiceManager.getLeadInstance.asInstanceOf[LeadImpl]
    val loader = lead.urlclassloader
    jars.foreach(j => {
      val url = new File(j).toURI.toURL
      loader.addURL(url)
    })
    // Close and reopen interpreter
    if (alias != null) {
      try {
        lead.closeAndReopenInterpreterServer()
      } catch {
        case ite: InvocationTargetException => assert(ite.getCause.isInstanceOf[SecurityException])
      }
    }
  }

  override def addURIsToExecutorClassLoader(jars: Array[String]): Unit = {
    if (ExecutorInitiator.snappyExecBackend != null) {
      val snappyexecutor = ExecutorInitiator.snappyExecBackend.executor.asInstanceOf[SnappyExecutor]
      snappyexecutor.updateMainLoader(jars)
    }
  }

  override def removeFunctionJars(args: Array[String]): Unit = {
    if (ExecutorInitiator.snappyExecBackend != null) {
      // Remove the file from work directory
      val jarFile = new File(SparkFiles.getRootDirectory(), args(0))
      if (jarFile.exists()) {
        jarFile.delete()
        logDebug(s"Deleted jarFile $jarFile for UDF ${args(0)}")
      }

      // Remove the file from spark directory
      if (!args(0).isEmpty) { // args(0) = appname-filename
        val appName = args(0).split('-')(0)
        // This url points to the jar on the file server
        val url = Misc.getMemStore.getGlobalCmdRgn.get(ContextJarUtils.functionKeyPrefix + appName)
        if (url != null && !url.isEmpty) {
          val executor = ExecutorInitiator.snappyExecBackend.executor.asInstanceOf[SnappyExecutor]
          val cachedFileName = s"${url.hashCode}-1_cache"
          val lockFileName = s"${url.hashCode}-1_lock"
          val localDir = new File(executor.getLocalDir())
          val lockFile = new File(localDir, lockFileName)
          val lockFileChannel = new RandomAccessFile(lockFile, "rw").getChannel()
          val lock = lockFileChannel.lock()
          try {
            val cachedFile = new File(localDir, cachedFileName)
            if (cachedFile.exists()) {
              cachedFile.delete()
              logDebug(s"Deleted $cachedFile for UDF ${args(0)}")
            }
          } finally {
            lock.release()
            lockFileChannel.close()
          }
        }
      }
    }
  }

  override def getAllGlobalCmnds: Array[String] = {
    GemFireXDUtils.waitForNodeInitialization()
    val r = Misc.getMemStore.getGlobalCmdRgn
    val keys = r.keySet().asScala.filter(p => !p.startsWith(ContextJarUtils.functionKeyPrefix))
    r.getAll(keys.asJava).values().toArray.map(_.asInstanceOf[String])
  }

  override def getGlobalCmndsSet: java.util.Set[java.util.Map.Entry[String, String]] = {
    GemFireXDUtils.waitForNodeInitialization()
    Misc.getMemStore.getGlobalCmdRgn.entrySet()
  }

  override def removePackage(alias: String): Unit = {
    GemFireXDUtils.waitForNodeInitialization()
    Misc.getMemStore.getGlobalCmdRgn.destroy(alias)
  }

  override def setLeadClassLoader(): Unit = {
    val instance = ServiceManager.currentFabricServiceInstance
    instance match {
      case li: LeadImpl =>
        val loader = li.urlclassloader
        if (loader != null) {
          Thread.currentThread().setContextClassLoader(loader)
        }
      case _ =>
    }
  }

  override def getLeadClassLoader: URLClassLoader = {
    var ret: URLClassLoader = null
    val instance = ServiceManager.currentFabricServiceInstance
    instance match {
      case li: LeadImpl =>
        val loader = li.urlclassloader
        if (loader != null) {
          ret = loader
        }
      case _ =>
    }
    ret
  }

  override def checkSchemaPermission(schema: String, currentUser: String): String =
    StoreCallbacksImpl.checkSchemaPermission(schema, currentUser)
}
