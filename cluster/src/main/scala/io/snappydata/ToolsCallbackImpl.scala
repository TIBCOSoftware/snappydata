/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
import java.net.{URI, URLClassLoader}

import scala.collection.JavaConverters._
import com.gemstone.gemfire.cache.EntryExistsException
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.iapi.error.StandardException
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import io.snappydata.cluster.ExecutorInitiator
import io.snappydata.impl.{ExtendibleURLClassLoader, LeadImpl}
import io.snappydata.remote.interpreter.SnappyInterpreterExecute
import org.apache.spark.executor.SnappyExecutor
import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.columnar.impl.StoreCallbacksImpl
import org.apache.spark.sql.execution.ui.SQLTab
import org.apache.spark.sql.hive.thriftserver.SnappyHiveThriftServer2
import org.apache.spark.sql.internal.ContextJarUtils
import org.apache.spark.ui.{JettyUtils, SnappyDashboardTab}
import org.apache.spark.util.SnappyUtils
import org.apache.spark.{Logging, SparkCallbacks, SparkContext, SparkFiles}

import scala.collection.immutable.HashSet

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
        Misc.getMemStore.getMetadataCmdRgn.create(alias, deploySql)
      } catch {
        case _: EntryExistsException => throw StandardException.newException(
          SQLState.LANG_DB2_DUPLICATE_NAMES, alias, "of deploying jars/packages")
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
        val urlObj = Misc.getMemStore.getMetadataCmdRgn.get(ContextJarUtils.functionKeyPrefix + appName)
        val url: String = if (urlObj != null) urlObj.toString else null
        if (url != null && !url.isEmpty) {
          val executor = ExecutorInitiator.snappyExecBackend.executor.asInstanceOf[SnappyExecutor]
          val cachedFileName = s"${url.hashCode}-1_cache"
          val lockFileName = s"${url.hashCode}-1_lock"
          val localDir = new File(executor.getLocalDir)
          val lockFile = new File(localDir, lockFileName)
          val lockFileChannel = new RandomAccessFile(lockFile, "rw").getChannel
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

  override def removeURIsFromExecutorClassLoader(jars: Array[String]): Unit = {
    if (!jars.isEmpty && "__REMOVE_FILES_ONLY__" != jars.head
        && ExecutorInitiator.snappyExecBackend != null) {
      val snappyexecutor = ExecutorInitiator.snappyExecBackend.executor.asInstanceOf[SnappyExecutor]
      snappyexecutor.removeJarsFromExecutorLoader(jars)
    } else removeJarFiles(jars)
  }

  def removeJarFiles(jars: Array[String]): Unit = {
    jars.foreach(name => {
      val localName = name.split("/").last
      val jarFile = new File(SparkFiles.getRootDirectory(), localName)
      if (jarFile.exists()) {
        jarFile.delete()
        logDebug(s"Deleted jarFile $jarFile")
      }
    })
  }

  override def getAllGlobalCmnds: Array[String] = {
    GemFireXDUtils.waitForNodeInitialization()
    val r = Misc.getMemStore.getMetadataCmdRgn
    val keys = r.keySet().asScala.filter(p =>
      !(p.startsWith(ContextJarUtils.functionKeyPrefix) ||
          p.equals(Constant.CLUSTER_ID) ||
          p.startsWith(Constant.MEMBER_ID_PREFIX)))
    r.getAll(keys.asJava).values().toArray.filter(
      _.isInstanceOf[String]).map(_.asInstanceOf[String])
  }

  override def getGlobalCmndsSet: java.util.Set[java.util.Map.Entry[String, Object]] = {
    GemFireXDUtils.waitForNodeInitialization()
    Misc.getMemStore.getMetadataCmdRgn.entrySet()
  }

  override def removePackage(alias: String): Unit = {
    GemFireXDUtils.waitForNodeInitialization()
    Misc.getMemStore.getMetadataCmdRgn.destroy(alias)
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

  override def removeURIs(uris: Array[String], isPackage: Boolean): Unit = {
    val snc: SparkContext = SnappyContext.globalSparkContext
    uris.foreach(uri => snc.removeFile(uri))
    val lead = ServiceManager.getLeadInstance.asInstanceOf[LeadImpl]
    val allURLs = lead.urlclassloader.getURLs
    val updatedURLs = allURLs.toBuffer
    uris.foreach(uri => {
      val newUri = new URI("file:" + uri)
      if (updatedURLs.contains(newUri.toURL)) {
        updatedURLs.remove(updatedURLs.indexOf(newUri.toURL))
      }
    })
    lead.urlclassloader = new ExtendibleURLClassLoader(lead.urlclassloader.getParent)
    updatedURLs.foreach(url => lead.urlclassloader.addURL(url))
    Thread.currentThread().setContextClassLoader(lead.urlclassloader)
  }

  override def updateIntpGrantRevoke(grantor: String, isGrant: Boolean, users: String): Unit = {
    SnappyInterpreterExecute.handleNewPermissions(grantor, isGrant, users)
  }
}
