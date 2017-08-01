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
package org.apache.spark.executor

import java.io.File
import java.net.URL
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.{MutableURLClassLoader, ShutdownHookManager, SnappyContextURLLoader, SparkExitCode, Utils}
import org.apache.spark.{Logging, SparkEnv, SparkFiles}

class SnappyExecutor(
    executorId: String,
    executorHostname: String,
    env: SparkEnv,
    userClassPath: Seq[URL] = Nil,
    exceptionHandler: SnappyUncaughtExceptionHandler,
    isLocal: Boolean = false)
    extends Executor(executorId, executorHostname, env, userClassPath, isLocal) {

  if (!isLocal) {
    // Setup an uncaught exception handler for non-local mode.
    // Make any thread terminations due to uncaught exceptions
    // kill the executor component
    Thread.setDefaultUncaughtExceptionHandler(exceptionHandler)
  }

  // appName -> comma separated jarFiles
  private val allJars = new ConcurrentHashMap[(String, String), MutableURLClassLoader]().asScala


  def getName(path: String): String = new File(path).getName

  override def updateDependencies(newFiles: mutable.HashMap[String, Long],
      newJars: mutable.HashMap[String, Long]): Unit = {
    super.updateDependencies(newFiles, newJars)
    synchronized {
      val taskDeserializationProps = Executor.taskDeserializationProps.get()
      if (null != taskDeserializationProps) {
        val appDetails = taskDeserializationProps.getProperty(io.snappydata.Constant
            .CHANGEABLE_JAR_NAME, "")
        logInfo(s"AppDetails $appDetails")
        if (!appDetails.isEmpty) {
          val appNameAndJars = appDetails.split(",")
          val appName = appNameAndJars(0)
          val appTime = appNameAndJars(1)
          logInfo(s"appName $appName appTime $appTime allJars ${allJars.size}")
          val threadClassLoader = allJars.get((appName, appTime)).getOrElse({
            lazy val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
            val appDependencies = appNameAndJars.drop(2).toSeq
            logInfo(s"appDependencies $appDependencies")
            val urls = appDependencies.map(name => {
              val localName = name.split("/").last
              logInfo(s"Fetching file $name")
              Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
                env.securityManager, hadoopConf, -1L, useCache = !isLocal)
              val url = new File(SparkFiles.getRootDirectory(), localName).toURI.toURL
              url
            })
            val newClassLoader = new MutableURLClassLoader(urls.toArray, replClassLoader)
            allJars.putIfAbsent((appName, appTime), newClassLoader)
            newClassLoader
          })
          Thread.currentThread().setContextClassLoader(threadClassLoader)
        }
      }
    }
  }
}

/**
 * The default uncaught exception handler for Executors
 */
private class SnappyUncaughtExceptionHandler(
    val executorBackend: SnappyCoarseGrainedExecutorBackend)
    extends Thread.UncaughtExceptionHandler with Logging {

  override def uncaughtException(thread: Thread, exception: Throwable) {
    try {
      // Make it explicit that uncaught exceptions are thrown when container is shutting down.
      // It will help users when they analyze the executor logs
      val inShutdownMsg = if (ShutdownHookManager.inShutdown()) "[Container in shutdown] " else ""
      val errMsg = "Uncaught exception in thread "
      logError(inShutdownMsg + errMsg + thread, exception)

      // We may have been called from a shutdown hook, there is no need to do anything
      if (!ShutdownHookManager.inShutdown()) {
        if (exception.isInstanceOf[OutOfMemoryError]) {
          executorBackend.exitExecutor(SparkExitCode.OOM, "Out of Memory", exception)
        } else {
          executorBackend.exitExecutor(
            SparkExitCode.UNCAUGHT_EXCEPTION, errMsg, exception)
        }
      }
    } catch {
      // Exception while handling an uncaught exception. we cannot do much here
      case _: OutOfMemoryError => Runtime.getRuntime.halt(SparkExitCode.OOM)
      case _: Throwable => Runtime.getRuntime.halt(SparkExitCode.UNCAUGHT_EXCEPTION_TWICE)
    }
  }
}

