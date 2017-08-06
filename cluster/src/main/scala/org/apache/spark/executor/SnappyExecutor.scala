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

import scala.collection.mutable

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.pivotal.gemfirexd.internal.engine.Misc

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.util.{MutableURLClassLoader, ShutdownHookManager, SparkExitCode, Utils}
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

  val classLoaderCache = {
    val loader = new CacheLoader[ClassLoaderKey, SnappyMutableURLClassLoader]() {
      override def load(key: ClassLoaderKey): SnappyMutableURLClassLoader = {
        val appName = key.appName
        val appNameAndJars = key.appNameAndJars
        lazy val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
        val appDependencies = appNameAndJars.drop(2).toSeq
        Misc.getCacheLogWriter.info(s"Creating ClassLoader for $appName" +
            s" with dependencies $appDependencies")
        val urls = appDependencies.map(name => {
          val localName = name.split("/").last
          Misc.getCacheLogWriter.info(s"Fetching file $name for App[$appName]")
          Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
            env.securityManager, hadoopConf, -1L, useCache = !isLocal)
          val url = new File(SparkFiles.getRootDirectory(), localName).toURI.toURL
          url
        })
        val newClassLoader = new SnappyMutableURLClassLoader(urls.toArray, replClassLoader)
        env.serializer.setDefaultClassLoader(replClassLoader)
        env.closureSerializer.setDefaultClassLoader(replClassLoader)
        newClassLoader
      }
    }
    // Keeping 500 as cache size. Can revisit the number
    CacheBuilder.newBuilder().maximumSize(500).build(loader)
  }

  class ClassLoaderKey(val appName: String,
      val appTime: String,
      val appNameAndJars: Array[String]) {

    override def hashCode(): Int = (appName, appTime).hashCode()

    override def equals(obj: Any): Boolean = {
      obj match {
        case x: ClassLoaderKey =>
          (x.appName, x.appTime).equals(appName, appTime)
        case _ => false
      }
    }
  }

  override def updateDependencies(newFiles: mutable.HashMap[String, Long],
      newJars: mutable.HashMap[String, Long]): Unit = {
    super.updateDependencies(newFiles, newJars)
    synchronized {
      val taskDeserializationProps = Executor.taskDeserializationProps.get()
      if (null != taskDeserializationProps) {
        val appDetails = taskDeserializationProps.getProperty(io.snappydata.Constant
            .CHANGEABLE_JAR_NAME, "")
        Misc.getCacheLogWriter.info(s"Submitted Application Details $appDetails")
        if (!appDetails.isEmpty) {
          val appNameAndJars = appDetails.split(",")
          val appName = appNameAndJars(0)
          val appTime = appNameAndJars(1)
          val threadClassLoader =
            classLoaderCache.getUnchecked(new ClassLoaderKey(appName, appTime, appNameAndJars))
          Misc.getCacheLogWriter.info(s"Setting thread classloader  $threadClassLoader")
          Thread.currentThread().setContextClassLoader(threadClassLoader)
        }
      }
    }
  }
}

class SnappyMutableURLClassLoader(urls: Array[URL],
    parent: ClassLoader)
    extends MutableURLClassLoader(urls, parent) with Logging {


  override def loadClass(name: String, resolve: Boolean): Class[_] = {
    loadJar(() => super.loadClass(name, resolve)).
        getOrElse(loadJar(() => Misc.getMemStore.getDatabase.getClassFactory.loadClassFromDB(name),
          throwException = true).get)
  }

  def loadJar(f: () => Class[_], throwException: Boolean = false): Option[Class[_]] = {
    try {
      Option(f())
    } catch {
      case cnfe: ClassNotFoundException => if (throwException) throw cnfe
      else None
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

