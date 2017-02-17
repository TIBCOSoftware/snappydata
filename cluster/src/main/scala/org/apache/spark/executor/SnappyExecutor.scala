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
import java.net.{URL, URLClassLoader}

import scala.collection.mutable

import com.pivotal.gemfirexd.internal.engine.Misc

import org.apache.spark.serializer.KryoSerializerPool
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

  override def createClassLoader(): MutableURLClassLoader = {
    // Bootstrap the list of jars with the user class path.
    val now = System.currentTimeMillis()
    userClassPath.foreach { url =>
      currentJars(url.getPath.split("/").last) = now
    }

    val currentLoader = Utils.getContextOrSparkClassLoader

    // For each of the jars in the jarSet, add them to the class loader.
    // We assume each of the files has already been fetched.
    val urls = userClassPath.toArray ++ currentJars.keySet.map { uri =>
      new File(uri.split("/").last).toURI.toURL
    }
    val jobJars = scala.collection.mutable.Map[String, URLClassLoader]()
    new SnappyMutableURLClassLoader(urls, currentLoader, jobJars)

  }

  def getName(path: String) = new File(path).getName

  override def updateDependencies(newFiles: mutable.HashMap[String, Long],
      newJars: mutable.HashMap[String, Long]): Unit = {
    synchronized {
      val classloader = urlClassLoader.asInstanceOf[SnappyMutableURLClassLoader]
      val addedJarFiles = classloader.getAddedURLs.toList
      val newJarFiles = newJars.keys.map(getName).toList
      val diffJars = addedJarFiles.diff(newJarFiles)
      if (diffJars.size > 0) {
        diffJars.foreach(classloader.removeURL)
        Misc.getCacheLogWriter.info("As some of the Jars have been deleted, setting up a new ClassLoader for subsequent Threads")
        diffJars.foreach(d => Misc.getCacheLogWriter.info(s"removed jar $d"))

        this.urlClassLoader = new SnappyMutableURLClassLoader(classloader.getURLs(),
          classloader.getParent, classloader.jobJars)
        this.replClassLoader = addReplClassLoaderIfNeeded(urlClassLoader)
        super.updateDependencies(newFiles, newJars)
        env.serializer.setDefaultClassLoader(this.replClassLoader)
        env.closureSerializer.setDefaultClassLoader(this.replClassLoader)
        Thread.currentThread().setContextClassLoader(this.replClassLoader)
      } else {
        super.updateDependencies(newFiles, newJars)
      }
    }
  }
}

class SnappyMutableURLClassLoader(urls: Array[URL],
    parent: ClassLoader,
    val jobJars : scala.collection.mutable.Map[String, URLClassLoader])
    extends MutableURLClassLoader(urls, parent) {


  protected def getJobName: String = {
    var jobFile = ""
    val taskDeserializationProps = Executor.taskDeserializationProps.get()
    if (null != taskDeserializationProps) {
      jobFile = taskDeserializationProps.getProperty(io.snappydata.Constant
          .CHANGEABLE_JAR_NAME, "")
    }
    new File(jobFile).getName
  }

  override def addURL(url: URL): Unit = {
    val jobName = getJobName
    if (jobName.isEmpty) {
      super.addURL(url)
    }
    else {
      jobJars.put(jobName, new URLClassLoader(Array(url)))
    }
  }

  def getAddedURLs: Array[String] = {
    jobJars.keys.toArray
  }

  def removeURL(jar: String): Unit = {
    if (jobJars.contains(jar)) {
      val urlLoader = jobJars.get(jar)
      if (urlLoader.isDefined) {
        val file = new File(SparkFiles.getRootDirectory() , jar)
        jobJars.remove(jar)
        if (file.exists()) {
          Misc.getCacheLogWriter.info(s"Removing $jar from Spark root directory")
          file.delete()
        }
      }
    }
  }

  override def loadClass(name: String, resolve: Boolean): Class[_] = {
    loadJar(() => super.loadClass(name, resolve)).
        getOrElse(loadJar(() => Misc.getMemStore.getDatabase.getClassFactory.loadClassFromDB(name))
            .getOrElse(loadJar(() => loadClassFromJobJar(name), true).get))
  }

  def loadJar(f: () => Class[_], throwException: Boolean = false): Option[Class[_]] = {
    try {
      Option(f())
    } catch {
      case cnfe: ClassNotFoundException => if (throwException) throw cnfe
      else None
    }
  }

  def loadClassFromJobJar(className: String): Class[_] = {
    val jobName = getJobName
    if (!jobName.isEmpty) {
      jobJars.get(jobName) match {
        case Some(loader) => loader.loadClass(className)
        case _ => throw new ClassNotFoundException(className)
      }
    }
    else throw new ClassNotFoundException(className)
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
      case oom: OutOfMemoryError => Runtime.getRuntime.halt(SparkExitCode.OOM)
      case t: Throwable => Runtime.getRuntime.halt(SparkExitCode.UNCAUGHT_EXCEPTION_TWICE)
    }
  }
}

