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

package org.apache.spark.sql.internal

import java.io.File
import java.net.{URL, URLClassLoader}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.sql.collection.{ToolsCallbackInit}
import org.apache.spark.sql.{SnappyContext, SnappySession}
import org.apache.spark.util.{Utils, MutableURLClassLoader}
import org.apache.spark.{SparkEnv, SparkFiles, Logging, SparkContext}

/**
 * An utility class to store jar file reference with their individual classloaders.
 * This is to reflect class changes at driver side.
 * e.g. If an UDF definition changes the driver should pick up the correct UDF class.
 * This class can not initialize itself after a driver failure. So the callers will have  to make sure that the classloader gets
 * initialized after a driver startup. Usually it can be achieved by adding classloader at query time.
 *
 */
object ContextJarUtils extends Logging {
  def sparkContext = SnappyContext.globalSparkContext

  val JAR_PATH = "snappy-jars"

  def jarDir = {
    val jarDirectory = new File(System.getProperty("user.dir"), JAR_PATH)
    if(!jarDirectory.exists()) jarDirectory.mkdir()
    jarDirectory
  }
  private val driverJars = new ConcurrentHashMap[String, URLClassLoader]().asScala

  def addDriverJar(key: String, classLoader: URLClassLoader): Option[URLClassLoader] = {
    driverJars.putIfAbsent(key, classLoader)
  }

  def addIfNotPresent(key: String, urls: Array[URL], parent: ClassLoader): Unit = {
    if (driverJars.get(key).isEmpty) {
      driverJars.putIfAbsent(key, new MutableURLClassLoader(urls, parent))
    }
  }

  def removeDriverJar(key: String) = driverJars.remove(key)

  def getDriverJar(key: String): Option[URLClassLoader] = driverJars.get(key)

  /**
   * This method will copy the given jar to Spark root directory and then it will add the same jar to Spark.
   * This ensures class & jar isolation although it might be repetitive.
   * @param prefix prefix to be given to the jar name
   * @param path  original path of the jar
   */
  def addToSparkJars(prefix: String, path: String) {
    val callbacks = ToolsCallbackInit.toolsCallback
    if (callbacks != null) {
      val localName = path.split("/").last
      val changedFileName = s"${prefix}-${localName}"
      logInfo("Adding jar to sc from driver loader" + path)
      val changedFile = new File(jarDir, changedFileName)
      if (!changedFile.exists()) {
        //After creation removeFromSparkJars() is the only place which can remove this jar
        val newFile = callbacks.doFetchFile(path, jarDir, changedFileName)
        sparkContext.addJar(newFile.getPath)
        //Setting the local property. Snappy Cluster executors will take appropriate actions
        sparkContext.
            setLocalProperty(io.snappydata.Constant.CHANGEABLE_JAR_NAME, newFile.getPath)
      } else {
        sparkContext.addJar(changedFile.getPath)
        //Setting the local property. Snappy Cluster executors will take appropriate actions
        sparkContext.
            setLocalProperty(io.snappydata.Constant.CHANGEABLE_JAR_NAME, changedFile.getPath)
      }
    } else {
      sparkContext.addJar(path)
    }
  }

  def removeFromSparkJars(prefix: String, path: String): Unit = {
    def getName(path: String): String = new File(path).getName

    val callbacks = ToolsCallbackInit.toolsCallback
    if (callbacks != null) {
      val localName = path.split("/").last
      val changedFileName = s"${prefix}-${localName}"
      val jarFile = new File(jarDir, changedFileName)

      if (jarFile.exists()) {
        jarFile.delete()
      }

      val keyToRemove = sparkContext.listJars().filter(getName(_) == getName(jarFile.getPath))
      if (keyToRemove.nonEmpty) {
        logInfo(s"Removing ${path} from Spark Jars by Driver loader")
        callbacks.removeAddedJar(sparkContext, keyToRemove.head)
      }
      //Remove the jar from all live executors.
      org.apache.spark.sql.collection.Utils.mapExecutors(
        sparkContext,
        (_, _) => Seq(1).iterator
      ).count
    }
  }
}

//@TODO To be implemented later
class SessionJarUtils(val snappySession: SnappySession) {

}

