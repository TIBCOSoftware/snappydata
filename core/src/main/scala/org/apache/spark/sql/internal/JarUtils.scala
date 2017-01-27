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

import org.apache.spark.sql.collection.ToolsCallbackInit
import org.apache.spark.sql.{SnappyContext, SnappySession}
import org.apache.spark.util.MutableURLClassLoader
import org.apache.spark.{Logging, SparkContext}

/**
 * An utility class to store jar file reference with their individual classloaders. This is reflect class changes at driver side.
 * e.g. If an UDF definition changes the driver should pick up the correct UDF class.
 * This class can not initialize itself after a driver failure. So the callers will have  to make sure that the classloader gets
 * initialized after a driver startup. Usually it can be achieved by adding classloader at query time.
 *
 */
class ContextJarUtils(sparkContext: SparkContext) extends Logging{

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
   * This ensures class & jar isolation although it might be repeatitive.
   * @param prefix prefix to be given to the jar name
   * @param path  original path of the jar/
   */
  def addToSparkJars(prefix: String , path: String) {
    logInfo("Adding jar to sc from driver loader" + path)
    sparkContext.addJar(path)
    //Setting the local property. Snappy Cluster executors will take appropriate actions
    sparkContext.
        setLocalProperty(io.snappydata.Constant.CHANGEABLE_JAR_NAME, path)
  }

  def removeFromSparkJars(path: String): Unit = {
    def getName(path: String): String = new File(path).getName
    val sc = SnappyContext.globalSparkContext

    val keyToRemove = sc.listJars().filter(getName(_) == getName(path))
    if (keyToRemove.nonEmpty) {
      val callbacks = ToolsCallbackInit.toolsCallback
      //@TODO This is a temp workaround to fix SNAP-1133. sc.addedJar should be directly be accessible from here.
      //May be due to scala version mismatch.
      if (callbacks != null) {
        logInfo(s"Removing ${path} from Spark Jars by Driver loader")
        callbacks.removeAddedJar(sc, keyToRemove.head)
      }
    }
    //Remove the jar from all live executors.
    org.apache.spark.sql.collection.Utils.mapExecutors(
      sparkContext,
      (_, _) => Seq(1).iterator
    ).count
  }

}

//@TODO To be implemented later
class SessionJarUtils(val snappySession: SnappySession) {

}

