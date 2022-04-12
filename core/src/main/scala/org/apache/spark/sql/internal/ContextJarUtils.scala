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

package org.apache.spark.sql.internal

import java.io.File
import java.net.{URL, URLClassLoader}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.matching.Regex

import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.sql.catalog.SnappyExternalCatalog

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException
import org.apache.spark.sql.catalyst.catalog.CatalogFunction
import org.apache.spark.sql.collection.ToolsCallbackInit
import org.apache.spark.sql.execution.RefreshMetadata
import org.apache.spark.sql.{AnalysisException, SnappyContext}

/**
  * An utility class to store jar file reference with their individual class loaders.
  * This is to reflect class changes at driver side.
  * e.g. If an UDF definition changes the driver should pick up the correct UDF class.
  * This class can not initialize itself after a driver failure.
  * So the callers will have  to make sure that the classloader gets
  * initialized after a driver startup. Usually it can be achieved by
  * adding classloader at query time.
  *
  */
object ContextJarUtils extends Logging {
  final val JAR_PATH: String = "snappy-jars"
  private final val driverJars = new ConcurrentHashMap[String, URLClassLoader]().asScala
  final val functionKeyPrefix: String = "__FUNC__"
  final val droppedFunctionsKey: String = functionKeyPrefix + "DROPPED__"
  final val DELIMITER: String = ","

  def addDriverJar(key: String, classLoader: URLClassLoader): Option[URLClassLoader] = {
    driverJars.putIfAbsent(key, classLoader)
  }

  def getDriverJar(key: String): Option[URLClassLoader] = driverJars.get(key)

  def removeDriverJar(key: String): Unit = driverJars.remove(key)

  def getDriverJarURLs: Array[URL] = {
    val urls = new mutable.HashSet[URL]()
    driverJars.foreach(_._2.getURLs.foreach(urls += _))
    urls.toArray
  }

  /**
    * This method will copy the given jar to Spark root directory
    * and then it will add the same jar to Spark.
    * This ensures class & jar isolation although it might be repetitive.
    *
    * @param prefix prefix to be given to the jar name
    * @param path   original path of the jar
    */
  def fetchFile(prefix: String, path: String): URL = {
    val callbacks = ToolsCallbackInit.toolsCallback
    val localName = path.split("/").last
    val changedFileName = s"$prefix-$localName"
    logInfo(s"Fetching jar $path to driver local directory $jarDir")
    val changedFile = new File(jarDir, changedFileName)
    if (!changedFile.exists()) {
      callbacks.doFetchFile(path, jarDir, changedFileName)
    }
    new File(jarDir, changedFileName).toURI.toURL
  }

  def deleteFile(prefix: String, path: String, isEmbedded: Boolean): Unit = {
    val callbacks = ToolsCallbackInit.toolsCallback
    if (callbacks != null) {
      val localName = path.split("/").last
      val changedFileName = s"$prefix-$localName"
      val jarFile = new File(jarDir, changedFileName)

      try {
        if (isEmbedded) {
          // Add to the list in (__FUNC__DROPPED__, dropped-udf-list)
          addToTheListInCmdRegion(droppedFunctionsKey, prefix, droppedFunctionsKey)
        }
        if (jarFile.exists()) {
          jarFile.delete()
          RefreshMetadata.executeOnAll(sparkContext, RefreshMetadata.REMOVE_FUNCTION_JAR,
            Array(changedFileName))
        }
      } finally {
        if (isEmbedded) {
          Misc.getMemStore.getMetadataCmdRgn.remove(functionKeyPrefix + prefix)
        }
      }
    }
  }

  def removeFunctionArtifacts(externalCatalog: SnappyExternalCatalog,
      sessionCatalog: Option[SnappySessionCatalog], schemaName: String, functionName: String,
      isEmbeddedMode: Boolean, ignoreIfNotExists: Boolean = false): Unit = {
    val identifier = FunctionIdentifier(functionName, Some(schemaName))
    removeDriverJar(identifier.unquotedString)

    try {
      val catalogFunction = externalCatalog.getFunction(schemaName, identifier.funcName)
      catalogFunction.resources.foreach { r =>
        deleteFile(catalogFunction.identifier.unquotedString, r.uri, isEmbeddedMode)
      }
    } catch {
      case e: AnalysisException =>
        if (!ignoreIfNotExists) {
          sessionCatalog match {
            case Some(ssc) => ssc.failFunctionLookup(functionName)
            case None => throw new NoSuchFunctionException(schemaName, identifier.funcName)
          }
        } else { // Log, just in case.
          logDebug(s"Function ${identifier.funcName} possibly not found: $e")
        }
    }
  }

  def addFunctionArtifacts(funcDefinition: CatalogFunction, key: String): Unit = {
    // resources has just one jar
    val jarPath = if (funcDefinition.resources.isEmpty) "" else funcDefinition.resources.head.uri
    Misc.getMemStore.getMetadataCmdRgn.put(functionKeyPrefix + key, jarPath)
    // Remove from the list in (__FUNC__DROPPED__, dropped-udf-list)
    removeFromTheListInCmdRegion(droppedFunctionsKey, key, droppedFunctionsKey)
  }

  private def sparkContext = SnappyContext.globalSparkContext

  private def jarDir = {
    val jarDirectory = new File(System.getProperty("user.dir"), JAR_PATH)
    if (!jarDirectory.exists()) jarDirectory.mkdir()
    jarDirectory
  }

  private def regexForItemInTheList(item: String, head: String): Regex = {
    val pattern = s"^$head(.*$DELIMITER)?($item$DELIMITER)(.*)$$"
    pattern.r
  }

  def addToTheListInCmdRegion(k: String, item: String, head: String): Unit = {
    val r = Misc.getMemStore.getMetadataCmdRgn
    var replaced = false
    val findItemRegex = regexForItemInTheList(item, head)
    do {
      replaced = r.get(k) match {
        // An older put may have already succeeded but another thread may have changed
        // the value, so first check if item is already present.
        // NOTE: not using region.replace(k, old, new) because it doesn't work correctly
        // from a data node though works from accessor node.
        case oldValue: String => findItemRegex.findFirstIn(oldValue).isDefined ||
            oldValue.equals(r.put(k, oldValue + item + DELIMITER))
        case _ => r.putIfAbsent(k, head + item + DELIMITER) eq null
      }
    } while (!replaced)
  }

  def removeFromTheListInCmdRegion(k: String, item: String, head: String): Unit = {
    val r = Misc.getMemStore.getMetadataCmdRgn
    var replaced = false
    val findItemRegex = regexForItemInTheList(item, head)
    do {
      replaced = r.get(k) match {
        // An older put may have already succeeded but another thread may have changed
        // the value, so first check if item has already been removed.
        // NOTE: not using region.replace(k, old, new) because it doesn't work correctly
        // from a data node though works from accessor node.
        case oldValue: String => findItemRegex.findFirstIn(oldValue).isEmpty ||
            oldValue.equals(r.put(k, findItemRegex.replaceFirstIn(oldValue, head + "$1$3")))
        case _ => true
      }
    } while (!replaced)
  }

  def checkItemExists(k: String, item: String, head: String): Boolean = {
    val value = Misc.getMemStore.getMetadataCmdRgn.get(k)
    if (value != null) {
      val valueStr = value.asInstanceOf[String]
      regexForItemInTheList(item, head).findFirstIn(valueStr).isDefined
    } else false
  }
}
