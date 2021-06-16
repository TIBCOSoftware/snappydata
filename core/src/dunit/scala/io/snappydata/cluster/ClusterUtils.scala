/*
 * Copyright (c) 2017-2021 TIBCO Software Inc. All rights reserved.
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

package io.snappydata.cluster

import java.io.{BufferedWriter, FileWriter, PrintWriter}
import java.nio.file.{Files, Path, Paths, StandardCopyOption}

import scala.collection.JavaConverters._
import scala.sys.process._
import scala.util.control.NonFatal

import io.snappydata.test.dunit.VM
import io.snappydata.test.util.TestException
import org.apache.commons.io.FileUtils

import org.apache.spark.Logging
import org.apache.spark.sql.SnappyContext

trait ClusterUtils extends Logging {

  val snappyHomeDir: String = System.getProperty("SNAPPY_HOME")

  lazy val snappyProductDir: String = createClusterDirectory(snappyHomeDir, isSnappy = true)

  protected def sparkProductDir: String = snappyHomeDir

  def createClusterDirectory(productDir: String, isSnappy: Boolean): String = {
    val source = Paths.get(productDir).toAbsolutePath
    val clusterDir = ClusterUtils.getClusterDirectory(source, isSnappy)
    val dest = Paths.get(clusterDir).toAbsolutePath
    if (!Files.exists(dest)) {
      // link most of product directory contents and copy the conf and scripts
      Files.createDirectories(dest)
      val contents = Files.list(source)
      try {
        for (item <- contents.iterator().asScala) {
          val fileName = item.getFileName
          val fileNameStr = fileName.toString
          if (ClusterUtils.copyDirs.contains(fileNameStr)) {
            ClusterUtils.copyDirectory(source, item, dest)
          } else if (!ClusterUtils.skipDirs.contains(fileNameStr)) {
            Files.createSymbolicLink(dest.resolve(fileName), item)
          }
        }
      } finally contents.close()
      ClusterUtils.deleteClusterRuntimeData(clusterDir)
    }
    clusterDir
  }

  def writeToFile(str: String, filePath: String, append: Boolean = false): Unit = {
    val fileWriter = new FileWriter(filePath, append)
    val bufferedWriter = new BufferedWriter(fileWriter)
    val pw = new PrintWriter(bufferedWriter)
    try {
      pw.write(str)
      pw.flush()
    } finally {
      pw.close()
    }
    // wait until file becomes available (e.g. running on NFS)
    var matched = append
    while (!matched) {
      Thread.sleep(100)
      try {
        val source = scala.io.Source.fromFile(filePath)
        val lines = try {
          source.mkString
        } finally {
          source.close()
        }
        matched = lines == str
      } catch {
        case NonFatal(_) =>
      }
    }
  }

  def startSparkCluster(vm: Option[VM] = None, productDir: String = sparkProductDir): String = {
    val clusterDir = createClusterDirectory(productDir, isSnappy = false)
    vm match {
      case None => ClusterUtils.startSparkCluster(clusterDir)
      case Some(v) =>
        v.invoke(ClusterUtils, "startSparkCluster", Array(clusterDir: AnyRef)).toString
    }
  }

  def stopSparkCluster(vm: Option[VM] = None, productDir: String = sparkProductDir): Unit = {
    val clusterDir = ClusterUtils.getSparkClusterDirectory(productDir)
    vm match {
      case None => ClusterUtils.stopSparkCluster(clusterDir)
      case Some(v) => v.invoke(ClusterUtils, "stopSparkCluster", Array(clusterDir: AnyRef))
    }
  }

  def startSnappyCluster(vm: Option[VM] = None, startArgs: String = ""): String = vm match {
    case None => ClusterUtils.startSnappyCluster(snappyProductDir, startArgs)
    case Some(v) =>
      v.invoke(ClusterUtils, "startSnappyCluster",
        Array(snappyProductDir: AnyRef, startArgs)).toString
  }

  def stopSnappyCluster(vm: Option[VM] = None, stopArgs: String = "",
      deleteData: Boolean = true): Unit = vm match {
    case None => ClusterUtils.stopSnappyCluster(snappyProductDir, stopArgs, deleteData)
    case Some(v) =>
      v.invoke(ClusterUtils, "stopSnappyCluster",
        Array(snappyProductDir, stopArgs, deleteData.asInstanceOf[AnyRef]))
  }
}

object ClusterUtils extends Serializable with Logging {

  private val snappyProductDir = getClusterDirectory("snappy")
  private val copyDirs = Set("bin", "conf", "python", "sbin")
  private val skipDirs = Set("logs", "work")

  private[this] def getClusterDirectory(suffix: String): String =
    s"${System.getProperty("user.dir")}/$suffix"

  private def getClusterDirectory(productPath: => Path, isSnappy: Boolean): String = {
    if (isSnappy) snappyProductDir else getClusterDirectory(productPath.getFileName.toString)
  }

  private def deleteClusterRuntimeData(clusterDir: String): Unit = {
    for (dir <- skipDirs) {
      FileUtils.deleteQuietly(new java.io.File(s"$clusterDir/$dir"))
    }
    Files.deleteIfExists(Paths.get(clusterDir, "conf", "locators"))
    Files.deleteIfExists(Paths.get(clusterDir, "conf", "servers"))
    Files.deleteIfExists(Paths.get(clusterDir, "conf", "leads"))
    Files.deleteIfExists(Paths.get(clusterDir, "conf", "snappy-env.sh"))
    Files.deleteIfExists(Paths.get(clusterDir, "conf", "spark-env.sh"))
    Files.deleteIfExists(Paths.get(clusterDir, "conf", "log4j.properties"))
  }

  def getEnvironmentVariable(name: String): String = {
    val value = System.getenv(name)
    if (name eq null) {
      throw new TestException(s"Environment variable $name is not defined")
    }
    value
  }

  def getSparkClusterDirectory(productDir: String): String =
    getClusterDirectory(Paths.get(productDir), isSnappy = false)

  def startSparkCluster(clusterDir: String): String = {
    logInfo(s"Starting spark cluster in $clusterDir/work")
    val output = s"$clusterDir/sbin/start-all.sh".!!
    logInfo(output)
    output
  }

  def stopSparkCluster(clusterDir: String): Unit = {
    stopSpark()
    logInfo(s"Stopping spark cluster in $clusterDir/work")
    logInfo(s"$clusterDir/sbin/stop-all.sh".!!)
  }

  def startSnappyCluster(clusterDir: String, startArgs: String): String = {
    logInfo(s"Starting SnappyData cluster in $clusterDir/work [startArgs=$startArgs]")
    val output =
      if (startArgs.isEmpty) s"$clusterDir/sbin/snappy-start-all.sh".!!
      else s"$clusterDir/sbin/snappy-start-all.sh $startArgs".!!
    logInfo(output)
    output
  }

  def stopSnappyCluster(clusterDir: String, stopArgs: String, deleteData: Boolean): Unit = {
    stopSpark()
    logInfo(s"Stopping SnappyData cluster in $clusterDir/work [stopArgs=$stopArgs]")
    if (stopArgs.isEmpty) logInfo(s"$clusterDir/sbin/snappy-stop-all.sh".!!)
    else logInfo(s"$clusterDir/sbin/snappy-stop-all.sh $stopArgs".!!)
    if (deleteData) deleteClusterRuntimeData(clusterDir)
  }

  def stopSpark(): Unit = {
    logInfo("Stopping spark")
    val sc = SnappyContext.globalSparkContext
    if (sc ne null) sc.stop()
  }

  /**
   * Copy a given item within source (can be same as source) to destination preserving attributes.
   */
  def copyDirectory(source: Path, item: Path, dest: Path): Unit = {
    val tree = Files.walk(item)
    try {
      for (p <- tree.iterator().asScala) {
        Files.copy(p, dest.resolve(source.relativize(p)), StandardCopyOption.COPY_ATTRIBUTES,
          StandardCopyOption.REPLACE_EXISTING)
      }
    } finally tree.close()
  }
}
