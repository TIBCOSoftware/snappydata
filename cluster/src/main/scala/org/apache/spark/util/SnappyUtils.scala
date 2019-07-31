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

package org.apache.spark.util

import java.io.File
import java.net.{URL, URLClassLoader}
import java.security.SecureClassLoader

import _root_.io.snappydata.Constant
import com.pivotal.gemfirexd.internal.engine.Misc
import org.joda.time.DateTime
import spark.jobserver.util.ContextURLClassLoader

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.internal.ContextJarUtils
import org.apache.spark.ui.SparkUI
import org.apache.spark.{SparkContext, SparkEnv}
import scala.util.Try

object SnappyUtils {

  def getSparkUI(sc: SparkContext): Option[SparkUI] = sc.ui

  def getSnappyStoreContextLoader(parent: ClassLoader): ClassLoader = parent match {
    case _: SnappyContextLoader => parent // no double wrap
    case _ => new SnappyContextLoader(parent)
  }

  def setSessionDependencies(sparkContext: SparkContext,
      appName: String,
      classLoader: ClassLoader, addAllJars: Boolean = false): Unit = {
    assert(classOf[URLClassLoader].isAssignableFrom(classLoader.getClass))
    val dependentJars = if (addAllJars) {
      ContextJarUtils.getDriverJarURLs()
    } else {
      classLoader.asInstanceOf[URLClassLoader].getURLs
    }
    val sparkJars = dependentJars.map(url => {
      Try(sparkContext.env.rpcEnv.fileServer.addJar(new File(url.toURI))).getOrElse("")
    })
    val localProperty = (Seq(appName, DateTime.now) ++ sparkJars.filterNot(
      _.isEmpty).toSeq).mkString(",")
    sparkContext.setLocalProperty(Constant.CHANGEABLE_JAR_NAME, localProperty)
  }

  def clearSessionDependencies(sparkContext: SparkContext): Unit = {
    sparkContext.setLocalProperty(Constant.CHANGEABLE_JAR_NAME, null)
  }

  def getSnappyContextURLClassLoader(
      parent: ContextURLClassLoader): ContextURLClassLoader = parent match {
    case _: SnappyContextURLLoader => parent // no double wrap
    case _ => new SnappyContextURLLoader(parent)
  }

  def doFetchFile(
      url: String,
      targetDir: File,
      filename: String): File = {

    val env = SparkEnv.get
    val conf = env.conf
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    org.apache.spark.util.Utils.doFetchFile(url, targetDir,
      filename, conf, env.securityManager, hadoopConf)
    new File(targetDir, filename)
  }
}

class SnappyContextLoader(parent: ClassLoader)
    extends SecureClassLoader(parent) {

  @throws(classOf[ClassNotFoundException])
  override def loadClass(name: String): Class[_] = {
    try {
      parent.loadClass(name)
    } catch {
      case _: ClassNotFoundException =>
        Misc.getMemStore.getDatabase.getClassFactory.loadClassFromDB(name)
    }
  }
}

class SnappyContextURLLoader(parent: ClassLoader)
    extends ContextURLClassLoader(Array[URL](), parent) {

  @throws(classOf[ClassNotFoundException])
  override def loadClass(name: String): Class[_] = {
    try {
      super.loadClass(name)
    } catch {
      case _: ClassNotFoundException =>
        Misc.getMemStore.getDatabase.getClassFactory.loadClassFromDB(name)
    }
  }
}
