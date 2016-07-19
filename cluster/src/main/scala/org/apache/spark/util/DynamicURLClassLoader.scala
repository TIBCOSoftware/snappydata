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

package org.apache.spark.util

import java.net.{URL, URLClassLoader}
import java.util
import scala.language.existentials
import scala.collection.JavaConverters._

import com.pivotal.gemfirexd.internal.engine.Misc
import sun.misc.CompoundEnumeration

private[spark] class DynamicURLClassLoader(urls: Array[URL],
    parentClassLoader: ClassLoader, parentFirst: Boolean)
    extends MutableURLClassLoader(Array[URL](), parentClassLoader) {
  final private val JOB_SERVER_TEXT = "-SNAPPY-JOB-SERVER-JAR-"

  private val urlList = new util.TreeMap[String, URLClassLoader]()

  // this is executor class Path which is not going to change
  private val parent = {
    if (parentFirst) new MutableURLClassLoader(urls, parentClassLoader)
    else new ChildFirstURLClassLoader(urls, parentClassLoader)
  }

  override def addURL(url: URL): Unit = {
    // workaround to identify the files loaded by the job server as
    // it always creates a new file with timestamp.
    val key = if (url.getFile.contains("-SNAPPY-JOB-SERVER-JAR-")) {
      url.getFile.substring(0, url.getFile.indexOf(JOB_SERVER_TEXT))
    } else url.getFile

    val loader = if (parentFirst) new MutableURLClassLoader(Array(url), parent)
    else new ChildFirstURLClassLoader(Array(url), parent)
    urlList.put(url.getPath, loader)
  }

  override def getURLs(): Array[URL] = {
    urlList.asScala.flatMap(_._2.getURLs).toArray ++ parent.getURLs
  }

  override def getResources(name: String): util.Enumeration[URL] = {
    val childResources =
      urlList.asScala.map(tuple => {
        tuple._2.getResources(name).asScala
            .filter(!parent.getResources(name).asScala.contains(_)).asJavaEnumeration
      }).toArray

    val parentResources = Array(parent.getResources(name).asInstanceOf[util.Enumeration[_]])

    new CompoundEnumeration(childResources ++ parentResources)
  }

  @throws[ClassNotFoundException]
  override protected def loadClass(className: String, resolve: Boolean): Class[_] = {

    var c: Class[_] = findLoadedClass(className)

    if (c != null) {
      if (resolve) resolveClass(c)
      return c
    }
    getClassLoadingLock(className).synchronized {
      if (parentFirst) {
        c = loadFromParent(parent, className, false)
            .getOrElse(loadFromStore(className, false)
                .getOrElse(loadFromJars(className, true).get))
      } else {
        c = loadFromJars(className, false)
            .getOrElse(loadFromParent(parent, className, false)
                .getOrElse(loadFromStore(className, true).get))
      }

      if (resolve) {
        resolveClass(c)
      }
    }

    c
  }

  private def loadClassFunction(f: () => Option[Class[_]], throwException: Boolean = false)
  : Option[Class[_]] = {
    var clazz: Option[Class[_]] = None
    try {
      f()
    } catch {
      case cnfe: ClassNotFoundException => if (throwException) throw cnfe else None
    }
  }

  @throws[ClassNotFoundException]
  private def loadFromJars(className: String, throwException: Boolean): Option[Class[_]] = {
    var c: Option[Class[_]] = None
    for (url <- urlList.asScala if (!c.isDefined)) {
      c = loadClassFunction(() => Some(url._2.loadClass(className)), false)
    }

    c match {
      case Some(_) => c
      case None =>
        if (throwException) throw new ClassNotFoundException(className)
        else None
    }

  }

  @throws[ClassNotFoundException]
  private def loadFromParent(parentLoader: ClassLoader, className: String,
      throwException: Boolean): Option[Class[_]] = {
    loadClassFunction(() =>
      Some(parentLoader.loadClass(className)), throwException)
  }

  @throws[ClassNotFoundException]
  private def loadFromStore(className: String, throwException: Boolean): Option[Class[_]] =
    loadClassFunction(() =>
      Some(Misc.getMemStore.getDatabase.getClassFactory.loadClassFromDB(className)),
      throwException)
}

