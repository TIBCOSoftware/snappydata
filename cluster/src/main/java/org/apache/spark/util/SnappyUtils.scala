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

// scalastyle:off println

package org.apache.spark.util

import java.net.{URLClassLoader, URL}
import java.security.SecureClassLoader
import scala.collection.mutable
import com.pivotal.gemfirexd.internal.engine.Misc

object SnappyUtils {

  def getSnappyStoreContextLoader(parent: ClassLoader): ClassLoader =
    new SecureClassLoader(parent) {
    override def loadClass(name: String): Class[_] = {
      try {
        super.loadClass(name)
      } catch {
        case cnfe: ClassNotFoundException =>
          Misc.getMemStore.getDatabase.getClassFactory.loadApplicationClass(name)
      }
    }
  }
}



class DynamicURLClassLoader(urls: Array[URL], parentClassLoader: ClassLoader, childFirst: Boolean)
    extends MutableURLClassLoader(Array[URL](), null) {
  val urlList = mutable.Map[String, URLClassLoader]()

  val parentLoader = new URLClassLoader(urls, parentClassLoader)

  override def addURL(url: URL): Unit = {
    val key = url.getPath
    if (!urlList.contains(key)) {
      val loader = new ExtendedJarURLClassLoader(Array(url), parentLoader, !childFirst)

      urlList(key) = loader
    } else {
      urlList.remove(key)
    }
  }

  override def getURLs(): Array[URL] = {
    urlList.flatMap(_._2.getURLs).toArray
  }

  override def loadClass(name: String): Class[_] = {
    def loadFromParent(): Option[Class[_]] = loadClassFunction(() => {
      parentLoader.loadClass(name)
    })

    def loadFromUrls(): Option[Class[_]] = {
      for (loader <- urlList.values) {
        println (" loading class " + name + " with url " + loader)
        val loaded = loadClassFunction(() => loader.loadClass(name))
        if (loaded.isDefined) return loaded
      }
      None
    }

    def loadFromStore(): Option[Class[_]] = loadClassFunction(
      () => Misc.getMemStore.getDatabase.getClassFactory.loadApplicationClass(name))

    loadFromUrls.getOrElse(loadFromParent().getOrElse(loadFromStore().
        getOrElse(super.loadClass(name))))
  }


  private def loadClassFunction(f: () => Class[_]): Option[Class[_]] = {
    var clazz: Option[Class[_]] = None
    try {
      clazz = Some(f())
    } catch {
      case cnfe: ClassNotFoundException => clazz = None
    }
    clazz
  }

}


class ExtendedJarURLClassLoader(
    classpath: Array[URL], parent: ClassLoader, parentFirst: Boolean)
    extends jodd.util.cl.ExtendedURLClassLoader(classpath, parent, parentFirst) {
   var requestedClass = ""
  @throws[ClassNotFoundException]
  override protected def loadClass(className: String, resolve: Boolean): Class[_] = {
    println ("load class is called " + className + ":" + this.hashCode())
    if (requestedClass.isEmpty ) {
      println ("setting the requested class to " + requestedClass)
      requestedClass = className
    }
    var c: Class[_] = findLoadedClass(className)
    if (c != null) {
      if (resolve) {
        resolveClass(c)
      }
      requestedClass = ""
      return c
    }
    println ("load class is called before loading parent ")
    val loadUsingParentFirst: Boolean = isParentFirst(className)
    try {
      if (loadUsingParentFirst) {
        try {
          c = parentClassLoader.loadClass(className)
        }
        catch {
          case ignore: ClassNotFoundException => {
          }
        }
        if (c == null) {
          c = findClass(className)
        }
      }
      else {
        try {
          println("finding class  " + className)
          c = findClass(className)
          println("found class  " + className)
        } catch {
          case ignore: ClassNotFoundException => {
            println("trying to load file from parent" + className)
            if (!requestedClass.equals(className)) {
              c = parentClassLoader.loadClass(className)
            }
          }
        }
      }
      //do not load class from parent as we have other Jars to look  into
      if (resolve) {
        resolveClass(c)
      }
    } catch {
      case cnfe : ClassNotFoundException => {
        requestedClass = ""
        throw cnfe
      }
    }
    requestedClass = ""
    return c
  }
}