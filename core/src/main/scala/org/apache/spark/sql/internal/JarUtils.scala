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

import java.net.{URL, URLClassLoader}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark.util.MutableURLClassLoader

/**
 * An utility class to store jar file reference with their individual classloaders. This is reflect class changes at driver side.
 * e.g. If an UDF definition changes the driver should pick up the correct UDF class.
 */
class JarUtils {

  private val entityJars = new ConcurrentHashMap[String, URLClassLoader]().asScala


  def addEntityJar(entityName: String, classLoader: URLClassLoader): Option[URLClassLoader] = {
    entityJars.putIfAbsent(entityName, classLoader)
  }

  def addIfNotPresent(entityName: String, urls: Array[URL], parent : ClassLoader): Unit = {
    if(entityJars.get(entityName).isEmpty){
      entityJars.putIfAbsent(entityName, new MutableURLClassLoader(urls, parent))
    }
  }

  def removeEntityJar(entityName: String) = entityJars.remove(entityName)


  def getEntityJar(entityName: String) : Option[URLClassLoader]  = entityJars.get(entityName)

}

