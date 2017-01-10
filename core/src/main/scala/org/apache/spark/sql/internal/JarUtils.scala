package org.apache.spark.sql.internal

import java.net.{URL, URLClassLoader}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark.util.MutableURLClassLoader


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

