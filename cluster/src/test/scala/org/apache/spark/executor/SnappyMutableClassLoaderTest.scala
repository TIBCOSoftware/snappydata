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

import java.net.{URLClassLoader, URL}
import java.util.Properties

import io.snappydata.SnappyFunSuite

import org.apache.spark.DynamicJarInstallationDUnitTest
import org.apache.spark.sql.collection.{Utils => Utility}

class SnappyMutableClassLoaderTest extends SnappyFunSuite {
  val testJar1 = DynamicJarInstallationDUnitTest.createJarWithClasses(
    classNames = Seq("FakeClass1", "FakeClass2", "FakeClass3"),
    toStringValue = "1",
    Seq.empty, Seq.empty,
    "testJar_SNAPPY_JOB_SERVER_JAR_%s.jar".format(System.currentTimeMillis()))

  val testJar2 = DynamicJarInstallationDUnitTest.createJarWithClasses(
    classNames = Seq("FakeClass1", "FakeClass2", "FakeClass3"),
    toStringValue = "2",
    Seq.empty, Seq.empty,
    "testJar_SNAPPY_JOB_SERVER_JAR_%s.jar".format(System.currentTimeMillis()))

  snc.dropTable("testTable", ifExists = true)


  def addjar(loader: SnappyMutableURLClassLoader, testJar: URL): Unit = {
    val taskProperties = new Properties()
    taskProperties.setProperty("SNAPPY_CHANGEABLE_JAR_NAME", testJar.getFile)
    org.apache.spark.executor.Executor.taskDeserializationProps.set(taskProperties)
    loader.addURL(testJar)
  }

  def verifyClass(loader: SnappyMutableURLClassLoader, className: String, version: String): Unit = {
    val fakeClass = Class.forName(className, false, loader).newInstance()
    assert(fakeClass.toString.equals(version))
  }

  test(" load class by setting job name in local properties") {
    val classloader = new SnappyMutableURLClassLoader(Array.empty[URL], null,
      scala.collection.mutable.Map.empty[String, URLClassLoader])
    addjar(classloader, testJar1)
    verifyClass(classloader, "FakeClass1", "1")
  }

  test("remove Jar from the MutableClassLoader") {
    val classloader = new SnappyMutableURLClassLoader(Array.empty[URL], null, scala.collection.mutable.Map.empty[String, URLClassLoader])
    val file = new java.io.File(testJar1.toURI)
    val fileName = file.getName
    addjar(classloader, testJar1)
    verifyClass(classloader, "FakeClass1", "1")
    classloader.removeURL(fileName)
    val newClassLoader = new SnappyMutableURLClassLoader(classloader.getURLs(), classloader.getParent, classloader.jobJars)
    intercept[ClassNotFoundException] {
      verifyClass(newClassLoader, "FakeClass1", "1")
    }
  }
}
