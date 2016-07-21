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


package org.apache.spark
import java.net.URLClassLoader

import scala.collection.JavaConverters._

import _root_.io.snappydata.SnappyFunSuite

import org.apache.spark.util.{DynamicURLClassLoader, Utils}


class DynamicURLClassLoaderSuite extends SnappyFunSuite {

  val urls2 = List(TestUtils.createJarWithClasses(
    classNames = Seq("FakeClass1", "FakeClass2", "FakeClass3"),
    toStringValue = "2")).toArray
  val urls = List(TestUtils.createJarWithClasses(
    classNames = Seq("FakeClass1"),
    classNamesWithBase = Seq(("FakeClass2", "FakeClass3")), // FakeClass3 is in parent
    toStringValue = "1",
    classpathUrls = urls2)).toArray

  val childUrls = List(TestUtils.createJarWithClasses(
    classNames = Seq("FakeClass1"),
    classNamesWithBase = Seq(("FakeClass2", "FakeClass3")), // FakeClass3 is in parent
    toStringValue = "1",
    classpathUrls = urls2)).toArray

  val fileUrlsChild = List(TestUtils.createJarWithFiles(Map(
    "resource1" -> "resource1Contents-child",
    "resource2" -> "resource2Contents"))).toArray
  val fileUrlsParent = List(TestUtils.createJarWithFiles(Map(
    "resource1" -> "resource1Contents-parent"))).toArray


  override def beforeAll(): Unit = {
   super.beforeAll()
  }

  override def afterAll(): Unit = {
   super.afterAll()

  }

  test("child first for  individual Jars") {
    val classLoader = new DynamicURLClassLoader(urls2,
      Utils.getContextOrSparkClassLoader,
      parentFirst = false)
    urls.foreach(classLoader.addURL(_))
    val fakeClass = classLoader.loadClass("FakeClass2").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "1")
    val fakeClass2 = classLoader.loadClass("FakeClass2").newInstance()
    assert(fakeClass.getClass === fakeClass2.getClass)
  }


  test("parent first for  individual Jars") {
    val classLoader = new DynamicURLClassLoader(urls2,
      Utils.getContextOrSparkClassLoader,
      parentFirst = false)
    urls.foreach(classLoader.addURL(_))
    val fakeClass = classLoader.loadClass("FakeClass1").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "1")
    val fakeClass2 = classLoader.loadClass("FakeClass1").newInstance()
    assert(fakeClass.getClass === fakeClass2.getClass)
  }

  test("child first from the classpath url") {
    snc.sql(" create table classpath_test ( x int)")
    val parentLoader = new URLClassLoader(urls2, Utils.getContextOrSparkClassLoader)
    val classLoader = new DynamicURLClassLoader(urls, parentLoader, parentFirst = false)
    val fakeClass = classLoader.loadClass("FakeClass2").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "1")
    val fakeClass2 = classLoader.loadClass("FakeClass2").newInstance()
    assert(fakeClass.getClass === fakeClass2.getClass)
    snc.sql(" drop table classpath_test ")
  }

  test("parent first") {
    val parentLoader = new URLClassLoader(urls2, Utils.getContextOrSparkClassLoader)
    val classLoader = new DynamicURLClassLoader(urls, parentLoader, parentFirst = true)
    val fakeClass = classLoader.loadClass("FakeClass1").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "2")
    val fakeClass2 = classLoader.loadClass("FakeClass1").newInstance()
    assert(fakeClass.getClass === fakeClass2.getClass)
  }

  test("child first can fall back") {
    val parentLoader = new URLClassLoader(urls2, Utils.getContextOrSparkClassLoader)
    val classLoader = new DynamicURLClassLoader(urls, parentLoader, parentFirst = false)
    val fakeClass = classLoader.loadClass("FakeClass3").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "2")
  }

  test("child first can fail") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader =
      new DynamicURLClassLoader(urls, parentLoader, parentFirst = true)
    intercept[ClassNotFoundException] {
      classLoader.loadClass("FakeClassDoesNotExist").newInstance()
    }
  }

  test("default JDK classloader get resources") {
    val classLoader = new DynamicURLClassLoader(fileUrlsParent,
      Utils.getContextOrSparkClassLoader, false)
    assert(classLoader.getResources("resource1").asScala.size === 1)
    assert(classLoader.getResources("resource2").asScala.size === 0)

    fileUrlsChild.foreach(classLoader.addURL)

    assert(classLoader.getResources("resource1").asScala.size === 2)
    assert(classLoader.getResources("resource2").asScala.size === 1)
  }

  test("parent first get resources") {
    val parentLoader = new URLClassLoader(fileUrlsParent, null)
    val classLoader = new DynamicURLClassLoader(fileUrlsParent,
      Utils.getContextOrSparkClassLoader, parentFirst = true)

    assert(classLoader.getResources("resource1").asScala.size === 1)
    assert(classLoader.getResources("resource2").asScala.size === 0)

    fileUrlsChild.foreach(classLoader.addURL)

    assert(classLoader.getResources("resource1").asScala.size === 2)
    assert(classLoader.getResources("resource2").asScala.size === 1)

  }

  test("child first get resources") {
    val classLoader = new DynamicURLClassLoader(fileUrlsParent,
      Utils.getContextOrSparkClassLoader, parentFirst = false)

    var res1 = classLoader.getResources("resource1").asScala.toList
    assert(res1.size === 1)
    assert(classLoader.getResources("resource2").asScala.size === 0)

    fileUrlsChild.foreach(classLoader.addURL)

    res1 = classLoader.getResources("resource1").asScala.toList
    assert(res1.size === 2)
    assert(classLoader.getResources("resource2").asScala.size === 1)

    res1.map(scala.io.Source.fromURL(_).mkString).toSeq.equals(
        Seq("resource1Contents-child", "resource1Contents-parent"))
  }
}
