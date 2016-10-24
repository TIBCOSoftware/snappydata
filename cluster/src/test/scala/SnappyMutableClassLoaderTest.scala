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

import java.net.URL
import java.util.Properties

import io.snappydata.SnappyFunSuite
import io.snappydata.core.Data

import org.apache.spark.DynamicJarInstallationDUnitTest
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.collection.{Utils => Utility}
import org.apache.spark.util.SnappyUtils

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

  snc.dropTable("testtable", ifExists = true)


  def addjar(loader: SnappyMutableURLClassLoader, testJar: URL): Unit = {
    val taskProperties = new Properties()
    taskProperties.setProperty("SNAPPY_JOB_SERVER_JAR_NAME", testJar.getFile)
    org.apache.spark.executor.Executor.taskDeserializationProps.set(taskProperties)
    loader.addURL(testJar)
  }

  def verifyClass(loader: SnappyMutableURLClassLoader, className: String, version: String): Unit = {
    val fakeClass = loader.loadClass(className).newInstance()
    assert(fakeClass.toString.equals(version))
  }

  test(" load class by setting job name in local properties") {
    val classloader = new SnappyMutableURLClassLoader(Array.empty[URL], null)
    addjar(classloader, testJar1)
    verifyClass(classloader, "FakeClass1", "1")
  }

  test (" test this scenario ") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    sc.setLocalProperty("SNAPPY_JOB_SERVER_JAR_NAME", "/home/namrata/snappy-commons/build-artifacts/scala-2.11/snappy/examples/jars/quickstart-0.6.jar")
    sc.addJar("/home/namrata/snappy-commons/build-artifacts/scala-2.11/snappy/examples/jars/quickstart-0.6.jar")
    dataDF.write.format("row").mode(SaveMode.Append).saveAsTable("MY_SCHEMA.MY_TABLE")
    var result = snc.sql("SELECT * FROM MY_SCHEMA.MY_TABLE").collect().length

    SnappyUtils.removeJobJar(sc)

  }

  test("remove Jar from the MutableClassLoader") {
    val classloader = new SnappyMutableURLClassLoader(Array.empty[URL], null)
    addjar(classloader, testJar1)
    verifyClass(classloader, "FakeClass1", "1")
    classloader.removeURL(testJar1.getFile)
    intercept[ClassNotFoundException] { verifyClass(classloader, "FakeClass1", "1")}
  }
}
