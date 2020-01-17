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


package org.apache.spark

import java.io.File
import java.net.URL
import java.sql.{Connection, DriverManager}

import _root_.io.snappydata.Constant
import _root_.io.snappydata.cluster.ClusterManagerTestBase
import org.joda.time.DateTime

import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.collection.{Utils => Utility}
import org.apache.spark.util.Utils

class DynamicJarInstallationDUnitTest(val s: String)
    extends ClusterManagerTestBase(s) {

  val currentLocatorPort = ClusterManagerTestBase.locPort

  override def tearDown2(): Unit = {
    sc.setLocalProperty("SNAPPY_CHANGEABLE_JAR_NAME", null)
    Array(vm3, vm2, vm1, vm0).foreach(_.invoke(getClass, "stopNetworkServers"))
    bootProps.clear()
  }

  private def getANetConnection(netPort: Int): Connection = {
    val driver = "io.snappydata.jdbc.ClientDriver"
    // scalastyle:off classforname
    Class.forName(driver).newInstance
    val url = "jdbc:snappydata://localhost:" + netPort + "/"
    DriverManager.getConnection(url)
  }


  def verifyClassOnExecutors(snc: SnappyContext, className: String,
      version: String, count: Int): Unit = {
    val countInstances = Utility.mapExecutors[Int](snc.sparkContext,
      () => {
        if (DynamicJarInstallationDUnitTest.loadClass(className, version)) {
          Seq(1).iterator
        } else Iterator.empty
      }).length

    assert(countInstances == count,
      s"Assertion failed as countInstances=$countInstances and count=$count did not match")
  }


  def testJarDeployedWithSparkContext(): Unit = {
    var testJar = DynamicJarInstallationDUnitTest.createJarWithClasses(
      classNames = Seq("FakeJobClass", "FakeJobClass1"),
      toStringValue = "1",
      Nil, Nil,
      "testJar_SNAPPY_JOB_SERVER_JAR_%s.jar".format(System.currentTimeMillis()))

    var jobCompleted = false

    var localProperty = (Seq("app1", DateTime.now, Constant.SNAPPY_JOB_URL)
        ++ Array[URL](testJar)).mkString(",")
    sc.setLocalProperty("SNAPPY_CHANGEABLE_JAR_NAME", localProperty)
    // verify that jar is loaded at executors
    val rdd = sc.parallelize(1 to 10, 2)

    sc.runJob(rdd, { iter: Iterator[Int] => {
      val currentLoader = Thread.currentThread().getContextClassLoader
      // scalastyle:off println
      println("Current classLoader is" + currentLoader)
      val fakeClass =
      Class.forName("FakeJobClass", false, currentLoader).newInstance()
      assert(fakeClass.toString == "1")
      1
    }
    })

    // removeJar
    sc.setLocalProperty("SNAPPY_CHANGEABLE_JAR_NAME", null)

    sc.runJob(rdd, { iter: Iterator[Int] => {
      org.scalatest.Assertions.intercept[ClassNotFoundException] {
      val currentLoader = Thread.currentThread().getContextClassLoader
      println("Current classLoader is" + currentLoader)
        Class.forName("FakeJobClass", false, currentLoader).newInstance()
      }
      1
    }
    })

    // Again add the same jar with a different name

    testJar = DynamicJarInstallationDUnitTest.createJarWithClasses(
      classNames = Seq("FakeJobClass", "FakeJobClass1"),
      toStringValue = "2",
      Nil, Nil,
      "testJar_SNAPPY_JOB_SERVER_JAR_%s.jar".format(System.currentTimeMillis()))

    localProperty = (Seq("app1", DateTime.now, Constant.SNAPPY_JOB_URL)
        ++ Array[URL](testJar)).mkString(",")
    sc.setLocalProperty("SNAPPY_CHANGEABLE_JAR_NAME", localProperty)
    // verify that jar is loaded at executors


    sc.runJob(rdd, { iter: Iterator[Int] => {
      val currentLoader = Thread.currentThread().getContextClassLoader
      println("Current classLoader is" + currentLoader)
      val fakeClass =
        Class.forName("FakeJobClass", false, currentLoader).newInstance()
      assert(fakeClass.toString == "2")
      1
    }
    })

  }
}


object DynamicJarInstallationDUnitTest {

  def createJarWithClasses(
      classNames: Seq[String],
      toStringValue: String = "",
      classNamesWithBase: Seq[(String, String)] = Seq(),
      classpathUrls: Seq[URL] = Seq(),
      jarName: String = ""
  ): URL = {
    val tempDir = Utils.createTempDir()
    val files1 = for (name <- classNames) yield {
      TestUtils.createCompiledClass(name, tempDir, toStringValue, classpathUrls = classpathUrls)
    }
    val files2 = for ((childName, baseName) <- classNamesWithBase) yield {
      TestUtils.createCompiledClass(childName, tempDir, toStringValue, baseName, classpathUrls)
    }
    val jarFile = if (jarName.isEmpty) {
      new File(tempDir, "testJar-%s.jar".format(System.currentTimeMillis()))
    }
    else new File(tempDir, jarName.format(System.currentTimeMillis()))
    TestUtils.createJar(files1 ++ files2, jarFile)
  }


  @throws[ClassNotFoundException]
  def loadClass(className: String,
      version: String = ""): Boolean = {
    val catchExpectedException: Boolean = version.isEmpty
    val loader = Thread.currentThread().getContextClassLoader
    assert(loader != null)
    try {
      val fakeClass = Class.forName(className, false, loader).newInstance()
      assert(fakeClass != null)
      assert(fakeClass.toString.equals(version))
      true
    } catch {
      case cnfe: ClassNotFoundException =>
        if (!catchExpectedException) throw cnfe
        else false
    }
  }
}
