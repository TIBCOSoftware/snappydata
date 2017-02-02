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

import java.io.File
import java.net.URL
import java.sql.{Connection, DriverManager}

import _root_.io.snappydata.cluster.ClusterManagerTestBase
import _root_.io.snappydata.test.dunit.{AvailablePortHelper, SerializableRunnable}
import org.apache.commons.io.FilenameUtils

import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.collection.{Utils => Utility}
import org.apache.spark.util.Utils

class DynamicJarInstallationDUnitTest(val s: String)
    extends ClusterManagerTestBase(s) {

  val currentLocatorPort = ClusterManagerTestBase.locPort

  override def tearDown2(): Unit = {
    Array(vm3, vm2, vm1, vm0).foreach(_.invoke(getClass, "stopNetworkServers"))
    bootProps.clear()
  }

  private def getANetConnection(netPort: Int): Connection = {
    val driver = "com.pivotal.gemfirexd.jdbc.ClientDriver"
    // scalastyle:off classforname
    Class.forName(driver).newInstance
    val url = "jdbc:snappydata://localhost:" + netPort + "/"
    DriverManager.getConnection(url)
  }


  def verifyClassOnExecutors(snc: SnappyContext, className: String,
      version: String, count: Int): Unit = {
    val countInstances = Utility.mapExecutors(snc,
      () => {
        if (DynamicJarInstallationDUnitTest.loadClass(className, version)) {
          Seq(1).iterator
        } else Iterator.empty
      }).count

    assert(countInstances == count, s"Assertion failed as countInstances=$countInstances and count=$count did not match")
  }


  def testJarDeployedWithSparkContext(): Unit = {
    var testJar = DynamicJarInstallationDUnitTest.createJarWithClasses(
      classNames = Seq("FakeJobClass", "FakeJobClass1"),
      toStringValue = "1",
      Seq.empty, Seq.empty,
      "testJar_SNAPPY_JOB_SERVER_JAR_%s.jar".format(System.currentTimeMillis()))

    var jobCompleted = false

    sc.addJar(testJar.getFile)
    sc.setLocalProperty("SNAPPY_CHANGEABLE_JAR_NAME", FilenameUtils.getName(testJar.getFile))
    // verify that jar is loaded at executors
    val rdd = sc.parallelize(1 to 10, 2)

    sc.runJob(rdd, { iter: Iterator[Int] => {
      val currentLoader = Thread.currentThread().getContextClassLoader
      println("Current classLoader is"+currentLoader)
      val fakeClass =
      Class.forName("FakeJobClass", false, currentLoader).newInstance()
      assert(fakeClass.toString == "1")
      1
    }
    })

    // removeJar
    var jarToDelete = sc.addedJars.keySet.filter(
      FilenameUtils.getName(_).equals(FilenameUtils.getName(testJar.getFile))).head
    sc.addedJars.remove(jarToDelete)

    sc.runJob(rdd, { iter: Iterator[Int] => {
      org.scalatest.Assertions.intercept[ClassNotFoundException] {
      val currentLoader = Thread.currentThread().getContextClassLoader
      println("Current classLoader is"+currentLoader)
        Class.forName("FakeJobClass", false, currentLoader).newInstance()
      }
      1
    }
    })

    //Again add the same jar with a different name

    testJar = DynamicJarInstallationDUnitTest.createJarWithClasses(
      classNames = Seq("FakeJobClass", "FakeJobClass1"),
      toStringValue = "2",
      Seq.empty, Seq.empty,
      "testJar_SNAPPY_JOB_SERVER_JAR_%s.jar".format(System.currentTimeMillis()))

    sc.addJar(testJar.getFile)
    sc.setLocalProperty("SNAPPY_CHANGEABLE_JAR_NAME", FilenameUtils.getName(testJar.getFile))
    // verify that jar is loaded at executors


    sc.runJob(rdd, { iter: Iterator[Int] => {
      val currentLoader = Thread.currentThread().getContextClassLoader
      println("Current classLoader is"+currentLoader)
      val fakeClass =
        Class.forName("FakeJobClass", false, currentLoader).newInstance()
      assert(fakeClass.toString == "2")
      1
    }
    })

  }

  //@TODO this test is invalid now. For backward compatibility we need to put a hook from replace jars to change the classloader in executors
  def _testJarDeployementWithThinClient(): Unit = {
    val snc = SnappyContext(sc)
    val sqlJars = DynamicJarInstallationDUnitTest.createJarWithClasses(
      classNames = Seq("FakeClass1", "FakeClass2", "FakeClass3"),
      toStringValue = "1",
      Seq.empty, Seq.empty,
      "testJar_SNAPPY_JOB_SERVER_JAR_%s.jar".format(System.currentTimeMillis()))


    val replaceJars = DynamicJarInstallationDUnitTest.createJarWithClasses(
      classNames = Seq("FakeClass1", "FakeClass2", "FakeClass4"),
      toStringValue = "2",
      Seq.empty, Seq.empty,
      "testJar_SNAPPY_JOB_SERVER_JAR_%s.jar".format(System.currentTimeMillis()))

    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    val conn = getANetConnection(netPort1)

    val stmt = conn.createStatement()

    stmt.executeUpdate("call sqlj.install_jar('" + sqlJars.getPath + "', 'app.sqlJars', 0)")
     println("Rishi verifying classes in controller")
    // look for jar inside the executors
    verifyClassOnExecutors(snc, "FakeClass1", "1", 3)
    verifyClassOnExecutors(snc, "FakeClass2", "1", 3)
    verifyClassOnExecutors(snc, "FakeClass3", "1", 3)


    // replace  the jar and check again
    stmt.executeUpdate("call sqlj.replace_jar('" + replaceJars.getPath + "', 'app.sqlJars')")

    // look for jar inside the executors
    verifyClassOnExecutors(snc, "FakeClass1", "2", 3)
    verifyClassOnExecutors(snc, "FakeClass2", "2", 3)
    verifyClassOnExecutors(snc, "FakeClass4", "2", 3)
    verifyClassOnExecutors(snc, "FakeClass3", "", 0)

    /*vm1.invoke(classOf[ClusterManagerTestBase], "stopAny")

    val props = bootProps
    val port = currentLocatorPort

    val restartServer = new SerializableRunnable() {
      override def run(): Unit = ClusterManagerTestBase.startSnappyServer(port, props)
    }

    vm1.invoke(restartServer)

    // verify jar after restart
    verifyClassOnExecutors(snc, "FakeClass1", "2", 3)
    verifyClassOnExecutors(snc, "FakeClass2", "2", 3)
    verifyClassOnExecutors(snc, "FakeClass4", "2", 3)
    verifyClassOnExecutors(snc, "FakeClass3", "", 0)


    // remove the jar and check

    stmt.executeUpdate("call sqlj.remove_jar('app.sqlJars', 0)")

    verifyClassOnExecutors(snc, "FakeClass1", "", 0)
    verifyClassOnExecutors(snc, "FakeClass2", "", 0)
    verifyClassOnExecutors(snc, "FakeClass3", "", 0)
    verifyClassOnExecutors(snc, "FakeClass4", "", 0)*/
    conn.close()
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