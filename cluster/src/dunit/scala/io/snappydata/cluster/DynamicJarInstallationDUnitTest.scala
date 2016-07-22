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


package io.snappydata.cluster

import java.io.File
import java.sql.{Connection, DriverManager}
import io.snappydata.test.dunit.AvailablePortHelper
import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.collection.{Utils => Utility}
import org.apache.spark.util.SnappyUtils

class DynamicJarInstallationDUnitTest(val s: String)
    extends ClusterManagerTestBase(s) {


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

  def testJarDeployedOnExecutors(): Unit = {
    val snc = SnappyContext(sc)

    sc.addJar(DynamicJarInstallationDUnitTest.original.getPath)

    // loading Jar class after installing the  jar.

    var countInstances = Utility.mapExecutors(snc,
      () => {
        if (DynamicJarInstallationDUnitTest.loadClass("FakeClass1", "1", false)) {
          Seq(1).iterator
        } else Iterator.empty
      }).count

    assert(countInstances == 3)

    // replace the jar file and load again

    val modifiedJarPath = DynamicJarInstallationDUnitTest.modified

    sc.addJar(modifiedJarPath.getPath)


    // loading Jar class after installing the  jar
    countInstances = Utility.mapExecutors(snc,
      () => {
        if (DynamicJarInstallationDUnitTest.loadClass("FakeClass1", "1", true)) {
          Seq(1).iterator
        } else Iterator.empty
      }).count

    assert(countInstances == 0)

    // try to verify  another class.
    countInstances = Utility.mapExecutors(snc,
      () => {
        if (DynamicJarInstallationDUnitTest.loadClass("FakeClass2", "2", false)) {
          Seq(1).iterator
        } else Iterator.empty
      }).count

    assert(countInstances == 3)
  }

  def testJarDeployementwithSnappyshell(): Unit = {
    val snc = SnappyContext(sc)
    val sqlJars = SnappyUtils.createJarWithClasses(
      classNames = Seq("FakeClass4", "FakeClass5", "FakeClass6"),
      toStringValue = "3" ,
      Seq.empty, Seq.empty ,
      "testJar_SNAPPY_JOB_SERVER_JAR_%s.jar".format(System.currentTimeMillis()))

    // test basic operations on snappyContext
    snc.sql("create table test (x int) using column")
    snc.dropTable("test")

    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)

    val stmt = conn.createStatement()

    stmt.execute("call sqlj.install_jar('" + sqlJars.getPath + "', 'app.sqlJars', 0)")

    // look for jar inside the executors
    var countInstances = Utility.mapExecutors(snc,
      () => {
        if (DynamicJarInstallationDUnitTest.loadClass("FakeClass5", "3")) {
          Seq(1).iterator
        } else Iterator.empty
      }).count

    assert(countInstances == 3)

    // remove the jar and check again
    stmt.execute("call sqlj.remove_jar('app.sqlJars', 0)")

    // look for jar inside the executors
    countInstances = Utility.mapExecutors(snc,
      () => {
        if (DynamicJarInstallationDUnitTest.loadClass("FakeClass5", "3", true)) {
          Seq(1).iterator
        } else Iterator.empty
      }).count

    assert(countInstances == 0)
  }
}


object DynamicJarInstallationDUnitTest {

  val original = SnappyUtils.createJarWithClasses(
    classNames = Seq("FakeClass1", "FakeClass2", "FakeClass3"),
    toStringValue = "1",
    Seq.empty, Seq.empty ,
    "testJar_SNAPPY_JOB_SERVER_JAR_%s.jar".format(System.currentTimeMillis()))

  val modified = SnappyUtils.createJarWithClasses(
    classNames = Seq("FakeClass2", "FakeClass3", "FakeClass4"),
    toStringValue = "2",
    Seq.empty, Seq.empty ,
    "testJar_SNAPPY_JOB_SERVER_JAR_%s.jar".format(System.currentTimeMillis()))

  @throws[ClassNotFoundException]
  def loadClass(className: String,
      version: String = "",
      catchExpectedException: Boolean = false): Boolean = {
    val loader = Thread.currentThread().getContextClassLoader
    assert(loader != null)
    try {
      val fakeClass = loader.loadClass(className).newInstance()
      assert(fakeClass != null)
      if (!version.isEmpty) assert(fakeClass.toString.equals(version))
      true
    } catch {
      case cnfe: ClassNotFoundException =>
        if (!catchExpectedException) throw cnfe
        else false
    }
  }

  def getModifiedJar: String = {
    val oldFile = new File(original.getPath)
    val newFile = new File(modified.getPath)
    if (oldFile.exists()) {
     assert( oldFile.delete() == true)
    }
    assert (newFile.renameTo(oldFile) == true)
    oldFile.getAbsolutePath
  }
}