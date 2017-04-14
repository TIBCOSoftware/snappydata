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
package io.snappydata.tools

import java.io.{ByteArrayOutputStream, PrintStream}

import scala.util.{Failure, Success, Try}

import com.gemstone.gemfire.internal.AvailablePort
import com.gemstone.gemfire.internal.cache.CacheServerLauncher
import com.pivotal.gemfirexd.tools.GfxdDistributionLocator
import com.pivotal.gemfirexd.{Attribute, FabricService, TestUtil}
import io.snappydata._
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{SparkConf, SparkContext}

class LeaderLauncherSuite extends SnappyFunSuite with BeforeAndAfterAll {

  private val availablePort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS)
  private  var locatorDirPath = ""
  override def beforeAll(): Unit = {
    val f = new java.io.File("tests-snappy-loc-dir")
    f.mkdir()
    locatorDirPath = f.getAbsolutePath
    dirList += locatorDirPath

    CacheServerLauncher.DONT_EXIT_AFTER_LAUNCH = true
    GfxdDistributionLocator.main(Array(
      "start",
      "-dir=" + f.getAbsolutePath,
      s"-peer-discovery-address=localhost",
      s"-peer-discovery-port=$availablePort"
    ))
  }

  override def afterAll(): Unit = {
    GfxdDistributionLocator.main(Array(
      "stop",
      "-dir=" + locatorDirPath
    ))
    CacheServerLauncher.DONT_EXIT_AFTER_LAUNCH = false
    dirCleanup()
  }

  test("leader api") {
    val dirname = createDir("tests-snappy-leader-api")
    val fs: Lead = ServiceManager.getLeadInstance

    val props = TestUtil.doCommonSetup(null)

    props.setProperty(Property.Locators.name, s"localhost[$availablePort]")
    props.setProperty(Attribute.SYS_PERSISTENT_DIR, dirname)
    fs.start(props)

    logInfo("Leader started successfully")

    assert(ServiceManager.getLeadInstance.status == FabricService.State.RUNNING)

    logInfo("Stopping leader now")
    fs.stop(null)

    logInfo("Leader stopped successfully")
  }

  test("simple leader launch") {
    val dirname = createDir("tests-snappy-leader")
    val stream = new ByteArrayOutputStream()

    val currentOut = System.out

    val start = Try {
      LeaderLauncher.main(Array(
        "start",
        "-dir=" + dirname,
        s"-locators=localhost[$availablePort]"
      ))
    }

    try {
      start transform( { _ =>
        Try {
          System.setOut(new PrintStream(stream))
          LeaderLauncher.main(Array(
            "status",
            "-dir=" + dirname))
        } map { _ =>
          val outputLines = stream.toString
          logInfo("Leader launched.. checking output")
          assert(outputLines.replaceAll("\n", "").matches(
            "SnappyData Leader pid: [0-9]+ status: running" +
                "  Distributed system now has [0-9]+ members." +
                "  Other members: .*([0-9]+:.*)<.*>:[0-9]+".r), outputLines)

        }
      }, {
        throw _
      }) match {
        case Failure(t) => throw t
        case _ =>
      }

    } finally {
      logInfo("Stopping Leader")
      System.setOut(currentOut)
      LeaderLauncher.main(Array(
        "stop",
        "-dir=" + dirname
      ))
    }
  }

  test("leader standby") {

    def verifyStatus(workingDir: String, expectedOutput: String): Try[Unit] = {
      val stream = new ByteArrayOutputStream()
      Try {
        System.setOut(new PrintStream(stream))
        LeaderLauncher.main(Array(
          "status",
          "-dir=" + workingDir))
      } map { _ =>
        val outputLines = stream.toString
        assert(outputLines.replaceAll("\n", "").matches(expectedOutput),
          workingDir + " returned with: \n" + outputLines)
      }
    }

    def waitTill(workingDir: String, status: String): Unit = {

      var output = ""
      do {
        Thread.sleep(500)
        val stream = new ByteArrayOutputStream()
        System.setOut(new PrintStream(stream))
        LeaderLauncher.main(Array(
          "status",
          "-dir=" + workingDir))
        output = stream.toString.replaceAll("\n", "")
      } while (output.contains(status))

    }

    val leader1 = createDir("tests-snappy-leader-1")
    val leader2 = createDir("tests-snappy-leader-2")
    val currentOut = System.out

    val start = Try {
      LeaderLauncher.main(Array(
        "start",
        "-dir=" + leader1,
        s"-locators=localhost[$availablePort]"
      ))
    } transform(_ => Try {

      logInfo("Leader 1 launched.. checking output")
      verifyStatus(leader1, "SnappyData Leader pid: [0-9]+ status: running.*").get

      LeaderLauncher.main(Array(
        "start",
        "-dir=" + leader2,
        s"-locators=localhost[$availablePort]"
      ))
      logInfo("Leader 2 launched..")
    }, {
      throw _
    })

    var isLeader1NotStopped = true
    try {
      val checkStandby = start transform(_ => {
        verifyStatus(leader2, "SnappyData Leader pid: [0-9]+ status: standby.*")
      }, throw _)


      val leader2TakeOver = checkStandby match {
        case Success(v) =>
          logInfo("Stopping Leader 1 ..")
          Try {
            LeaderLauncher.main(Array(
              "stop",
              "-dir=" + leader1))
            logInfo("Leader 1 stopped ..")
            isLeader1NotStopped = false
          } transform(_ => {

            logInfo("Waiting till Leader 2 is in starting status")
            waitTill(leader2, "starting")

            logInfo("Checking Leader 2 running status")
            verifyStatus(leader2, "SnappyData Leader pid: [0-9]+ status: running.*")
          }, throw _)

        case Failure(t) => throw t
      }

      leader2TakeOver match {
        case Failure(t) => throw t
        case _ =>
      }

    } finally {
      System.setOut(currentOut)
      if (isLeader1NotStopped) {
        LeaderLauncher.main(Array(
          "stop",
          "-dir=" + leader1
        ))
      }

      LeaderLauncher.main(Array(
        "stop",
        "-dir=" + leader2
      ))
    }
  }

  test("leader startup using SparkContext") {
    val dirname = createDir("tests-snappy-leader-by-conf")

    val conf = new SparkConf()
        .setAppName(testName)
        .setMaster(Constant.SNAPPY_URL_PREFIX + s"localhost[$availablePort]")
        // .set(Prop.Store.locators, s"localhost[${availablePort}]")
        .set(Constant.STORE_PROPERTY_PREFIX + Attribute.SYS_PERSISTENT_DIR, dirname)

    val sc = new SparkContext(conf)

    sc.stop()
  }

  test("simple leader spark properties") {

    val dirname = createDir("tests-snappy-leader-spark-prop")

    try {
      LeaderLauncher.main(Array(
        "start",
        "-dir=" + dirname,
        s"-locators=localhost[$availablePort]",
        s"-spark.ui.port=3344",
        s"-jobserver.enabled=true",
        s"-embedded=true"
      ))
    } finally {
      LeaderLauncher.main(Array(
        "stop",
        "-dir=" + dirname
      ))
    }

  }

}
