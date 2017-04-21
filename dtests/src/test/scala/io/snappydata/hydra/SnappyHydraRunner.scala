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
package io.snappydata.hydra

import java.io.File

import io.snappydata.SnappyTestRunner
import org.apache.commons.io.FileUtils

/**
 * Extending SnappyTestRunner. This class runs the snappy hydra smoke.bt
 */
class SnappyHydraRunner extends SnappyTestRunner {
  var SNAPPYDATA_SOURCE_DIR = ""

  override def beforeAll(): Unit = {
    snappyHome = System.getenv("SNAPPY_HOME")
    SNAPPYDATA_SOURCE_DIR = s"$snappyHome/../../.."
    if (snappyHome == null) {
      throw new Exception("SNAPPY_HOME should be set as an environment variable")
    }
    currWorkingDir = System.getProperty("user.dir")
  }

  override def afterAll(): Unit = {
  }

  test("smokeBT") {
    val logDir = new File(s"$snappyHome/tests/snappy/scalatest/smokeBT")
    if (logDir.exists) {
      FileUtils.deleteDirectory(logDir)
    }
    /*val command: String =  s"$SNAPPYDATA_SOURCE_DIR/store/tests/core/src/main/java/bin/sample" +
      s"-runbt.sh $logDir $SNAPPYDATA_SOURCE_DIR -d false io/snappydata/hydra/nwSmoke.bt"*/
    val command: String = s"$SNAPPYDATA_SOURCE_DIR/dtests/src/test/java/io/snappydata/hydra" +
      s"/smoke.sh $SNAPPYDATA_SOURCE_DIR $logDir"
    println("SS - command : " + command)
    val (out, err) = executeProcess("smokeBT", command)

    import scala.sys.process._
    val c1 = s"grep -r Exception $logDir"
    val c2 = "grep -v  java.net.BindException"
    val c3 = "grep -v NoSuchObjectException"
    val c4 = "grep -v RegionDestroyedException"
    val c5 = "grep -v statArchive.gfs"
    val c6 = "grep -v DistributedSystemDisconnectedException"
    val c7 = "grep -v newDisconnectedException"
    val c8 = "grep -v CacheClosedException"
    val command1 = c1 #| c2 #| c3 #| c4 #| c5 #| c6 #| c7 #| c8
    val output1: String = command1.!!
    /*try {
      if (!output1.replace("Binary file (standard input) matches\n", "").equals("")) {
        println("=======================Exception Start===================")
        println(s"smokeBT Failed with below Exceptions:\n" + output1 + "\nCheck the logs at " +
          logDir + " for more details on Exception.... ")
        println("=======================Exception End===================")
        throw new Exception(s"smokeBT Failed with below Exceptions:\n" + output1 +
          "\nCheck the logs at " + logDir + " for more details on Exception.... ")
      } else {
        println("No unexpected Exceptions observed during smoke bt run.")
      }
    }
    catch {
      case i => throw i
    }*/
    try {
      throw new Exception(s"smokeBT Failed with below Exceptions:\n" + output1 +
        "\nCheck the logs at " + logDir + " for more details on Exception.... ")
    }
    catch {
      case r: java.lang.RuntimeException =>
        println("No unexpected Exceptions observed during smoke bt run.")
      case i => throw i
    }

    val c9 = s"grep -r \'result mismatch observed\' $logDir"
    val c10 = "grep -v Q37"
    val c11 = "grep -v Q36"
    val command2 = c9 #| c10 #| c11
    val output2: String = command2.!!
    try {
      throw new Exception(s"Result mismatch observed in smoke bt run:\n " + output2 +
        "\nCheck the logs at " + logDir + " for more details.")
    }
    catch {
      case r: java.lang.RuntimeException =>
        println("smoke bt run is successful.")
      case i => throw i
    }
  }
}

