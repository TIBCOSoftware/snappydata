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
    /*val dtestsDir = new File(s"$SNAPPYDATA_SOURCE_DIR/dtests/build-artifacts/scala-2.11/")
    if (dtestsDir.exists) {
      FileUtils.deleteDirectory(dtestsDir)
    }
    val command: String = s"$SNAPPYDATA_SOURCE_DIR/gradlew snappy-dtests:packageTests"
    val (out, err) = executeProcess("buildDtests", command)
    val dtestsBuildStatus = out*/
    currWorkingDir = System.getProperty("user.dir")
  }

  override def afterAll(): Unit = {
  }

  test("smokeBT") {
    val logDir = new File(s"$snappyHome/scalatest")
    if (logDir.exists) {
      FileUtils.deleteDirectory(logDir)
    }
    val command: String = s"$SNAPPYDATA_SOURCE_DIR/store/tests/core/src/main/java/bin/sample-runbt.sh $logDir $SNAPPYDATA_SOURCE_DIR -d false io/snappydata/hydra/smoke.bt"
    val (out, err) = executeProcess("smokeBT", command)
    val smokeBTRunStatus = out
  }
}

