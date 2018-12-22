/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

import scala.sys.process._

import org.apache.commons.io.FileUtils

/**
 * Extending SnappyTestRunner. This class runs the snappy hydra smoke.bt and smokePerf.bt
 */
class SnappyHydraRunner extends SnappyHydraTestRunner {

  override def afterAll(): Unit = {
  }

  test("smokeBT") {
    val logDir = new File(s"$snappyHome/../tests/snappy/scalatest/smokeBT")
    if (logDir.exists) {
      FileUtils.deleteDirectory(logDir)
    }
    /* val command: String =  s"$SNAPPYDATA_SOURCE_DIR/store/tests/core/src/main/java/bin/sample" +
      s"-runbt.sh $logDir $SNAPPYDATA_SOURCE_DIR -d false io/snappydata/hydra/nwSmoke.bt" */
    val command: String = s"$SNAPPYDATA_SOURCE_DIR/dtests/src/test/java/io/snappydata/hydra" +
        s"/smoke.sh $SNAPPYDATA_SOURCE_DIR $logDir"
    val (out, err) = executeProcess("smokeBT", command)

    searchExceptions(logDir)

    val c9 = s"grep -r \'result mismatch observed\' $logDir"
    val c10 = "grep -v Q37"
    val c11 = "grep -v Q36"
    val command2 = c9 #| c10 #| c11
    try {
      val output2: String = command2.!!
      throw new Exception(s"Result mismatch observed in smoke bt run:\n " + output2 +
          "\nCheck the logs at " + logDir + " for more details.")
    }
    catch {
      case r: java.lang.RuntimeException =>
        if (r.getMessage().contains("Nonzero exit value: 1")) {
          // scalastyle:off println
          println("smoke bt run is successful.")
        }
        else {
          throw r
        }
      case i: Throwable => throw i
    }
  }

  test("smokePerfBT") {
    val smokePerf = System.getenv("SMOKE_PERF")
    if (smokePerf == null) {
      println("SMOKE_PERF should be set as an environment variable in order to run smokePerf bt")
    } else if (smokePerf.equalsIgnoreCase("true")) {
      val perfLogDir = new File(s"$snappyHome/../tests/snappy/scalatest/smokePerfBT")
      if (perfLogDir.exists) {
        FileUtils.deleteDirectory(perfLogDir)
      }
      val command: String = s"$SNAPPYDATA_SOURCE_DIR/dtests/src/test/java/io/snappydata/hydra" +
          s"/smokePerf.sh $SNAPPYDATA_SOURCE_DIR $perfLogDir"
      val (out, err) = executeProcess("smokePerfBT", command)

      searchExceptions(perfLogDir)
    }
  }
}

