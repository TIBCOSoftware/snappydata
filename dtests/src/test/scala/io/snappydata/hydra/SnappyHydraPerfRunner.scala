/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
  * Extending SnappyHydraRunner. This class runs the snappy hydra smokePerf.bt
  */
class SnappyHydraPerfRunner extends SnappyHydraTestRunner {

  test("smokePerfBT") {
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
