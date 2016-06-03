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
package io.snappydata

import java.util.Properties

import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.fabricservice.FabricServiceImpl
import com.pivotal.gemfirexd.{FabricLocator, FabricService, TestUtil}
import org.apache.spark.sql.SnappyContext
import org.scalatest.BeforeAndAfterAll

class ServerStartSuite extends SnappyFunSuite with BeforeAndAfterAll {
  var props: Properties = null

  override def beforeAll(): Unit = {
    val sparkContext = SnappyContext.globalSparkContext
    if(sparkContext != null) sparkContext.stop()
    props = TestUtil.doCommonSetup(null)
    GemFireXDUtils.IS_TEST_MODE = true
  }

  override def afterAll(): Unit = {
    GemFireXDUtils.IS_TEST_MODE = false
    dirCleanup()
  }

  test("Snappy Server start") {
    val fs: Server = ServiceManager.getServerInstance

    fs.start(props)

    assert(ServiceManager.getServerInstance.status == FabricService.State.RUNNING)

    fs.stop(null)
  }

  ignore("Snappy Lead start") {
    val fs: Lead = ServiceManager.getLeadInstance

    // right version of the test is now in LeaderLauncherSuite.
    try {
      fs.start(props)
      fail("must fail as locator info not present")
    } catch {
      case e: Exception =>
        var ex: Throwable = e
        var found = false
        while (ex != null) {
          if (ex.getMessage.contains("locator info not provided in the snappy embedded url")) {
            found = true
          }
          ex = ex.getCause
        }
        if (!found) {
          throw e
        }
      case other: Throwable => throw other
    }

  }

  test("Snappy Locator start") {
    val fs: Locator = ServiceManager.getLocatorInstance

    fs.start(FabricLocator.LOCATOR_DEFAULT_BIND_ADDRESS, FabricLocator.LOCATOR_DEFAULT_PORT, props)

    assert(ServiceManager.getLocatorInstance.status == FabricService.State.RUNNING)

    fs.stop(null)
  }

}
