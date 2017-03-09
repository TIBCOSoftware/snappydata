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

import com.pivotal.gemfirexd.FabricService
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException
import io.snappydata.test.dunit.{DistributedTestBase, SerializableRunnable}
import io.snappydata.{Locator, ServiceManager}

class ThinConnectorSnappyDUnitTest(s: String)
    extends SplitSnappyClusterDUnitTest(s) {

  override protected val useThinClientConnector = true

//  override def setUp(): Unit = {
//    super.setUp()
//    DistributedTestBase.invokeInLocator(new SerializableRunnable() {
//      override def run(): Unit = {
//        val loc: Locator = ServiceManager.getLocatorInstance
//        if (loc.status != FabricService.State.RUNNING) {
//          loc.start("localhost", locatorClientPort, locatorNetProps)
//        }
//        if (locatorClientPort > 0) {
//          try {
//            loc.startNetworkServer("localhost", locatorClientPort, locatorNetProps)
//          } catch {
//            case g: GemFireXDRuntimeException
//              if g.getMessage.contains("The netserver address")
//                  && g.getMessage.contains("is already in use") => //ignored if already started
//          }
//        }
//        assert(loc.status == FabricService.State.RUNNING)
//      }
//    })
//  }

  override def tearDown2(): Unit = {
    super.tearDown2()
    vm3.invoke(getClass, "stopSparkContext")
  }

  override def testColumnTableStatsInSplitMode(): Unit = {
  }

  override def testColumnTableStatsInSplitModeWithHA(): Unit = {
  }



}


