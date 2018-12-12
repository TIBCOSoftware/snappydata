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
package io.snappydata.hydra.putInto;

import hydra.TestConfig;
import io.snappydata.hydra.cluster.SnappyPrms;
import io.snappydata.hydra.cluster.SnappyTest;


public class SnappyPutIntoTest extends SnappyTest {

  public static int numThreads = TestConfig.tasktab().intAt(SnappyPrms.numThreadsForConcExecution, TestConfig.tab().
      intAt(SnappyPrms.numThreadsForConcExecution, 15));

  public static void HydraTask_concPutIntoUsingJDBCConn() {

    String primaryLocatorHost = getPrimaryLocatorHost();
    String primaryLocatorPort = getPrimaryLocatorPort();
    ConcPutIntoTest.concPutInto(primaryLocatorHost, primaryLocatorPort, numThreads);
  }

  public static void HydraTask_concSelectUsingJDBCConn() {
    String primaryLocatorHost = getPrimaryLocatorHost();
    String primaryLocatorPort = getPrimaryLocatorPort();
    ConcPutIntoTest.conSelect(primaryLocatorHost, primaryLocatorPort, numThreads);
  }

}
