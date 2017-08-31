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
package org.apache.spark.sql

import scala.util.control.NonFatal

import io.snappydata.SnappyFunSuite

import org.apache.spark.Logging
import org.apache.spark.scheduler._

/**
 * Tests that don't fall under any other category
 */
class MiscTest extends SnappyFunSuite with Logging {

  test("Pool test") {
    // create a dummy pool
    val rootPool = new Pool("lowlatency", SchedulingMode.FAIR, 0, 0)
    sc.taskScheduler.rootPool.addSchedulable(rootPool)

    try {
      snc.sql("set snappydata.scheduler.pool=xyz")
      fail("unknown spark scheduler cannot be set")
    } catch {
      case _: IllegalArgumentException => // do nothing
      case NonFatal(e) =>
        fail("setting unknown spark scheduler with a different error", e)
    }

    snc.sql("set snappydata.scheduler.pool=lowlatency")
    snc.sql("select 1").count
    assert(sc.getLocalProperty("spark.scheduler.pool") === "lowlatency")
  }
}
