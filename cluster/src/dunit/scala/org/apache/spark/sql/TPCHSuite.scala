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

package org.apache.spark.sql

import io.snappydata.{Property, SnappyFunSuite}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.internal.SQLConf

/**
 * Suite to run TPCH in a single VM. Disabled, by default,
 * as TPCH is run with the TPCHDunitTest. This is primarily
 * for debugging.
 */
class TPCHSuite extends SnappyFunSuite with BeforeAndAfterAll  {

  ignore("Test TPCH") {
    val snc = SnappyContext(sc)
    snc.conf.setConfString(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "104857600")
    Property.HashJoinSize.set(snc.conf, "1g")
    TPCHUtils.createAndLoadTables(snc, isSnappy = true)
    TPCHUtils.queryExecution(snc, isSnappy = true)
    // TPCHUtils.queryExecution(snc, isSnappy = true, warmup = 6, runsForAverage = 10,
    //  isResultCollection = false)
    TPCHUtils.validateResult(snc, isSnappy = true)
  }
}
