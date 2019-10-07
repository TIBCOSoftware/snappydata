/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package io.snappydata.app

import io.snappydata.SnappyFunSuite

class ColumnTableSpec extends SnappyFunSuite {

  test( """ This test will create simple table""") {
    val data = Seq(Seq(1, "1"), Seq(7, "8"))
    val rdd = sc.parallelize(data, data.length).map(s =>
      new Data1(s(0).asInstanceOf[Int], s(1).asInstanceOf[String]))
    val dataDF = snc.createDataFrame(rdd)
    dataDF.registerTempTable("temp_table")

    // snc.registerAndInsertIntoExternalStore("table_name", props)
  }
}
