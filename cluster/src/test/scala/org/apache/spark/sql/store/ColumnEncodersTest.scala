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
package org.apache.spark.sql.store

import io.snappydata.Property

import org.apache.spark.sql.SnappySession

/**
 * Tests for ColumnEncoder and ColumnDecoder implementations.
 */
class ColumnEncodersTest extends ColumnTablesTestBase {

  test("Type encoders/decoders test") {
    val session = new SnappySession(sc)
    session.conf.set(Property.ColumnBatchSize.name, "8k")
    runAllTypesTest(session, numRowsLower = 10000, numRowsUpper = 20000, numIterations = 5)
  }
}
