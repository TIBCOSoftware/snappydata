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

import org.apache.spark.sql.sources.FilteredScanSuite
import org.apache.spark.sql.test.{SharedSnappySessionContext, SnappySparkTestUtil}

class SnappyFilteredScanSuite extends FilteredScanSuite
    with SharedSnappySessionContext with SnappySparkTestUtil {

  override def ignored: Seq[String] = Seq(
    "PushDown Returns 3: SELECT a FROM oneToTenFiltered WHERE a + b > 9 AND b < 16 " +
        "AND c IN ('bbbbbBBBBB', 'cccccCCCCC', 'dddddDDDDD', 'foo') "
  )
}
