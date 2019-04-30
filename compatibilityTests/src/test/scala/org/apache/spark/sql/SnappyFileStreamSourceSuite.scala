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

import org.apache.spark.sql.streaming.{FileStreamSourceStressTestSuite, FileStreamSourceSuite}
import org.apache.spark.sql.test.{SharedSnappySessionContext, SnappySparkTestUtil}

/**
 * Following test resources are copied from spark sql module to fix some failing tests
 * (see [[https://jira.snappydata.io/browse/SNAP-2725 SNAP-2725]] &
 * [[https://jira.snappydata.io/browse/SNAP-2726 SNAP-2726]]):
 *
 *  {{{
 *  - structured-streaming/file-source-offset-version-2.1.0-long.txt
 *  - structured-streaming/file-source-offset-version-2.1.0-json.txt
 *  - structured-streaming/file-source-log-version-2.1.0/2.compact
 *  - structured-streaming/file-source-log-version-2.1.0/3
 *  - structured-streaming/file-source-log-version-2.1.0/4
 *  }}}
 *
 * Above mentioned resources may need to be copied again when spark code is updated
 * with upstream spark.
 */
class SnappyFileStreamSourceSuite extends FileStreamSourceSuite
    with SharedSnappySessionContext with SnappySparkTestUtil

class SnappyFileStreamSourceStressTestSuite extends FileStreamSourceStressTestSuite
    with SharedSnappySessionContext with SnappySparkTestUtil
