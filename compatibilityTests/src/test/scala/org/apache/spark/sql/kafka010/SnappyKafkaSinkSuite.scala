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
package org.apache.spark.sql.kafka010

import org.apache.spark.DebugFilesystem
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.test.{SharedSnappySessionContext, SnappySparkTestUtil, TestSnappySession}

class SnappyKafkaSinkSuite extends KafkaSinkSuite
with SharedSnappySessionContext with SnappySparkTestUtil {
    override def createSparkSession: SnappySession = {
        sparkConf.set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName)

        // setting case sensitivity to true to pass some failing tests.
        // See https://jira.snappydata.io/browse/SNAP-2732
        sparkConf.set("spark.sql.caseSensitive", "true")
        new TestSnappySession(sparkConf)
    }
}