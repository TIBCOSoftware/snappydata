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
package org.apache.spark.sql.kafka010

import org.apache.spark.SparkContext
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.test.{SharedSnappySessionContext, SnappySparkTestUtil, TestSnappySession}

class SnappyKafkaSourceSuite extends KafkaSourceSuite
    with SharedSnappySessionContext with SnappySparkTestUtil {

  override def ignored: Seq[String] =
    Seq("deserialization of initial offset written by Spark 2.1.0")
}

class SnappyKafkaSourceStressSuite extends KafkaSourceStressSuite
    with SharedSnappySessionContext with SnappySparkTestUtil {

}

class SnappyKafkaSourceStressForDontFailOnDataLossSuite
    extends KafkaSourceStressForDontFailOnDataLossSuite
        with SharedSnappySessionContext with SnappySparkTestUtil {

  override def createSparkSession(): SnappySession = {
    // Set maxRetries to 3 to handle NPE from `poll` when deleting a topic
    new TestSnappySession(
      new SparkContext("local[2,3]", "test-sql-context", sparkConf))
  }
}