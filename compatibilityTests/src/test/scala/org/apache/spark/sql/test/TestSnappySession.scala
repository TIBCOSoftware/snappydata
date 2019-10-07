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
package org.apache.spark.sql.test

import org.apache.spark.sql.{SnappySession, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * A special [[SparkSession]] prepared for testing.
 */
private[sql] class TestSnappySession(sc: SparkContext) extends SnappySession(sc) {

  self =>

  def this(snappyConf: SparkConf) {
    this(
      new SparkContext("local[2]", "test-sql-context",
        snappyConf.set("spark.sql.testkey", "true")
      )
    )
  }

  def this() {
    this(new SparkConf)
  }

  override private[sql] def overrideConfs: Map[String, String] = TestSQLContext.overrideConfs

  // Needed for Java tests
  def loadTestData(): Unit = {
    testData.loadTestData()
  }

  private object testData extends SQLTestData {
    protected override def spark: SparkSession = self
  }

}
