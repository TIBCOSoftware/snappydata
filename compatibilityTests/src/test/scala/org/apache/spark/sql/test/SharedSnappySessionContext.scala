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
package org.apache.spark.sql.test

import scala.util.Random

import org.apache.spark.DebugFilesystem
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.test.SharedSnappySessionContext.random

/**
 * Extension to use SnappySession instead of SparkSession in spark-sql-core tests.
 */
trait SharedSnappySessionContext extends SharedSQLContext {

  protected def codegenFallback: Boolean = false

  override protected def createSparkSession: SnappySession = {
    /**
     * Pls do not change the flag values of snappydaya.sql.TestDisableCodeGenFlag
     * and snappydaya.sql.UseOptimizedHashAggregateForSingleKey.name
     * They are meant to suppress CodegenFallback Plan so that optimized
     * byte buffer code path is tested & prevented from false passing.
     * If your test needs CodegenFallback, then override the newConf function
     * & clear the flag from the conf of the test locally.
     */
    val session = new TestSnappySession(sparkConf
        .set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName)
        .set("spark.sql.codegen.fallback", codegenFallback.toString)
        .set("snappydata.sql.planCaching.", random.nextBoolean().toString)
        .set("snappydata.sql.disableCodegenFallback", "true")
        .set("snappydata.sql.useOptimizedHashAggregateForSingleKey", "true"))
    session.setCurrentSchema("default")
    session
  }
}

object SharedSnappySessionContext {
  val random = new Random()
}
