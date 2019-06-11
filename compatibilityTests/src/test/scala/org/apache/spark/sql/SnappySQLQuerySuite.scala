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
/*
 * Test for SPARK-10316 taken from Spark's DataFrameSuite having license as below.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.apache.spark.sql.execution.aggregate
import org.apache.spark.sql.test.{SharedSnappySessionContext, SnappySparkTestUtil}

class SnappySQLQuerySuite extends SQLQuerySuite with
    SharedSnappySessionContext with SnappySparkTestUtil {

  override def ignored: Seq[String] = Seq(
    // TODO: these two need to be fixed by moving ParamLiteral conversion after optimization
    "Eliminate noop ordinal ORDER BY",
    "SPARK-16644: Aggregate should not put aggregate expressions to constraints"
  )

  override protected def testCodeGen(sqlText: String, expectedResults: Seq[Row]): Unit = {
    val df = sql(sqlText)
    // First, check if we have GeneratedAggregate.
    val hasGeneratedAgg = df.queryExecution.sparkPlan
        .collect { case _: aggregate.SnappyHashAggregateExec => true }
        .nonEmpty
    if (!hasGeneratedAgg) {
      fail(
        s"""
           |Codegen is enabled, but query $sqlText does not have SnappyHashAggregateExec in plan.
           |${df.queryExecution.simpleString}
         """.stripMargin)
    }
    // Then, check results.
    checkAnswer(df, expectedResults)
  }
}
