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
package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlanner
import org.apache.spark.sql.execution.sources.StoreDataSourceStrategy
import org.apache.spark.sql.internal.DefaultPlanner
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{SnappySession, Strategy}

class SnappyIncrementalExecution(snappySession: SnappySession,
    logicalPlan: LogicalPlan,
    override val outputMode: OutputMode,
    override val checkpointLocation: String,
    override val currentBatchId: Long,
    override val currentEventTimeWatermark: Long) extends
    IncrementalExecution(snappySession, logicalPlan, outputMode, checkpointLocation,
      currentBatchId, currentEventTimeWatermark) {

  // Modified planner with stateful operations.
  override def planner: SparkPlanner =
    new DefaultPlanner(
      snappySession,
      snappySession.sessionState.conf,
      stateStrategy) {

      // todo: skipping SnappyAggregation optimization as it is clashing with
      // org.apache.spark.sql.execution.SparkStrategies.StatefulAggregationStrategy$
      // this needs to be further analyzed to make this both strategies work together well
      override protected val storeOptimizedRules: Seq[Strategy] =
      Seq(StoreDataSourceStrategy, HashJoinStrategies)
    }
}
