/*
 * Copyright (c) 2017-2020 TIBCO Software Inc. All rights reserved.
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

package org.apache.spark.sql.execution.ui

import java.util.concurrent.ConcurrentMap

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.{CachedDataFrame, SparkListenerSQLPlanExecutionEnd, SparkListenerSQLPlanExecutionStart}
import org.apache.spark.status.ElementTrackingStore

/**
 * SnappyData's SQL Listener. This extends Spark's SQL listener to handle
 * combining the two part execution with CachedDataFrame where first execution
 * does the caching ("prepare" phase) along with the actual execution while subsequent
 * executions only do the latter. This listener also shortens the SQL string
 * to display properly in the UI (CachedDataFrame already takes care of posting
 * the SQL string rather than method name unlike Spark).
 *
 * @param context the active SparkContext
 */
class SnappySQLAppListener(context: SparkContext)
    extends SQLAppStatusListener(context.conf,
      context.statusStore.store.asInstanceOf[ElementTrackingStore], live = true) {

  private[this] val baseLiveExecutions: ConcurrentMap[Long, LiveExecutionData] = {
    val f = classOf[SQLAppStatusListener].getDeclaredFields
        .find(_.getName.contains("liveExecutions")).get
    f.setAccessible(true)
    f.get(this).asInstanceOf[ConcurrentMap[Long, LiveExecutionData]]
  }

  /**
   * Snappy's execution happens in two phases. First phase the plan is executed
   * to create a rdd which is then used to create a CachedDataFrame.
   * In second phase, the CachedDataFrame is then used for further actions.
   * For accumulating the metrics for first phase,
   * SparkListenerSQLPlanExecutionStart is fired. This keeps the current
   * executionID in _executionIdToData but does not add it to the active
   * executions. This ensures that query is not shown in the UI but the
   * new jobs that are run while the plan is being executed are tracked
   * against this executionID. In the second phase, when the query is
   * actually executed, SparkListenerSQLPlanExecutionStart adds the execution
   * data to the active executions. SparkListenerSQLPlanExecutionEnd is
   * then sent with the accumulated time of both the phases.
   */
  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case SparkListenerSQLPlanExecutionStart(executionId, description, details,
    physicalPlanDescription, sparkPlanInfo, time) =>
      super.onOtherEvent(SparkListenerSQLExecutionStart(executionId, description, details,
        physicalPlanDescription, sparkPlanInfo, time))

    case SparkListenerSQLExecutionStart(executionId, description, details,
    physicalPlanDescription, sparkPlanInfo, time) =>
      // if executionId already exists (from SparkListenerSQLPlanExecutionStart) then
      // use the submissionTime from those details
      val submissionTime = try {
        context.statusStore.store.read(classOf[SQLExecutionUIData], executionId) match {
          case null => time
          case data if data.submissionTime <= 0 => time
          case data => data.submissionTime
        }
      } catch {
        case _: NoSuchElementException => time
      }
      // description and details strings being reference equals means
      // trim off former here
      if (description eq details) {
        val desc = CachedDataFrame.queryStringShortForm(details)
        super.onOtherEvent(SparkListenerSQLExecutionStart(executionId, desc, details,
          physicalPlanDescription, sparkPlanInfo, submissionTime))
      } else if (submissionTime != time) {
        super.onOtherEvent(SparkListenerSQLExecutionStart(executionId, description, details,
          physicalPlanDescription, sparkPlanInfo, submissionTime))
      } else super.onOtherEvent(event)

    case SparkListenerSQLPlanExecutionEnd(executionId) => baseLiveExecutions.remove(executionId)

    case _ => super.onOtherEvent(event)
  }
}
