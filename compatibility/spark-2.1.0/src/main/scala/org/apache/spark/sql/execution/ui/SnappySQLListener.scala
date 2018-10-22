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
package org.apache.spark.sql.execution.ui

import scala.collection.mutable

import org.apache.spark.scheduler.{SparkListenerEvent, SparkListenerJobStart}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.{CachedDataFrame, SparkListenerSQLPlanExecutionStart}
import org.apache.spark.{JobExecutionStatus, SparkConf}

/**
 * SnappyData's SQL Listener. This extends Spark's SQL listener to handle
 * combining the two part execution with CachedDataFrame where first execution
 * does the caching ("prepare" phase) along with the actual execution while subsequent
 * executions only do the latter. This listener also shortens the SQL string
 * to display properly in the UI (CachedDataFrame already takes care of posting
 * the SQL string rather than method name unlike Spark).
 *
 * @param conf SparkConf of active SparkContext
 */
class SnappySQLListener(conf: SparkConf) extends SQLListener(conf) {
  // base class variables that are private
  private val baseStageIdToStageMetrics = {
    getInternalField("org$apache$spark$sql$execution$ui$SQLListener$$_stageIdToStageMetrics").
        asInstanceOf[mutable.HashMap[Long, SQLStageMetrics]]
  }
  private val baseJobIdToExecutionId = {
    getInternalField("org$apache$spark$sql$execution$ui$SQLListener$$_jobIdToExecutionId").
        asInstanceOf[mutable.HashMap[Long, Long]]
  }
  private val baseActiveExecutions = {
    getInternalField("activeExecutions").asInstanceOf[mutable.HashMap[Long, SQLExecutionUIData]]
  }
  private val baseExecutionIdToData = {
    getInternalField("org$apache$spark$sql$execution$ui$SQLListener$$_executionIdToData").
        asInstanceOf[mutable.HashMap[Long, SQLExecutionUIData]]
  }

  def getInternalField(fieldName: String): Any = {
    val resultField = classOf[SQLListener].getDeclaredField(fieldName)
    resultField.setAccessible(true)
    resultField.get(this)
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val executionIdString = jobStart.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)
    if (executionIdString == null) {
      // This is not a job created by SQL
      return
    }
    val executionId = executionIdString.toLong
    val jobId = jobStart.jobId
    val stageIds = jobStart.stageIds

    synchronized {
      // For queries whose plans are getting executed inside
      // CachedDataFrame, their execution id will not be found
      // in the active executions. For such cases, we need to
      // look up the executionUIToData as well.
      val executionData = baseActiveExecutions.get(executionId).
          orElse(baseExecutionIdToData.get(executionId))
      executionData.foreach { executionUIData =>
        executionUIData.jobs(jobId) = JobExecutionStatus.RUNNING
        executionUIData.stages ++= stageIds
        stageIds.foreach(stageId =>
          baseStageIdToStageMetrics(stageId) = new SQLStageMetrics(stageAttemptId = 0))
        baseJobIdToExecutionId(jobId) = executionId
      }
    }
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
  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case SparkListenerSQLPlanExecutionStart(executionId, description, details,
      physicalPlanDescription, sparkPlanInfo, time) =>
        val physicalPlanGraph = SparkPlanGraph(sparkPlanInfo)
        val sqlPlanMetrics = physicalPlanGraph.allNodes.flatMap { node =>
          node.metrics.map(metric => metric.accumulatorId -> metric)
        }
        val executionUIData = new SQLExecutionUIData(
          executionId,
          description,
          details,
          physicalPlanDescription,
          physicalPlanGraph,
          sqlPlanMetrics.toMap,
          time)
        synchronized {
          baseExecutionIdToData(executionId) = executionUIData
        }

      case SparkListenerSQLExecutionStart(executionId, description, details,
      physicalPlanDescription, sparkPlanInfo, time) => synchronized {
        val executionUIData = baseExecutionIdToData.getOrElseUpdate(executionId, {
          val physicalPlanGraph = SparkPlanGraph(sparkPlanInfo)
          val sqlPlanMetrics = physicalPlanGraph.allNodes.flatMap { node =>
            node.metrics.map(metric => metric.accumulatorId -> metric)
          }
          // description and details strings being reference equals means
          // trim off former here
          val desc = if (description eq details) {
            CachedDataFrame.queryStringShortForm(details)
          } else description
          new SQLExecutionUIData(
            executionId,
            desc,
            details,
            physicalPlanDescription,
            physicalPlanGraph,
            sqlPlanMetrics.toMap,
            time)
        })
        baseActiveExecutions(executionId) = executionUIData
      }

      case _ => super.onOtherEvent(event)
    }
  }
}
