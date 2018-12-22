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

import java.util.AbstractMap.SimpleEntry

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.eclipse.collections.api.block.function.{Function0 => JFunction}
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList
import org.eclipse.collections.impl.map.mutable.UnifiedMap

import org.apache.spark.scheduler.{SparkListenerEvent, SparkListenerJobStart}
import org.apache.spark.sql.CachedDataFrame
import org.apache.spark.sql.execution.{SQLExecution, SnappyMetrics, SparkPlanInfo}
import org.apache.spark.{JobExecutionStatus, SparkConf}

/**
 * A new event that is fired when a plan is executed to get an RDD.
 */
case class SparkListenerSQLPlanExecutionStart(
    executionId: Long,
    description: String,
    details: String,
    physicalPlanDescription: String,
    sparkPlanInfo: SparkPlanInfo,
    time: Long)
    extends SparkListenerEvent

/**
 * Snappy's SQL Listener.
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
      case _ => super.onOtherEvent(event)
    }
  }

  /**
   * Get all accumulator updates from all tasks which belong to this execution and merge them.
   */
  override def getExecutionMetrics(executionId: Long): Map[Long, String] = synchronized {
    baseExecutionIdToData.get(executionId) match {
      case Some(executionUIData) =>
        val accumulatorUpdates = {
          for (stageId <- executionUIData.stages;
               stageMetrics <- baseStageIdToStageMetrics.get(stageId).toIterable;
               taskMetrics <- stageMetrics.taskIdToMetricUpdates.values;
               accumulatorUpdate <- taskMetrics.accumulatorUpdates) yield {
            (accumulatorUpdate._1, accumulatorUpdate._2)
          }
        }

        val driverUpdates = executionUIData.driverAccumUpdates.toSeq
        val totalUpdates = (accumulatorUpdates ++ driverUpdates).filter {
          case (id, _) => executionUIData.accumulatorMetrics.contains(id)
        }
        mergeAccumulatorUpdates(totalUpdates, accumulatorId =>
          executionUIData.accumulatorMetrics(accumulatorId).metricType)
      case None =>
        // This execution has been dropped
        Map.empty
    }
  }

  private def expandBuffer(b: mutable.ArrayBuffer[LongArrayList], size: Int): Unit = {
    var i = b.length
    while (i < size) {
      b += null
      i += 1
    }
  }

  private def mergeAccumulatorUpdates(
      accumulatorUpdates: Seq[(Long, Any)],
      metricTypeFunc: Long => String): Map[Long, String] = {
    // Group by accumulatorId but also group on splitSum metric
    // to include display of all into the first accumulator of a split series.
    // The map below either has accumulatorId as key or the splitSum metric series type
    // as the key, and second part of value is metric type for former case while
    // accumulatorId of the first in series for latter case.
    type MapValue = SimpleEntry[Any, Any]
    val accumulatorMap = new UnifiedMap[Any, MapValue](8)
    for ((accumulatorId, value) <- accumulatorUpdates) {
      val metricType = metricTypeFunc(accumulatorId)
      if (metricType.startsWith(SnappyMetrics.SPLIT_SUM_METRIC)) {
        val splitIndex = metricType.indexOf('_')
        val key = metricType.substring(0, splitIndex)
        val index = metricType.substring(splitIndex + 1).toInt
        val mapValue = accumulatorMap.getIfAbsentPut(key, new JFunction[MapValue] {
          override def value(): MapValue =
            new MapValue(new mutable.ArrayBuffer[LongArrayList](math.max(index + 1, 4)), 0L)
        })
        val valueList = mapValue.getKey.asInstanceOf[mutable.ArrayBuffer[LongArrayList]]
        expandBuffer(valueList, index + 1)
        val values = valueList(index) match {
          case null => val l = new LongArrayList(4); valueList(index) = l; l
          case l => l
        }
        values.add(value.asInstanceOf[Long])
        if (index == 0) mapValue.setValue(accumulatorId)
      } else {
        val mapValue = accumulatorMap.getIfAbsentPut(accumulatorId, new JFunction[MapValue] {
          override def value(): MapValue = new MapValue(new LongArrayList(4), metricType)
        })
        mapValue.getKey.asInstanceOf[LongArrayList].add(value.asInstanceOf[Long])
      }
    }
    // now create a map on accumulatorId and the values (which are either a
    // list of longs or a list of list of longs) as string
    accumulatorMap.asInstanceOf[java.util.Map[Any, MapValue]].asScala.map {
      case (id: Long, entry) =>
        id -> SnappyMetrics.stringValue(entry.getValue.asInstanceOf[String], entry.getKey)
      case (metricType: String, entry) =>
        entry.getValue.asInstanceOf[Long] -> SnappyMetrics.stringValue(metricType, entry.getKey)
    }.toMap
  }
}
