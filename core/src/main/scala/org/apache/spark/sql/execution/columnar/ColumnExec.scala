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

package org.apache.spark.sql.execution.columnar

import java.sql.Connection

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, OrderedDistribution}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.row.RowExec
import org.apache.spark.sql.execution.{SnapshotConnectionListener, WholeStageCodegenExec}
import org.apache.spark.sql.store.StoreUtils

/**
 * Base class for bulk column table insert, update, put, delete operations.
 */
trait ColumnExec extends RowExec {

  override def onExecutor: Boolean = false

  def externalStore: ExternalStore

  override def resolvedName: String = externalStore.tableName

  protected def delayRollover: Boolean = false

  def keyColumns: Seq[Attribute]

  protected final def compactionMetricName: String = "numCompactions"

  protected final def rolloverMetricName: String = "numDelayedRollovers"

  override lazy val metrics: Map[String, SQLMetric] = createMetrics

  protected def createMetrics: Map[String, SQLMetric] = {
    val opAction = s"${opType.toLowerCase}s"
    if (onExecutor) Map.empty
    else Map(
      s"num${opType}Rows" -> SQLMetrics.createMetric(sparkContext,
        s"number of $opAction in row buffer"),
      s"num${opType}ColumnBatchRows" -> SQLMetrics.createMetric(sparkContext,
        s"number of $opAction in column batches"),
      s"num${opType}ColumnBatches" -> SQLMetrics.createMetric(sparkContext,
        s"number of column batches with $opAction"),
      compactionMetricName -> SQLMetrics.createMetric(sparkContext, "number of batches compacted"),
      rolloverMetricName -> SQLMetrics.createMetric(sparkContext, "number of delayed rollovers"))
  }

  override def requiredChildDistribution: Seq[Distribution] = {
    if (partitioned) super.requiredChildDistribution
    else {
      // ClusteredDistribution(keyColumns(keyColumns.length - 3) :: Nil) :: Nil
      // for tables with no partitioning, require range partitioning on bucketId so that all
      // rows of a batch are together else it results in very large number of changes for each
      // batch strewn across all partitions; this also matches ColumnTableScan's outputPartitioning
      OrderedDistribution(StoreUtils.getColumnUpdateDeleteOrdering(
        keyColumns(keyColumns.length - 2)) :: Nil) :: Nil
    }
  }

  // Require per-partition sort on batchId+ordinal because deltas/deletes are accumulated for
  // consecutive batchIds+ordinals else it will  be very inefficient for bulk updates/deletes.
  // BatchId attribute is always third last in the keyColumns while ordinal
  // (index of row in the batch) is the one before that.
  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    Seq(StoreUtils.getColumnUpdateDeleteOrdering(keyColumns(keyColumns.length - 2)),
      StoreUtils.getColumnUpdateDeleteOrdering(keyColumns(keyColumns.length - 3)),
      StoreUtils.getColumnUpdateDeleteOrdering(keyColumns(keyColumns.length - 4))) :: Nil
  }

  override protected def initConnectionCode(ctx: CodegenContext): String = {
    val connectionClass = classOf[Connection].getName
    val externalStoreTerm = ctx.addReferenceObj("externalStore", externalStore)
    val listenerClass = classOf[SnapshotConnectionListener].getName
    val compactionMetric = metricTerm(ctx, compactionMetricName)
    val rolloverMetric = metricTerm(ctx, rolloverMetricName)
    taskListener = ctx.freshName("taskListener")
    connTerm = ctx.freshName("connection")
    val getContext = Utils.genTaskContextFunction(ctx)
    val catalogVersion = ctx.addReferenceObj("catalogVersion", catalogSchemaVersion)

    ctx.addMutableState(listenerClass, taskListener, "")
    ctx.addMutableState(connectionClass, connTerm, "")

    s"""
       |$taskListener = $listenerClass$$.MODULE$$.apply($getContext(),
       |    $externalStoreTerm, $delayRollover, new scala.util.Left($catalogVersion));
       |$taskListener.setMetrics($compactionMetric, $rolloverMetric);
       |$connTerm = $taskListener.connection();
       |""".stripMargin
  }

  override protected def doExecute(): RDD[InternalRow] = {
    // [sumedh] old code released write lock here which is incorrect which should be done only
    // after a collect() operation or equivalent and is already handled in ExecutePlan

    // don't expect code generation to fail
    WholeStageCodegenExec(this).execute()
  }

}
