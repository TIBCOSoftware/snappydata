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
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.execution.columnar.impl.{JDBCSourceAsColumnarStore, SnapshotConnectionListener}
import org.apache.spark.sql.execution.row.RowExec
import org.apache.spark.sql.store.StoreUtils

/**
 * Base class for bulk column table insert, update, put, delete operations.
 */
trait ColumnExec extends RowExec {

  @transient protected final var taskListener: String = _

  override def onExecutor: Boolean = false

  def externalStore: ExternalStore

  override def resolvedName: String = externalStore.tableName

  protected def delayRollover: Boolean = false

  def keyColumns: Seq[Attribute]

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

  override protected def connectionCodes(ctx: CodegenContext): (String, String, String) = {
    val connectionClass = classOf[Connection].getName
    val externalStoreTerm = ctx.addReferenceObj("externalStore", externalStore)
    val listenerClass = classOf[SnapshotConnectionListener].getName
    val storeClass = classOf[JDBCSourceAsColumnarStore].getName
    taskListener = ctx.freshName("taskListener")
    connTerm = ctx.freshName("connection")
    val getContext = Utils.genTaskContextFunction(ctx)

    ctx.addMutableState(listenerClass, taskListener, "")
    ctx.addMutableState(connectionClass, connTerm, "")

    val initCode =
      s"""
         |$taskListener = new $listenerClass(($storeClass)$externalStoreTerm, $delayRollover);
         |$connTerm = $taskListener.getConn();
         |if ($getContext() != null) {
         |   $getContext().addTaskCompletionListener($taskListener);
         |}
         | """.stripMargin
    (initCode, "", "")
  }

  override protected def doExecute(): RDD[InternalRow] = {
    // [sumedh] old code released write lock here which is incorrect which should be done only
    // after a collect() operation or equivalent and is already handled in ExecutePlan

    // don't expect code generation to fail
    WholeStageCodegenExec(this).execute()
  }

}
