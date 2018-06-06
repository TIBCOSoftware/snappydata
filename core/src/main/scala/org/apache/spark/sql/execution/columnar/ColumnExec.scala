/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.impl.{JDBCSourceAsColumnarStore, SnapshotConnectionListener}
import org.apache.spark.sql.execution.row.RowExec

/**
 * Base class for bulk column table insert, update, put, delete operations.
 */
trait ColumnExec extends RowExec {

  @transient protected final var taskListener: String = _

  def externalStore: ExternalStore

  override def resolvedName: String = externalStore.tableName

  protected def delayRollover: Boolean = false

  override protected def connectionCodes(ctx: CodegenContext): (String, String, String) = {
    val connectionClass = classOf[Connection].getName
    val externalStoreTerm = ctx.addReferenceObj("externalStore", externalStore)
    val listenerClass = classOf[SnapshotConnectionListener].getName
    val storeClass = classOf[JDBCSourceAsColumnarStore].getName
    val getContext = Utils.genTaskContextFunction(ctx)

    taskListener = ctx.addMutableState(listenerClass, "taskListener",
      _ => "", forceInline = true)
    connTerm = ctx.addMutableState(connectionClass, "connection",
      _ => "", forceInline = true)

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
}
