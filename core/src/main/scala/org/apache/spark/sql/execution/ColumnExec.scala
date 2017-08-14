package org.apache.spark.sql.execution

import java.sql.Connection

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.execution.columnar.ExternalStore
import org.apache.spark.sql.execution.columnar.impl.{JDBCSourceAsColumnarStore, SnapshotConnectionListener}
import org.apache.spark.sql.execution.row.RowExec

trait ColumnExec extends RowExec {

  def externalStore: ExternalStore

  @transient protected var taskListener: String = _

  override protected def connectionCodes(ctx: CodegenContext): (String, String, String) = {
    val connectionClass = classOf[Connection].getName
    val externalStoreTerm = ctx.addReferenceObj("externalStore", externalStore)
    val listenerClass = classOf[SnapshotConnectionListener].getName
    val jdbcColStoreClass = classOf[JDBCSourceAsColumnarStore].getName
    taskListener = ctx.freshName("taskListener")
    connTerm = ctx.freshName("connection")

    val contextClass = classOf[TaskContext].getName
    val context = ctx.freshName("taskContext")

    ctx.addMutableState(listenerClass, taskListener, "")
    ctx.addMutableState(connectionClass, connTerm, "")

    val initCode =
      s"""
         |$taskListener = new $listenerClass(($jdbcColStoreClass)$externalStoreTerm);
         |$connTerm = $taskListener.getConn();
         |final $contextClass $context = $contextClass.get();
         |if ($context != null) {
         |   $context.addTaskCompletionListener($taskListener);
         |}
         | """.stripMargin

    val endCode =""
    val commitCode =""
    (initCode, commitCode, endCode)
  }
}
