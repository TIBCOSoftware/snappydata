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
package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.sources.ConnectionProperties
import org.apache.spark.sql.types.LongType

/**
 * Common methods for updates and deletes in column and row tables.
 */
abstract class TableMutationExec(forUpdate: Boolean)
    extends UnaryExecNode with CodegenSupportOnExecutor {

  @transient protected lazy val (metricAdd, _) = Utils.metricMethods

  var connTerm: String = _

  def resolvedName: String

  def keyColumns: Seq[Attribute]

  def connProps: ConnectionProperties

  def onExecutor: Boolean

  override lazy val output: Seq[Attribute] =
    AttributeReference("count", LongType, nullable = false)() :: Nil

  override lazy val metrics: Map[String, SQLMetric] = {
    if (onExecutor) Map.empty
    else if (forUpdate) Map("numUpdatedRows" -> SQLMetrics.createMetric(
      sparkContext, "number of updated rows"))
    else Map("numDeletedRows" -> SQLMetrics.createMetric(
      sparkContext, "number of deleted rows"))
  }

  override protected def doExecute(): RDD[InternalRow] = {
    // don't expect code generation to fail
    WholeStageCodegenExec(this).execute()
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] =
    child.asInstanceOf[CodegenSupport].inputRDDs()

  protected def connectionCodes(ctx: CodegenContext): (String, String) = {
    if (connTerm eq null) {
      val utilsClass = ExternalStoreUtils.getClass.getName
      connTerm = ctx.freshName("connection")
      val props = ctx.addReferenceObj("connecionProperties", connProps)
      val initCode =
        s"""
           |final java.sql.Connection $connTerm = $utilsClass.MODULE$$
           |    .getConnection("$resolvedName", $props, true);""".stripMargin
      val endCode =
        s""" finally {
           |  try {
           |    $connTerm.close();
           |  } catch (java.sql.SQLException sqle) {
           |    throw new java.io.IOException(sqle.toString(), sqle);
           |  }
           |}""".stripMargin
      (initCode, endCode)
    } else ("", "")
  }

  @transient protected lazy val whereClauseString: String = {
    keyColumns.map(_.name).mkString("=? AND ")
  }

  protected def doChildProduce(ctx: CodegenContext): String = {
    val childProduce = child match {
      case c: CodegenSupportOnExecutor if onExecutor =>
        c.produceOnExecutor(ctx, this)
      case c: CodegenSupport => c.produce(ctx, this)
      case _ => throw new UnsupportedOperationException(
        s"Expected a child supporting code generation. Got: $child")
    }
    if (!ctx.addedFunctions.contains("shouldStop")) {
      // no need to stop in iteration at any point
      ctx.addNewFunction("shouldStop",
        s"""
           |@Override
           |protected final boolean shouldStop() {
           |  return false;
           |}
        """.stripMargin)
    }
    childProduce
  }
}
