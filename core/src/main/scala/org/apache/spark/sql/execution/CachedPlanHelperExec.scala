/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

import org.apache.hadoop.hive.metastore.parser.ExpressionTree.TreeNode
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, LiteralValue, ParamLiteral}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.collection.mutable

case class CachedPlanHelperExec(childPlan: CodegenSupport)
  extends UnaryExecNode with CodegenSupport {

  var ctxReferences: mutable.ArrayBuffer[Any] = _

  override def child: SparkPlan = childPlan

  override def inputRDDs(): Seq[RDD[InternalRow]] = childPlan.inputRDDs()

  override protected def doProduce(ctx: CodegenContext): String = {
    ctxReferences = ctx.references
    childPlan.produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String =
    parent.doConsume(ctx, input, row)

  override protected def doExecute(): RDD[InternalRow] = childPlan.execute()

  override def output: Seq[Attribute] = childPlan.output

  private lazy val allLiterals: Array[LiteralValue] = {
    ctxReferences.filter(
      { p: Any => p.isInstanceOf[LiteralValue] }).map(
      _.asInstanceOf[LiteralValue]).sortBy(_.position).toArray
  }

  private lazy val hasParamLiteralNode = allLiterals.size > 0

  def collectParamLiteralNodes(lp: LogicalPlan): Unit = {
    if ( hasParamLiteralNode ) {
      lp transformAllExpressions {
        case p: ParamLiteral => {
          allLiterals(p.pos - 1).value = p.value
          p
        }
      }
    }
  }

  def replaceConstants(currLogicalPlan: LogicalPlan): Unit = {
    collectParamLiteralNodes(currLogicalPlan)
  }
}
