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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, LiteralValue}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.collection.mutable

class CachedPlanHelperExec(childPlan: CodegenSupport)
  extends UnaryExecNode with CodegenSupport {

  var ctxReferences: mutable.ArrayBuffer[Any] = null;

  override def child: SparkPlan = childPlan

  override def inputRDDs(): Seq[RDD[InternalRow]] = childPlan.inputRDDs()

  override protected def doProduce(ctx: CodegenContext): String = {
    ctxReferences = ctx.references
    // childPlan.doProduce(ctx)
    ""
  }

  override protected def doExecute(): RDD[InternalRow] = childPlan.execute()

  override def output: Seq[Attribute] = childPlan.output

  override def productElement(n: Int): Any = n match {
    case 0 => childPlan
  }

  override def productArity: Int = 1

  override def canEqual(that: Any): Boolean = {
    if (that.isInstanceOf[CachedPlanHelperExec]) {
      that == this
    }
    else false
  }

  private lazy val allLiterals: Array[LiteralValue] = {
    ctxReferences.filter(
      { p: Any => p.isInstanceOf[LiteralValue] }).map(
      _.asInstanceOf[LiteralValue]).sortBy(_.position).toArray
  }

  def replaceConstants(currParsedLp: LogicalPlan): Unit = {
    val itr = currParsedLp.productIterator.filter({ p: Any => p.isInstanceOf[LiteralValue] })
    itr.map({ x: Any =>
      assert(x.isInstanceOf[LiteralValue])
      val lv = x.asInstanceOf[LiteralValue]
      allLiterals(lv.position).value = lv.value
    })
  }
}

object CachedPlanHelperExec {
  def apply(childPlan: CodegenSupport): CachedPlanHelperExec = new CachedPlanHelperExec(childPlan)
}
