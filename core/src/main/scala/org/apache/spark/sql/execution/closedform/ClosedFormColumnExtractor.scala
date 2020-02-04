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
package org.apache.spark.sql.execution.closedform

import org.apache.spark.sql.SparkSupport
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, NamedExpression, UnaryExpression}
import org.apache.spark.sql.execution.common.HAC
import org.apache.spark.sql.types.Metadata

trait ClosedFormColumnExtractor extends UnaryExpression with NamedExpression with SparkSupport {

  val confidence: Double

  val confFactor: Double

  val aggType: ErrorAggregate.Type

  val error: Double

  val behavior: HAC.Type

  // Alias(Generator, xx) need to be transformed into Generate(generator, ...)
  override lazy val resolved = true

  override def eval(input: InternalRow): Any = {
    val errorStats = child.eval(input).asInstanceOf[ClosedFormStats]
    val retVal: Double = SparkSupport.contextFunctionsStateless.finalizeEvaluation(
      errorStats, confidence, confFactor, aggType, error, behavior)
    if (retVal.isNaN) null else retVal
  }

  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childEval = child.genCode(ctx)
    val statClass = classOf[ClosedFormStats].getName
    val statVar = ctx.freshName("errorStats")
    val returnValue = ctx.freshName("returnValue")
    val statCounterUDTF = "org.apache.spark.sql.execution.closedform.StatCounterUDTCF"
    val behaviorString = HAC.getBehaviorAsString(behavior)
    val hacClass = HAC.getClass.getName
    val aggTypeStr = aggType.toString
    val aggTypeClass = ErrorAggregate.getClass.getName

    val code = childEval.code.toString +
        s"""
          $statClass $statVar = ($statClass)${internals.exprCodeValue(childEval)};
          double $returnValue = $statCounterUDTF.MODULE$$.finalizeEvaluation($statVar,
          $confidence, $confFactor,$aggTypeClass.MODULE$$.withName("$aggTypeStr"), $error,
            $hacClass.MODULE$$.getBehavior("$behaviorString"));
          boolean ${internals.exprCodeIsNull(ev)} = Double.isNaN($returnValue);
          double ${internals.exprCodeValue(ev)} = $returnValue;
        """
    internals.copyExprCode(ev, code = code)
  }

  override def metadata: Metadata = Metadata.empty

  override def toAttribute: Attribute =
    if (resolved) {
      internals.newAttributeReference(name, dataType, nullable, metadata, exprId, qualifier.toSeq)
    } else {
      UnresolvedAttribute(name)
    }

  override def toString: String = s"$child AS $name#${exprId.id}$typeSuffix"

  override protected final def otherCopyArgs: Seq[AnyRef] = exprId :: qualifier :: Nil

  override def equals(other: Any): Boolean = other match {
    case a: Alias =>
      name == a.name && exprId == a.exprId && child == a.child

    case _ => false
  }

  /** Returns a copy of this expression with a new `exprId`. */
  override def newInstance(): NamedExpression = internals.newClosedFormColumnExtractor(
    child, name, confidence, confFactor, aggType, error, dataType, behavior,
    nullable, qualifier = qualifier.toSeq)
}
