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
package org.apache.spark.sql.execution.bootstrap

import org.apache.spark.sql.SparkSupport
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, NamedExpression, UnaryExpression}
import org.apache.spark.sql.types.Metadata

trait ApproxColumnExtractor extends UnaryExpression with NamedExpression with SparkSupport {

  val ordinal: Int

  override lazy val resolved: Boolean = true

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("not implemented")

  override protected def doGenCode(ctx: CodegenContext,
      ev: ExprCode): ExprCode = {

    val childEval = child.genCode(ctx)
    val evIsNull = internals.exprCodeIsNull(ev)
    val evVal = internals.exprCodeValue(ev)
    val childVal = internals.exprCodeValue(childEval)
    val code =
      s"""
        ${childEval.code}
        double $evVal = 0d;
        boolean $evIsNull =  ((InternalRow) $childVal).isNullAt($ordinal);
        if (!$evIsNull) {
          $evVal = ((InternalRow) $childVal).getDouble($ordinal);
        }
      """
    internals.copyExprCode(ev, code = code)
  }

  override def metadata: Metadata = Metadata.empty

  override def toAttribute: Attribute = {
    if (resolved) {
      internals.newAttributeReference(name, dataType, nullable, metadata, exprId, qualifier.toSeq)
    } else {
      UnresolvedAttribute(name)
    }
  }

  override def toString: String = s"$child AS $name#${exprId.id}$typeSuffix"

  override protected final def otherCopyArgs: Seq[AnyRef] = {
    exprId :: qualifier :: Nil
  }

  override def equals(other: Any): Boolean = other match {
    case a: Alias =>
      name == a.name && exprId == a.exprId && child == a.child

    case _ => false
  }

  /** Returns a copy of this expression with a new `exprId`. */
  override def newInstance(): NamedExpression =
    internals.newApproxColumnExtractor(child, name, ordinal, dataType, nullable,
      qualifier = qualifier.toSeq)
}
