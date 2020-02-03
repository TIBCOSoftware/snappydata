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
package org.apache.spark.sql.execution.common

import org.apache.spark.sql.SparkSupport
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, ExprId, Expression, NamedExpression, UnaryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.types.Metadata


trait ErrorEstimateAttribute extends Attribute with Unevaluable with SparkSupport {

  def singleQualifier: Option[String]

  def realExprId: ExprId

  /**
   * Returns true iff the expression id is the same for both attributes.
   */
  def sameRef(other: AttributeReference): Boolean = this.exprId == other.exprId

  override def equals(other: Any): Boolean = other match {
    case ar: AttributeReference => name == ar.name && dataType == ar.dataType &&
        nullable == ar.nullable && metadata == ar.metadata && exprId == ar.exprId &&
        qualifier == ar.qualifier
    case eea: ErrorEstimateAttribute => (eea eq this) || (name == eea.name &&
        dataType == eea.dataType && nullable == eea.nullable && metadata == eea.metadata &&
        exprId == eea.exprId && qualifier == eea.qualifier)
    case _ => false
  }

  override def semanticEquals(other: Expression): Boolean = other match {
    case ar: AttributeReference => sameRef(ar)
    case _ => false
  }

  override def semanticHash(): Int = {
    this.exprId.hashCode()
  }

  override def hashCode(): Int = {
    // See http://stackoverflow.com/questions/113511/hash-code-implementation
    var h = 17
    h = h * 37 + name.hashCode()
    h = h * 37 + dataType.hashCode()
    h = h * 37 + nullable.hashCode()
    h = h * 37 + metadata.hashCode()
    h = h * 37 + exprId.hashCode()
    h = h * 37 + qualifier.hashCode()
    h
  }

  override def newInstance(): AttributeReference =
    internals.toAttributeReference(this)(exprId = NamedExpression.newExprId)

  /**
   * Returns a copy of this [[ErrorEstimateAttribute]] with changed nullability.
   */
  override def withNullability(newNullability: Boolean): ErrorEstimateAttribute = {
    if (nullable == newNullability) {
      this
    } else {
      internals.newErrorEstimateAttribute(name, dataType, newNullability, metadata, exprId,
        realExprId, qualifier.toSeq)
    }
  }

  override def withName(newName: String): ErrorEstimateAttribute = {
    if (name == newName) {
      this
    } else {
      internals.newErrorEstimateAttribute(newName, dataType, nullable, metadata, exprId,
        realExprId, qualifier.toSeq)
    }
  }

  def withExprId(newExprId: ExprId): ErrorEstimateAttribute = {
    if (exprId == newExprId) {
      this
    } else {
      internals.newErrorEstimateAttribute(name, dataType, nullable, metadata, newExprId,
        realExprId, qualifier.toSeq)
    }
  }

  override def references: AttributeSet = AttributeSet(internals.toAttributeReference(this)())

  override def withMetadata(newMetadata: Metadata): Attribute = {
    internals.newErrorEstimateAttribute(name, dataType, nullable, newMetadata, exprId,
      realExprId, qualifier.toSeq)
  }

  /** Used to signal the column used to calculate an eventTime watermark (e.g. a#1-T{delayMs}) */
  private def delaySuffix = if (metadata.contains(EventTimeWatermark.delayKey)) {
    s"-T${metadata.getLong(EventTimeWatermark.delayKey)}ms"
  } else {
    ""
  }

  override protected final def otherCopyArgs: Seq[AnyRef] = {
    qualifier :: Nil
  }

  override def toString: String = s"$name#${exprId.id}$typeSuffix$delaySuffix"

  // Since the expression id is not in the first constructor it is missing from the default
  // tree string.
  override def simpleString: String = s"$name#${exprId.id}: ${dataType.simpleString}"

  override def sql: String = {
    val qualifierPrefix = if (qualifier.isEmpty) "" else qualifier.head + '.'
    s"$qualifierPrefix${quoteIdentifier(name)}"
  }
}

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
      internals.newAttributeReference(name, dataType, nullable, metadata, exprId, qualifier)
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
    internals.toAttributeReference(this)(exprId = NamedExpression.newExprId)
}
