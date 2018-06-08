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
package org.apache.spark.sql.catalyst.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, ExprId, Expression, NamedExpression, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.internal.GemFireLimitTag
import org.apache.spark.sql.types.{DataType, Metadata}

trait Tag {
  def symbol: String

  def simpleString: String = ""
}

trait TransformableTag extends Tag {
  def toTag: Tag
}

case class TaggedAttribute(
    tag: Tag,
    name: String,
    dataType: DataType,
    nullable: Boolean = true,
    override val metadata: Metadata = Metadata.empty)(
    val exprId: ExprId = NamedExpression.newExprId,
    val qualifier: Option[String] = None)
    extends Attribute with Unevaluable {

  override def equals(other: Any): Boolean = other match {
    case ar: TaggedAttribute => tag == ar.tag && name == ar.name &&
        exprId == ar.exprId && dataType == ar.dataType
    case _ => false
  }


  override def hashCode(): Int = {
    // See http://stackoverflow.com/questions/113511/hash-code-implementation
    var h = 17
    h = h * 37 + exprId.hashCode()
    h = h * 37 + dataType.hashCode()
    h = h * 37 + metadata.hashCode()
    h = h * 37 + tag.hashCode()
    h
  }

  override def newInstance(): TaggedAttribute = TaggedAttribute(tag, name,
    dataType, nullable, metadata)(qualifier = qualifier)

  /**
   * Returns a copy of this [[TaggedAttribute]] with changed nullability.
   */
  override def withNullability(newNullability: Boolean): TaggedAttribute = {
    if (nullable == newNullability) {
      this
    } else {
      TaggedAttribute(tag, name, dataType, newNullability,
        metadata)(exprId, qualifier)
    }
  }

  override def withName(newName: String): TaggedAttribute = {
    if (name == newName) {
      this
    } else {
      TaggedAttribute(tag, newName, dataType, nullable)(exprId, qualifier)
    }
  }

  /**
   * Returns a copy of this [[TaggedAttribute]] with new qualifiers.
   */
  override def withQualifier(newQualifier: Option[String]): TaggedAttribute = {
    if (newQualifier == qualifier) {
      this
    } else {
      TaggedAttribute(tag, name, dataType, nullable,
        metadata)(exprId, newQualifier)
    }
  }

  override def references: AttributeSet = tag match {
    case GemFireLimitTag => AttributeSet(this.toAttributeReference)
    case _ => super.references
  }

  def toAttributeReference: AttributeReference = AttributeReference(name,
    dataType, nullable, metadata)(exprId, qualifier)

  override def withMetadata(newMetadata: Metadata): Attribute = {
    null
  }
}

case class TaggedAlias(tag: TransformableTag, child: Expression, name: String)(
    val exprId: ExprId = NamedExpression.newExprId,
    val qualifier: Option[String] = None)
    extends NamedExpression  {

  // override type EvaluatedType = Any
  /** Just a simple passthrough for code generation. */
  override def genCode(ctx: CodegenContext): ExprCode = child.genCode(ctx)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ev.copy("")


  override def eval(input: InternalRow): Any = throw new TreeNodeException(
    this, s"No function to evaluate expression. type: ${this.nodeName}")

  override def dataType: DataType = child.dataType

  override def nullable: Boolean = child.nullable

  override def metadata: Metadata = {
    child match {
      case named: NamedExpression => named.metadata
      case _ => Metadata.empty
    }
  }

  def children: Seq[Expression] = child :: Nil

  override def toAttribute: Attribute = {
    if (resolved) {
      TaggedAttribute(tag.toTag, name, child.dataType, child.nullable,
        metadata)(exprId, qualifier)
    } else {
      UnresolvedAttribute(name)
    }
  }

  override def toString: String =
    s"$child${tag.simpleString} AS ${tag.symbol}$name#${exprId.id}$typeSuffix"

  override protected final def otherCopyArgs = exprId :: qualifier :: Nil

  def toAlias: Alias = Alias(child, name)(exprId, qualifier)

  /** Returns a copy of this expression with a new `exprId`. */
  override def newInstance(): NamedExpression = TaggedAlias(tag, child,
    name)(qualifier = qualifier)
}
