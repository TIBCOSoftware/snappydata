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
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, ExprId, Expression, NamedExpression, Unevaluable}
import org.apache.spark.sql.types.{DataType, Metadata}

trait Tag {

  def symbol: String

  def simpleString: String = ""
}

trait TransformableTag extends Tag {
  def toTag: Tag
}

object Seed extends TransformableTag {

  val symbol = ":"

  def toTag: TransformableTag = this
}

object Bootstrap extends TransformableTag {

  val symbol = ":"

  def toTag: TransformableTag = this

  override lazy val simpleString = "No Op" // s"^${branches.mkString("[", ", ", "]")}"
}

trait TaggedAttribute extends Attribute with Unevaluable with SparkSupport {

  val tag: Tag

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

  override def newInstance(): TaggedAttribute = internals.newTaggedAttribute(tag, name,
    dataType, nullable, metadata, qualifier = qualifier.toSeq)

  /**
   * Returns a copy of this [[TaggedAttribute]] with changed nullability.
   */
  override def withNullability(newNullability: Boolean): TaggedAttribute = {
    if (nullable == newNullability) {
      this
    } else {
      internals.newTaggedAttribute(tag, name, dataType, newNullability, metadata,
        exprId, qualifier.toSeq)
    }
  }

  override def withName(newName: String): TaggedAttribute = {
    if (name == newName) {
      this
    } else {
      internals.newTaggedAttribute(tag, newName, dataType, nullable, metadata,
        exprId, qualifier.toSeq)
    }
  }

  def withExprId(newExprId: ExprId): TaggedAttribute = {
    if (exprId == newExprId) {
      this
    } else {
      internals.newTaggedAttribute(tag, name, dataType, nullable, metadata,
        newExprId, qualifier.toSeq)
    }
  }

  def toAttributeReference: AttributeReference = internals.newAttributeReference(name,
    dataType, nullable, metadata, exprId, qualifier.toSeq)

  override def withMetadata(newMetadata: Metadata): Attribute = {
    internals.newTaggedAttribute(tag, name, dataType, nullable, metadata,
      exprId, qualifier.toSeq)
  }

  override protected final def otherCopyArgs: Seq[AnyRef] = exprId :: qualifier :: Nil
}

trait TaggedAlias extends NamedExpression with SparkSupport {

  val child: Expression

  val tag: TransformableTag

  // override type EvaluatedType = Any
  /** Just a simple passthrough for code generation. */
  override def genCode(ctx: CodegenContext): ExprCode = child.genCode(ctx)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    internals.copyExprCode(ev, code = "")

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
      internals.newTaggedAttribute(tag.toTag, name, child.dataType, child.nullable,
        metadata, exprId, qualifier.toSeq)
    } else {
      UnresolvedAttribute(name)
    }
  }

  override def toString: String =
    s"$child${tag.simpleString} AS ${tag.symbol}$name#${exprId.id}$typeSuffix"

  override protected final def otherCopyArgs: Seq[AnyRef] = exprId :: qualifier :: Nil

  def toAlias: Alias = internals.newAlias(child, name, copyAlias = None, exprId, qualifier.toSeq)

  /** Returns a copy of this expression with a new `exprId`. */
  override def newInstance(): NamedExpression = internals.newTaggedAlias(tag, child,
    name, qualifier = qualifier.toSeq)
}
