/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.bootstrap

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.{InternalRow, trees}
import org.apache.spark.sql.types.{DataType, DoubleType, Metadata}

import scala.collection.mutable

trait Tag {
  def symbol: String
  def simpleString = ""
}

trait TransformableTag extends Tag {
  def toTag: Tag
}

case class Seed() extends TransformableTag {
  val symbol = ":"
  def toTag = this
}

case class Bootstrap() extends TransformableTag {
  val symbol = ":"
  def toTag = this
  override lazy val simpleString =  "No Op"//s"^${branches.mkString("[", ", ", "]")}"
}

case object Uncertain extends TransformableTag {
  val symbol = "~"
  def toTag = this
}

object BoundType extends Enumeration {
  type BoundType = Value
  val Lower, Upper, Mixed = Value
}

case class Bound(boundType: BoundType.BoundType, child: ExprId)
    extends TransformableTag {
  import BoundType._

  val symbol = "~"
  def toTag = this
  override lazy val simpleString = boundType match {
    case Lower => s"^[]$child"
    case Upper => s"^$child[]"
    case Mixed => s"^[$child]"
  }

  var boundAttributes: Seq[Attribute] = Nil
}

case object Flag extends TransformableTag {
  val symbol = ":"
  def toTag = this
}

trait GenerateLazyEvaluate {
  def lineage: Seq[NamedExpression]
  def lazyEval: Expression
  def dependencies: Map[OpId, (Seq[Attribute], Seq[NamedExpression], Int)]
}

trait LazyEvaluate {
  def lazyEval: Expression
  def lineage: Seq[Attribute] = lazyEval.references.toSeq
  def dependencies: Map[OpId, (Seq[Attribute], Seq[NamedExpression], Int)]
}

case class LazyAttribute(
    lazyEval: Expression,
    dependencies: Map[OpId, (Seq[Attribute], Seq[NamedExpression], Int)])
    extends Tag with LazyEvaluate {
  val symbol = "`"
  override lazy val simpleString = s"=$lazyEval"
}

case class LazyAggregate(
    opId: OpId,
    numPartitions: Int, // !Hack: we should find number of partitions at run time
    keys: Seq[NamedExpression],
    schema: Seq[Attribute],
    attribute: Attribute)
    extends TransformableTag with GenerateLazyEvaluate {
  val symbol = "`"
  def toTag = LazyAttribute(lazyEval, dependencies)
  override lazy val simpleString = s"=(${lineage.mkString("[",",","]")}, $lazyEval)"

  val dependencies = Map(opId -> (schema, keys, numPartitions))
  val lineage = dependencies.head._2._2
  val lazyEval = GetPlaceholder(attribute, lineage)
}

case class LazyAggrBound(
    opId: OpId,
    numPartitions: Int, // !Hack: we should find number of partitions at run time
    keys: Seq[NamedExpression],
    schema: Seq[Attribute],
    attribute: Attribute,
    bound: Bound)
    extends TransformableTag with GenerateLazyEvaluate {
  val symbol = "`"
  def toTag = LazyAttribute(lazyEval, dependencies)
  override lazy val simpleString = s"=(${lineage.mkString("[",",","]")}, $lazyEval)"

  val dependencies = Map(opId -> (schema, keys, numPartitions))
  val lineage = dependencies.head._2._2
  val lazyEval = GetPlaceholder(attribute, lineage)
}

case class LazyAlias(child: Expression) extends TransformableTag with GenerateLazyEvaluate {
  val symbol = "`"
  def toTag = LazyAttribute(lazyEval, dependencies)
  override lazy val simpleString = s"=(${lineage.mkString("[", ",", "]")}, $lazyEval)"

  val (lineage, lazyEval, dependencies: Map[OpId, (Seq[Attribute], Seq[NamedExpression], Int)]) = {
    val certain = new mutable.ArrayBuffer[Expression]()
    val lineage = new mutable.ArrayBuffer[NamedExpression]()
    val schema = new mutable.HashMap[OpId, (Seq[Attribute], Seq[NamedExpression], Int)]()

    def isCertain(e: Expression) = certain.exists(_.fastEquals(e))

    // 1. Replace lazy evaluates
    // 2. Mark certain nodes
    val replaced = child.transformUp {
      case TaggedAttribute(lazyEval: LazyAttribute, _, _, _, _) =>
        lineage ++= lazyEval.lineage
        schema ++= lazyEval.dependencies
        lazyEval.lazyEval
      case expr =>
        if (expr.children.forall(isCertain)) {
          certain += expr
        }
        expr
    }

    val lazyEval = replaced.transformDown {
      case named: NamedExpression if isCertain(named) =>
        lineage += named
        named.toAttribute
      case literal: Literal => literal
      case expr if isCertain(expr) =>
        val named = Alias(expr, "_lineage")()
        lineage += named
        named.toAttribute
    }

    (lineage.distinct, lazyEval, schema.toMap)
  }
}

case class TaggedAttribute(
    tag: Tag,
    name: String,
    dataType: DataType,
    nullable: Boolean = true,
    override val metadata: Metadata = Metadata.empty)(
    val exprId: ExprId = NamedExpression.newExprId,
    val qualifiers: Seq[String] = Nil) extends Attribute with CodegenFallback {

  override def equals(other: Any) = other match {
    case ar: TaggedAttribute =>
      tag == ar.tag && name == ar.name && exprId == ar.exprId && dataType == ar.dataType
    case _ => false
  }

  override def children: Seq[Expression] = Nil

  override def hashCode: Int = {
    // See http://stackoverflow.com/questions/113511/hash-code-implementation
    var h = 17
    h = h * 37 + exprId.hashCode()
    h = h * 37 + dataType.hashCode()
    h = h * 37 + metadata.hashCode()
    h
  }

  override def newInstance() =
    TaggedAttribute(tag, name, dataType, nullable, metadata)(qualifiers = qualifiers)

  /**
   * Returns a copy of this [[TaggedAttribute]] with changed nullability.
   */
  override def withNullability(newNullability: Boolean): TaggedAttribute = {
    if (nullable == newNullability) {
      this
    } else {
      TaggedAttribute(tag, name, dataType, newNullability, metadata)(exprId, qualifiers)
    }
  }

  override def withName(newName: String): TaggedAttribute = {
    if (name == newName) {
      this
    } else {
      TaggedAttribute(tag, newName, dataType, nullable)(exprId, qualifiers)
    }
  }

  /**
   * Returns a copy of this [[TaggedAttribute]] with new qualifiers.
   */
  override def withQualifiers(newQualifiers: Seq[String]) = {
    if (newQualifiers.toSet == qualifiers.toSet) {
      this
    } else {
      TaggedAttribute(tag, name, dataType, nullable, metadata)(exprId, newQualifiers)
    }
  }

  // Unresolved attributes are transient at compile time and don't get evaluated during execution.
  override def eval(input: InternalRow = null): Any =
    throw new TreeNodeException(this, s"No function to evaluate expression. type: ${this.nodeName}")

  override def toString: String = s"${tag.symbol}$name#${exprId.id}$typeSuffix${tag.simpleString}"

  def toAttributeReference =
    AttributeReference(name, dataType, nullable, metadata)(exprId, qualifiers)
}

case class TaggedAlias(tag: TransformableTag, child: Expression, name: String)
    (val exprId: ExprId = NamedExpression.newExprId, val qualifiers: Seq[String] = Nil)
    extends NamedExpression with CodegenFallback {

  //override type EvaluatedType = Any

  override def eval(input: InternalRow) =
    throw new TreeNodeException(this, s"No function to evaluate expression. type: ${this.nodeName}")

  override def dataType = child.dataType
  override def nullable = child.nullable
  override def metadata: Metadata = {
    child match {
      case named: NamedExpression => named.metadata
      case _ => Metadata.empty
    }
  }

  def children: Seq[Expression] = child::Nil

  override def toAttribute = {
    if (resolved) {
      TaggedAttribute(
        tag.toTag, name, child.dataType, child.nullable, metadata)(exprId, qualifiers)
    } else {
      UnresolvedAttribute(name)
    }
  }

  override def toString: String =
    s"$child${tag.simpleString} AS ${tag.symbol}$name#${exprId.id}$typeSuffix"

  override protected final def otherCopyArgs = exprId :: qualifiers :: Nil

  def toAlias = Alias(child, name)(exprId, qualifiers)
}

case class ScaleFactor extends LeafExpression with CodegenFallback {
  type EvaluatedType = Any

  override def foldable = true
  def nullable = false
  def dataType = DoubleType

  override def eval(input: InternalRow):Any =
    throw new TreeNodeException(this, s"No function to evaluate expression. type: ${this.nodeName}")

  override def toString = "Not implemented"
}

case class GetPlaceholder(attribute: Attribute, children: Seq[Expression]) extends Expression with CodegenFallback {

  override def nullable: Boolean = attribute.nullable

  override def dataType: DataType = attribute.dataType

  override def eval(input: InternalRow): Any =
    throw new TreeNodeException(this, s"No function to evaluate expression. type: ${this.nodeName}")

  override def toString = s"Get(${children.mkString(",")})"
}
