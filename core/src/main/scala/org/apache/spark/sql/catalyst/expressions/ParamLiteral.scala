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

package org.apache.spark.sql.catalyst.expressions

import java.util.Objects

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}

import org.apache.spark.serializer.StructTypeSerializer
import org.apache.spark.sql.catalyst.CatalystTypeConverters._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.types._

// A marker interface to extend usage of Literal case matching.
// A literal that can change across multiple query execution.
trait DynamicReplacableConstant {
  def eval(input: InternalRow = null): Any

  def convertedLiteral: Any
}

// whereever ParamLiteral case matching is required, it must match
// for DynamicReplacableConstant and use .eval(..) for code generation.
// see SNAP-1597 for more details.
class ParamLiteral(_value: Any, _dataType: DataType, val pos: Int)
    extends Literal(_value, _dataType) with DynamicReplacableConstant {

  // override def toString: String = s"ParamLiteral ${super.toString}"

  private[this] var _foldable = false

  private[this] var literalValueRef: String = _

  private[this] val literalValue: LiteralValue = LiteralValue(value, dataType, pos)()

  private[this] def lv(ctx: CodegenContext) = if (ctx.references.exists(_ equals literalValue)) {
    assert(literalValueRef != null)
    literalValueRef
  } else {
    literalValueRef = ctx.addReferenceObj("literal", literalValue)
    literalValueRef
  }

  override def nullable: Boolean = super.nullable

  override def eval(input: InternalRow): Any = literalValue.value

  def convertedLiteral: Any = literalValue.converter(literalValue.value)

  override def foldable: Boolean = _foldable

  def markFoldable(param: Boolean): Unit = _foldable = param

//  override def toString: String = s"pl[${super.toString}]"

  override def hashCode(): Int = {
    31 * (31 * Objects.hashCode(dataType)) + Objects.hashCode(pos)
  }

  override def equals(obj: Any): Boolean = obj match {
    case a: AnyRef if this eq a => true
    case pl: ParamLiteral =>
      pl.dataType == dataType && pl.pos == pos
    case _ => false
  }

  override def productElement(n: Int): Any = {
    val parentFields = super.productArity
    if (n < parentFields) {
      super.productElement(n)
    } else {
      n match {
        case v if v == parentFields => pos
      }
    }
  }

  override def productArity: Int = super.productArity + 1

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // change the isNull and primitive to consts, to inline them
    val value = this.value
    val isNull = ctx.freshName("isNull")
    val valueTerm = ctx.freshName("value")
    val isNullLocal = s"${isNull}Local"
    val valueLocal = s"${valueTerm}Local"
    val valueRef = lv(ctx)
    val dataType = Utils.getSQLDataType(this.dataType)
    val javaType = ctx.javaType(dataType)
    val box = ctx.boxedType(javaType)

    val unbox = dataType match {
      case BooleanType =>
        assert(value.isInstanceOf[Boolean], s"unexpected type $dataType instead of BooleanType")
        ".booleanValue()"
      case FloatType =>
        assert(value.isInstanceOf[Float], s"unexpected type $dataType instead of FloatType")
        ".floatValue()"
      case DoubleType =>
        assert(value.isInstanceOf[Double], s"unexpected type $dataType instead of DoubleType")
        ".doubleValue()"
      case ByteType =>
        assert(value.isInstanceOf[Byte], s"unexpected type $dataType instead of ByteType")
        ".byteValue()"
      case ShortType =>
        assert(value.isInstanceOf[Short], s"unexpected type $dataType instead of ShortType")
        ".shortValue()"
      case t@(IntegerType | DateType) =>
        assert(value.isInstanceOf[Int], s"unexpected type $dataType instead of $t")
        ".intValue()"
      case t@(TimestampType | LongType) =>
        assert(value.isInstanceOf[Long], s"unexpected type $dataType instead of $t")
        ".longValue()"
      case _ => ""
    }
    ctx.addMutableState("boolean", isNull, "")
    ctx.addMutableState(javaType, valueTerm,
      s"""
         |$isNull = $valueRef.value() == null;
         |$valueTerm = $isNull ? ${ctx.defaultValue(dataType)} : (($box)$valueRef.value())$unbox;
      """.stripMargin)
    ev.copy(
      s"""
         |final boolean $isNullLocal = $isNull;
         |final $javaType $valueLocal = $valueTerm;
      """.stripMargin, isNullLocal, valueLocal)
  }
}

object ParamLiteral {
  def apply(_value: Any, _dataType: DataType, pos: Int): ParamLiteral =
    new ParamLiteral(_value, _dataType, pos)

  def unapply(arg: ParamLiteral): Option[(Any, DataType, Int)] =
    Some((arg.value, arg.dataType, arg.pos))
}

case class LiteralValue(var value: Any, var dataType: DataType, var position: Int)
    (var converter: Any => Any = createToScalaConverter(dataType))
    extends KryoSerializable {

  @transient var collectedForPlanCaching = false

  override def write(kryo: Kryo, output: Output): Unit = {
    kryo.writeClassAndObject(output, value)
    StructTypeSerializer.writeType(kryo, output, dataType)
    output.writeVarInt(position, true)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    value = kryo.readClassAndObject(input)
    dataType = StructTypeSerializer.readType(kryo, input)
    position = input.readVarInt(true)
    converter = createToScalaConverter(dataType)
  }
}


/**
 * Wrap any ParamLiteral expression with this so that we can generate literal initialization code
 * within the <code>.init()</code> method of the generated class.
 * <br><br>
 *
 * We try to locate first foldable expression in a query tree such that all its child is foldable
 * but parent isn't. That way we locate the exact point where an expression is safe to evalute
 * once instead of evaluating every row.
 * <br><br>
 *
 * Expressions like <code> select c from tab where
 *  case col2 when 1 then col3 else 'y' end = 22 </code>
 * like queries doesn't converts literal evaluation into init method.
 *
 * @param expr minimal expression tree that can be evaluated only once and turn into a constant.
 */
case class DynamicFoldableExpression(expr: Expression) extends Expression
    with DynamicReplacableConstant {
  override def nullable: Boolean = expr.nullable

  override def eval(input: InternalRow): Any = expr.eval(input)

  def convertedLiteral: Any = createToScalaConverter(dataType)(eval(null))

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = expr.genCode(ctx)
    val newVar = ctx.freshName("paramLiteralExpr")
    val newVarIsNull = ctx.freshName("paramLiteralExprIsNull")
    val comment = ctx.registerComment(expr.toString)
    // initialization for both variable and isNull is being done together
    // due to dependence of latter on the variable and the two get
    // separated due to Spark's splitExpressions -- SNAP-1794
    ctx.addMutableState(ctx.javaType(expr.dataType), newVar,
      s"$comment\n${eval.code}\n$newVar = ${eval.value};\n" +
        s"$newVarIsNull = ${eval.isNull};")
    ctx.addMutableState("boolean", newVarIsNull, "")
    ev.copy(code = "", value = newVar, isNull = newVarIsNull)
  }

  override def dataType: DataType = expr.dataType

  override def children: Seq[Expression] = Seq(expr)

  override def canEqual(that: Any): Boolean = that match {
    case thatExpr: DynamicFoldableExpression => expr.canEqual(thatExpr.expr)
    case other => expr.canEqual(other)
  }

  override def nodeName: String = "DynamicExpression"

  override def prettyName: String = "DynamicExpression"
}
