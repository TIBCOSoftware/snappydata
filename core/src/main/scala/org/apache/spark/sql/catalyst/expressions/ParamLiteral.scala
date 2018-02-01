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

import scala.collection.mutable.ArrayBuffer

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.gemstone.gemfire.internal.shared.ClientResolverUtils

import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.serializer.StructTypeSerializer
import org.apache.spark.sql.catalyst.CatalystTypeConverters._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, SubExprEliminationState}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

// A marker interface to extend usage of Literal case matching.
// A literal that can change across multiple query execution.
trait DynamicReplacableConstant {
  def eval(input: InternalRow = null): Any

  def convertedLiteral: Any
}

// whereever ParamLiteral case matching is required, it must match
// for DynamicReplacableConstant and use .eval(..) for code generation.
// see SNAP-1597 for more details.
final class ParamLiteral(override val value: Any, _dataType: DataType, val pos: Int)
    extends Literal(null, _dataType) with DynamicReplacableConstant {

  // override def toString: String = s"ParamLiteral ${super.toString}"

  private[this] var _foldable = false

  private[this] var literalValueRef: String = _
  private[this] val literalValue: LiteralValue = LiteralValue(value, dataType, pos)()

  private[this] var isNull: String = _
  private[this] var valueTerm: String = _

  private[this] def lv(ctx: CodegenContext) = if (ctx.references.contains(literalValue)) {
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

  override def hashCode(): Int = ClientResolverUtils.fastHashLong(
    Objects.hashCode(dataType).toLong << 32L | (pos & 0xffffffffL))

  // Literal cannot equal ParamLiteral since runtime value can be different
  override def canEqual(that: Any): Boolean = that.isInstanceOf[ParamLiteral]

  override def equals(obj: Any): Boolean = obj match {
    case a: AnyRef if this eq a => true
    case pl: ParamLiteral => pl.pos == pos && pl.dataType == dataType
    case _ => false
  }

  override def productElement(n: Int): Any = {
    val parentFields = super.productArity
    if (n < parentFields) {
      super.productElement(n)
    } else {
      assert (n == parentFields, s"unexpected n = $n but expected $parentFields")
      pos
    }
  }

  override def productArity: Int = super.productArity + 1

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // change the isNull and primitive to consts, to inline them
    val value = this.value
    val addMutableState = (isNull eq null) || !ctx.mutableStates.exists(_._2 == isNull)
    if (addMutableState) {
      isNull = ctx.freshName("isNullTerm")
      valueTerm = ctx.freshName("valueTerm")
    }
    val isNullLocal = ev.isNull
    val valueLocal = ev.value
    val dataType = Utils.getSQLDataType(this.dataType)
    val javaType = ctx.javaType(dataType)
    val initCode =
      s"""
         |final boolean $isNullLocal = $isNull;
         |final $javaType $valueLocal = $valueTerm;
      """.stripMargin
    if (!addMutableState) {
      // use the already added fields
      return ev.copy(initCode, isNullLocal, valueLocal)
    }
    val valueRef = lv(ctx)
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
      case StringType =>
        // allocate UTF8String on off-heap so that Native can be used if possible
        assert(value.isInstanceOf[UTF8String],
          s"unexpected type $dataType instead of UTF8String")

        val getContext = Utils.genTaskContextFunction(ctx)
        val memoryManagerClass = classOf[TaskMemoryManager].getName
        val memoryModeClass = classOf[MemoryMode].getName
        val consumerClass = classOf[DirectStringConsumer].getName
        ctx.addMutableState(javaType, valueTerm,
          s"""
             |if (($isNull = $valueRef.value() == null)) {
             |  $valueTerm = ${ctx.defaultValue(dataType)};
             |} else {
             |  $valueTerm = ($box)$valueRef.value();
             |  if (com.gemstone.gemfire.internal.cache.GemFireCacheImpl.hasNewOffHeap() &&
             |      $getContext() != null) {
             |    // convert to off-heap value if possible
             |    $memoryManagerClass mm = $getContext().taskMemoryManager();
             |    if (mm.getTungstenMemoryMode() == $memoryModeClass.OFF_HEAP) {
             |      $consumerClass consumer = new $consumerClass(mm);
             |      $valueTerm = consumer.copyUTF8String($valueTerm);
             |    }
             |  }
             |}
          """.stripMargin)
        // indicate that code for valueTerm has already been generated
        null.asInstanceOf[String]
      case _ => ""
    }
    ctx.addMutableState("boolean", isNull, "")
    if (unbox ne null) {
      ctx.addMutableState(javaType, valueTerm,
        s"""
           |$isNull = $valueRef.value() == null;
           |$valueTerm = $isNull ? ${ctx.defaultValue(dataType)} : (($box)$valueRef.value())$unbox;
        """.stripMargin)
    }
    ev.copy(initCode, isNullLocal, valueLocal)
  }
}

object ParamLiteral {
  def apply(_value: Any, _dataType: DataType, pos: Int): ParamLiteral =
    new ParamLiteral(_value, _dataType, pos)

  def unapply(arg: ParamLiteral): Option[(Any, DataType, Int)] =
    Some((arg.value, arg.dataType, arg.pos))
}

final class DirectStringConsumer(memoryManager: TaskMemoryManager, pageSize: Int)
    extends MemoryConsumer(memoryManager, pageSize, MemoryMode.OFF_HEAP) {

  def this(memoryManager: TaskMemoryManager) = this(memoryManager, 8)

  override def spill(size: Long, trigger: MemoryConsumer): Long = 0L

  def copyUTF8String(s: UTF8String): UTF8String = {
    if ((s ne null) && (s.getBaseObject ne null)) {
      val size = s.numBytes()
      val page = taskMemoryManager.allocatePage(Math.max(pageSize, size), this)
      if ((page ne null) && page.size >= size) {
        used += page.size
        val ds = UTF8String.fromAddress(null, page.getBaseOffset, size)
        Platform.copyMemory(s.getBaseObject, s.getBaseOffset, null, ds.getBaseOffset, size)
        return ds
      } else if (page ne null) {
        taskMemoryManager.freePage(page, this)
      }
    }
    s
  }
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
 * like queries don't convert literal evaluation into init method.
 *
 * @param expr minimal expression tree that can be evaluated only once and turn into a constant.
 */
case class DynamicFoldableExpression(expr: Expression) extends Expression
    with DynamicReplacableConstant {
  override def nullable: Boolean = expr.nullable

  override def eval(input: InternalRow): Any = expr.eval(input)

  def convertedLiteral: Any = createToScalaConverter(dataType)(eval(null))

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // subexpression elimination can lead to local variables but this expression
    // needs in mutable state, so disable it (extra evaluations only once in init in any case)
    val numSubExprs = ctx.subExprEliminationExprs.size
    val oldSubExprs = if (numSubExprs != 0) {
      val exprs = new ArrayBuffer[(Expression, SubExprEliminationState)](numSubExprs)
      exprs ++= ctx.subExprEliminationExprs
      ctx.subExprEliminationExprs.clear()
      exprs
    } else null
    val eval = expr.genCode(ctx)
    if (oldSubExprs ne null) {
      ctx.subExprEliminationExprs ++= oldSubExprs
    }
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
    // allow sub-expression elimination of this expression itself
    ctx.subExprEliminationExprs += this -> SubExprEliminationState(newVarIsNull, newVar)
    ev.copy(code = "", value = newVar, isNull = newVarIsNull)
  }

  override def dataType: DataType = expr.dataType

  override def children: Seq[Expression] = Seq(expr)

  override def nodeName: String = "DynamicExpression"

  override def prettyName: String = "DynamicExpression"
}
