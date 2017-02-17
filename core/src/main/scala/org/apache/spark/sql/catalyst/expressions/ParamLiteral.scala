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

package org.apache.spark.sql.catalyst.expressions

import java.util.Objects

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.types._

class ParamLiteral(val l: Literal, val pos: Int) extends LeafExpression with CodegenFallback {

  //override def eval(input: InternalRow): Any = l.eval()

  override def hashCode(): Int = {
    31 * (31 * Objects.hashCode(dataType)) + Objects.hashCode(pos)
  }

  //override def productArity: Int = 0

  override def equals(obj: Any): Boolean = {
    obj match {
      case pl: ParamLiteral => (pl.l.dataType == l.dataType && pl.pos == pos)
      case _ => false
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // change the isNull and primitive to consts, to inline them
    val value = l.value
    if (value == null) {
      ev.isNull = "true"
      ev.copy(s"final ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};")
    } else {
      dataType match {
        case BooleanType =>
          ev.isNull = "false"
          assert (value.isInstanceOf[Boolean], "KN: unexpected type")
          val valueRef = ctx.addReferenceObj("literal",
            LiteralValue(value, pos))
          ev.value = ctx.freshName("value")
          ev.copy(s"final boolean ${ev.value} = $valueRef != null " +
            s"? ((Boolean)$valueRef.value()).booleanValue() : ${ctx.defaultValue(dataType)};")
        case FloatType =>
          val v = value.asInstanceOf[Float]
          if (v.isNaN || v.isInfinite) {
            super[CodegenFallback].doGenCode(ctx, ev)
          } else {
            ev.isNull = "false"
            assert (value.isInstanceOf[Float], "KN: unexpected type")
            val valueRef = ctx.addReferenceObj("literal",
              LiteralValue(value, pos))
            ev.value = ctx.freshName("value")
            ev.copy(s"float ${ev.value} = ((Float)$valueRef.value()).floatValue();")
          }
        case DoubleType =>
          val v = value.asInstanceOf[Double]
          if (v.isNaN || v.isInfinite) {
            super[CodegenFallback].doGenCode(ctx, ev)
          } else {
            ev.isNull = "false"
            ev.value = s"${value}D"
            ev.copy("")
          }
        case ByteType | ShortType =>
          ev.isNull = "false"
          ev.value = s"(${ctx.javaType(dataType)})$value"
          ev.copy("")
        case IntegerType | DateType =>
          ev.isNull = "false"
          ev.value = value.toString
          ev.copy("")
        case TimestampType | LongType =>
          ev.isNull = "false"
          ev.value = s"${value}L"
          ev.copy("")
        // eval() version may be faster for non-primitive types
        case other =>
          super[CodegenFallback].doGenCode(ctx, ev)
      }
    }
  }

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = l.eval()

  override def dataType: DataType = l.dataType

  override def productElement(n: Int): Any = Nil

  override def productArity: Int = 0

  override def canEqual(that: Any): Boolean = true
}

object ParamLiteral {
  def apply(l: Literal, pos: Int) = new ParamLiteral(l, pos)
}

case class LiteralValue(var value: AnyRef, var position: Int)
  extends KryoSerializable {

  override def write(kryo: Kryo, output: Output): Unit = {
    kryo.writeClassAndObject(output, value)
    output.writeVarInt(position, true)
  }

  override def read(kryo: Kryo, input: Input): Unit = ???
}
