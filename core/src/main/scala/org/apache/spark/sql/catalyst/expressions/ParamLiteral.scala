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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.types._

class ParamLiteral(val l: Literal, val pos: Int) extends Literal(l.value, l.dataType) {

  override def eval(input: InternalRow): Any = l.eval()

  override def hashCode(): Int = {
    var hash = dataType.hashCode()
    hash = hash ^ pos
    hash
  }

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case pl: ParamLiteral => ( pl.dataType == dataType && pl.pos == pos)
      case _ => false
    }
  }

//  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
//    // change the isNull and primitive to consts, to inline them
//    if (value == null) {
//      ev.isNull = "true"
//      ev.copy(s"final ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};")
//    } else {
//      dataType match {
//        case BooleanType =>
//          ev.isNull = "false"
//          ev.value = value.toString
//          ev.copy("")
//        case FloatType =>
//          val v = value.asInstanceOf[Float]
//          if (v.isNaN || v.isInfinite) {
//            super[CodegenFallback].doGenCode(ctx, ev)
//          } else {
//            ev.isNull = "false"
//            ev.value = s"${value}f"
//            ev.copy("")
//          }
//        case DoubleType =>
//          val v = value.asInstanceOf[Double]
//          if (v.isNaN || v.isInfinite) {
//            super[CodegenFallback].doGenCode(ctx, ev)
//          } else {
//            ev.isNull = "false"
//            ev.value = s"${value}D"
//            ev.copy("")
//          }
//        case ByteType | ShortType =>
//          ev.isNull = "false"
//          ev.value = s"(${ctx.javaType(dataType)})$value"
//          ev.copy("")
//        case IntegerType | DateType =>
//          ev.isNull = "false"
//          ev.value = value.toString
//          ev.copy("")
//        case TimestampType | LongType =>
//          ev.isNull = "false"
//          ev.value = s"${value}L"
//          ev.copy("")
//        // eval() version may be faster for non-primitive types
//        case other =>
//          super[CodegenFallback].doGenCode(ctx, ev)
//      }
//    }
//  }
}

object ParamLiteral {
  def apply(l: Literal, pos: Int) = new ParamLiteral(l, pos)
}
