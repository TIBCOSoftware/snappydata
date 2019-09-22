/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}

import org.apache.spark.serializer.StructTypeSerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.types.DataType

/**
 * Wrap any TokenizedLiteral expression with this so that we can invoke literal
 * initialization code within the <code>.init()</code> method of the generated class.
 * <br><br>
 *
 * This pushes itself as reference object and uses a call to eval() on itself for actual
 * evaluation and avoids embedding any generated code. This allows it to keep the
 * generated code identical regardless of the constant expression (and in addition
 * DynamicReplacableConstant trait casts to itself rather than actual object type).
 * <br><br>
 *
 * We try to locate first foldable expression in a query tree such that all its child is foldable
 * but parent isn't. That way we locate the exact point where an expression is safe to evaluate
 * once instead of evaluating every row.
 * <br><br>
 *
 * Expressions like <code> select c from tab where
 * case col2 when 1 then col3 else 'y' end = 22 </code>
 * like queries don't convert literal evaluation into init method.
 *
 * @param expr minimal expression tree that can be evaluated only once and turn into a constant.
 */
case class DynamicFoldableExpression(var expr: Expression) extends UnaryExpression
    with DynamicReplacableConstant with KryoSerializable {

  override def checkInputDataTypes(): TypeCheckResult = expr.checkInputDataTypes()

  override def child: Expression = expr

  override def eval(input: InternalRow): Any = expr.eval(input)

  override def dataType: DataType = expr.dataType

  override def value: Any = eval(null)

  override private[sql] def getValueForTypeCheck: Any = null

  override def nodeName: String = "DynamicExpression"

  override def prettyName: String = "DynamicExpression"

  override def toString: String = {
    def removeCast(expr: Expression): Expression = expr match {
      case c: Cast => removeCast(c.child)
      case _ => expr
    }

    "DynExpr(" + removeCast(expr) + ")"
  }

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    assert(newArgs.length == 1)
    if (newArgs(0) eq expr) this
    else DynamicFoldableExpression(newArgs(0).asInstanceOf[Expression])
  }

  override def withNewChildren(newChildren: Seq[Expression]): Expression = {
    assert(newChildren.length == 1)
    if (newChildren.head ne expr) expr = newChildren.head
    this
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    kryo.writeClassAndObject(output, value)
    StructTypeSerializer.writeType(kryo, output, dataType)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    val value = kryo.readClassAndObject(input)
    val dateType = StructTypeSerializer.readType(kryo, input)
    expr = new TokenLiteral(value, dateType)
  }
}
