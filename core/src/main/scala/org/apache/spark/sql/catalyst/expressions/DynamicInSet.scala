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

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}

/**
 * Unlike Spark's InSet expression, this allows for TokenizedLiterals that can
 * change dynamically in executions.
 */
case class DynamicInSet(child: Expression, hset: IndexedSeq[Expression])
    extends UnaryExpression with Predicate {

  require((hset ne null) && hset.nonEmpty, "hset cannot be null or empty")
  // all expressions must be constant types
  require(hset.forall(TokenLiteral.isConstant), "hset can only have constant expressions")

  override def toString: String =
    s"$child DynInSet ${hset.mkString("(", ",", ")")}"

  override def nullable: Boolean = hset.exists(_.nullable)

  @transient private lazy val (hashSet, hasNull) = {
    val m = new java.util.HashMap[AnyRef, AnyRef](hset.length)
    var hasNull = false
    for (e <- hset) {
      val v = e.eval(null).asInstanceOf[AnyRef]
      if (v ne null) {
        m.put(v, v)
      } else if (!hasNull) {
        hasNull = true
      }
    }
    (m, hasNull)
  }

  protected override def nullSafeEval(value: Any): Any = {
    if (hashSet.containsKey(value)) {
      true
    } else if (hasNull) {
      null
    } else {
      false
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // JDK8 HashMap consistently clocks fastest for gets at small/medium sizes
    // over scala maps, Fastutil/Koloboke and others.
    val setName = classOf[java.util.HashMap[AnyRef, AnyRef]].getName
    val exprClass = classOf[Expression].getName
    val elements = new Array[AnyRef](hset.length)
    val childGen = child.genCode(ctx)
    val hsetTerm = ctx.freshName("hset")
    val elementsTerm = ctx.freshName("elements")
    val idxTerm = ctx.freshName("idx")
    val idx = ctx.references.length
    ctx.references += elements
    val hasNullTerm = ctx.freshName("hasNull")

    for (i <- hset.indices) {
      val e = hset(i)
      val v = e match {
        case d: DynamicReplacableConstant => d
        case _ => e.eval(null).asInstanceOf[AnyRef]
      }
      elements(i) = v
    }

    ctx.addMutableState("boolean", hasNullTerm, "")
    ctx.addMutableState(setName, hsetTerm,
      s"""
         |Object[] $elementsTerm = (Object[])references[$idx];
         |$hsetTerm = new $setName($elementsTerm.length, 0.7f);
         |for (int $idxTerm = 0; $idxTerm < $elementsTerm.length; $idxTerm++) {
         |  Object e = $elementsTerm[$idxTerm];
         |  if (e instanceof $exprClass) e = (($exprClass)e).eval(null);
         |  if (e != null) {
         |    $hsetTerm.put(e, e);
         |  } else if (!$hasNullTerm) {
         |    $hasNullTerm = true;
         |  }
         |}
      """.stripMargin)

    ev.copy(code =
        s"""
      ${childGen.code}
      boolean ${ev.isNull} = ${childGen.isNull};
      boolean ${ev.value} = false;
      if (!${ev.isNull}) {
        ${ev.value} = $hsetTerm.containsKey(${childGen.value});
        if (!${ev.value} && $hasNullTerm) {
          ${ev.isNull} = true;
        }
      }
     """)
  }

  override def sql: String = {
    val valueSQL = child.sql
    val listSQL = hset.map(_.eval(null)).mkString(", ")
    s"($valueSQL IN ($listSQL))"
  }
}
