/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

package org.apache.spark.sql.internal

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.collection.Utils.unAlias

trait JoinQueryPlanning {

  def getKeyOrder(plan: LogicalPlan, joinKeys: Seq[Expression],
      partitioning: Seq[NamedExpression]): (Seq[Int], Boolean) = {
    val part = partitioning.map(unAlias(_))
    lazy val planExpressions = plan.expressions
    val keyOrder = joinKeys.map { k =>
      val key = unAlias(k)
      val i = part.indexWhere(_.semanticEquals(key))
      if (i < 0) {
        // search for any view aliases (SNAP-2204)
        key match {
          case ke: NamedExpression =>
            planExpressions.collectFirst {
              case a: Alias if ke.exprId == a.exprId => unAlias(a.child)
              case e: NamedExpression if (ke ne e) && ke.exprId == e.exprId => e
            } match {
              case Some(e) =>
                val j = part.indexWhere(_.semanticEquals(e))
                if (j < 0) {
                  Int.MaxValue
                } else j
              case None => Int.MaxValue
            }
          case _ => Int.MaxValue
        }
      } else i
    }
    (keyOrder, joinKeySubsetOfPart(keyOrder, partitioning))
  }

  private def joinKeySubsetOfPart(keyOrder: Seq[Int],
      partitioning: Seq[NamedExpression]): Boolean = {
    !keyOrder.forall(_ == Int.MaxValue)
  }
}
