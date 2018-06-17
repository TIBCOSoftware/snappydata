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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, PredicateHelper}
import org.apache.spark.sql.types.BooleanType

/**
 * Copy of Join in same package
 */
case class DeltaInsertFullOuterJoin(
    left: LogicalPlan,
    right: LogicalPlan,
    condition: Option[Expression])
    extends BinaryNode with PredicateHelper {

  override def output: Seq[Attribute] =
    left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))

  override protected def validConstraints: Set[Expression] = Set.empty[Expression]

  def duplicateResolved: Boolean = left.outputSet.intersect(right.outputSet).isEmpty

  // if not a natural join, use `resolvedExceptNatural`.
  override lazy val resolved: Boolean = {
    childrenResolved &&
        expressions.forall(_.resolved) &&
        duplicateResolved &&
        condition.forall(_.dataType == BooleanType)
  }

  override lazy val statistics: Statistics = super.statistics.copy(isBroadcastable = false)
}
