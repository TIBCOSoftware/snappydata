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
package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.SparkPlan

/**
 * Extension to Spark's SortMergeJoinExec to avoid exchange for cases when join keys are
 * a subset of child plan partitioning.
 */
class SnappySortMergeJoinExec(_leftKeys: Seq[Expression], _rightKeys: Seq[Expression],
    _joinType: JoinType, _condition: Option[Expression], _left: SparkPlan, _right: SparkPlan,
    override val leftSizeInBytes: BigInt, override val rightSizeInBytes: BigInt)
    extends SortMergeJoinExec(_leftKeys, _rightKeys, _joinType, _condition, _left, _right)
        with SnappyJoinLike {

  override def productArity: Int = 8

  override def productElement(n: Int): Any = n match {
    case 6 => leftSizeInBytes
    case 7 => rightSizeInBytes
    case _ => super.productElement(n)
  }

  override def copy(leftKeys: Seq[Expression] = leftKeys, rightKeys: Seq[Expression] = rightKeys,
      joinType: JoinType = joinType, condition: Option[Expression] = condition,
      left: SparkPlan = left, right: SparkPlan = right): SnappySortMergeJoinExec = {
    new SnappySortMergeJoinExec(leftKeys, rightKeys, joinType, condition, left, right,
      leftSizeInBytes, rightSizeInBytes)
  }

  def copy(leftKeys: Seq[Expression], rightKeys: Seq[Expression], joinType: JoinType,
      condition: Option[Expression], left: SparkPlan, right: SparkPlan,
      leftSizeInBytes: BigInt, rightSizeInBytes: BigInt): SnappySortMergeJoinExec = {
    new SnappySortMergeJoinExec(leftKeys, rightKeys, joinType, condition, left, right,
      leftSizeInBytes, rightSizeInBytes)
  }
}
