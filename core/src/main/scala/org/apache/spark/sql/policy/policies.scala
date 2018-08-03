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
package org.apache.spark.sql.policy

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Expression, In, Literal, Not, Or}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.hive.QualifiedTableName
import org.apache.spark.sql.internal.BypassRowLevelSecurity
import org.apache.spark.unsafe.types.UTF8String

object PolicyProperties {
  val targetTable = "targetTable"
  val filterString = "filter"
  val policyFor = "policyFor"
  val policyApplyTo = "policyApplyTo"
  val expandedPolicyApplyTo = "expandedPolicyApplyTo"
  val policyOwner = "policyOwner" // should be same as table owner
  val rlsConditionString = "row-level-security"
  val rlsConditionStringUtf8 = UTF8String.fromString(rlsConditionString)
  val rlsAppliedCondition = EqualTo(Literal(rlsConditionString), Literal(rlsConditionString))

  def createFilterPlan(filterExpression: Expression, targetTable: QualifiedTableName,
      policyOwner: String, applyTo: Seq[String]): BypassRowLevelSecurity = {
    val userCheckCond = if (applyTo.isEmpty) {
      // apply to all except owner
      EqualTo(CurrentUser(), Literal(policyOwner))
    } else {
      val cond1 = EqualTo(CurrentUser(), Literal(policyOwner))
      val cond2 = Not(In(CurrentUser(), applyTo.map(Literal(_))))
      Or(cond1, cond2)
    }

    BypassRowLevelSecurity(Filter(And(rlsAppliedCondition,
      Or(userCheckCond, filterExpression)),
      UnresolvedRelation(targetTable)))
  }

}
