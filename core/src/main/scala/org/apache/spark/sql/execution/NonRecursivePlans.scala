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
package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSupport
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.internal.CodeGenerationException

/**
 * Base class for SparkPlan implementations that have only the code-generated
 * version and use the same for non-codegenerated case. For that case this
 * prevents recursive calls into code generation in case it fails for some reason.
 */
trait NonRecursivePlans extends SparkPlan with SparkSupport {

  /**
   * Variable to disallow recursive generation so will mark the case of
   * non-codegenerated case and throw back exception to use CodegenSparkFallback.
   */
  protected final var nonCodeGeneratedPlanCalls: Int = _

  override protected def doExecute(): RDD[InternalRow] = {
    if (nonCodeGeneratedPlanCalls > 4) {
      throw new CodeGenerationException("Code generation failed for some of the child plans")
    }
    nonCodeGeneratedPlanCalls += 1
    internals.newWholeStagePlan(this).execute()
  }

  override def makeCopy(newArgs: Array[AnyRef]): NonRecursivePlans = {
    val plan = super.makeCopy(newArgs).asInstanceOf[NonRecursivePlans]
    plan.nonCodeGeneratedPlanCalls = nonCodeGeneratedPlanCalls
    plan
  }
}
