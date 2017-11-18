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

package org.apache.spark.sql.execution

import com.gemstone.gemfire.SystemFailure

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.internal.CodeGenerationException

/**
 * Catch exceptions in code generation of SnappyData plans and fallback
 * to Spark plans as last resort (including non-code generated paths).
 */
case class CodegenSparkFallback(var child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  private def executeWithFallback[T](f: SparkPlan => T, plan: SparkPlan): T = {
    try {
      val pool = plan.sqlContext.sparkSession.asInstanceOf[SnappySession].
        sessionState.conf.activeSchedulerPool
      sparkContext.setLocalProperty("spark.scheduler.pool", pool)
      f(plan)
    } catch {
      case t: Throwable =>
        var useFallback = false
        t match {
          case e: Error =>
            if (SystemFailure.isJVMFailureError(e)) {
              SystemFailure.initiateFailure(e)
              // If this ever returns, rethrow the error. We're poisoned
              // now, so don't let this thread continue.
              throw e
            }
            // assume all other errors will be some stack/assertion failures
            // or similar compilation issues in janino for very large code
            useFallback = true
          case _ =>
            // search for any janino or code generation exception
            var cause = t
            do {
              if (cause.isInstanceOf[CodeGenerationException] ||
                  cause.toString.contains("janino")) {
                useFallback = true
              }
              cause = cause.getCause
            } while ((cause ne null) && !useFallback)
        }
        // Whenever you catch Error or Throwable, you must also
        // check for fatal JVM error (see above).  However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure()

        if (!useFallback) throw t

        // fallback to Spark plan
        val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
        session.getContextObject[() => QueryExecution](SnappySession.ExecutionKey) match {
          case Some(exec) =>
            logInfo("SnappyData code generation failed. Falling back to Spark plans.")
            session.sessionState.disableStoreOptimizations = true
            try {
              val plan = exec().executedPlan
              val result = f(plan)
              // update child for future executions
              child = plan
              result
            } finally {
              session.sessionState.disableStoreOptimizations = false
            }
          case None => throw t
        }
    }
  }

  override protected def doExecute(): RDD[InternalRow] =
    executeWithFallback(_.execute(), child)

  override def executeCollect(): Array[InternalRow] =
    executeWithFallback(_.executeCollect(), child)

  override def executeToIterator(): Iterator[InternalRow] =
    executeWithFallback(_.executeToIterator(), child)

  override def executeTake(n: Int): Array[InternalRow] =
    executeWithFallback(_.executeTake(n), child)

  def execute(plan: SparkPlan): RDD[InternalRow] =
    executeWithFallback(_.execute(), plan)

  override def generateTreeString(depth: Int, lastChildren: Seq[Boolean],
      builder: StringBuilder, verbose: Boolean, prefix: String): StringBuilder =
    child.generateTreeString(depth, lastChildren, builder, verbose, prefix)

  // override def children: Seq[SparkPlan] = child.children

  // override private[sql] def metrics = child.metrics

  // override private[sql] def metadata = child.metadata

  // override def subqueries: Seq[SparkPlan] = child.subqueries

  override def nodeName: String = "CollectResults"

  override def simpleString: String = "CollectResults"
}
