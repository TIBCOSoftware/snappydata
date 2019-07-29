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

package org.apache.spark.sql.execution

import com.gemstone.gemfire.SystemFailure
import org.codehaus.commons.compiler.CompileException

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.CodeGenerationException
import org.apache.spark.sql.{CachedDataFrame, SnappySession}

/**
 * Catch exceptions in code generation of SnappyData plans and fallback
 * to Spark plans as last resort (including non-code generated paths).
 */
case class CodegenSparkFallback(var child: SparkPlan,
    @transient session: SnappySession) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  @transient private[this] val execution =
    session.getContextObject[() => QueryExecution](SnappySession.ExecutionKey)

  protected[sql] def isCodeGenerationException(t: Throwable): Boolean = {
    // search for any janino or code generation exception
    var cause = t
    do {
      cause match {
        // assume all stack overflow failures are due to compilation issues in janino
        // for very large code
        case _: StackOverflowError | _: CodeGenerationException | _: CompileException =>
          return true
        case e: Error =>
          if (SystemFailure.isJVMFailureError(e)) {
            SystemFailure.initiateFailure(e)
            // If this ever returns, rethrow the error. We're poisoned
            // now, so don't let this thread continue.
            throw e
          }
        case _ if cause.toString.contains("janino") => return true
        case _ =>
      }
      cause = cause.getCause
    } while (cause ne null)
    false
  }

  private def executeWithFallback[T](f: SparkPlan => T, plan: SparkPlan): T = {
    try {
      f(plan)
    } catch {
      case t: Throwable =>
        // Whenever you catch Error or Throwable, you must also
        // check for fatal JVM error (see above).  However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure()

        val isCatalogStale = CachedDataFrame.isConnectorCatalogStaleException(t, session)
        if (isCatalogStale) {
          handleStaleCatalogException(f, plan, t)
        } else if (isCodeGenerationException(t)) {
          // fallback to Spark plan for code-generation exception
          execution match {
            case Some(exec) =>
              if (!isCatalogStale) {
                val msg = new StringBuilder
                var cause = t
                while (cause ne null) {
                  if (msg.nonEmpty) msg.append(" => ")
                  msg.append(cause)
                  cause = cause.getCause
                }
                logInfo(s"SnappyData code generation failed due to $msg." +
                    s" Falling back to Spark plans.")
                session.sessionState.disableStoreOptimizations = true
              }
              try {
                val plan = exec().executedPlan.transform {
                  case CodegenSparkFallback(p, _) => p
                }
                val result = f(plan)
                // update child for future executions
                child = plan
                result
              } catch {
                case t: Throwable if CachedDataFrame.isConnectorCatalogStaleException(t, session) =>
                  session.externalCatalog.invalidateAll()
                  SnappySession.clearAllCache()
                  throw CachedDataFrame.catalogStaleFailure(t, session)
              } finally {
                session.sessionState.disableStoreOptimizations = false
              }
            case _ => throw t
          }
        } else {
          throw t
        }
    }
  }

  private def handleStaleCatalogException[T](f: SparkPlan => T, plan: SparkPlan, t: Throwable) = {
    session.externalCatalog.invalidateAll()
    SnappySession.clearAllCache()
    // fail immediate for insert/update/delete, else retry entire query
    val action = plan.find {
      case _: ExecutePlan | _: ExecutedCommandExec => true
      case _ => false
    }
    if (action.isDefined) throw CachedDataFrame.catalogStaleFailure(t, session)

    execution match {
      case Some(exec) =>
        CachedDataFrame.retryOnStaleCatalogException(snappySession = session) {
          val plan = exec().executedPlan
          // update child for future executions
          child = plan
          f(plan)
        }
      case _ => throw t
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

  // override private[sql] def metadata = child.metadata

  // override def subqueries: Seq[SparkPlan] = child.subqueries

  override def nodeName: String = "CollectResults"

  override def simpleString: String = "CollectResults"

  override def longMetric(name: String): SQLMetric = child match {
    case w: WholeStageCodegenExec => w.child.longMetric(name)
    case o => o.longMetric(name)
  }
}
