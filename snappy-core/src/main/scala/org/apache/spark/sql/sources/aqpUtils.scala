package org.apache.spark.sql.sources

import org.apache.spark.sql.catalyst.expressions.{NamedExpression, Attribute}
import org.apache.spark.sql.catalyst.plans.logical.{UnaryNode => LogicalUnary, LogicalPlan}
import org.apache.spark.sql.execution.QueryExecution

/**
 * Created by ashahid on 11/16/15.
 */
case class SampleTableQuery(child : LogicalPlan, queryExecutor: QueryExecution,
   error: Double, confidence: Double,  errorEstimates: Option[Seq[(NamedExpression, Int)]] ) extends LogicalUnary {
  override def output: Seq[Attribute] = child.output
  override lazy val schema = queryExecutor.executedPlan.schema

}