package org.apache.spark.sql.sources

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{ExecutedCommand, RunnableCommand, SparkPlan}
import org.apache.spark.sql.types.StructType

/**
 * Created by rishim on 17/7/15.

 */

object StoreStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case CreateExternalTableUsing(tableName, userSpecifiedSchema, provider, options) =>
      ExecutedCommand(
        CreateExternalTableCmd(tableName, userSpecifiedSchema, provider, options)) :: Nil
    case _ => Nil
  }
}

private[sql] case class CreateExternalTableCmd(
                                             tableName: String,
                                             userSpecifiedSchema: Option[StructType],
                                             provider: String,
                                             options: Map[String, String]) extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    val snc = SnappyContext(sqlContext.sparkContext)
    val resolved = ResolvedDataSource(
      sqlContext, userSpecifiedSchema, Array.empty[String], provider, options)
    snc.registerExternalTable(
      DataFrame(sqlContext, LogicalRelation(resolved.relation)), tableName)
    Seq.empty
  }
}




