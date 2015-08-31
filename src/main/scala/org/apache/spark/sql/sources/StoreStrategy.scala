package org.apache.spark.sql.sources

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.row.JDBCUpdatableRelation
import org.apache.spark.sql.execution.{ExecutedCommand, RunnableCommand, SparkPlan}

/**
 * Support for DML and other operations on external tables.
 *
 * @author rishim
 */
object StoreStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case create: CreateExternalTableUsing =>
      ExecutedCommand(create) :: Nil
    case drop: DropExternalTable =>
      ExecutedCommand(drop) :: Nil
    case DMLExternalTable(name, storeRelation: LogicalRelation, insertCommand) =>
      ExecutedCommand(ExternalTableDMLCmd(storeRelation, insertCommand)) :: Nil
    case _ => Nil
  }
}

private[sql] case class ExternalTableDMLCmd(
    storeRelation: LogicalRelation,
    command: String) extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    storeRelation.relation match {
      case relation: JDBCUpdatableRelation => relation.dmlCommand(command)
      case other => throw new AnalysisException("DML support requires " +
          "JDBCUpdatableRelation but found " + other)
    }
    Seq.empty
  }
}
