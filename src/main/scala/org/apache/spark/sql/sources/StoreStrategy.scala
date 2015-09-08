package org.apache.spark.sql.sources

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{ExecutedCommand, RunnableCommand, SparkPlan}

/**
 * Support for DML and other operations on external tables.
 *
 * @author rishim
 */
object StoreStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case CreateTableUsing(tableName, userSpecifiedSchema, provider,
    false, opts, allowExisting, _) =>
      ExecutedCommand(CreateExternalTableUsing(tableName,
        userSpecifiedSchema, None, provider, allowExisting, opts)) :: Nil

    case CreateTableUsingAsSelect(tableName, provider, false,
    partitionCols, mode, opts, query) =>
      ExecutedCommand(CreateExternalTableUsingSelect(
        tableName, provider, partitionCols, mode, opts, query)) :: Nil

    case create: CreateExternalTableUsing =>
      ExecutedCommand(create) :: Nil
    case createSelect: CreateExternalTableUsingSelect =>
      ExecutedCommand(createSelect) :: Nil
    case drop: DropTable =>
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
      case relation: UpdatableRelation => relation.executeUpdate(command)
      case other => throw new AnalysisException("DML support requires " +
          "UpdatableRelation but found " + other)
    }
    Seq.empty
  }
}
