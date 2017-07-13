/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql.sources

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.backwardcomp.{ExecuteCommand, ExecutedCommand}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.{CreateTable, LogicalRelation}
import org.apache.spark.sql.execution.{EncoderPlan, EncoderScanExec, ExecutePlan, SparkPlan}
import org.apache.spark.sql.types.{DataType, LongType}

/**
 * Support for DML and other operations on external tables.
 */
object StoreStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case CreateTable(tableDesc, mode, None) =>
      val userSpecifiedSchema = SparkSession.getActiveSession.get
        .asInstanceOf[SnappySession].normalizeSchema(tableDesc.schema)
      val options = Map.empty[String, String] ++ tableDesc.storage.properties
      val cmd =
        CreateMetastoreTableUsing(tableDesc.identifier, None, Some(userSpecifiedSchema),
          None, SnappyContext.getProvider(tableDesc.provider.get, false), false,
          options, false)
      ExecutedCommand(cmd) :: Nil

    case CreateTable(tableDesc, mode, Some(query)) =>
      val userSpecifiedSchema = SparkSession.getActiveSession.get
        .asInstanceOf[SnappySession].normalizeSchema(query.schema)
      val options = Map.empty[String, String] ++ tableDesc.storage.properties
      val cmd =
        CreateMetastoreTableUsingSelect(tableDesc.identifier, None, Some(userSpecifiedSchema), None,
          SnappyContext.getProvider(tableDesc.provider.get, onlyBuiltIn = false),
          temporary = false, tableDesc.partitionColumnNames.toArray, mode,
          options, query, isBuiltIn = false)
      ExecutedCommand(cmd) :: Nil
    case create: CreateMetastoreTableUsing =>
      ExecutedCommand(create) :: Nil
    case createSelect: CreateMetastoreTableUsingSelect =>
      ExecutedCommand(createSelect) :: Nil
    case drop: DropTable =>
      ExecutedCommand(drop) :: Nil

    case p: EncoderPlan[_] =>
      val plan = p.asInstanceOf[EncoderPlan[Any]]
      EncoderScanExec(plan.rdd.asInstanceOf[RDD[Any]],
        plan.encoder, plan.isFlat, plan.output) :: Nil

    case logical.InsertIntoTable(l@LogicalRelation(p: PlanInsertableRelation,
    _, _), part, query, overwrite, false) if part.isEmpty =>
      val preAction = if (overwrite.enabled) () => p.truncate() else () => ()
      ExecutePlan(p.getInsertPlan(l, planLater(query)), preAction) :: Nil

    case DMLExternalTable(_, storeRelation: LogicalRelation, insertCommand) =>
      ExecutedCommand(ExternalTableDMLCmd(storeRelation, insertCommand)) :: Nil

    case PutIntoTable(l@LogicalRelation(p: RowPutRelation, _, _), query) =>
      ExecutePlan(p.getPutPlan(l, planLater(query))) :: Nil

    case Update(l@LogicalRelation(m: MutableRelation, _, _), child,
    keyColumns, updateColumns, updateExpressions) =>
      ExecutePlan(m.getUpdatePlan(l, planLater(child), updateColumns,
        updateExpressions, keyColumns)) :: Nil

    case Delete(l@LogicalRelation(m: MutableRelation, _, _), child,
    keyColumns) =>
      ExecutePlan(m.getDeletePlan(l, planLater(child), keyColumns)) :: Nil

    case DeleteFromTable(l@LogicalRelation(p: DeletableRelation, _, _), query) =>
      ExecutePlan(p.getDeletePlan(l, planLater(query))) :: Nil

    case r: ExecuteCommand => ExecutedCommand(r) :: Nil

    case _ => Nil
  }
}

case class ExternalTableDMLCmd(
    storeRelation: LogicalRelation,
    command: String) extends ExecuteCommand {

  override def run(session: SparkSession): Seq[Row] = {
    storeRelation.relation match {
      case relation: SingleRowInsertableRelation =>
        relation.executeUpdate(command)
      case other => throw new AnalysisException("DML support requires " +
          "SingleRowInsertableRelation but found " + other)
    }
    Seq.empty[Row]
  }
}

case class PutIntoTable(table: LogicalPlan, child: LogicalPlan)
    extends LogicalPlan {

  override def children: Seq[LogicalPlan] = table :: child :: Nil

  override lazy val output: Seq[Attribute] = AttributeReference(
    "count", LongType)() :: Nil

  override lazy val resolved: Boolean = childrenResolved &&
      child.output.zip(table.output).forall {
        case (childAttr, tableAttr) =>
          DataType.equalsIgnoreCompatibleNullability(childAttr.dataType,
            tableAttr.dataType)
      }
}

case class Update(table: LogicalPlan, child: LogicalPlan,
    keyColumns: Seq[Attribute], updateColumns: Seq[Attribute],
    updateExpressions: Seq[Expression]) extends LogicalPlan {

  assert(updateColumns.length == updateExpressions.length,
    s"Internal error: updateColumns=${updateColumns.length} " +
        s"updateExpressions=${updateExpressions.length}")

  override def children: Seq[LogicalPlan] = table :: child :: Nil

  override lazy val output: Seq[Attribute] = AttributeReference(
    "count", LongType)() :: Nil
}

case class Delete(table: LogicalPlan, child: LogicalPlan,
    keyColumns: Seq[Attribute]) extends LogicalPlan {

  override def children: Seq[LogicalPlan] = table :: child :: Nil

  override lazy val output: Seq[Attribute] = AttributeReference(
    "count", LongType)() :: Nil

  override lazy val resolved: Boolean = childrenResolved
}

private[sql] case class DeleteFromTable(
    table: LogicalPlan,
    child: LogicalPlan)
    extends LogicalPlan {

  override def children: Seq[LogicalPlan] = table :: child :: Nil

  override def output: Seq[Attribute] = Seq.empty

  override lazy val resolved: Boolean = childrenResolved &&
      child.output.zip(table.output).forall {
        case (childAttr, tableAttr) =>
          DataType.equalsIgnoreCompatibleNullability(childAttr.dataType,
            tableAttr.dataType)
      }
}
