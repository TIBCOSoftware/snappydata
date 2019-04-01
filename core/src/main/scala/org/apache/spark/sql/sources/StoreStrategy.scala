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
package org.apache.spark.sql.sources

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.{ExecutedCommandExec, RunnableCommand}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.PutIntoColumnTable
import org.apache.spark.sql.types.{DataType, LongType}
import org.apache.spark.sql.{Strategy, _}

/**
 * Support for DML and other operations on external tables.
 */
object StoreStrategy extends Strategy with SparkSupport {

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case p: EncoderPlan[_] =>
      val plan = p.asInstanceOf[EncoderPlan[Any]]
      EncoderScanExec(plan.rdd.asInstanceOf[RDD[Any]],
        plan.encoder, plan.isFlat, plan.output) :: Nil

    case insert: InsertIntoTable if insert.partition.isEmpty &&
        !internals.getIfNotExistsOption(insert) && insert.table.isInstanceOf[LogicalRelation] &&
        insert.table.asInstanceOf[LogicalRelation].relation.isInstanceOf[PlanInsertableRelation] =>
      val l = insert.table.asInstanceOf[LogicalRelation]
      val p = l.relation.asInstanceOf[PlanInsertableRelation]
      val preAction = if (internals.getOverwriteOption(insert)) () => p.truncate() else () => ()
      ExecutePlan(p.getInsertPlan(l, planLater(insert.children.head)), preAction) :: Nil

    case d@DMLExternalTable(_, storeRelation: LogicalRelation, insertCommand) =>
      ExecutedCommandExec(ExternalTableDMLCmd(storeRelation, insertCommand, d.output)) :: Nil

    case PutIntoTable(l: LogicalRelation, query) if l.relation.isInstanceOf[RowPutRelation] =>
      ExecutePlan(l.relation.asInstanceOf[RowPutRelation].getPutPlan(l, planLater(query))) :: Nil

    case PutIntoColumnTable(l: LogicalRelation, left, right)
      if l.relation.isInstanceOf[BulkPutRelation] =>
      ExecutePlan(l.relation.asInstanceOf[BulkPutRelation].getPutPlan(planLater(left),
        planLater(right))) :: Nil

    case Update(l: LogicalRelation, child, keyColumns, updateColumns, updateExpressions)
      if l.relation.isInstanceOf[MutableRelation] =>
      ExecutePlan(l.relation.asInstanceOf[MutableRelation].getUpdatePlan(l, planLater(child),
        updateColumns, updateExpressions, keyColumns)) :: Nil

    case Delete(l: LogicalRelation, child, keyCols) if l.relation.isInstanceOf[MutableRelation] =>
      ExecutePlan(l.relation.asInstanceOf[MutableRelation].getDeletePlan(l, planLater(child),
        keyCols)) :: Nil

    case DeleteFromTable(l: LogicalRelation, q) if l.relation.isInstanceOf[DeletableRelation] =>
      ExecutePlan(l.relation.asInstanceOf[DeletableRelation].getDeletePlan(l, planLater(q),
        q.output)) :: Nil

    case r: RunnableCommand => ExecutedCommandExec(r) :: Nil

    case _ => Nil
  }
}

// marker trait to indicate a plan that can mutate a table
trait TableMutationPlan

case class ExternalTableDMLCmd(
    storeRelation: LogicalRelation,
    command: String, childOutput: Seq[Attribute]) extends RunnableCommand with TableMutationPlan {

  override def run(session: SparkSession): Seq[Row] = {
    storeRelation.relation match {
      case relation: SingleRowInsertableRelation =>
        Seq(Row(relation.executeUpdate(command, session.catalog.currentDatabase)))
      case other => throw new AnalysisException("DML support requires " +
          "SingleRowInsertableRelation but found " + other)
    }
  }

  override lazy val output: Seq[Attribute] = childOutput
}

case class PutIntoTable(table: LogicalPlan, child: LogicalPlan)
    extends LogicalPlan with TableMutationPlan {

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

/**
 * Plan for update of a column or row table. The "table" passed should be
 * a resolved one (by parser and other callers) else there is ambiguity
 * in column resolution of updateColumns/expressions between table and child.
 */
case class Update(table: LogicalPlan, child: LogicalPlan,
    keyColumns: Seq[Attribute], updateColumns: Seq[Attribute],
    updateExpressions: Seq[Expression]) extends LogicalPlan with TableMutationPlan {

  assert(updateColumns.length == updateExpressions.length,
    s"Internal error: updateColumns=${updateColumns.length} " +
        s"updateExpressions=${updateExpressions.length}")

  override def children: Seq[LogicalPlan] = table :: child :: Nil

  override lazy val output: Seq[Attribute] = AttributeReference(
    "count", LongType)() :: Nil
}

/**
 * Plan for delete from a column or row table.
 */
case class Delete(table: LogicalPlan, child: LogicalPlan,
    keyColumns: Seq[Attribute]) extends LogicalPlan with TableMutationPlan {

  override def children: Seq[LogicalPlan] = table :: child :: Nil

  override lazy val output: Seq[Attribute] = AttributeReference(
    "count", LongType)() :: Nil

  override lazy val resolved: Boolean = childrenResolved
}

private[sql] case class DeleteFromTable(
    table: LogicalPlan,
    child: LogicalPlan)
    extends LogicalPlan with TableMutationPlan {

  override def children: Seq[LogicalPlan] = table :: child :: Nil

  override def output: Seq[Attribute] = Nil

  override lazy val resolved: Boolean = childrenResolved &&
      child.output.zip(table.output).forall {
        case (childAttr, tableAttr) =>
          DataType.equalsIgnoreCompatibleNullability(childAttr.dataType,
            tableAttr.dataType)
      }
}
