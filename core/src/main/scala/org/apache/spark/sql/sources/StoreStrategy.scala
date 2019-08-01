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

import scala.reflect.{ClassTag, classTag}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, OverwriteOptions}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.{ExecutedCommandExec, RunnableCommand}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.PutIntoColumnTable
import org.apache.spark.sql.types.{DataType, LongType}

/**
 * Support for DML and other operations on external tables.
 */
object StoreStrategy extends Strategy {

  private def findLogicalRelation[T: ClassTag](table: LogicalPlan): Option[LogicalRelation] = {
    table.find(_.isInstanceOf[LogicalRelation]) match {
      case s@Some(lr) if classTag[T].runtimeClass.isInstance(
        lr.asInstanceOf[LogicalRelation].relation) => s.asInstanceOf[Option[LogicalRelation]]
      case _ => None
    }
  }

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case p: EncoderPlan[_] =>
      val plan = p.asInstanceOf[EncoderPlan[Any]]
      EncoderScanExec(plan.rdd.asInstanceOf[RDD[Any]],
        plan.encoder, plan.isFlat, plan.output) :: Nil

    case InsertIntoTable(l@LogicalRelation(p: PlanInsertableRelation,
    _, _), part, query, overwrite, false) if part.isEmpty =>
      val preAction = if (overwrite.enabled) () => p.truncate() else () => ()
      ExecutePlan(p.getInsertPlan(l, planLater(query)), preAction) :: Nil

    case d@DMLExternalTable(table, cmd) => findLogicalRelation[BaseRelation](table) match {
      case Some(l) => ExecutedCommandExec(ExternalTableDMLCmd(l, cmd, d.output)) :: Nil
      case _ => Nil
    }

    case PutIntoTable(table, query) => findLogicalRelation[RowPutRelation](table) match {
      case Some(l) => ExecutePlan(l.relation.asInstanceOf[RowPutRelation].getPutPlan(
        l, planLater(query))) :: Nil
      case _ => Nil
    }

    case PutIntoColumnTable(t, left, right) => findLogicalRelation[BulkPutRelation](t) match {
      case Some(l) => ExecutePlan(l.relation.asInstanceOf[BulkPutRelation].getPutPlan(
        planLater(left), planLater(right))) :: Nil
      case _ => Nil
    }

    case Update(table, child, keyColumns, updateColumns, updateExpressions) =>
      findLogicalRelation[MutableRelation](table) match {
        case Some(l) => ExecutePlan(l.relation.asInstanceOf[MutableRelation].getUpdatePlan(
          l, planLater(child), updateColumns, updateExpressions, keyColumns)) :: Nil
        case _ => Nil
      }

    case Delete(table, child, keyColumns) => findLogicalRelation[MutableRelation](table) match {
      case Some(l) => ExecutePlan(l.relation.asInstanceOf[MutableRelation].getDeletePlan(
        l, planLater(child), keyColumns)) :: Nil
      case _ => Nil
    }

    case DeleteFromTable(table, query) => findLogicalRelation[DeletableRelation](table) match {
      case Some(l) => ExecutePlan(l.relation.asInstanceOf[DeletableRelation].getDeletePlan(
        l, planLater(query), query.output)) :: Nil
      case _ => Nil
    }

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
        Seq(Row(relation.executeUpdate(command,
          JdbcExtendedUtils.toUpperCase(session.catalog.currentDatabase))))
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
 * Unlike Spark's InsertIntoTable this plan provides the count of rows
 * inserted as the output.
 */
final class Insert(
    table: LogicalPlan,
    partition: Map[String, Option[String]],
    child: LogicalPlan,
    overwrite: OverwriteOptions,
    ifNotExists: Boolean)
    extends InsertIntoTable(table, partition, child, overwrite, ifNotExists) {

  override def output: Seq[Attribute] = AttributeReference(
    "count", LongType)() :: Nil

  override def copy(table: LogicalPlan = table,
      partition: Map[String, Option[String]] = partition,
      child: LogicalPlan = child,
      overwrite: OverwriteOptions = overwrite,
      ifNotExists: Boolean = ifNotExists): Insert = {
    new Insert(table, partition, child, overwrite, ifNotExists)
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
