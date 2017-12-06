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
package org.apache.spark.sql.internal

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, Join, LogicalPlan, OverwriteOptions, Project}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftAnti}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.{Insert, MutableRelation, Update}
import org.apache.spark.sql.types.{DataType, LongType}
import org.apache.spark.sql.{AnalysisException, SparkSession}

/**
  * Helper class for PutInto operations for column tables.
  * This class takes the logical plans from SnappyParser
  * and converts it into another plan.
  */
class PutIntoColumnTableOp(sparkSession: SparkSession) {

  def convertedPlan(table: LogicalPlan, subQuery: LogicalPlan,
      condition: Expression): PutIntoColumnTable = {

    val notExists = Join(subQuery, table, LeftAnti, Some(condition))

    val keyColumns = getKeyColumns(table)
    val insertPlan = new Insert(table, Map.empty[String,
        Option[String]], Project(subQuery.output, notExists),
      OverwriteOptions(false), ifNotExists = false)

    val updateSubQuery = Join(table, subQuery, Inner, Some(condition))
    val updateColumns = table.output.filterNot(a => keyColumns.contains(a.name))
    val updateExpressions = notExists.output.filterNot(a => keyColumns.contains(a.name))
    val updatePlan = Update(table, updateSubQuery, Seq.empty,
      updateColumns, updateExpressions, putAll = true)

    PutIntoColumnTable(table, insertPlan, updatePlan)
  }

  def getKeyColumns(table: LogicalPlan): Seq[String] = {
    table.collectFirst {
      case lr@LogicalRelation(mutable: MutableRelation, _, _) =>
        val ks = mutable.getKeyColumns
        ks
    }.getOrElse(throw new AnalysisException(
      s"Update/Delete requires a MutableRelation but got $table"))

  }
}

case class PutIntoColumnTable(table: LogicalPlan,
    insert: Insert, update: Update) extends BinaryNode {

  override lazy val output: Seq[Attribute] = AttributeReference(
    "insertCount", LongType)() :: AttributeReference(
    "updateCount", LongType)() :: Nil

  override lazy val resolved: Boolean = childrenResolved &&
      update.output.zip(insert.output).forall {
        case (childAttr, tableAttr) =>
          DataType.equalsIgnoreCompatibleNullability(childAttr.dataType,
            tableAttr.dataType)
      }

  override def left: LogicalPlan = update
  override def right: LogicalPlan = insert
}
