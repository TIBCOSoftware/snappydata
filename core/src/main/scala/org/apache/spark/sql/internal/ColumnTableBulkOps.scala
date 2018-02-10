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

import io.snappydata.Property

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, Join, LogicalPlan, OverwriteOptions, Project}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftAnti}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, LongType}
import org.apache.spark.sql.{AnalysisException, Dataset, SnappySession, SparkSession}

/**
  * Helper object for PutInto operations for column tables.
  * This class takes the logical plans from SnappyParser
  * and converts it into another plan.
  */
object ColumnTableBulkOps {

  val CACHED_PUTINTO_UPDATE_PLAN = "cached_putinto_logical_plan"

  def validateOp(originalPlan: PutIntoTable) {
    originalPlan match {
      case PutIntoTable(LogicalRelation(t: BulkPutRelation, _, _), query) =>
        val srcRelations = query.collect {
          case LogicalRelation(src: BaseRelation, _, _) => src
        }
        if (srcRelations.contains(t)) {
          throw Utils.analysisException(
            "Cannot put into table that is also being read from.")
        } else {
          // OK
        }
      case _ => // OK
    }
  }

  def transformPutPlan(sparkSession: SparkSession, originalPlan: PutIntoTable): LogicalPlan = {
    validateOp(originalPlan)
    val table = originalPlan.table
    val subQuery = originalPlan.child
    var transFormedPlan: LogicalPlan = originalPlan

    table.collectFirst {
      case LogicalRelation(mutable: BulkPutRelation, _, _) =>
        val putKeys = mutable.getPutKeys()
        if (putKeys.isEmpty) {
          throw new AnalysisException(
            s"PutInto in a column table requires key column(s) but got empty string")
        }
        val condition = prepareCondition(sparkSession, table, subQuery, putKeys.get)

        val keyColumns = getKeyColumns(table)
        val updateSubQuery = Join(table, subQuery, Inner, condition)
        val updateColumns = table.output.filterNot(a => keyColumns.contains(a.name))

        val cacheSize = Property.PutIntoInnerJoinCacheSize
            .getOption(sparkSession.sparkContext.conf) match {
          case Some(size) => size.toInt
          case None => Property.PutIntoInnerJoinCacheSize.defaultValue.get
        }

        val doInsertJoin = if (updateSubQuery.statistics.sizeInBytes <= cacheSize) {
          val joinDS = new Dataset(sparkSession,
            updateSubQuery, RowEncoder(updateSubQuery.schema))
          joinDS.cache()
          sparkSession.asInstanceOf[SnappySession].
              addContextObject(CACHED_PUTINTO_UPDATE_PLAN, updateSubQuery)
          joinDS.count() > 0
        } else true

        val insertChild = if (doInsertJoin) {
          Join(subQuery, updateSubQuery, LeftAnti, condition)
        } else subQuery

        val insertPlan = new Insert(table, Map.empty[String,
            Option[String]], Project(subQuery.output, insertChild),
          OverwriteOptions(enabled = false), ifNotExists = false)

        val updateExpressions = insertChild.output.filterNot(a => keyColumns.contains(a.name))
        val updatePlan = Update(table, updateSubQuery, Nil,
          updateColumns, updateExpressions)

        transFormedPlan = PutIntoColumnTable(table, insertPlan, updatePlan)
      case _ => // Do nothing, original putInto plan is enough
    }
    transFormedPlan
  }

  private def prepareCondition(sparkSession: SparkSession,
      table: LogicalPlan,
      child: LogicalPlan,
      columnNames: Seq[String]): Option[Expression] = {
    val analyzer = sparkSession.sessionState.analyzer
    val leftKeys = columnNames.map { keyName =>
      table.output.find(attr => analyzer.resolver(attr.name, keyName)).getOrElse {
        throw new AnalysisException(s"key column `$keyName` cannot be resolved on the left " +
            s"side of the operation. The left-side columns: [${
              table.
                  output.map(_.name).mkString(", ")
            }]")
      }
    }
    val rightKeys = columnNames.map { keyName =>
      child.output.find(attr => analyzer.resolver(attr.name, keyName)).getOrElse {
        throw new AnalysisException(s"USING column `$keyName` cannot be resolved on the right " +
            s"side of the operation. The right-side columns: [${
              child.
                  output.map(_.name).mkString(", ")
            }]")
      }
    }
    val joinPairs = leftKeys.zip(rightKeys)
    val newCondition = joinPairs.map(EqualTo.tupled).reduceOption(And)
    newCondition
  }

  def getKeyColumns(table: LogicalPlan): Seq[String] = {
    table.collectFirst {
      case LogicalRelation(mutable: MutableRelation, _, _) => mutable.getKeyColumns
    }.getOrElse(throw new AnalysisException(
      s"Update/Delete requires a MutableRelation but got $table"))

  }

  def transformDeletePlan(sparkSession: SparkSession,
      originalPlan: DeleteFromTable): LogicalPlan = {
    val table = originalPlan.table
    val subQuery = originalPlan.child
    var transFormedPlan: LogicalPlan = originalPlan

    table.collectFirst {
      case LogicalRelation(mutable: BulkPutRelation, _, _) =>
        val putKeys = mutable.getPutKeys()
        if (putKeys.isEmpty) {
          throw new AnalysisException(
            s"DeleteFrom in a column table requires key column(s) but got empty string")
        }
        val condition = prepareCondition(sparkSession, table, subQuery, putKeys.get)
        val exists = Join(subQuery, table, Inner, condition)
        transFormedPlan = Delete(table, exists, Nil)
      case _ => // Do nothing, original DeleteFromTable plan is enough
    }
    transFormedPlan
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
