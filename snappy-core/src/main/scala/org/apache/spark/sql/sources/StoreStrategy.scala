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

import org.apache.spark.sql.{DataFrame, Row, SQLContext, _}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan}
import org.apache.spark.sql.execution.datasources.{CreateTableUsing, CreateTableUsingAsSelect, LogicalRelation}
import org.apache.spark.sql.execution.{ExecutedCommand, RunnableCommand, SparkPlan}
/**
 * Support for DML and other operations on external tables.
 *
 * @author rishim
 */
object StoreStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case CreateTableUsing(tableIdent, userSpecifiedSchema, provider,
    false, opts, allowExisting, _) =>
      ExecutedCommand(CreateExternalTableUsing(tableIdent,
        userSpecifiedSchema, None, provider, allowExisting, opts)) :: Nil

    case CreateTableUsingAsSelect(tableIdent, provider, false,
    partitionCols, mode, opts, query) =>
      ExecutedCommand(CreateExternalTableUsingSelect(
        tableIdent, provider, partitionCols, mode, opts, query)) :: Nil

    case create: CreateExternalTableUsing =>
      ExecutedCommand(create) :: Nil
    case createSelect: CreateExternalTableUsingSelect =>
      ExecutedCommand(createSelect) :: Nil
    case drop: DropTable =>
      ExecutedCommand(drop) :: Nil

    case DMLExternalTable(name, storeRelation: LogicalRelation, insertCommand) =>
      ExecutedCommand(ExternalTableDMLCmd(storeRelation, insertCommand)) :: Nil

    case InsertIntoTable(l @ LogicalRelation(t: RowPutRelation, _),
    part, query, overwrite, false) if part.isEmpty =>
      ExecutedCommand(PutIntoDataSource(l, query, overwrite)) :: Nil

    case _ => Nil
  }
}

private[sql] case class ExternalTableDMLCmd(
    storeRelation: LogicalRelation,
    command: String) extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    storeRelation.relation match {
      case relation: UpdatableRelation => relation.executeUpdate(command)
      case relation: RowPutRelation => relation.executeUpdate(command)
      case relation: SingleRowInsertableRelation =>
        relation.executeUpdate(command)
      case other => throw new AnalysisException("DML support requires " +
          "UpdatableRelation/SingleRowInsertableRelation/RowPutRelation" +
          " but found " + other)
    }
    Seq.empty
  }
}

/**
 * Puts the results of `query` in to a relation that extends [[RowPutRelation]].
 */
private[sql] case class PutIntoDataSource(
    logicalRelation: LogicalRelation,
    query: LogicalPlan,
    overwrite: Boolean)
    extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val relation = logicalRelation.relation.asInstanceOf[RowPutRelation]
    val data = DataFrame(sqlContext, query)
    // Apply the schema of the existing table to the new data.
    val df = sqlContext.internalCreateDataFrame(data.queryExecution.toRdd,
      logicalRelation.schema)
    relation.put(df)

    // Invalidate the cache.
    sqlContext.cacheManager.invalidateCache(logicalRelation)

    Seq.empty[Row]
  }
}