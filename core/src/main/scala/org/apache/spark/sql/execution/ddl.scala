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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.SortDirection
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.streaming.{Duration, SnappyStreamingContext}


private[sql] case class CreateMetastoreTableUsing(
    tableIdent: TableIdentifier,
    baseTable: Option[TableIdentifier],
    userSpecifiedSchema: Option[StructType],
    schemaDDL: Option[String],
    provider: String,
    allowExisting: Boolean,
    options: Map[String, String],
    isBuiltIn: Boolean) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
    snc.createTable(snc.sessionState.catalog
        .newQualifiedTableName(tableIdent), provider, userSpecifiedSchema,
      schemaDDL, mode, snc.addBaseTableOption(baseTable, options), isBuiltIn)
    Seq.empty
  }
}

private[sql] case class CreateMetastoreTableUsingSelect(
    tableIdent: TableIdentifier,
    baseTable: Option[TableIdentifier],
    userSpecifiedSchema: Option[StructType],
    schemaDDL: Option[String],
    provider: String,
    temporary: Boolean,
    partitionColumns: Array[String],
    mode: SaveMode,
    options: Map[String, String],
    query: LogicalPlan,
    isBuiltIn: Boolean) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val catalog = snc.sessionState.catalog
    if (temporary) {
      // the equivalent of a registerTempTable of a DataFrame
      if (tableIdent.database.isDefined) {
        throw Utils.analysisException(
          s"Temporary table '$tableIdent' should not have specified a database")
      }
      Dataset.ofRows(session, query).createTempView(tableIdent.table)
    } else {
      snc.createTable(catalog.newQualifiedTableName(tableIdent), provider,
        userSpecifiedSchema, schemaDDL, partitionColumns, mode,
        snc.addBaseTableOption(baseTable, options), query, isBuiltIn)
    }
    Seq.empty
  }
}

private[sql] case class DropTableCommand(ifExists: Boolean,
    tableIdent: TableIdentifier) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val catalog = snc.sessionState.catalog
    snc.dropTable(catalog.newQualifiedTableName(tableIdent), ifExists, resolveRelation = false)
    Seq.empty
  }
}

private[sql] case class TruncateTableCommand(ifExists: Boolean,
    tableIdent: TableIdentifier) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val catalog = snc.sessionState.catalog
    snc.truncateTable(catalog.newQualifiedTableName(tableIdent),
      ifExists, ignoreIfUnsupported = false)
    Seq.empty
  }
}

private[sql] case class AlterTableAddColumnCommand(tableIdent: TableIdentifier,
    addColumn: StructField) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val catalog = snc.sessionState.catalog
    snc.alterTable(catalog.newQualifiedTableName(tableIdent), isAddColumn = true, addColumn)
    Seq.empty
  }
}

private[sql] case class AlterTableDropColumnCommand(
    tableIdent: TableIdentifier, column: String) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val catalog = snc.sessionState.catalog
    val plan = try {
      snc.sessionCatalog.lookupRelation(tableIdent)
    } catch {
      case tnfe: TableNotFoundException =>
        throw tnfe
    }
    val structField: StructField =
      plan.schema.find(_.name.equalsIgnoreCase(column)) match {
        case None => throw Utils.analysisException(s"$column column" +
            s" doesn't exists in table ${tableIdent.table}")
        case Some(field) => field
      }
    val table = catalog.newQualifiedTableName(tableIdent)
    snc.alterTable(table, isAddColumn = false, structField)
    Seq.empty
  }
}

private[sql] case class CreateIndexCommand(indexName: TableIdentifier,
    baseTable: TableIdentifier,
    indexColumns: Map[String, Option[SortDirection]],
    options: Map[String, String]) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val catalog = snc.sessionState.catalog
    val tableIdent = catalog.newQualifiedTableName(baseTable)
    val indexIdent = catalog.newQualifiedTableName(indexName)
    snc.createIndex(indexIdent, tableIdent, indexColumns, options)
    Seq.empty
  }
}

private[sql] case class DropIndexCommand(
    indexName: TableIdentifier,
    ifExists: Boolean) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val catalog = snc.sessionState.catalog
    val indexIdent = catalog.newQualifiedTableName(indexName)
    snc.dropIndex(indexIdent, ifExists)
    Seq.empty
  }
}

private[sql] case class SetSchemaCommand(schemaName: String) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.asInstanceOf[SnappySession].setSchema(schemaName)
    Seq.empty[Row]
  }
}

private[sql] case class SnappyStreamingActionsCommand(action: Int,
    batchInterval: Option[Duration]) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {

    def creatingFunc(): SnappyStreamingContext = {
      // batchInterval will always be defined when action == 0
      new SnappyStreamingContext(session.sparkContext, batchInterval.get)
    }

    action match {
      case 0 =>
        val ssc = SnappyStreamingContext.getInstance()
        ssc match {
          case Some(_) => // TODO .We should create a named Streaming
          // Context and check if the configurations match
          case None => SnappyStreamingContext.getActiveOrCreate(creatingFunc)
        }
      case 1 =>
        val ssc = SnappyStreamingContext.getInstance()
        ssc match {
          case Some(x) => x.start()
          case None => throw Utils.analysisException(
            "Streaming Context has not been initialized")
        }
      case 2 =>
        val ssc = SnappyStreamingContext.getActive
        ssc match {
          case Some(strCtx) => strCtx.stop(stopSparkContext = false,
            stopGracefully = true)
          case None => // throw Utils.analysisException(
          // "There is no running Streaming Context to be stopped")
        }
    }
    Seq.empty[Row]
  }
}
