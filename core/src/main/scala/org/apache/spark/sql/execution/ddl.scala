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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Expression, SortDirection}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hive.QualifiedTableName
import org.apache.spark.sql.internal.BypassRowLevelSecurity
import org.apache.spark.sql.types.{StructField, StructType}
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
    Nil
  }
}

private[sql] case class CreateMetastoreTableUsingSelect(
    tableIdent: TableIdentifier,
    baseTable: Option[TableIdentifier],
    userSpecifiedSchema: Option[StructType],
    schemaDDL: Option[String],
    provider: String,
    partitionColumns: Array[String],
    mode: SaveMode,
    options: Map[String, String],
    query: LogicalPlan,
    isBuiltIn: Boolean) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val catalog = snc.sessionState.catalog
    snc.createTable(catalog.newQualifiedTableName(tableIdent), provider,
      userSpecifiedSchema, schemaDDL, partitionColumns, mode,
      snc.addBaseTableOption(baseTable, options), query, isBuiltIn)
    Nil
  }
}

private[sql] case class DropTableOrViewCommand(isView: Boolean, ifExists: Boolean,
    tableIdent: TableIdentifier) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val catalog = snc.sessionState.catalog
    // check for table/view
    val qualifiedName = catalog.newQualifiedTableName(tableIdent)
    if (isView) {
      if (!catalog.isView(qualifiedName) && !catalog.isTemporaryTable(qualifiedName)) {
        throw new AnalysisException(
          s"Cannot drop a table with DROP VIEW. Please use DROP TABLE instead")
      }
    } else if (catalog.isView(qualifiedName)) {
      throw new AnalysisException(
        "Cannot drop a view with DROP TABLE. Please use DROP VIEW instead")
    }
    snc.dropTable(qualifiedName, ifExists, resolveRelation = true)
    Nil
  }
}

private[sql] case class DropPolicyCommand(ifExists: Boolean,
    policyIdentifer: TableIdentifier) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val catalog = snc.sessionState.catalog
    // check for table/view
    val qualifiedName = catalog.newQualifiedTableName(policyIdentifer)

    snc.dropPolicy(qualifiedName, ifExists)
    Nil
  }
}

private[sql] case class TruncateManagedTableCommand(ifExists: Boolean,
    tableIdent: TableIdentifier) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val catalog = snc.sessionState.catalog
    snc.truncateTable(catalog.newQualifiedTableName(tableIdent),
      ifExists, ignoreIfUnsupported = false)
    Nil
  }
}

private[sql] case class AlterTableAddColumnCommand(tableIdent: TableIdentifier,
    addColumn: StructField) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val catalog = snc.sessionState.catalog
    snc.alterTable(catalog.newQualifiedTableName(tableIdent), isAddColumn = true, addColumn)
    Nil
  }
}

private[sql] case class AlterTableToggleRowLevelSecurityCommand(tableIdent: TableIdentifier,
    enableRls: Boolean) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val catalog = snc.sessionState.catalog
    snc.alterTableToggleRLS(catalog.newQualifiedTableName(tableIdent), enableRls)
    Nil
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
            s" doesn't exist in table ${tableIdent.table}")
        case Some(field) => field
      }
    val table = catalog.newQualifiedTableName(tableIdent)
    snc.alterTable(table, isAddColumn = false, structField)
    Nil
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
    Nil
  }
}

private[sql] case class CreatePolicyCommand(policyIdent: QualifiedTableName,
    tableIdent: QualifiedTableName,
    policyFor: String, applyTo: Seq[String], expandedPolicyApplyTo: Seq[String],
    currentUser: String, filterStr: String,
    filter: BypassRowLevelSecurity) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    // TODO: Only allow the owner of the target table to create a policy on it
    val snc = session.asInstanceOf[SnappySession]
    val catalog = snc.sessionState.catalog
    SparkSession.setActiveSession(snc)
    snc.createPolicy(policyIdent, tableIdent, policyFor, applyTo, expandedPolicyApplyTo,
      currentUser, filterStr, filter)
    Nil
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
    Nil
  }
}

private[sql] case class SetSchemaCommand(schemaName: String) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.asInstanceOf[SnappySession].setSchema(schemaName)
    Nil
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
    Nil
  }
}
