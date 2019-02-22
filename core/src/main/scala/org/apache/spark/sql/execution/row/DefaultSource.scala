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
package org.apache.spark.sql.execution.row

import org.apache.spark.sql._
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.execution.datasources.jdbc.JDBCPartition
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.{CreatableRelationProvider, DataSourceRegister, ExternalSchemaRelationProvider, JdbcExtendedUtils, SchemaRelationProvider}
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Logging, Partition, SparkContext}

final class DefaultSource extends ExternalSchemaRelationProvider with SchemaRelationProvider
    with CreatableRelationProvider with DataSourceRegister with Logging with SparkSupport {

  override def shortName(): String = SnappyParserConsts.ROW_SOURCE

  private def getSchemaString(options: Map[String, String], schema: => StructType,
      context: SparkContext): String = {
    getSchemaString(options) match {
      case Some(s) => s
      case None =>
        val url = options.getOrElse("url", ExternalStoreUtils.defaultStoreURL(Some(context)))
        JdbcExtendedUtils.schemaString(schema, JdbcDialects.get(url))
    }
  }

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String]): RowFormatRelation = {
    val schemaString = getSchemaString(options) match {
      case Some(s) => s
      case None => "" // table may already exist
    }
    // table has already been checked for existence by callers if required so here mode is Ignore
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    createRelation(session, SaveMode.Ignore, options, schemaString)
  }

  override def createRelation(sqlContext: SQLContext, options: Map[String, String],
      schema: StructType): RowFormatRelation = {
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    val schemaString = getSchemaString(options, schema, session.sparkContext)
    // table has already been checked for existence by callers if required so here mode is Ignore
    createRelation(session, SaveMode.Ignore, options, schemaString)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], data: DataFrame): RowFormatRelation = {
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    val schemaString = getSchemaString(options, session.sessionCatalog.normalizeSchema(
      data.schema), sqlContext.sparkContext)
    val relation = createRelation(session, mode, options, schemaString)
    var success = false
    try {
      // Need to create a catalog entry before insert since it will read the catalog
      // on the servers to determine table properties like compression etc.
      // SnappyExternalCatalog will alter the definition for final entry if required.
      session.sessionCatalog.createTableForBuiltin(relation.resolvedName,
        getClass.getCanonicalName, relation.schema, relation.origOptions,
        mode != SaveMode.ErrorIfExists)
      // SaveMode.Overwrite already taken care by createTable to truncate
      relation.insert(data, overwrite = false)
      success = true
      relation
    } finally {
      if (!success && relation.tableCreated) {
        // remove the catalog entry
        session.sessionCatalog.externalCatalog.dropTable(relation.schemaName,
          relation.tableName, ignoreIfNotExists = true, purge = false)
        // destroy the relation
        relation.destroy(ifExists = true)
      }
    }
  }

  private[sql] def createRelation(session: SnappySession, mode: SaveMode,
      options: Map[String, String], schemaString: String): RowFormatRelation = {

    val parameters = new CaseInsensitiveMutableHashMap(options)
    val fullTableName = ExternalStoreUtils.removeInternalProps(parameters)
    ExternalStoreUtils.getAndSetTotalPartitions(session, parameters,
      forManagedTable = true, forColumnTable = false)
    StoreUtils.getAndSetPartitioningAndKeyColumns(session, schema = null, parameters)
    val tableOptions = internals.newCaseInsensitiveMap(parameters.toMap)
    val ddlExtension = StoreUtils.ddlExtensionString(parameters,
      isRowTable = true, isShadowTable = false)
    val schemaExtension = s"$schemaString $ddlExtension"
    val connProperties = ExternalStoreUtils.validateAndGetAllProps(
      Some(session), parameters)

    StoreUtils.validateConnProps(parameters)
    var success = false
    val relation = new RowFormatRelation(connProperties,
      fullTableName,
      mode,
      schemaExtension,
      Array[Partition](JDBCPartition(null, 0)),
      tableOptions,
      session.sqlContext)
    try {
      logDebug(s"Trying to create table $fullTableName")
      relation.createTable(mode)
      logDebug(s"Successfully created the table ${relation.resolvedName}")
      success = true
      relation
    } finally {
      if (!success && relation.tableCreated) {
        // destroy the relation
        logDebug(s"Failed in creating the table $fullTableName hence destroying")
        relation.destroy(ifExists = true)
      }
    }
  }
}
