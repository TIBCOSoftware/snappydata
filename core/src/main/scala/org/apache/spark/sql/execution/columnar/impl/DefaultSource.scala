/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package org.apache.spark.sql.execution.columnar.impl

import io.snappydata.Constant
import io.snappydata.sql.catalog.SnappyExternalCatalog

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.sources.{CreatableRelationProvider, DataSourceRegister, ExternalSchemaRelationProvider, JdbcExtendedUtils, SchemaRelationProvider}
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext, SaveMode, SnappyParserConsts, SnappySession}

/**
 * Column tables don't support any extensions over regular Spark schema syntax,
 * but the support for ExternalSchemaRelationProvider has been added as a workaround
 * to allow for specifying schema in a CREATE TABLE AS SELECT statement.
 *
 * Normally Spark does not allow specifying schema in a CTAS statement for DataSources
 * (except its special "hive" provider), so schema is passed here as string
 * which is parsed locally in the CreatableRelationProvider implementation.
 */
final class DefaultSource extends ExternalSchemaRelationProvider with SchemaRelationProvider
    with CreatableRelationProvider with DataSourceRegister with Logging {

  override def shortName(): String = SnappyParserConsts.COLUMN_SOURCE

  private def getSchema(session: SnappySession,
      options: Map[String, String]): Option[StructType] = getSchemaString(options) match {
    case None => None
    case Some(s) =>
      // Parse locally using SnappyParser that supports complex types unlike store parser.
      // Use a new parser because DataSource.resolveRelation can be invoked by parser itself.
      val parser = session.snappyParser.newInstance()
      parser.parseSQLOnly(s, parser.tableSchemaOpt.run()).map(StructType(_))
  }

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String]): BaseColumnFormatRelation = {
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    val schema = getSchema(session, options) match {
      case None => JdbcExtendedUtils.EMPTY_SCHEMA // table may already exist
      case Some(s) => s
    }
    // table has already been checked for existence by callers if required so here mode is Ignore
    createRelation(session, SaveMode.Ignore, options, schema)
  }

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String], schema: StructType): BaseColumnFormatRelation = {
    // table has already been checked for existence by callers if required so here mode is Ignore
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    createRelation(session, SaveMode.Ignore, options, schema)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], data: DataFrame): BaseColumnFormatRelation = {
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    val schema = getSchema(session, options) match {
      case None => data.schema
      case Some(s) => s
    }
    val relation = createRelation(session, mode, options, schema)
    var success = false
    try {
      // Need to create a catalog entry before insert since it will read the catalog
      // on the servers to determine table properties like compression etc.
      // SnappyExternalCatalog will alter the definition for final entry if required.
      session.sessionCatalog.createTableForBuiltin(relation.resolvedName,
        getClass.getCanonicalName, relation.schema, relation.origOptions,
        mode != SaveMode.ErrorIfExists)
      relation.insert(data, mode == SaveMode.Overwrite)
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
      options: Map[String, String], specifiedSchema: StructType): BaseColumnFormatRelation = {

    val parameters = new CaseInsensitiveMutableHashMap(options)
    val fullTableName = ExternalStoreUtils.removeInternalPropsAndGetTable(parameters)

    // don't allow commas in column names since it is used as separator in multiple places
    specifiedSchema.find(_.name.indexOf(',') != -1) match {
      case None =>
      case Some(f) => throw new AnalysisException(
        s"Column '${f.name}' in '$fullTableName' cannot contain comma in its name")
    }

    val partitions = ExternalStoreUtils.getAndSetTotalPartitions(session, parameters,
      forManagedTable = true)

    val parametersForShadowTable = new CaseInsensitiveMutableHashMap(parameters)

    // change the schema to use VARCHAR for StringType for partitioning columns
    // so that the row buffer table can use it as part of primary key
    val (primaryKeyClause, stringPKCols) = StoreUtils.getPrimaryKeyClause(
      parameters, specifiedSchema, session)
    val schema = if (stringPKCols.isEmpty) specifiedSchema
    else {
      StructType(specifiedSchema.map { field =>
        if (stringPKCols.contains(field)) {
          field.copy(metadata = Utils.varcharMetadata(Constant.MAX_VARCHAR_SIZE,
            field.metadata))
        } else field
      })
    }
    val partitioningColumns = StoreUtils.getAndSetPartitioningAndKeyColumns(session,
      schema, parameters)
    val tableOptions = new CaseInsensitiveMap(parameters.toMap)

    val ddlExtension = StoreUtils.ddlExtensionString(parameters,
      isRowTable = false, isShadowTable = false)

    val ddlExtensionForShadowTable = StoreUtils.ddlExtensionString(
      parametersForShadowTable, isRowTable = false, isShadowTable = true)

    val connProperties = ExternalStoreUtils.validateAndGetAllProps(Some(session), parameters)

    StoreUtils.validateConnProps(parameters)

    val schemaString = JdbcExtendedUtils.schemaString(schema,
      connProperties.dialect)
    val schemaExtension = if (schemaString.length > 0) {
      val temp = schemaString.substring(0, schemaString.length - 1).
          concat(s", ${StoreUtils.ROWID_COLUMN_DEFINITION}, $primaryKeyClause )")
      s"$temp $ddlExtension"
    } else {
      s"$schemaString $ddlExtension"
    }

    var success = false
    val externalStore = new JDBCSourceAsColumnarStore(connProperties,
      partitions, fullTableName, schema)

    // create an index relation if it is an index table
    val relation = parameters.get(SnappyExternalCatalog.INDEXED_TABLE) match {
      case Some(baseTable) => new IndexColumnFormatRelation(
        fullTableName,
        getClass.getCanonicalName,
        mode,
        schema,
        schemaExtension,
        ddlExtensionForShadowTable,
        tableOptions,
        externalStore,
        partitioningColumns,
        session.sqlContext,
        baseTable)
      case None => new ColumnFormatRelation(
        fullTableName,
        getClass.getCanonicalName,
        mode,
        schema,
        schemaExtension,
        ddlExtensionForShadowTable,
        tableOptions,
        externalStore,
        partitioningColumns,
        session.sqlContext)
    }
    try {
      logDebug(s"Trying to create table $fullTableName")
      relation.createTable(mode)
      logDebug(s"Successfully created the table ${relation.resolvedName}")
      success = true
      relation
    } finally {
      if (!success && relation.tableCreated) {
        // destroy the relation
        relation.destroy(ifExists = true)
      }
    }
  }
}
