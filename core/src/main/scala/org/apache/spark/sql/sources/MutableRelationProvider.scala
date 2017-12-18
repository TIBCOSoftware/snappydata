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
package org.apache.spark.sql.sources

import java.util.Properties

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCPartitioningInfo, JDBCRelation}
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.row.JDBCMutableRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SaveMode, SnappySession}

abstract class MutableRelationProvider
    extends ExternalSchemaRelationProvider
        with SchemaRelationProvider
        with RelationProvider
        with CreatableRelationProvider {

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], schema: String,
      data: Option[LogicalPlan]): JDBCMutableRelation = {
    val parameters = new CaseInsensitiveMutableHashMap(options)
    val partitionColumn = parameters.remove("partitioncolumn")
    val lowerBound = parameters.remove("lowerbound")
    val upperBound = parameters.remove("upperbound")
    val numPartitions = parameters.remove("numpartitions")

    val table = ExternalStoreUtils.removeInternalProps(parameters)
    val tableOptions = new CaseInsensitiveMap(parameters.toMap)
    val catalog = sqlContext.sparkSession.asInstanceOf[SnappySession].sessionCatalog
    val qualifiedTableName = catalog.newQualifiedTableName(table)
    val connProperties = ExternalStoreUtils.validateAndGetAllProps(
      Some(sqlContext.sparkSession), parameters)

    val partitionInfo = if (partitionColumn.isEmpty) {
      null
    } else {
      if (lowerBound.isEmpty || upperBound.isEmpty || numPartitions.isEmpty) {
        throw new IllegalArgumentException("JDBCUpdatableRelation: " +
            "incomplete partitioning specified")
      }
      JDBCPartitioningInfo(
        partitionColumn.get,
        lowerBound.get.toLong,
        upperBound.get.toLong,
        numPartitions.get.toInt)
    }
    val parts = JDBCRelation.columnPartition(partitionInfo)
    val tableName = SnappyStoreHiveCatalog.processIdentifier(qualifiedTableName.toString,
      sqlContext.conf)
    var success = false
    val relation = JDBCMutableRelation(connProperties, tableName,
      getClass.getCanonicalName, mode, schema, parts, tableOptions, sqlContext)
    try {
      relation.createTable(mode)

      val catalog = sqlContext.sparkSession.asInstanceOf[SnappySession].sessionCatalog
      data match {
        case Some(plan) =>
          relation.insert(Dataset.ofRows(sqlContext.sparkSession, plan),
            overwrite = false)
        case None =>
      }
      catalog.registerDataSourceTable(
        catalog.newQualifiedTableName(tableName), None, Array.empty[String],
        classOf[org.apache.spark.sql.row.DefaultSource].getCanonicalName,
        tableOptions, Some(relation))
      success = true
      relation
    } finally {
      if (!success && !relation.tableExists) {
        // destroy the relation
        relation.destroy(ifExists = true)
      }
    }
  }

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String], schema: StructType): JDBCMutableRelation = {
    val url = options.getOrElse("url",
      ExternalStoreUtils.defaultStoreURL(Some(sqlContext.sparkContext)))
    val dialect = JdbcDialects.get(url)
    val schemaString = JdbcExtendedUtils.schemaString(schema, dialect)

    val allowExisting = options.get(JdbcExtendedUtils
        .ALLOW_EXISTING_PROPERTY).exists(_.toBoolean)
    val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
    createRelation(sqlContext, mode, options, schemaString, None)
  }

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String]): JDBCMutableRelation = {

    val allowExisting = options.get(JdbcExtendedUtils
        .ALLOW_EXISTING_PROPERTY).exists(_.toBoolean)
    val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
    // will work only if table is already existing
    createRelation(sqlContext, mode, options, "", None)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], data: DataFrame): JDBCMutableRelation = {
    val url = options.getOrElse("url",
      ExternalStoreUtils.defaultStoreURL(Some(sqlContext.sparkContext)))
    val dialect = JdbcDialects.get(url)
    val schemaString = JdbcExtendedUtils.schemaString(data.schema, dialect)
    val relation = createRelation(sqlContext, mode, options, schemaString, None)
    var success = false
    try {
      relation.insert(data, overwrite = false)
      success = true
      relation
    } finally {
      if (!success && !relation.tableExists) {
        val catalog = sqlContext.sparkSession.asInstanceOf[SnappySession].sessionCatalog
        catalog.unregisterDataSourceTable(catalog.newQualifiedTableName(relation.table),
          Some(relation))
        // destroy the relation
        relation.destroy(ifExists = true)
      }
    }
  }
}

// IMPORTANT: if any changes are made to this class then update the
// serialization correspondingly in ConnectionPropertiesSerializer
case class ConnectionProperties(url: String, driver: String,
    dialect: JdbcDialect, poolProps: Map[String, String],
    connProps: Properties, executorConnProps: Properties, hikariCP: Boolean)
