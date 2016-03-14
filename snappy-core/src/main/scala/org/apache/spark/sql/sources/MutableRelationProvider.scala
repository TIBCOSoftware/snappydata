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

import scala.collection.mutable

import org.apache.spark.sql.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCRelation, JDBCPartitioningInfo}
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.row.JDBCMutableRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}


abstract class MutableRelationProvider
    extends ExternalSchemaRelationProvider
    with SchemaRelationProvider
    with RelationProvider
    with CreatableRelationProvider {

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], schema: String): JDBCMutableRelation = {
    val parameters = new mutable.HashMap[String, String]
    parameters ++= options
    val partitionColumn = parameters.remove("partitioncolumn")
    val lowerBound = parameters.remove("lowerbound")
    val upperBound = parameters.remove("upperbound")
    val numPartitions = parameters.remove("numpartitions")

    val table = ExternalStoreUtils.removeInternalProps(parameters)
    val sc = sqlContext.sparkContext
    val connProperties =
      ExternalStoreUtils.validateAndGetAllProps(sc, parameters)

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

    var success = false
    val relation = new JDBCMutableRelation(connProperties.url,
      SnappyStoreHiveCatalog.processTableIdentifier(table, sqlContext.conf),
      getClass.getCanonicalName, mode, schema, parts,
      connProperties.poolProps, connProperties.connProps,
      connProperties.hikariCP, options, sqlContext)
    try {
      relation.tableSchema = relation.createTable(mode)
      success = true
      relation
    } finally {
      if (!success) {
        // destroy the relation
        relation.destroy(ifExists = true)
      }
    }
  }

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String], schema: StructType) = {
    val url = options.getOrElse("url",
      ExternalStoreUtils.defaultStoreURL(sqlContext.sparkContext))
    val dialect = JdbcDialects.get(url)
    val schemaString = JdbcExtendedUtils.schemaString(schema, dialect)

    val allowExisting = options.get(JdbcExtendedUtils
        .ALLOW_EXISTING_PROPERTY).exists(_.toBoolean)
    val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
    createRelation(sqlContext, mode, options, schemaString)
  }

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String]) = {
    val allowExisting = options.get(JdbcExtendedUtils
        .ALLOW_EXISTING_PROPERTY).exists(_.toBoolean)
    val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
    // will work only if table is already existing
    createRelation(sqlContext, mode, options, "")
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], data: DataFrame) = {
    val url = options.getOrElse("url",
      ExternalStoreUtils.defaultStoreURL(sqlContext.sparkContext))
    val dialect = JdbcDialects.get(url)
    val schemaString = JdbcExtendedUtils.schemaString(data.schema, dialect)

    val relation = createRelation(sqlContext, mode, options, schemaString)
    var success = false
    try {
      relation.insert(data)
      success = true
      relation
    } finally {
      if (!success) {
        // destroy the relation
        relation.destroy(ifExists = true)
      }
    }
  }
}
