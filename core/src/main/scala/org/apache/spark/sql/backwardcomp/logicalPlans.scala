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
package org.apache.spark.sql.backwardcomp

import java.util.Date

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.catalog.{CatalogColumn, CatalogTable, CatalogTablePartition, CatalogTableType, SessionCatalog}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.command.{CreateDataSourceTableUtils, DDLUtils}
import org.apache.spark.sql.types.{MetadataBuilder, StringType, StructType}

/**
 * **Copy of RunnableCommand to ensure a compatibility between
 * Spark 2.0.0 and Snappy Spark 2.0.2**
 *
 * A logical command that is executed for its side-effects.  `SnappyRunnableCommand`s are
 * wrapped in `SnappyExecutedCommandExec` during execution.
 */
trait ExecuteCommand extends LeafNode with logical.Command  {
  override def output: Seq[Attribute] = Seq.empty
  def run(sparkSession: SparkSession): Seq[Row]
}

/**
 * **Copy of DescribeTableCommand to ensure a compatibility between
 * Spark 2.0.0 and Snappy Spark 2.0.2
 *
 * Command that looks like
 * {{{
 *   DESCRIBE [EXTENDED|FORMATTED] table_name partitionSpec?;
 * }}}
 */
case class DescribeTable(
    table: TableIdentifier,
    partitionSpec: Map[String, String],
    isExtended: Boolean,
    isFormatted: Boolean)
    extends ExecuteCommand {

  override val output: Seq[Attribute] = Seq(
    // Column names are based on Hive.
    AttributeReference("col_name", StringType, nullable = false,
      new MetadataBuilder().putString("comment", "name of the column").build())(),
    AttributeReference("data_type", StringType, nullable = false,
      new MetadataBuilder().putString("comment", "data type of the column").build())(),
    AttributeReference("comment", StringType, nullable = true,
      new MetadataBuilder().putString("comment", "comment of the column").build())()
  )

  def run(sparkSession: SparkSession): Seq[Row] = {
    val result = new ArrayBuffer[Row]
    val catalog = sparkSession.sessionState.catalog

    if (catalog.isTemporaryTable(table)) {
      if (partitionSpec.nonEmpty) {
        throw new AnalysisException(
          s"DESC PARTITION is not allowed on a temporary view: ${table.identifier}")
      }
      describeSchema(catalog.lookupRelation(table).schema, result)
    } else {
      val metadata = catalog.getTableMetadata(table)

      if (DDLUtils.isDatasourceTable(metadata)) {
        DDLUtils.getSchemaFromTableProperties(metadata) match {
          case Some(userSpecifiedSchema) => describeSchema(userSpecifiedSchema, result)
          case None => describeSchema(catalog.lookupRelation(table).schema, result)
        }
      } else {
        describeSchema(metadata.schema, result)
      }

      describePartitionInfo(metadata, result)

      if (partitionSpec.isEmpty) {
        if (isExtended) {
          describeExtendedTableInfo(metadata, result)
        } else if (isFormatted) {
          describeFormattedTableInfo(metadata, result)
        }
      } else {
        describeDetailedPartitionInfo(catalog, metadata, result)
      }
    }

    result
  }

  private def describePartitionInfo(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    if (DDLUtils.isDatasourceTable(table)) {
      val userSpecifiedSchema = DDLUtils.getSchemaFromTableProperties(table)
      val partColNames = DDLUtils.getPartitionColumnsFromTableProperties(table)
      for (schema <- userSpecifiedSchema if partColNames.nonEmpty) {
        append(buffer, "# Partition Information", "", "")
        append(buffer, s"# ${output.head.name}", output(1).name, output(2).name)
        describeSchema(StructType(partColNames.map(schema(_))), buffer)
      }
    } else {
      if (table.partitionColumns.nonEmpty) {
        append(buffer, "# Partition Information", "", "")
        append(buffer, s"# ${output.head.name}", output(1).name, output(2).name)
        describeSchema(table.partitionColumns, buffer)
      }
    }
  }

  private def describeExtendedTableInfo(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    append(buffer, "", "", "")
    append(buffer, "# Detailed Table Information", table.toString, "")
  }

  private def describeFormattedTableInfo(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    append(buffer, "", "", "")
    append(buffer, "# Detailed Table Information", "", "")
    append(buffer, "Database:", table.database, "")
    append(buffer, "Owner:", table.owner, "")
    append(buffer, "Create Time:", new Date(table.createTime).toString, "")
    append(buffer, "Last Access Time:", new Date(table.lastAccessTime).toString, "")
    append(buffer, "Location:", table.storage.locationUri.getOrElse(""), "")
    append(buffer, "Table Type:", table.tableType.name, "")

    append(buffer, "Table Parameters:", "", "")
    table.properties.filterNot {
      // Hides schema properties that hold user-defined schema, partition columns, and bucketing
      // information since they are already extracted and shown in other parts.
      case (key, _) => key.startsWith(CreateDataSourceTableUtils.DATASOURCE_SCHEMA)
    }.foreach { case (key, value) =>
      append(buffer, s"  $key", value, "")
    }

    describeStorageInfo(table, buffer)
  }

  private def describeStorageInfo(metadata: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    append(buffer, "", "", "")
    append(buffer, "# Storage Information", "", "")
    metadata.storage.serde.foreach(serdeLib => append(buffer, "SerDe Library:", serdeLib, ""))
    metadata.storage.inputFormat.foreach(format => append(buffer, "InputFormat:", format, ""))
    metadata.storage.outputFormat.foreach(format => append(buffer, "OutputFormat:", format, ""))
    append(buffer, "Compressed:", if (metadata.storage.compressed) "Yes" else "No", "")
    describeBucketingInfo(metadata, buffer)

    append(buffer, "Storage Desc Parameters:", "", "")
    metadata.storage.serdeProperties.foreach { case (key, value) =>
      append(buffer, s"  $key", value, "")
    }
  }

  private def describeBucketingInfo(metadata: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    def appendBucketInfo(numBuckets: Int, bucketColumns: Seq[String], sortColumns: Seq[String]) = {
      append(buffer, "Num Buckets:", numBuckets.toString, "")
      append(buffer, "Bucket Columns:", bucketColumns.mkString("[", ", ", "]"), "")
      append(buffer, "Sort Columns:", sortColumns.mkString("[", ", ", "]"), "")
    }

    DDLUtils.getBucketSpecFromTableProperties(metadata) match {
      case Some(bucketSpec) =>
        appendBucketInfo(
          bucketSpec.numBuckets,
          bucketSpec.bucketColumnNames,
          bucketSpec.sortColumnNames)
      case None =>
        appendBucketInfo(
          metadata.numBuckets,
          metadata.bucketColumnNames,
          metadata.sortColumnNames)
    }
  }

  private def describeDetailedPartitionInfo(
      catalog: SessionCatalog,
      metadata: CatalogTable,
      result: ArrayBuffer[Row]): Unit = {
    if (metadata.tableType == CatalogTableType.VIEW) {
      throw new AnalysisException(
        s"DESC PARTITION is not allowed on a view: ${table.identifier}")
    }
    if (DDLUtils.isDatasourceTable(metadata)) {
      throw new AnalysisException(
        s"DESC PARTITION is not allowed on a datasource table: ${table.identifier}")
    }
    val partition = catalog.getPartition(table, partitionSpec)
    if (isExtended) {
      describeExtendedDetailedPartitionInfo(table, metadata, partition, result)
    } else if (isFormatted) {
      describeFormattedDetailedPartitionInfo(table, metadata, partition, result)
      describeStorageInfo(metadata, result)
    }
  }

  private def describeExtendedDetailedPartitionInfo(
      tableIdentifier: TableIdentifier,
      table: CatalogTable,
      partition: CatalogTablePartition,
      buffer: ArrayBuffer[Row]): Unit = {
    append(buffer, "", "", "")
    append(buffer, "Detailed Partition Information " + partition.toString, "", "")
  }

  private def describeFormattedDetailedPartitionInfo(
      tableIdentifier: TableIdentifier,
      table: CatalogTable,
      partition: CatalogTablePartition,
      buffer: ArrayBuffer[Row]): Unit = {
    append(buffer, "", "", "")
    append(buffer, "# Detailed Partition Information", "", "")
    append(buffer, "Partition Value:", s"[${partition.spec.values.mkString(", ")}]", "")
    append(buffer, "Database:", table.database, "")
    append(buffer, "Table:", tableIdentifier.table, "")
    append(buffer, "Location:", partition.storage.locationUri.getOrElse(""), "")
    // **Removed the partition parameters info from the output to ensure a compatibility
    // between Spark 2.0.0 and Snappy Spark 2.0.2**

    //    append(buffer, "Partition Parameters:", "", "")
    //    partition.parameters.foreach { case (key, value) =>
    //      append(buffer, s"  $key", value, "")
    //    }
  }

  private def describeSchema(schema: Seq[CatalogColumn], buffer: ArrayBuffer[Row]): Unit = {
    schema.foreach { column =>
      append(buffer, column.name, column.dataType.toLowerCase, column.comment.orNull)
    }
  }

  private def describeSchema(schema: StructType, buffer: ArrayBuffer[Row]): Unit = {
    schema.foreach { column =>
      val comment =
        if (column.metadata.contains("comment")) column.metadata.getString("comment") else null
      append(buffer, column.name, column.dataType.simpleString, comment)
    }
  }

  private def append(
      buffer: ArrayBuffer[Row], column: String, dataType: String, comment: String): Unit = {
    buffer += Row(column, dataType, comment)
  }
}