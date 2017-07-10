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

import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogUtils, CatalogTable, CatalogTablePartition, CatalogTableType, SessionCatalog}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.command.DDLUtils
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

  override def run(sparkSession: SparkSession): Seq[Row] = {
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
      if (metadata.schema.isEmpty) {
        // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
        // inferred at runtime. We should still support it.
        describeSchema(catalog.lookupRelation(metadata.identifier).schema, result)
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
        describeDetailedPartitionInfo(sparkSession, catalog, metadata, result)
      }
    }

    result
  }

  private def describePartitionInfo(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    if (table.partitionColumnNames.nonEmpty) {
      append(buffer, "# Partition Information", "", "")
      append(buffer, s"# ${output.head.name}", output(1).name, output(2).name)
      describeSchema(table.partitionSchema, buffer)
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
    table.stats.foreach(s => append(buffer, "Statistics:", s.simpleString, ""))

    append(buffer, "Table Parameters:", "", "")
    table.properties.foreach { case (key, value) =>
      append(buffer, s"  $key", value, "")
    }

    describeStorageInfo(table, buffer)

    if (table.tableType == CatalogTableType.VIEW) describeViewInfo(table, buffer)

    if (DDLUtils.isDatasourceTable(table) && table.tracksPartitionsInCatalog) {
      append(buffer, "Partition Provider:", "Catalog", "")
    }
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
    val maskedProperties = CatalogUtils.maskCredentials(metadata.storage.properties)
    maskedProperties.foreach { case (key, value) =>
      append(buffer, s"  $key", value, "")
    }
  }

  private def describeViewInfo(metadata: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    append(buffer, "", "", "")
    append(buffer, "# View Information", "", "")
    append(buffer, "View Original Text:", metadata.viewOriginalText.getOrElse(""), "")
    append(buffer, "View Expanded Text:", metadata.viewText.getOrElse(""), "")
  }

  private def describeBucketingInfo(metadata: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    metadata.bucketSpec match {
      case Some(BucketSpec(numBuckets, bucketColumnNames, sortColumnNames)) =>
        append(buffer, "Num Buckets:", numBuckets.toString, "")
        append(buffer, "Bucket Columns:", bucketColumnNames.mkString("[", ", ", "]"), "")
        append(buffer, "Sort Columns:", sortColumnNames.mkString("[", ", ", "]"), "")

      case _ =>
    }
  }

  private def describeDetailedPartitionInfo(
      spark: SparkSession,
      catalog: SessionCatalog,
      metadata: CatalogTable,
      result: ArrayBuffer[Row]): Unit = {
    if (metadata.tableType == CatalogTableType.VIEW) {
      throw new AnalysisException(
        s"DESC PARTITION is not allowed on a view: ${table.identifier}")
    }
    DDLUtils.verifyPartitionProviderIsHive(spark, metadata, "DESC PARTITION")
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

  private def describeSchema(schema: StructType, buffer: ArrayBuffer[Row]): Unit = {
    schema.foreach { column =>
      append(buffer, column.name, column.dataType.simpleString, column.getComment().orNull)
    }
  }

  private def append(
      buffer: ArrayBuffer[Row], column: String, dataType: String, comment: String): Unit = {
    buffer += Row(column, dataType, comment)
  }
}
