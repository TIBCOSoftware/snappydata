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
package org.apache.spark.sql.internal

import io.snappydata.sql.catalog.impl.SmartConnectorExternalCatalog
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan, Statistics}
import org.apache.spark.sql.execution.CacheManager
import org.apache.spark.sql.hive.SnappyHiveExternalCatalog
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkException}

/**
 * Implementation of [[SparkInternals]] for Spark 2.1.1.
 */
class Spark211Internals extends Spark210Internals {

  override def version: String = "2.1.1"

  override def uncacheQuery(spark: SparkSession, plan: LogicalPlan, blocking: Boolean): Unit = {
    spark.sharedState.cacheManager.uncacheQuery(spark, plan, blocking)
  }

  override def mapExpressions(plan: LogicalPlan, f: Expression => Expression): LogicalPlan = {
    plan.mapExpressions(f)
  }

  // scalastyle:off

  override def newCatalogTable(identifier: TableIdentifier, tableType: CatalogTableType,
      storage: CatalogStorageFormat, schema: StructType, provider: Option[String],
      partitionColumnNames: Seq[String], bucketSpec: Option[BucketSpec],
      owner: String, createTime: Long, lastAccessTime: Long, properties: Map[String, String],
      stats: Option[(BigInt, Option[BigInt], Map[String, ColumnStat])],
      viewOriginalText: Option[String], viewText: Option[String],
      comment: Option[String], unsupportedFeatures: Seq[String],
      tracksPartitionsInCatalog: Boolean, schemaPreservesCase: Boolean,
      ignoredProperties: Map[String, String]): CatalogTable = {
    if (ignoredProperties.nonEmpty) {
      throw new SparkException(s"ignoredProperties should be always empty in Spark $version")
    }
    val statistics = stats match {
      case None => None
      case Some(s) => Some(Statistics(s._1, s._2, s._3))
    }
    CatalogTable(identifier, tableType, storage, schema, provider, partitionColumnNames,
      bucketSpec, owner, createTime, lastAccessTime, properties, statistics, viewOriginalText,
      viewText, comment, unsupportedFeatures, tracksPartitionsInCatalog, schemaPreservesCase)
  }

  // scalastyle:on

  override def catalogTableSchemaPreservesCase(catalogTable: CatalogTable): Boolean =
    catalogTable.schemaPreservesCase

  override def newEmbeddedHiveCatalog(conf: SparkConf, hadoopConf: Configuration,
      createTime: Long): SnappyHiveExternalCatalog = {
    new SnappyEmbeddedHiveCatalog211(conf, hadoopConf, createTime)
  }

  override def newSmartConnectorExternalCatalog(
      session: SparkSession): SmartConnectorExternalCatalog = {
    new SmartConnectorExternalCatalog211(session)
  }

  override def newCacheManager(): CacheManager = new SnappyCacheManager211
}

/**
 * Simple extension to CacheManager to enable clearing cached plan on cache create/drop.
 */
final class SnappyCacheManager211 extends CacheManager {

  override def cacheQuery(query: Dataset[_], tableName: Option[String],
      storageLevel: StorageLevel): Unit = {
    super.cacheQuery(query, tableName, storageLevel)
    // clear plan cache since cached representation can change existing plans
    query.sparkSession.asInstanceOf[SnappySession].clearPlanCache()
  }

  override def uncacheQuery(session: SparkSession, plan: LogicalPlan, blocking: Boolean): Unit = {
    super.uncacheQuery(session, plan, blocking)
    session.asInstanceOf[SnappySession].clearPlanCache()
  }

  override def recacheByPlan(session: SparkSession, plan: LogicalPlan): Unit = {
    super.recacheByPlan(session, plan)
    session.asInstanceOf[SnappySession].clearPlanCache()
  }

  override def recacheByPath(session: SparkSession, resourcePath: String): Unit = {
    super.recacheByPath(session, resourcePath)
    session.asInstanceOf[SnappySession].clearPlanCache()
  }
}

final class SnappyEmbeddedHiveCatalog211(conf: SparkConf,
    hadoopConf: Configuration, createTime: Long)
    extends SnappyEmbeddedHiveCatalog21(conf, hadoopConf, createTime) {

  override def alterTableSchema(schemaName: String, table: String, newSchema: StructType): Unit =
    alterTableSchemaImpl(schemaName, table, newSchema)
}

final class SmartConnectorExternalCatalog211(session: SparkSession)
    extends SmartConnectorExternalCatalog21(session) {

  override def alterTableSchema(schemaName: String, table: String, newSchema: StructType): Unit =
    alterTableSchemaImpl(schemaName, table, newSchema)
}
