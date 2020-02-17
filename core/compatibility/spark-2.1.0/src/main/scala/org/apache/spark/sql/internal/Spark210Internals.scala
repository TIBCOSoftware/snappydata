/*
 * Copyright (c) 2017-2020 TIBCO Software Inc. All rights reserved.
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

import io.snappydata.sql.catalog.SnappyExternalCatalog

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.CacheManager
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SparkInternals, SparkSession}

/**
 * Implementation of [[SparkInternals]] for Spark 2.1.0.
 */
class Spark210Internals(override val version: String) extends Spark21Internals {

  override def newCaseInsensitiveMap(map: Map[String, String]): Map[String, String] = {
    new CaseInsensitiveMap(map)
  }

  // scalastyle:off

  override def newCatalogTable(identifier: TableIdentifier, tableType: CatalogTableType,
      storage: CatalogStorageFormat, schema: StructType, provider: Option[String],
      partitionColumnNames: Seq[String], bucketSpec: Option[BucketSpec],
      owner: String, createTime: Long, lastAccessTime: Long, properties: Map[String, String],
      stats: Option[AnyRef], viewOriginalText: Option[String], viewText: Option[String],
      comment: Option[String], unsupportedFeatures: Seq[String],
      tracksPartitionsInCatalog: Boolean, schemaPreservesCase: Boolean,
      ignoredProperties: Map[String, String]): CatalogTable = {
    if (!schemaPreservesCase) {
      throw new SparkException(s"schemaPreservesCase should be always true in Spark $version")
    }
    if (ignoredProperties.nonEmpty) {
      throw new SparkException(s"ignoredProperties should be always empty in Spark $version")
    }
    CatalogTable(identifier, tableType, storage, schema, provider, partitionColumnNames,
      bucketSpec, owner, createTime, lastAccessTime, properties,
      stats.asInstanceOf[Option[Statistics]], viewOriginalText, viewText, comment,
      unsupportedFeatures, tracksPartitionsInCatalog)
  }

  // scalastyle:on

  override def newSmartConnectorExternalCatalog(session: SparkSession): SnappyExternalCatalog = {
    new SmartConnectorExternalCatalog210(session)
  }

  override def newCacheManager(): CacheManager = new SnappyCacheManager210
}

final class SmartConnectorExternalCatalog210(session: SparkSession)
    extends SmartConnectorExternalCatalog21(session)
