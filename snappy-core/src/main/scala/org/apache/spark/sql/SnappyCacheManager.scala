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
package org.apache.spark.sql

import org.apache.spark.sql.execution.columnar.InMemoryAppendableRelation
import org.apache.spark.sql.execution.{Queryable, SparkPlan}
import org.apache.spark.storage.StorageLevel

/**
 * Snappy CacheManager extension to allow for appending data to cache.
 */
private[sql] class SnappyCacheManager extends execution.CacheManager {

  /**
   * Caches the data produced by the logical representation of the given
   * schema rdd. Unlike `RDD.cache()`, the default storage level is set to be
   * `MEMORY_AND_DISK` because recomputing the in-memory columnar representation
   * of the underlying table is expensive.
   */
  override protected[sql] def cacheQuery(query: Queryable,
      tableName: Option[String] = None,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK) = writeLock {

    val alreadyCached = lookupCachedData(query.queryExecution.analyzed)
    if (alreadyCached.nonEmpty) {
      logWarning("SnappyCacheManager: asked to cache already cached data.")
    } else {

      val sqlContext = query.sqlContext

      cachedData += execution.CachedData(query.queryExecution.analyzed,
        getRelation(sqlContext, storageLevel,
          query.queryExecution.executedPlan, tableName))
    }
  }

  def getRelation(sqlContext: SQLContext, storageLevel: StorageLevel,
      executedPlan: SparkPlan,
      tableName: Option[String]): InMemoryAppendableRelation =
    InMemoryAppendableRelation(
      sqlContext.conf.useCompression,
      sqlContext.conf.columnBatchSize,
      storageLevel,
      executedPlan,
      tableName)
}
