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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.CacheManager
import org.apache.spark.sql.{Dataset, SnappySession, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * Simple extension to CacheManager to enable clearing cached plan on cache create/drop.
 */
class SnappyCacheManager210 extends CacheManager {

  override def cacheQuery(query: Dataset[_], tableName: Option[String],
      storageLevel: StorageLevel): Unit = {
    super.cacheQuery(query, tableName, storageLevel)
    // clear plan cache since cached representation can change existing plans
    query.sparkSession.asInstanceOf[SnappySession].clearPlanCache()
  }

  override def uncacheQuery(query: Dataset[_], blocking: Boolean): Boolean = {
    if (super.uncacheQuery(query, blocking)) {
      query.sparkSession.asInstanceOf[SnappySession].clearPlanCache()
      true
    } else false
  }

  override def invalidateCache(plan: LogicalPlan): Unit = {
    super.invalidateCache(plan)
    SparkSession.getActiveSession match {
      case None =>
      case Some(session) => session.asInstanceOf[SnappySession].clearPlanCache()
    }
  }

  override def invalidateCachedPath(session: SparkSession, resourcePath: String): Unit = {
    super.invalidateCachedPath(session, resourcePath)
    session.asInstanceOf[SnappySession].clearPlanCache()
  }
}
