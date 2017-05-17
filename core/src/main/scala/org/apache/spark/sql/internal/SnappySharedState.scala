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
package org.apache.spark.sql.internal

import io.snappydata.impl.SnappyHiveCatalog
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.CacheManager
import org.apache.spark.sql.hive.{HiveClientUtil, SnappyConnectorExternalCatalog, SnappyExternalCatalog}
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

/**
 * Right now we are not overriding anything from Spark's built in SharedState. We need to
 * re-visit if we need this at all.
 *
 */
private[sql] class SnappySharedState(val sparkContext: SparkContext, sessionId: Int) {
  /**
   * Class for caching query results reused in future executions.
   */
  /* override */ val cacheManager = new SnappyCacheManager(sessionId)

  /**
   * A Hive client used to interact with the metastore.
   */
  private[sql] lazy val metadataHive = {
    val oldFlag = SnappyHiveCatalog.SKIP_HIVE_TABLE_CALLS.get()
    SnappyHiveCatalog.SKIP_HIVE_TABLE_CALLS.set(java.lang.Boolean.TRUE)
    try {
      // avoid inheritance of activeSession
      SparkSession.clearActiveSession()
      new HiveClientUtil(sparkContext).client
    } finally {
      SnappyHiveCatalog.SKIP_HIVE_TABLE_CALLS.set(oldFlag)
    }
  }

  val externalCatalog = SnappyContext.getClusterMode(sparkContext) match {
    case ThinClientConnectorMode(_, _) =>
      new SnappyConnectorExternalCatalog(metadataHive, sparkContext.hadoopConfiguration)
    case _ =>
      new SnappyExternalCatalog(metadataHive, sparkContext.hadoopConfiguration)
  }

}

private[sql] class SnappyCacheManager(sessionId: Int) extends CacheManager {

  override def cacheQuery(query: Dataset[_], tableName: Option[String],
      storageLevel: StorageLevel): Unit = {
    super.cacheQuery(query, tableName, storageLevel)
    SnappySession.clearSessionCache(sessionId)
  }

  override def uncacheQuery(query: Dataset[_], blocking: Boolean): Boolean = {
    if (super.uncacheQuery(query, blocking)) {
      SnappySession.clearSessionCache(sessionId)
      true
    } else false
  }

  override def clearCache(): Unit = {
    super.clearCache()
    SnappySession.clearSessionCache(sessionId)
  }

  override def invalidateCache(plan: LogicalPlan): Unit = {
    super.invalidateCache(plan)
    SnappySession.clearSessionCache(sessionId)
  }

  override def invalidateCachedPath(sparkSession: SparkSession,
      resourcePath: String): Unit = {
    super.invalidateCachedPath(sparkSession, resourcePath)
    SnappySession.clearSessionCache(sessionId)
  }
}
