package org.apache.spark.sql

import java.util.Properties

import org.apache.spark.sql.execution.StratifiedSample
import org.apache.spark.sql.execution.row.JDBCUpdatableSource
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
 * Snappy CacheManager extension to allow for appending data to cache.
 */
private[sql] class SnappyCacheManager(sqlContext: SnappyContext)
    extends execution.CacheManager(sqlContext) {

  /**
   * Caches the data produced by the logical representation of the given
   * schema rdd. Unlike `RDD.cache()`, the default storage level is set to be
   * `MEMORY_AND_DISK` because recomputing the in-memory columnar representation
   * of the underlying table is expensive.
   */
  override private[sql] def cacheQuery(query: DataFrame,
      tableName: Option[String] = None,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK) = writeLock {

    val alreadyCached = lookupCachedData(query.logicalPlan)
    if (alreadyCached.nonEmpty) {
      logWarning("SnappyCacheManager: asked to cache already cached data.")
    }
    else {
      val isSampledTable = query.logicalPlan match {
        case s: StratifiedSample => true
        case _ => false
      }
      cachedData += execution.CachedData(query.logicalPlan,
        columnar.InMemoryAppendableRelation(
          sqlContext.conf.useCompression,
          sqlContext.conf.columnBatchSize,
          storageLevel,
          query.queryExecution.executedPlan,
          tableName, isSampledTable))
    }
  }

  private[sql] def cacheQuery_ext(query: DataFrame,
      tableName: Option[String] = None, jdbcSource: JDBCUpdatableSource) = writeLock {
    val alreadyCached = lookupCachedData(query.logicalPlan)
    if (alreadyCached.nonEmpty) {
      logWarning("SnappyCacheManager: asked to cache already cached data.")
    }
    else {
      val isSampledTable = query.logicalPlan match {
        case s: StratifiedSample => true
        case _ => false
      }
      cachedData += execution.CachedData(query.logicalPlan,
        columnar.ExternalStoreRelation(
          sqlContext.conf.useCompression,
          sqlContext.conf.columnBatchSize,
          StorageLevel.NONE, // storage level is meaningless in external store. set anything
          query.queryExecution.executedPlan,
          tableName, isSampledTable, jdbcSource)) // all the properties inclusing url should be in props
    }
  }

  private val stores = new mutable.HashMap[String, Map[String, Any]]()

  def registerExternalStore(name: String, connProps: Map[String, Any]) = {
    if (stores.contains(name)) {
      throw new IllegalArgumentException(s"a store already registered with this name $name")
    }
    stores.put(name, connProps)
  }
}
