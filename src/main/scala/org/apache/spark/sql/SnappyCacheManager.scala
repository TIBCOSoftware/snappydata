package org.apache.spark.sql

import scala.collection.mutable

import org.apache.spark.sql.execution.StratifiedSample
import org.apache.spark.sql.execution.row.JDBCUpdatableSource
import org.apache.spark.storage.StorageLevel

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
    } else {
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
      tableName: Option[String], jdbcSource: Map[String, String]) = writeLock {
    val alreadyCached = lookupCachedData(query.logicalPlan)
    if (alreadyCached.nonEmpty) {
      logWarning("SnappyCacheManager: asked to cache already cached data.")
    } else {
      val isSampledTable = query.logicalPlan match {
        case s: StratifiedSample => true
        case _ => false
      }
      cachedData += execution.CachedData(query.logicalPlan,
        columnar.ExternalStoreRelation(
          sqlContext.conf.useCompression,
          sqlContext.conf.columnBatchSize,
          StorageLevel.MEMORY_AND_DISK, // soubhik: RDD[UUID] should spill to disk. No ?
          // StorageLevel.NONE, // storage level is meaningless in external store. set anything
          query.queryExecution.executedPlan,
          // all the properties including url should be in props
          tableName, isSampledTable, jdbcSource))
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
