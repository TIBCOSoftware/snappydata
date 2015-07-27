package org.apache.spark.sql.store

import org.apache.spark.sql.collection.UUIDRegionKey
import org.apache.spark.sql.columnar.CachedBatch

/**
 * Created by neeraj on 16/7/15.
 */
trait ExternalStore {
  def initSource(): Unit
  def storeCachedBatch(batch: CachedBatch, tableName: String) : UUIDRegionKey
  def truncate(tableName: String)
  def cleanup(): Unit
  def getCachedBatchIterator(tableName: String, itr: Iterator[UUIDRegionKey],
                             getAll: Boolean = false): Iterator[CachedBatch]
}
