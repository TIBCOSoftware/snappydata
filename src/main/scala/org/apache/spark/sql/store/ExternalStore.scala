package org.apache.spark.sql.store

import java.util.UUID

import org.apache.spark.sql.columnar.CachedBatch

/**
 * Created by neeraj on 16/7/15.
 */
trait ExternalStore {
  def initSource(): Unit
  def storeCachedBatch(batch: CachedBatch, tablename: String): UUID
  def truncate(tablename: String)
  def cleanup(): Unit
  def getCachedBatchIterator(tableName: String, itr:
    Iterator[UUID], getAll: Boolean = false): Iterator[CachedBatch]
}
