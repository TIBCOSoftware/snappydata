package org.apache.spark.sql.store

import org.apache.spark.sql.columnar.CachedBatch

/**
 * Created by neeraj on 16/7/15.
 */
trait ExternalStore {
  def initSource(): Unit
  def storeCachedBatch(batch: CachedBatch, tname: String): Any
  def cleanup(): Unit
}
