package org.apache.spark.sql.store

import java.sql.Connection

import org.apache.spark.sql.collection.UUIDRegionKey
import org.apache.spark.sql.columnar.CachedBatch

import scala.reflect.ClassTag

/**
 * Created by neeraj on 16/7/15.
 */
trait ExternalStore extends Serializable {
  def initSource(): Unit
  def storeCachedBatch(batch: CachedBatch, tableName: String) : UUIDRegionKey
  def truncate(tableName: String)
  def cleanup(): Unit
  def getCachedBatchIterator(tableName: String, itr: Iterator[UUIDRegionKey],
                             getAll: Boolean = false): Iterator[CachedBatch]

  def getConnection(id: String, onMaster: Boolean = false): java.sql.Connection

  def url: String

  def driver: String

  def poolProps: Map[String, String]

  def connProps: java.util.Properties

  def tryExecute[T: ClassTag](tableName: String, f: PartialFunction[(Connection), T], closeOnSuccess: Boolean = true): T = {
    val conn = getConnection(tableName)
    var isClosed = false;
    try {
      f(conn)
    } catch {
      case t: Throwable => {
        conn.close()
        isClosed = true
        throw t;
      }
    } finally {
      if (closeOnSuccess && !isClosed) {
        conn.close()
      }
    }
  }


}
