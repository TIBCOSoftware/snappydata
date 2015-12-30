package org.apache.spark.sql.store

import java.sql.Connection
import java.util.UUID

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.collection.UUIDRegionKey
import org.apache.spark.sql.columnar.{ConnectionProperties, CachedBatch}

/**
 * Created by neeraj on 16/7/15.
 */
trait ExternalStore extends Serializable {

  final val shadowTableNamePrefix = "_shadow_"
  final val columnPrefix = "Col_"

  def storeCachedBatch(tableName: String, batch: CachedBatch, bucketId: Int = -1,
      batchId: Option[UUID] = None): UUIDRegionKey

  def getCachedBatchRDD(tableName: String, requiredColumns: Array[String],
      sparkContext: SparkContext): RDD[CachedBatch]

  def getConnection(id: String): java.sql.Connection

  def getUUIDRegionKey(tableName: String, bucketId: Int = -1, batchId: Option[UUID] = None): UUIDRegionKey

  def connProperties:ConnectionProperties

  def tryExecute[T: ClassTag](tableName: String,
      f: PartialFunction[(Connection), T],
      closeOnSuccess: Boolean = true): T = {
    val conn = getConnection(tableName)
    var isClosed = false
    try {
      f(conn)
    } catch {
      case t: Throwable =>
        conn.close()
        isClosed = true
        throw t
    } finally {
      if (closeOnSuccess && !isClosed) {
        conn.close()
      }
    }
  }
}
