/*
 * Copyright (c) 2010-2016 SnappyData, Inc. All rights reserved.
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
