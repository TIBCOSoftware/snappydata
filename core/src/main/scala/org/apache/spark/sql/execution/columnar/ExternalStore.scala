/*
 * Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
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
package org.apache.spark.sql.execution.columnar

import java.nio.ByteBuffer
import java.sql.Connection

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.SnapshotConnectionListener
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.sources.ConnectionProperties
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Logging, TaskContext}

trait ExternalStore extends Serializable with Logging {

  def tableName: String

  def withTable(tableName: String, numPartitions: Int): ExternalStore

  def storeColumnBatch(tableName: String, batch: ColumnBatch, partitionId: Int, batchId: Long,
      maxDeltaRows: Int, compressionCodecId: Int, changesToDeltaBuffer: SQLMetric,
      numColumnBatches: SQLMetric, changesToColumnStore: SQLMetric,
      listener: SnapshotConnectionListener): Unit

  def storeDelete(tableName: String, buffer: ByteBuffer, partitionId: Int,
      batchId: Long, compressionCodecId: Int, conn: Connection): Unit

  def getColumnBatchRDD(tableName: String, rowBuffer: String, projection: Array[Int],
      filters: Array[Expression], prunePartitions: () => Int, session: SparkSession,
      schema: StructType, delayRollover: Boolean): RDD[Any]

  def getExistingConnection(context: Option[TaskContext]): Option[Connection]

  def connProperties: ConnectionProperties

  def tryExecute[T](tableName: String, onExecutor: Boolean = false)(f: Connection => T): T = {
    var success = false
    val existing = getExistingConnection(context = None)
    val conn = existing match {
      case Some(c) => c
      case None => ExternalStoreUtils.getConnection(
        tableName, connProperties, onExecutor, resetIsolationLevel = true)
    }
    try {
      val ret = f(conn)
      success = true
      ret
    } finally {
      if (existing.isEmpty) {
        commitOrRollback(success, conn)
      }
    }
  }

  def commitOrRollback(success: Boolean, conn: Connection): Unit = {
    if (!conn.isClosed) {
      try {
        if (success) conn.commit()
        else ExternalStoreUtils.handleRollback(conn.rollback)
      } finally {
        conn.close()
      }
    }
  }
}
