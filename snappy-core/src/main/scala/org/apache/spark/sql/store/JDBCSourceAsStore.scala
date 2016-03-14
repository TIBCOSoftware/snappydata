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
package org.apache.spark.sql.store

import java.sql.{Connection, ResultSet, Statement}
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.control.NonFatal

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.collection.UUIDRegionKey
import org.apache.spark.sql.columnar.{CachedBatch, ConnectionProperties, ExternalStoreUtils}
import org.apache.spark.sql.execution.{ConnectionPool, SparkSqlSerializer}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.storage.{BlockId, BlockStatus, RDDBlockId, StorageLevel}
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}

/*
Generic class to query column table from Snappy.
 */
class JDBCSourceAsStore(override val connProperties: ConnectionProperties,
    numPartitions: Int) extends ExternalStore {

  @transient
  protected lazy val rand = new Random

  protected val dialect = JdbcDialects.get(connProperties.url)

  lazy val connectionType = ExternalStoreUtils.getConnectionType(connProperties.url)

  def getCachedBatchRDD(tableName: String,
      requiredColumns: Array[String],
      sparkContext: SparkContext): RDD[CachedBatch] = {
    new ExternalStorePartitionedRDD(sparkContext, tableName, requiredColumns, numPartitions, this)
  }

  override def storeCachedBatch(tableName: String, batch: CachedBatch,
      bucketId: Int = -1, batchId: Option[UUID] = None, rddId: Int = -1): UUIDRegionKey = {
    val uuid = getUUIDRegionKey(tableName, bucketId, batchId)
    storeCurrentBatch(tableName, batch, uuid, rddId)
    uuid
  }

  override def getUUIDRegionKey(tableName: String, bucketId: Int = -1,
      batchId: Option[UUID] = None): UUIDRegionKey = {
    genUUIDRegionKey(rand.nextInt(numPartitions))
  }

  def storeCurrentBatch(tableName: String, batch: CachedBatch,
      uuid: UUIDRegionKey, rddId: Int): Unit = {
    var cachedBatchSizeInBytes :Long = 0L
    tryExecute(tableName, {
      case connection =>
        val rowInsertStr = getRowInsertStr(tableName, batch.buffers.length)
        val stmt = connection.prepareStatement(rowInsertStr)
        stmt.setString(1, uuid.getUUID.toString)
        stmt.setInt(2, uuid.getBucketId)
        stmt.setInt(3, batch.numRows)
        val stats: Array[Byte] = SparkSqlSerializer.serialize(batch.stats)
        stmt.setBytes(4, stats)
        var columnIndex = 5
        batch.buffers.foreach(buffer => {
          stmt.setBytes(columnIndex, buffer)
          columnIndex += 1
          cachedBatchSizeInBytes += buffer.size
        })
        stmt.executeUpdate()
        stmt.close()
        cachedBatchSizeInBytes += uuid.getUUID.toString.size +
            2*4 /*size of bucket id and numrows*/ + stats.length
    })

//    log.trace("cachedBatchSizeInBytes =" + cachedBatchSizeInBytes
//    + " rddId=" + rddId + " bucketId =" + uuid.getBucketId )
    val taskContext = TaskContext.get
    if (Option(taskContext) != None && cachedBatchSizeInBytes > 0L) {
      val metrics = taskContext.taskMetrics
      val lastUpdatedBlocks = metrics.updatedBlocks.getOrElse(
        new ArrayBuffer[(BlockId, BlockStatus)]())
      val blockIdAndStatus = (RDDBlockId(rddId, uuid.getBucketId),
          BlockStatus(StorageLevel.OFF_HEAP, 0L, 0L, cachedBatchSizeInBytes))
      metrics.updatedBlocks = Some(lastUpdatedBlocks.+:(blockIdAndStatus))
    }
  }

  override def getConnection(id: String): Connection = {
    ConnectionPool.getPoolConnection(id, dialect, connProperties.poolProps,
      connProperties.connProps, connProperties.hikariCP)
  }

  protected def genUUIDRegionKey(bucketId: Int = -1) = new UUIDRegionKey(bucketId)

  protected def genUUIDRegionKey(bucketID: Int, batchID: UUID) =
    new UUIDRegionKey(bucketID, batchID)

  protected val insertStrings: mutable.HashMap[String, String] =
    new mutable.HashMap[String, String]()

  protected def getRowInsertStr(tableName: String, numOfColumns: Int): String = {
    val istr = insertStrings.getOrElse(tableName, {
      lock(makeInsertStmnt(tableName, numOfColumns))
    })
    istr
  }

  protected def makeInsertStmnt(tableName: String, numOfColumns: Int) = {
    if (!insertStrings.contains(tableName)) {
      val s = insertStrings.getOrElse(tableName,
        s"insert into $tableName values(?,?,?,?${",?" * numOfColumns})")
      insertStrings.put(tableName, s)
    }
    insertStrings.get(tableName).get
  }

  protected val insertStmntLock = new ReentrantLock()

  /** Acquires a read lock on the cache for the duration of `f`. */
  protected[sql] def lock[A](f: => A): A = {
    insertStmntLock.lock()
    try f finally {
      insertStmntLock.unlock()
    }
  }
}

abstract class ResultSetIterator[A](conn: Connection,
    stmt: Statement, rs: ResultSet, context: TaskContext)
    extends Iterator[A] with Logging {

  protected final var hasNextValue = true

  context.addTaskCompletionListener { context => close() }
  moveNext()

  override final def hasNext: Boolean = hasNextValue

  protected final def moveNext(): Unit = {
    var success = false
    try {
      // TODO: see if optimization using rs.lightWeightNext
      // and explicit context pop in close possible (was causing trouble)
      success = rs.next()
    } catch {
      case NonFatal(e) => logWarning("Exception iterating resultSet", e)
    } finally {
      if (!success) {
        close()
      }
    }
  }

  protected def getNextValue(rs: ResultSet): A

  final def next(): A = {
    val result = getNextValue(rs)
    moveNext()
    result
  }

  final def close() {
    if (!hasNextValue) return
    try {
      // GfxdConnectionWrapper.restoreContextStack(stmt, rs)
      // rs.lightWeightClose()
      rs.close()
    } catch {
      case e: Exception => logWarning("Exception closing resultSet", e)
    }
    try {
      stmt.close()
    } catch {
      case e: Exception => logWarning("Exception closing statement", e)
    }
    try {
      conn.close()
      logDebug("closed connection for task " + context.partitionId())
    } catch {
      case e: Exception => logWarning("Exception closing connection", e)
    }
    hasNextValue = false
  }
}

final class CachedBatchIteratorOnRS(conn: Connection,
    requiredColumns: Array[String],
    stmt: Statement, rs: ResultSet, context: TaskContext)
    extends ResultSetIterator[CachedBatch](conn, stmt, rs, context) {

  private val numCols = requiredColumns.length
  private val colBuffers = new Array[Array[Byte]](numCols)

  protected override def getNextValue(rs: ResultSet): CachedBatch = {
    var i = 0
    while (i < numCols) {
      colBuffers(i) = rs.getBytes(i + 1)
      i += 1
    }
    val stats = SparkSqlSerializer.deserialize[InternalRow](rs.getBytes("stats"))
    CachedBatch(rs.getInt("numRows"), colBuffers, stats)
  }
}

class ExternalStorePartitionedRDD[T: ClassTag](@transient _sc: SparkContext,
    tableName: String, requiredColumns: Array[String],
    numPartitions: Int,
    store: JDBCSourceAsStore)
    extends RDD[CachedBatch](_sc, Nil) {

  override def compute(split: Partition,
      context: TaskContext): Iterator[CachedBatch] = {
    store.tryExecute(tableName, {
      case conn =>

        val resolvedName = {
          if (tableName.indexOf(".") <= 0) {
            conn.getSchema + "." + tableName
          } else tableName
        }

        val par = split.index
        val stmt = conn.createStatement()
        val query = "select " + requiredColumns.mkString(", ") +
            s", numRows, stats from $resolvedName where bucketid = $par"
        val rs = stmt.executeQuery(query)
        new CachedBatchIteratorOnRS(conn, requiredColumns, stmt, rs, context)
    }, closeOnSuccess = false)
  }

  override protected def getPartitions: Array[Partition] = {
    for (p <- 0 until numPartitions) {
      partitions(p) = new Partition {
        override def index: Int = p
      }
    }
    partitions
  }
}
