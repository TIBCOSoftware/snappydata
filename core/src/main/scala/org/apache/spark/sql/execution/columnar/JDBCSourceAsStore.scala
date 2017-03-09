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
package org.apache.spark.sql.execution.columnar

import java.nio.ByteBuffer
import java.sql.{Connection, ResultSet, Statement}
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Random

import com.gemstone.gemfire.internal.cache.{TXId, NonLocalRegionEntry, OffHeapRegionEntry}
import com.pivotal.gemfirexd.internal.engine.store.{AbstractCompactExecRow, GemFireContainer, OffHeapCompactExecRowWithLobs, RegionEntryUtils, RowFormatter}
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation
import io.snappydata.thrift.common.BufferedBlob
import io.snappydata.thrift.internal.ClientBlob

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.row.PRValuesIterator
import org.apache.spark.sql.execution.{ConnectionPool, PartitionedPhysicalScan}
import org.apache.spark.sql.sources.ConnectionProperties
import org.apache.spark.{Logging, Partition, TaskContext}

/*
Generic class to query column table from SnappyData execution.
 */
class JDBCSourceAsStore(override val connProperties: ConnectionProperties,
    val numPartitions: Int) extends ExternalStore {
  self =>

  @transient
  protected lazy val rand = new Random

  lazy val connectionType: ConnectionType.Value =
    ExternalStoreUtils.getConnectionType(connProperties.dialect)

  def getConnectedExternalStore(tableName: String,
      onExecutor: Boolean): ConnectedExternalStore = new JDBCSourceAsStore(
    this.connProperties,
    this.numPartitions) with ConnectedExternalStore {
    protected[this] override val connectedInstance: Connection =
      self.getConnection(tableName, onExecutor)
  }

  override def getColumnBatchRDD(tableName: String,
      requiredColumns: Array[String],
      session: SparkSession): RDD[ColumnBatch] = {
    new ExternalStorePartitionedRDD(session, tableName, requiredColumns,
      numPartitions, this)
  }

  override def storeColumnBatch(tableName: String, batch: ColumnBatch,
      partitionId: Int, batchId: Option[String], maxDeltaRows: Int): Unit = {
    // noinspection RedundantDefaultArgument
    tryExecute(tableName, doInsert(tableName, batch, batchId,
      getPartitionID(tableName, partitionId), maxDeltaRows),
      closeOnSuccess = true, onExecutor = true)
  }

  protected def getPartitionID(tableName: String, partitionId: Int): Int = {
    if (partitionId < 0) rand.nextInt(numPartitions) else partitionId
  }

  protected def doInsert(tableName: String, batch: ColumnBatch,
      batchId: Option[String], partitionId: Int,
      maxDeltaRows: Int): (Connection => Any) = {
    {
      (connection: Connection) => {
        val rowInsertStr = getRowInsertStr(tableName, batch.buffers.length)
        val stmt = connection.prepareStatement(rowInsertStr)
        stmt.setString(1, batchId.getOrElse(UUID.randomUUID().toString))
        stmt.setInt(2, partitionId)
        stmt.setInt(3, batch.numRows)
        stmt.setBytes(4, batch.statsData)
        var columnPosition = 5
        batch.buffers.foreach { buffer =>
          val blob = new ClientBlob(buffer)
          stmt.setBlob(columnPosition, blob)
          columnPosition += 1
        }
        stmt.executeUpdate()
        stmt.close()
      }
    }
  }

  override def getConnection(id: String, onExecutor: Boolean): Connection = {
    val connProps = if (onExecutor) connProperties.executorConnProps
    else connProperties.connProps
    ConnectionPool.getPoolConnection(id, connProperties.dialect,
      connProperties.poolProps, connProps, connProperties.hikariCP)
  }

  protected val insertStrings: mutable.HashMap[String, String] =
    new mutable.HashMap[String, String]()

  protected def getRowInsertStr(tableName: String, numOfColumns: Int): String = {
    val istr = insertStrings.getOrElse(tableName, {
      lock(makeInsertStmnt(tableName, numOfColumns))
    })
    istr
  }

  protected def makeInsertStmnt(tableName: String, numOfColumns: Int): String = {
    if (!insertStrings.contains(tableName)) {
      val s = insertStrings.getOrElse(tableName,
        s"insert into $tableName values(?,?,?,?${",?" * numOfColumns})")
      insertStrings.put(tableName, s)
    }
    insertStrings(tableName)
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

  protected[this] final var doMove = true

  protected[this] final var hasNextValue: Boolean = rs ne null

  if (context ne null) {
    context.addTaskCompletionListener { _ => close() }
  }

  override final def hasNext: Boolean = {
    var success = false
    try {
      if (doMove && hasNextValue) {
        success = rs.next()
        doMove = false
        success
      } else {
        success = hasNextValue
        success
      }
    } finally {
      if (!success) {
        close()
      }
    }
  }

  override final def next(): A = {
    if (doMove) {
      hasNext
      doMove = true
      if (!hasNextValue) return null.asInstanceOf[A]
    }
    val result = getCurrentValue
    doMove = true
    result
  }

  protected def getCurrentValue: A

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
      conn.commit()
      conn.close()
      logDebug("closed connection for task " + context.partitionId())
    } catch {
      case e: Exception => logWarning("Exception closing connection", e)
    }
    hasNextValue = false
  }
}

case class ColumnBatch(numRows: Int, buffers: Array[ByteBuffer],
    statsData: Array[Byte])

final class ColumnBatchIterator(container: GemFireContainer,
    bucketIds: java.util.Set[Integer])
    extends PRValuesIterator[ColumnBatch](container, bucketIds) {

  private val templateRow: AbstractCompactExecRow = container
      .newTemplateRow().asInstanceOf[AbstractCompactExecRow]

  protected var currentVal: ColumnBatch = _

  private val startPosition = PartitionedPhysicalScan.CT_COLUMN_START
  private val endPosition = container.numColumns()

  override protected def moveNext(): Unit = {
    val row = this.templateRow
    while (itr.hasNext) {
      val rl = itr.next().asInstanceOf[RowLocation]
      val owner = itr.getHostedBucketRegion
      if (((owner ne null) || rl.isInstanceOf[NonLocalRegionEntry]) &&
          RegionEntryUtils.fillRowWithoutFaultInOptimized(container, owner,
            rl.asInstanceOf[RowLocation], row)) {
        // create the ColumnBatch
        val numRows = row.getAsInt(
          PartitionedPhysicalScan.CT_NUMROWS_POSITION, null)
        val statsData = row.getAsBytes(
          PartitionedPhysicalScan.CT_STATROW_POSITION, null)
        val buffers = new Array[ByteBuffer](endPosition - startPosition + 1)
        var position = startPosition
        while (position <= endPosition) {
          buffers(position - startPosition) = ByteBuffer.wrap(
            row.getRowBytes(position))
          position += 1
        }
        currentVal = ColumnBatch(numRows, buffers, statsData)
        return
      }
    }
    hasNextValue = false
  }
}

final class ColumnBatchIteratorOnRS(conn: Connection,
    requiredColumns: Array[String],
    stmt: Statement, rs: ResultSet, context: TaskContext)
    extends ResultSetIterator[ColumnBatch](conn, stmt, rs, context) {

  private val numCols = requiredColumns.length
  private val colBuffers = new Array[ByteBuffer](numCols)

  override protected def getCurrentValue: ColumnBatch = {
    var i = 0
    while (i < numCols) {
      colBuffers(i) = rs.getBlob(i + 1) match {
        case blob: BufferedBlob => blob.getAsBuffer
        case blob => ByteBuffer.wrap(blob.getBytes(
          1, blob.length().asInstanceOf[Int]))
      }
      i += 1
    }
    i += 1
    val numRows = rs.getInt(i)
    val statsData = rs.getBytes(i + 1)
    ColumnBatch(numRows, colBuffers, statsData)
  }
}

class ExternalStorePartitionedRDD[T: ClassTag](
    @transient val session: SparkSession,
    tableName: String, requiredColumns: Array[String],
    numPartitions: Int, store: JDBCSourceAsStore)
    extends RDD[ColumnBatch](session.sparkContext, Nil) {

  override def compute(split: Partition,
      context: TaskContext): Iterator[ColumnBatch] = {
    store.tryExecute(tableName, {
      conn =>
        val resolvedName = {
          if (tableName.indexOf(".") <= 0) {
            conn.getSchema + "." + tableName
          } else tableName
        }

        val par = split.index
        val stmt = conn.createStatement()
        val query = "select " + requiredColumns.mkString(", ") +
            s", numRows, stats from $resolvedName where bucketId = $par"
        val rs = stmt.executeQuery(query)
        new ColumnBatchIteratorOnRS(conn, requiredColumns, stmt, rs, context)
    }, closeOnSuccess = false, onExecutor = true)
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
