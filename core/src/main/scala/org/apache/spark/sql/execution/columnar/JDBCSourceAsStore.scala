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

import java.sql.{Connection, ResultSet, Statement}

import scala.language.implicitConversions

import com.gemstone.gemfire.internal.cache.{BucketRegion, LocalRegion, NonLocalRegionEntry, OffHeapRegionEntry}
import com.pivotal.gemfirexd.internal.engine.store.{CompactCompositeKey, CompactCompositeRegionKey,
GemFireContainer, OffHeapCompactExecRowWithLobs, RegionEntryUtils, RowFormatter}
import com.pivotal.gemfirexd.internal.iapi.types.{DataValueDescriptor, RowLocation, SQLInteger}

import org.apache.spark.sql.execution.PartitionedPhysicalScan
import org.apache.spark.sql.execution.row.PRValuesIterator
import org.apache.spark.{Logging, TaskContext}

abstract class ResultSetIterator[A](conn: Connection,
    stmt: Statement, rs: ResultSet, context: TaskContext)
    extends Iterator[A] with Logging {

  protected[this] final var doMove = true

  protected[this] final var hasNextValue = true

  context.addTaskCompletionListener { _ => close() }

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

  def close() {
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
    stmt: Statement, rs: ResultSet,
    context: TaskContext,
    fetchColQuery: String)
    extends ResultSetIterator[Array[Byte]](conn, stmt, rs, context) {
  var currentUUID: String = _
  val ps = conn.prepareStatement(fetchColQuery)
  var colBuffers: Option[scala.collection.mutable.HashMap[Int, Array[Byte]]] = null

  def getColumnLob(bufferPosition: Int): Array[Byte] = {
    colBuffers match {
      case Some(map) => map(bufferPosition)
      case None =>
        ps.setString(1, currentUUID)
        val colIter = ps.executeQuery()
        val bufferMap = new scala.collection.mutable.HashMap[Int, Array[Byte]]
        while(colIter.next()) {
          bufferMap.put(colIter.getInt(2), colIter.getBytes(1))
        }
        colBuffers = Some(bufferMap)
        bufferMap(bufferPosition)
    }
  }

  override protected def getCurrentValue: Array[Byte] = {
    currentUUID = rs.getString(2)
    colBuffers = None
    rs.getBytes(1)
  }

}

final class ByteArraysIteratorOnScan(container: GemFireContainer,
    bucketIds: java.util.Set[Integer])
    extends PRValuesIterator[Array[Array[Byte]]](container, bucketIds) with Logging {

  assert(!container.isOffHeap,
    s"Unexpected byte[][] iterator call for off-heap $container")

  protected var currentVal: Array[Array[Byte]] = _
  var rowFormatter: RowFormatter = _
  var currentKeyUUID: DataValueDescriptor = _
  var currentKeyPartitionId: DataValueDescriptor = _
  var currentBucketRegion: BucketRegion = _
  val baseRegion: LocalRegion = container.getRegion

  def getColumnLob(bufferPosition: Int): Array[Byte] = {
    val key = new CompactCompositeRegionKey(Array(
      currentKeyUUID, currentKeyPartitionId, new SQLInteger(bufferPosition)),
      container.getExtraTableInfo());
    val rl = if (currentBucketRegion != null) currentBucketRegion.get(key) else baseRegion.get(key)
    val value = rl.asInstanceOf[Array[Array[Byte]]]
    val rf = container.getRowFormatter(value(0))
    rf.getLob(value, PartitionedPhysicalScan.CT_BLOB_POSITION)
  }

  override protected def moveNext(): Unit = {
    while (itr.hasNext) {
      val rl = itr.next().asInstanceOf[RowLocation]
      currentBucketRegion = itr.getHostedBucketRegion
      // get the stat row region entries only. region entries for individual columns
      // will be fetched on demand
      if ((currentBucketRegion ne null) || rl.isInstanceOf[NonLocalRegionEntry]) {
        val key = rl.getKeyCopy.asInstanceOf[CompactCompositeKey]
        if (key.getKeyColumn(2).getInt ==
            JDBCSourceAsStore.STATROW_COL_INDEX) {
          val v = RegionEntryUtils.getValueWithoutFaultInOrOffHeapEntry(currentBucketRegion, rl)
          if (v ne null) {
            currentVal = v.asInstanceOf[Array[Array[Byte]]]
            currentKeyUUID = key.getKeyColumn(0)
            currentKeyPartitionId = key.getKeyColumn(1)
            rowFormatter = container.getRowFormatter(currentVal(0))
            return
          }
        }
      }
    }
    hasNextValue = false
  }
}

final class OffHeapLobsIteratorOnScan(container: GemFireContainer,
    bucketIds: java.util.Set[Integer])
    extends PRValuesIterator[OffHeapCompactExecRowWithLobs](container,
      bucketIds) {

  assert(container.isOffHeap,
    s"Unexpected off-heap iterator call for on-heap $container")

  override protected val currentVal: OffHeapCompactExecRowWithLobs = container
      .newTemplateRow().asInstanceOf[OffHeapCompactExecRowWithLobs]

  override protected def moveNext(): Unit = {
    while (itr.hasNext) {
      val rl = itr.next().asInstanceOf[RowLocation]
      val owner = itr.getHostedBucketRegion
      if ((owner ne null) || rl.isInstanceOf[NonLocalRegionEntry]) {
        val v = RegionEntryUtils.getValueWithoutFaultInOrOffHeapEntry(owner, rl)
        if ((v ne null) && (RegionEntryUtils.fillRowUsingAddress(container, owner,
          v.asInstanceOf[OffHeapRegionEntry], currentVal, false) ne null)) {
          return
        }
      }
    }
    hasNextValue = false
  }
}

object JDBCSourceAsStore {
  val STATROW_COL_INDEX = -1
}
