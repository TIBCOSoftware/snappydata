/*
 * Copyright (c) 2017-2021 TIBCO Software Inc. All rights reserved.
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

package org.apache.spark.sql.execution

import java.sql.Connection
import java.util.concurrent.ConcurrentHashMap
import java.util.function.{Consumer, Function}

import scala.collection.JavaConverters._

import com.gemstone.gemfire.SystemFailure
import com.gemstone.gemfire.cache.IsolationLevel
import com.gemstone.gemfire.internal.cache.BucketRegion.RolloverResult
import com.gemstone.gemfire.internal.cache.{DiskStoreImpl, LocalRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.ddl.catalog.GfxdSystemProcedures
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextService
import com.pivotal.gemfirexd.internal.impl.jdbc.{EmbedConnection, EmbedConnectionContext}
import io.snappydata.thrift.StatementAttrs
import io.snappydata.thrift.internal.ClientConnection

import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.ConnectionPool.SNAPSHOT_POOL_SUFFIX
import org.apache.spark.sql.execution.SnapshotConnectionListener.{parseRolloverResult, trace, warn}
import org.apache.spark.sql.execution.columnar.ConnectionType.ConnectionType
import org.apache.spark.sql.execution.columnar.impl.JDBCSourceAsColumnarStore
import org.apache.spark.sql.execution.columnar.{ConnectionType, ExternalStoreUtils}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.sources.ConnectionProperties
import org.apache.spark.util.{TaskCompletionListener, TaskFailureListener}
import org.apache.spark.{Logging, TaskContext}

/**
 * This is a TaskContext listener (for both success and failure of the task) that handles startup,
 * commit and rollback of snapshot transactions for the task. It also provides a common connection
 * that can be shared by all plans executing in the task. In conjunction with the apply methods of
 * the companion object, it ensures that only one instance of this listener is attached in a
 * TaskContext which is automatically removed at the end of the task execution.
 *
 * This is the preferred way for all plans that need connections and/or snapshot transactions so
 * that handling transaction start/commit for any level of plan nesting etc can be dealt with
 * cleanly for the entire duration of the task. Additionally cases where an EXCHANGE gets inserted
 * between two plans are also handled as expected where separate transactions and connections will
 * be used for the two plans. Both generated code and non-generated code (including RDD.compute)
 * should use the apply methods of the companion object to obtain an instance of the listener,
 * then use its connection() method to obtain the connection.
 *
 * One of the overloads of the apply method also allows one to send a custom connection creator
 * instead of using the default one, but it is also assumed to return SnappyData connection only
 * (either embedded or thin) for snapshot transactions to work. Typical usage of custom creator
 * is for smart connector RDDs to use direct URLs without load-balance to the preferred hosts
 * for the buckets being targeted instead of the default creator that will always use the locator.
 */
class SnapshotConnectionListener private(private var startSnapshotTx: Boolean,
    private val connectionType: ConnectionType, rowTable: String, connProps: ConnectionProperties,
    private var delayRollover: Boolean, creator: Either[Long, () => (Connection, Long)])
    extends TaskCompletionListener with TaskFailureListener {

  private[this] var conn: Connection = _
  private[this] var catalogVersion: Long = -1
  private[this] var lockedDiskStores: List[DiskStoreImpl] = Nil
  private[this] var compactionMetric: Option[SQLMetric] = None
  private[this] var rolloverMetric: Option[SQLMetric] = None
  private[this] val isTransactionalPool = startSnapshotTx
  private[this] var isSuccess = true

  @inline
  private def isInitialized: Boolean = conn ne null

  private def initialize(): Unit = {
    if (isInitialized) {
      throw new IllegalStateException("SnapshotConnectionListener already initialized")
    }
    creator match {
      case Left(version) =>
        catalogVersion = version
        beginSnapshotTx()
      case Right(f) =>
        val result = f()
        conn = result._1
        catalogVersion = result._2
        beginSnapshotTx()
        // custom creator may not have applied the catalogVersion and delayRollover attributes
        if (!startSnapshotTx) applyTransientAttributes()
    }
  }

  override def onTaskCompletion(context: TaskContext): Unit = {
    try {
      if (!isInitialized) return
      if (connectionType != ConnectionType.Embedded) {
        conn.unwrap(classOf[ClientConnection]).clearCommonStatementAttributes()
      }
      if (success()) {
        val results = commitSnapshotTx()
        // both local and remote node rollovers/compactions will be in the before-commit results
        rolloverMetric match {
          case Some(metric) =>
            val numRollovers = results match {
              case Left(beforeCommitResults) =>
                if (beforeCommitResults.isEmpty) 0
                else beforeCommitResults.asScala.foldLeft(0) {
                  case (s, r: RolloverResult) if r.numKeysRolledOver > 0 => s + 1
                  case (s, _) => s
                }
              case Right(str) =>
                if (str.isEmpty) 0
                else parseRolloverResult.findAllIn(str).size
            }
            metric.add(numRollovers)
          case _ =>
        }
        /*
        compactionMetric match {
          case Some(metric) =>
            val numCompacted = results match {
              case Left(beforeCommitResults) =>
                if (beforeCommitResults.isEmpty) 0
                else beforeCommitResults.asScala.foldLeft(0) {
                  case (s, CompactionResult(_, _, true)) => s + 1
                  case (s, _) => s
                }
              case Right(str) =>
                if (str.isEmpty) 0
                else parseCompactionResult.findAllIn(str).size
            }
            metric.add(numCompacted)
          case _ =>
        }
        */
      } else {
        rollbackSnapshotTx()
      }
    } finally {
      SnapshotConnectionListener.clearContext(context, this)
      try {
        if (lockedDiskStores.nonEmpty) {
          lockedDiskStores.foreach(_.releaseDiskStoreReadLock())
        }
        // reset isolation level for non-transactional pool connections
        if ((conn ne null) && !isTransactionalPool && startSnapshotTx && !conn.isClosed) {
          trace(s"Resetting isolation level for table $rowTable with $conn")
          conn.setTransactionIsolation(Connection.TRANSACTION_NONE)
        }
      } finally {
        if (conn ne null) conn.close()
      }
    }
  }

  override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
    isSuccess = false
    // trying to log when JVM is failing will cause trouble
    if (!SystemFailure.isJVMFailureError(error)) {
      warn(s"Going to rollback active transaction on $conn", error)
    }
  }

  private def doLockDiskStore(tableName: String): Unit = {
    // TODO: SW: lock disk store in Net mode too using a statement attribute through this
    // but existing locking does not seem correct since the bucket primary might be a remote node
    if (connectionType == ConnectionType.Embedded) {
      val rgn = Misc.getRegionForTable(tableName, false)
      if (rgn ne null) {
        val ds = rgn.asInstanceOf[LocalRegion].getDiskStore
        if (ds ne null) {
          if (!lockedDiskStores.contains(ds)) {
            ds.acquireDiskStoreReadLock()
            lockedDiskStores = ds :: lockedDiskStores
          }
        }
      }
    }
  }

  // begin should decide the connection which will be used by insert/commit/rollback
  private def beginSnapshotTx(): Unit = {
    val initialized = isInitialized
    if (!initialized) {
      val poolId = if (isTransactionalPool) rowTable + SNAPSHOT_POOL_SUFFIX else rowTable
      conn = ExternalStoreUtils.getConnection(poolId, connProps,
        forExecutor = true, resetIsolationLevel = false)
    }
    assert(!conn.isClosed)
    if (!startSnapshotTx) return

    trace(s"Going to start snapshot transaction for table $rowTable with $conn")
    conn.setTransactionIsolation(IsolationLevel.SNAPSHOT_JDBC_LEVEL)
    if (conn.getAutoCommit) conn.setAutoCommit(false)
    applyTransientAttributes()
    trace(s"Started snapshot transaction for table $rowTable")
  }

  private def commitSnapshotTx(): Either[java.util.List[AnyRef], String] = {
    if (!startSnapshotTx && conn.getTransactionIsolation == Connection.TRANSACTION_NONE) {
      return Left(java.util.Collections.emptyList[AnyRef])
    }

    var success = false
    var failure: Throwable = null
    try {
      trace(s"Going to commit active transaction on $conn")
      connectionType match {
        case ConnectionType.Embedded =>
          val beforeCommitResults = GfxdSystemProcedures.commitTransaction(
            conn.unwrap(classOf[EmbedConnection]))
          success = true
          Left(beforeCommitResults)
        case _ =>
          conn.commit()
          success = true
          Right(conn.unwrap(classOf[ClientConnection]).getLastCommitResults)
      }
    } catch {
      case t: Throwable if !SystemFailure.isJVMFailureError(t) => failure = t; throw t
    } finally {
      if (!success) {
        // trying to log when JVM is failing will cause trouble but we can try to rollback
        if (failure ne null) {
          warn(s"Failed to commit active transaction on $conn. Trying to rollback.", failure)
        }
        rollbackSnapshotTx()
      }
    }
  }

  private def rollbackSnapshotTx(): Unit = {
    ExternalStoreUtils.handleRollback(() => {
      connectionType match {
        case ConnectionType.Embedded =>
          conn.rollback()
          val lcc = conn.unwrap(classOf[EmbedConnection]).getLanguageConnectionContext
          if (lcc ne null) {
            lcc.clearExecuteLocally()
          }
        case _ => conn.rollback()
      }
    }, () => {
      trace(s"Finished rollback of active transaction on $conn")
    })
  }

  final def success(): Boolean = {
    isSuccess
  }

  /**
   * Apply `delayRollover` and `catalogVersion` attributes on the current context.
   */
  private def applyTransientAttributes(): Unit = {
    connectionType match {
      case ConnectionType.Embedded =>
        if (delayRollover) {
          GfxdSystemProcedures.setDelayRollover(
            conn.unwrap(classOf[EmbedConnection]).getLanguageConnection, true)
        }
      case _ =>
        conn.unwrap(classOf[ClientConnection]).updateCommonStatementAttributes(
          new Consumer[StatementAttrs] {
            override def accept(attrs: StatementAttrs): Unit = {
              if (catalogVersion != -1) attrs.setCatalogVersion(catalogVersion)
              else attrs.unsetCatalogVersion()
              if (delayRollover) attrs.setDelayRollover(delayRollover)
              else attrs.unsetDelayRollover()
            }
          })
    }
  }

  final def setMetrics(compactionMetric: SQLMetric, rolloverMetric: SQLMetric): Unit = {
    this.compactionMetric = Option(compactionMetric)
    this.rolloverMetric = Option(rolloverMetric)
  }

  final def connection: Connection = {
    if (isInitialized) return conn

    // should only happen for rollover that should have an existing temporary connection
    assert(connectionType == ConnectionType.Embedded)
    val currentCM = ContextService.getFactory.getCurrentContextManager
    if (currentCM ne null) {
      conn = EmbedConnectionContext.getEmbedConnection(currentCM)
    }
    if (isInitialized) conn
    else {
      throw new IllegalStateException("Unexpected call to SnapshotConnectionListener.connection " +
          "with empty TaskContext and no existing EmbedConnectionContext")
    }
  }
}

/**
 * This companion class is primarily to ensure that only a single listener is attached in a
 * TaskContext (e.g. delta buffer + column table scan, or putInto may try to attach twice).
 */
object SnapshotConnectionListener extends Logging {

  private[this] val contextToListener =
    new ConcurrentHashMap[TaskContext, SnapshotConnectionListener]()

  // directly match successful CompactionResults so the count of matches will suffice
  // private val parseCompactionResult =
  //  "CompactionResult\\(ColumnKey\\([^)]*\\),[0-9]*,true\\)".r

  // directly match non-zero numKeysRolledOver so the count of matches will suffice
  private val parseRolloverResult =
    "RolloverResult\\(numKeysRolledOver=0*[1-9][0-9]*,[^)]*\\)".r

  private def taskContext(context: TaskContext): TaskContext =
    if (context ne null) context else Utils.getTaskContext

  def apply(context: TaskContext, externalStore: JDBCSourceAsColumnarStore, delayRollover: Boolean,
      creator: Either[Long, () => (Connection, Long)]): SnapshotConnectionListener = {
    apply(context, startSnapshotTx = true, externalStore.connectionType, externalStore.tableName,
      Some(externalStore.tableName), externalStore.connProperties, delayRollover, creator)
  }

  def apply(context: TaskContext, startSnapshotTx: Boolean, connectionType: ConnectionType,
      rowTable: String, lockDiskStore: Option[String],
      connProps: ConnectionProperties, delayRollover: Boolean,
      creator: Either[Long, () => (Connection, Long)]): SnapshotConnectionListener = {
    // check for the temporary one in Utils too
    val useContext = taskContext(context)
    if (useContext ne null) {
      // initialization of the listener is delayed to keep the computeIfAbsent method short
      val taskListener = contextToListener.computeIfAbsent(useContext,
        new Function[TaskContext, SnapshotConnectionListener] {
          override def apply(context: TaskContext): SnapshotConnectionListener = {
            val listener = new SnapshotConnectionListener(startSnapshotTx, connectionType,
              rowTable, connProps, delayRollover, creator)
            context.addTaskCompletionListener(listener)
            context.addTaskFailureListener(listener)
            listener
          }
        })
      // there cannot be multiple threads accessing the same TaskContext so below is safe
      if (taskListener.isInitialized) {
        // existing listener may see change in attributes
        if (startSnapshotTx && !taskListener.startSnapshotTx) {
          trace(s"Enabling snapshot on a non-transactional pool for ${taskListener.connection}")
          taskListener.startSnapshotTx = true
          taskListener.delayRollover ||= delayRollover
          taskListener.beginSnapshotTx()
        }
        if (delayRollover && !taskListener.delayRollover) {
          trace(s"Changing delayRollover to true on ${taskListener.connection}")
          taskListener.delayRollover = true
          taskListener.applyTransientAttributes()
        }
      } else if (context ne null) { // skip initialization for temporary context
        taskListener.initialize()
      }
      if (lockDiskStore.isDefined) taskListener.doLockDiskStore(lockDiskStore.get)
      taskListener
    } else {
      throw new IllegalArgumentException("SnapshotConnectionListener: TaskContext was null")
    }
  }

  def getExisting(context: TaskContext): Option[SnapshotConnectionListener] = {
    val useContext = taskContext(context)
    if (useContext ne null) Option(contextToListener.get(useContext)) else None
  }

  def trace(msg: => String): Unit = super.logDebug(msg)

  def warn(msg: => String, throwable: Throwable): Unit = super.logWarning(msg, throwable)

  private[sql] def clearContext(context: TaskContext, listener: SnapshotConnectionListener): Unit =
    contextToListener.remove(context, listener)
}
