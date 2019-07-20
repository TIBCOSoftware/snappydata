/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql.execution.row

import java.lang.reflect.Field
import java.sql.{Connection, ResultSet, Statement}
import java.util.GregorianCalendar

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.gemstone.gemfire.cache.IsolationLevel
import com.gemstone.gemfire.internal.cache._
import com.gemstone.gemfire.internal.shared.ClientSharedData
import com.pivotal.gemfirexd.internal.engine.ddl.catalog.GfxdSystemProcedures
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.store.{AbstractCompactExecRow, GemFireContainer, RawStoreResultSet, RegionEntryUtils}
import com.pivotal.gemfirexd.internal.iapi.sql.conn.Authorizer
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedResultSet
import com.zaxxer.hikari.pool.ProxyResultSet

import org.apache.spark.serializer.ConnectionPropertiesSerializer
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.collection.{MultiBucketExecutorPartition, Utils}
import org.apache.spark.sql.execution.columnar.{ExternalStoreUtils, ResultSetIterator}
import org.apache.spark.sql.execution.sources.StoreDataSourceStrategy.translateToFilter
import org.apache.spark.sql.execution.{RDDKryo, SecurityUtils}
import org.apache.spark.sql.sources.JdbcExtendedUtils.quotedName
import org.apache.spark.sql.sources._
import org.apache.spark.{Partition, TaskContext, TaskContextImpl, TaskKilledException}

/**
 * A scanner RDD which is very specific to Snappy store row tables.
 * This scans row tables in parallel unlike Spark's inbuilt JDBCRDD.
 */
class RowFormatScanRDD(@transient val session: SnappySession,
    protected var tableName: String,
    protected var isPartitioned: Boolean,
    @transient private val columns: Array[String],
    var pushProjections: Boolean,
    protected var useResultSet: Boolean,
    protected var connProperties: ConnectionProperties,
    @transient private[sql] val filters: Array[Expression] = Array.empty[Expression],
    @transient protected val partitionEvaluator: () => Array[Partition] = () =>
      Array.empty[Partition], protected val partitionPruner: () => Int = () => -1,
    protected var commitTx: Boolean,
    protected var delayRollover: Boolean, protected var projection: Array[Int],
    @transient protected val region: Option[LocalRegion])
    extends RDDKryo[Any](session.sparkContext, Nil) with KryoSerializable {

  protected var filterWhereArgs: ArrayBuffer[Any] = _
  /**
   * `filters`, but as a WHERE clause suitable for injection into a SQL query.
   */
  protected var filterWhereClause: String = _

  protected def evaluateWhereClause(): Unit = {
    val numFilters = filters.length
    filterWhereClause = if (numFilters > 0) {
      val sb = new StringBuilder().append(" WHERE ")
      val args = new ArrayBuffer[Any](numFilters)
      val initLen = sb.length
      filters.foreach(translateToFilter(_) match {
        case Some(f) => compileFilter(f, sb, args, sb.length > initLen)
        case _ =>
      })
      if (args.nonEmpty) {
        filterWhereArgs = args
        sb.toString()
      } else ""
    } else ""
  }

  protected lazy val resultSetField: Field = {
    val field = classOf[ProxyResultSet].getDeclaredField("delegate")
    field.setAccessible(true)
    field
  }

  private def appendCol(sb: StringBuilder, col: String): StringBuilder =
    sb.append(Utils.toUpperCase(col))

  // below should exactly match ExternalStoreUtils.handledFilter
  private def compileFilter(f: Filter, sb: StringBuilder,
      args: ArrayBuffer[Any], addAnd: Boolean, literal: String = ""): Unit = f match {
    case EqualTo(col, value) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      appendCol(sb, col).append(" = ?")
      args += value
    case LessThan(col, value) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      appendCol(sb, col).append(" < ?")
      args += value
    case GreaterThan(col, value) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      appendCol(sb, col).append(" > ?")
      args += value
    case LessThanOrEqual(col, value) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      appendCol(sb, col).append(" <= ?")
      args += value
    case GreaterThanOrEqual(col, value) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      appendCol(sb, col).append(" >= ?")
      args += value
    case StringStartsWith(col, value) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      appendCol(sb, col).append(s" LIKE $value%")
    case In(col, values) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      appendCol(sb, col).append(" IN (")
      (1 until values.length).foreach(_ => sb.append("?,"))
      sb.append("?)")
      args ++= values
    case And(left, right) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append('(')
      compileFilter(left, sb, args, addAnd = false, "TRUE")
      sb.append(") AND (")
      compileFilter(right, sb, args, addAnd = false, "TRUE")
      sb.append(')')
    case Or(left, right) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append('(')
      compileFilter(left, sb, args, addAnd = false, "FALSE")
      sb.append(") OR (")
      compileFilter(right, sb, args, addAnd = false, "FALSE")
      sb.append(')')
    case _ => sb.append(literal)
     // no filter pushdown
  }

  /**
   * `columns`, but as a String suitable for injection into a SQL query.
   */
  protected var columnList: String = {
    if (!pushProjections) "*"
    else if (columns.length > 0) {
      val sb = new StringBuilder()
      columns.foreach { s =>
        if (sb.nonEmpty) sb.append(',')
        appendCol(sb.append('"'), s).append('"')
      }
      sb.toString()
    } else "1"
  }

  def computeResultSet(
      thePart: Partition, context: TaskContext): (Connection, Statement, ResultSet) = {
    val conn = ExternalStoreUtils.getConnection(tableName,
      connProperties, forExecutor = true)

    if (context ne null) {
      val partitionId = context.partitionId()
      context.addTaskCompletionListener { _ =>
        logDebug(s"closed connection for task from listener $partitionId")
        try {
          conn.commit()
          conn.close()
          logDebug("closed connection for task " + context.partitionId())
        } catch {
          case NonFatal(e) => logWarning("Exception closing connection", e)
        }
      }
    }

    if (isPartitioned) {
      val ps = conn.prepareStatement(
        "call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION(?, ?, ?)")
      try {
        ps.setString(1, tableName)
        val bucketString = thePart match {
          case p: MultiBucketExecutorPartition => p.bucketsString
          case _ => thePart.index.toString
        }
        ps.setString(2, bucketString)
        ps.setLong(3, -1)
        ps.executeUpdate()
      } finally {
        ps.close()
      }
    }
    val sqlText = s"SELECT $columnList FROM ${quotedName(tableName)}$filterWhereClause"
    val args = filterWhereArgs
    val stmt = conn.prepareStatement(sqlText)
    if (args ne null) {
      ExternalStoreUtils.setStatementParameters(stmt, args)
    }
    val fetchSize = connProperties.executorConnProps.getProperty("fetchSize")
    if (fetchSize ne null) {
      stmt.setFetchSize(fetchSize.toInt)
    }
    val rs = stmt.executeQuery()
    /* (hangs for some reason)
    // setup context stack for lightWeightNext calls
    val rs = stmt.executeQuery().asInstanceOf[EmbedResultSet]
    val embedConn = stmt.getConnection.asInstanceOf[EmbedConnection]
    val lcc = embedConn.getLanguageConnectionContext
    embedConn.getTR.setupContextStack()
    rs.pushStatementContext(lcc, true)
    */
    // set the delayRollover flag on current transaction
    if (delayRollover) {
      val tx = TXManagerImpl.getCurrentTXState
      if (tx ne null) {
        tx.getProxy.setColumnRolloverDisabled(true)
      }
    }
    (conn, stmt, rs)
  }


  def commitTxBeforeTaskCompletion(conn: Option[Connection], context: TaskContext): Unit = {
    Option(context).foreach(_.addTaskCompletionListener(_ => {
      val tx = TXManagerImpl.getCurrentSnapshotTXState
      if (tx != null /* && !(tx.asInstanceOf[TXStateProxy]).isClosed() */ ) {
        // if rollover was marked as delayed, then do the rollover before commit
        if (delayRollover) {
          GfxdSystemProcedures.flushLocalBuckets(tableName, false)
        }
        val txMgr = tx.getTxMgr
        txMgr.masqueradeAs(tx)
        txMgr.commit()
      }
    }))
  }

  /**
   * Runs the SQL query against the JDBC driver.
   */
  override def compute(thePart: Partition, context: TaskContext): Iterator[Any] = {

    if (pushProjections) {
      val (conn, stmt, rs) = computeResultSet(thePart, context)
      val itr = new ResultSetTraversal(conn, stmt, rs, context)
      if (commitTx) {
        commitTxBeforeTaskCompletion(Option(conn), context)
      }
      itr
    } else {
      // explicitly check authorization for the case of column table scan
      // !pushProjections && useResultSet means a column table
      if (useResultSet) {
        SecurityUtils.authorizeTableOperation(tableName, projection,
          Authorizer.SELECT_PRIV, Authorizer.SQL_SELECT_OP, connProperties)
      }
      val txManagerImpl = GemFireCacheImpl.getExisting.getCacheTransactionManager
      var tx = txManagerImpl.getTXState
      val startTX = tx eq null
      if (startTX) {
        tx = txManagerImpl.beginTX(TXManagerImpl.getOrCreateTXContext,
          IsolationLevel.SNAPSHOT, null, null)
      }
      // use iterator over CompactExecRows directly when no projection;
      // higher layer PartitionedPhysicalRDD will take care of conversion
      // or direct code generation as appropriate
      val itr = if (isPartitioned && filterWhereClause.isEmpty) {
        val container = GemFireXDUtils.getGemFireContainer(tableName, true)
        val bucketIds = thePart match {
          case p: MultiBucketExecutorPartition => p.buckets
          case _ => java.util.Collections.singleton(Int.box(thePart.index))
        }

        val txId = if (tx ne null) tx.getTransactionId else null
        val itr = new CompactExecRowIteratorOnScan(container, bucketIds, txId, context)
        if (useResultSet) {
          // row buffer of column table: wrap a result set around the scan
          val dataItr = itr.map(r =>
            if (r.hasByteArrays) r.getRowByteArrays(null) else r.getRowBytes(null): AnyRef).asJava
          val rs = new RawStoreResultSet(dataItr, container, container.getCurrentRowFormatter)
          new ResultSetTraversal(conn = null, stmt = null, rs, context)
        } else itr
      } else {
        val (conn, stmt, rs) = computeResultSet(thePart, context)
        val ers = rs match {
          case e: EmbedResultSet => e
          case p: ProxyResultSet =>
            resultSetField.get(p).asInstanceOf[EmbedResultSet]
        }
        new CompactExecRowIteratorOnRS(conn, stmt, ers, context)
      }
      // add the listener after the close listener added by iterator
      // so its invoked just before it
      if (startTX) {
        // if (commitTx) {
        commitTxBeforeTaskCompletion(None, context)
        // }
      }
      itr
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[MultiBucketExecutorPartition].hostExecutorIds
  }

  override def getPartitions: Array[Partition] = {
    // evaluate the filter clause at this point since it can change in every execution
    // (updated values in ParamLiteral will take care of updating filters)
    evaluateWhereClause()
    // use incoming partitions if provided (e.g. for collocated tables)
    var parts = partitionEvaluator()
    if (parts != null && parts.length > 0) {
      return parts
    }

    // In the case of Direct Row scan, partitionEvaluator will be always empty.
    // So, evaluating partition here again..
    parts = evaluatePartitions()
    if (parts != null && parts.length > 0) {
      return parts
    }

    region match {
      case Some(pr: PartitionedRegion) => session.snappySessionState.getTablePartitions(pr)
      case Some(dr: CacheDistributionAdvisee) => session.snappySessionState.getTablePartitions(dr)
      // system table/VTI is shown as a replicated table having a single partition
      case _ => Array(new MultiBucketExecutorPartition(0, null, 0, Nil))
    }
  }

  private def evaluatePartitions(): Array[Partition] = {
    partitionPruner() match {
      case -1 =>
        Array.empty[Partition]
      case bucketId: Int =>
        if (!session.partitionPruning) {
          Array.empty[Partition]
        } else {
          Utils.getPartitions(region.get, bucketId)
        }
    }
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    super.write(kryo, output)
    output.writeString(tableName)
    output.writeBoolean(isPartitioned)
    output.writeBoolean(pushProjections)
    output.writeBoolean(useResultSet)
    output.writeBoolean(commitTx)
    output.writeBoolean(delayRollover)

    output.writeString(columnList)
    val filterArgs = filterWhereArgs
    val len = if (filterArgs eq null) 0 else filterArgs.size
    if (len == 0) {
      output.writeVarInt(0, true)
    } else {
      var i = 0
      output.writeVarInt(len, true)
      output.writeString(filterWhereClause)
      while (i < len) {
        kryo.writeClassAndObject(output, filterArgs(i))
        i += 1
      }
    }
    if (useResultSet) {
      output.writeVarInt(projection.length, true)
      output.writeInts(projection, true)
    }
    // need connection properties only if computing ResultSet
    if (pushProjections || useResultSet || !isPartitioned || len > 0) {
      ConnectionPropertiesSerializer.write(kryo, output, connProperties)
    }
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    super.read(kryo, input)
    tableName = input.readString()
    isPartitioned = input.readBoolean()
    pushProjections = input.readBoolean()
    useResultSet = input.readBoolean()
    commitTx = input.readBoolean()
    delayRollover = input.readBoolean()

    columnList = input.readString()
    val numFilters = input.readVarInt(true)
    if (numFilters == 0) {
      filterWhereClause = ""
      filterWhereArgs = null
    } else {
      filterWhereClause = input.readString()
      filterWhereArgs = new ArrayBuffer[Any](numFilters)
      var i = 0
      while (i < numFilters) {
        filterWhereArgs += kryo.readClassAndObject(input)
        i += 1
      }
    }
    if (useResultSet) {
      val numProjections = input.readVarInt(true)
      projection = input.readInts(numProjections, true)
    }
    // read connection properties only if computing ResultSet
    if (pushProjections || useResultSet || !isPartitioned || numFilters > 0) {
      connProperties = ConnectionPropertiesSerializer.read(kryo, input)
    }
  }
}

/**
 * This does not return any valid results from result set rather caller is
 * expected to explicitly invoke ResultSet.next()/get*.
 * This is primarily intended to be used for cleanup.
 */
final class ResultSetTraversal(conn: Connection,
    stmt: Statement, val rs: ResultSet, context: TaskContext)
    extends ResultSetIterator[Void](conn, stmt, rs, context) {

  lazy val defaultCal: GregorianCalendar =
    ClientSharedData.getDefaultCleanCalendar

  override protected def getCurrentValue: Void = null
}

final class CompactExecRowIteratorOnRS(conn: Connection,
    stmt: Statement, ers: EmbedResultSet, context: TaskContext)
    extends ResultSetIterator[AbstractCompactExecRow](conn, stmt,
      ers, context) {

  override protected def getCurrentValue: AbstractCompactExecRow = {
    ers.currentRow.asInstanceOf[AbstractCompactExecRow]
  }
}

abstract class PRValuesIterator[T](container: GemFireContainer, region: LocalRegion,
    bucketIds: java.util.Set[Integer], context: TaskContext) extends Iterator[T] {

  protected type PRIterator = PartitionedRegion#PRLocalScanIterator

  protected[this] final val taskContext = context.asInstanceOf[TaskContextImpl]
  protected[this] final var hasNextValue = true
  protected[this] final var doMove = true
  // transaction started by row buffer scan should be used here
  private[this] val tx = TXManagerImpl.getCurrentSnapshotTXState
  private[execution] final val itr = createIterator(container, region, tx)

  protected def createIterator(container: GemFireContainer, region: LocalRegion,
      tx: TXStateInterface): PRIterator = if (container ne null) {
    container.getEntrySetIteratorForBucketSet(
      bucketIds.asInstanceOf[java.util.Set[Integer]], null, tx, 0,
      false, true).asInstanceOf[PRIterator]
  } else if (region ne null) {
    region.getDataView(tx).getLocalEntriesIterator(
      bucketIds.asInstanceOf[java.util.Set[Integer]], false, false, true,
      region, true).asInstanceOf[PRIterator]
  } else null

  protected[sql] def currentVal: T

  protected[sql] def moveNext(): Unit

  override final def hasNext: Boolean = {
    if (doMove) {
      // check for task killed before moving to next element
      if ((taskContext ne null) && taskContext.isInterrupted()) {
        throw new TaskKilledException
      }
      moveNext()
      doMove = false
    }
    hasNextValue
  }

  override final def next: T = {
    if (doMove) {
      // check for task killed before moving to next element
      if ((taskContext ne null) && taskContext.isInterrupted()) {
        throw new TaskKilledException
      }
      moveNext()
    }
    doMove = true
    currentVal
  }
}

final class CompactExecRowIteratorOnScan(container: GemFireContainer,
    bucketIds: java.util.Set[Integer], txId: TXId, context: TaskContext)
    extends PRValuesIterator[AbstractCompactExecRow](container,
      region = null, bucketIds, context) {

  override protected[sql] val currentVal: AbstractCompactExecRow = container
      .newTemplateRow().asInstanceOf[AbstractCompactExecRow]

  override protected[sql] def moveNext(): Unit = {
    val itr = this.itr
    while (itr.hasNext) {
      val rl = itr.next()
      val owner = itr.getHostedBucketRegion
      if (((owner ne null) || rl.isInstanceOf[NonLocalRegionEntry]) &&
          RegionEntryUtils.fillRowWithoutFaultInOptimized(container, owner,
            rl.asInstanceOf[RowLocation], currentVal)) {
        return
      }
    }
    hasNextValue = false
  }
}
