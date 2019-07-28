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
package org.apache.spark.sql.execution.columnar.impl

import java.nio.ByteBuffer
import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.util.Collections

import scala.annotation.meta.param
import scala.util.Random
import scala.util.control.NonFatal

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.gemstone.gemfire.cache.IsolationLevel
import com.gemstone.gemfire.internal.cache.{BucketRegion, CachePerfStats, GemFireCacheImpl, LocalRegion, PartitionedRegion, TXManagerImpl}
import com.gemstone.gemfire.internal.shared.{BufferAllocator, SystemProperties}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.ddl.catalog.GfxdSystemProcedures
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextService
import com.pivotal.gemfirexd.internal.impl.jdbc.{EmbedConnection, EmbedConnectionContext}
import io.snappydata.impl.SmartConnectorRDDHelper
import io.snappydata.sql.catalog.SmartConnectorHelper
import io.snappydata.thrift.StatementAttrs
import io.snappydata.thrift.internal.{ClientBlob, ClientPreparedStatement, ClientStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.{ConnectionPropertiesSerializer, KryoSerializerPool, StructTypeSerializer}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.collection._
import org.apache.spark.sql.execution.columnar._
import org.apache.spark.sql.execution.columnar.encoding.ColumnDeleteDelta
import org.apache.spark.sql.execution.row.{ResultSetTraversal, RowFormatScanRDD, RowInsertExec}
import org.apache.spark.sql.execution.sources.StoreDataSourceStrategy.translateToFilter
import org.apache.spark.sql.execution.{BufferedRowIterator, ConnectionPool, RDDKryo, WholeStageCodegenExec}
import org.apache.spark.sql.sources.JdbcExtendedUtils.quotedName
import org.apache.spark.sql.sources.{ConnectionProperties, JdbcExtendedUtils}
import org.apache.spark.sql.store.CodeGeneration
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SnappySession, SparkSession}
import org.apache.spark.util.TaskCompletionListener
import org.apache.spark.{Partition, TaskContext, TaskKilledException}

/**
 * Column Store implementation for GemFireXD.
 */
class JDBCSourceAsColumnarStore(private var _connProperties: ConnectionProperties,
    var numPartitions: Int, private var _tableName: String, var schema: StructType)
    extends ExternalStore with KryoSerializable {

  self =>

  override final def tableName: String = _tableName

  override def withTable(tableName: String, numPartitions: Int): ExternalStore =
    new JDBCSourceAsColumnarStore(connProperties, numPartitions, tableName, schema)

  override final def connProperties: ConnectionProperties = _connProperties

  private lazy val connectionType = ExternalStoreUtils.getConnectionType(
    connProperties.dialect)

  override def write(kryo: Kryo, output: Output): Unit = {
    ConnectionPropertiesSerializer.write(kryo, output, _connProperties)
    output.writeInt(numPartitions)
    output.writeString(_tableName)
    StructTypeSerializer.write(kryo, output, schema)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    _connProperties = ConnectionPropertiesSerializer.read(kryo, input)
    numPartitions = input.readInt()
    _tableName = input.readString()
    schema = StructTypeSerializer.read(kryo, input, c = null)
  }

  private def checkTaskCancellation(): Unit = {
    val context = TaskContext.get()
    if ((context ne null) && context.isInterrupted()) {
      throw new TaskKilledException
    }
  }

  override def storeColumnBatch(columnTableName: String, batch: ColumnBatch,
      partitionId: Int, batchId: Long, maxDeltaRows: Int,
      compressionCodecId: Int, conn: Option[Connection]): Unit = {
    // check for task cancellation before further processing
    checkTaskCancellation()
    if (partitionId >= 0) {
      doInsertOrPut(columnTableName, batch, batchId, partitionId, maxDeltaRows,
        compressionCodecId, conn)
    } else {
      val (bucketId, br, batchSize) = getPartitionID(columnTableName,
        () => batch.buffers.foldLeft(0L)(_ + _.capacity()))
      try {
        doInsertOrPut(columnTableName, batch, batchId, bucketId, maxDeltaRows,
          compressionCodecId, conn)
      } finally br match {
        case None =>
        case Some(bucket) => bucket.updateInProgressSize(-batchSize)
      }
    }
  }

  def beginTxSmartConnector(delayRollover: Boolean, catalogVersion: Long): Array[_ <: Object] = {
    val txIdConnArray = beginTx(delayRollover)
    val conn: Connection = txIdConnArray(0).asInstanceOf[Connection]
    ExternalStoreUtils.setSchemaVersionOnConnection(catalogVersion, conn)
    txIdConnArray
  }

  // begin should decide the connection which will be used by insert/commit/rollback
  def beginTx(delayRollover: Boolean): Array[_ <: Object] = {
    val conn = self.getConnection(tableName, onExecutor = true)

    assert(!conn.isClosed)
    tryExecute(tableName, closeOnSuccessOrFailure = false, onExecutor = true) {
      conn: Connection => {
        connectionType match {
          case ConnectionType.Embedded =>
            val rgn = Misc.getRegionForTable(
              JdbcExtendedUtils.toUpperCase(tableName), true).asInstanceOf[LocalRegion]
            val ds = rgn.getDiskStore
            if (ds != null) {
              ds.acquireDiskStoreReadLock()
            }
            val context = TXManagerImpl.currentTXContext()
            if (context == null ||
                (context.getSnapshotTXState == null && context.getTXState == null)) {
              val txMgr = Misc.getGemFireCache.getCacheTransactionManager
              val tx = txMgr.beginTX(TXManagerImpl.getOrCreateTXContext(),
                IsolationLevel.SNAPSHOT, null, null)
              tx.setColumnRolloverDisabled(delayRollover)
              Array(conn, txMgr.getTransactionId.stringFormat())
            } else {
              Array(conn, null)
            }
          case _ =>
            val txId = SmartConnectorHelper.snapshotTxIdForWrite.get
            if (txId == null) {
              logDebug(s"Going to start the transaction on server on conn $conn ")
              val startAndGetSnapshotTXId = conn.prepareCall(s"call sys.START_SNAPSHOT_TXID(?,?)")
              startAndGetSnapshotTXId.setBoolean(1, delayRollover)
              startAndGetSnapshotTXId.registerOutParameter(2, java.sql.Types.VARCHAR)
              startAndGetSnapshotTXId.execute()
              val txid = startAndGetSnapshotTXId.getString(2)
              startAndGetSnapshotTXId.close()
              SmartConnectorHelper.snapshotTxIdForWrite.set(txid)
              logDebug(s"The snapshot tx id is $txid and tablename is $tableName")
              Array(conn, txid)
            } else {
              logDebug(s"Going to use the transaction $txId on server on conn $conn ")
              // it should always be not null.
              if (!txId.isEmpty) {
                val statement = conn.createStatement()
                statement.execute(
                  s"call sys.USE_SNAPSHOT_TXID('$txId')")
              }
              Array(conn, null)
            }
        }
      }
    }(Some(conn))
  }

  def commitTx(txId: String, delayRollover: Boolean, conn: Option[Connection]): Unit = {
    tryExecute(tableName, closeOnSuccessOrFailure = false, onExecutor = true)(conn => {
      var success = false
      try {
        connectionType match {
          case ConnectionType.Embedded =>
            // if rollover was marked as delayed, then do the rollover before commit
            if (delayRollover) {
              GfxdSystemProcedures.flushLocalBuckets(tableName, false)
            }
            val rgn = Misc.getRegionForTable(
              JdbcExtendedUtils.toUpperCase(tableName), true).asInstanceOf[LocalRegion]
            try {
              Misc.getGemFireCache.getCacheTransactionManager.commit()
            } finally {
              val ds = rgn.getDiskStore
              if (ds != null) {
                ds.releaseDiskStoreReadLock()
              }
            }
          case _ =>
            logDebug(s"Going to commit $txId the transaction on server conn is $conn")
            val ps = conn.prepareStatement(s"call sys.COMMIT_SNAPSHOT_TXID(?,?)")
            ps.setString(1, if (txId ne null) txId else "")
            ps.setString(2, if (delayRollover) tableName else "")
            try {
              ps.executeUpdate()
              logDebug(s"The txid being committed is $txId")
            }
            finally {
              ps.close()
              SmartConnectorHelper.snapshotTxIdForWrite.set(null)
              logDebug(s"Committed $txId the transaction on server ")
            }
        }
        success = true
      } finally {
        try {
          if (!success && !conn.isClosed) {
            handleRollback(conn.rollback)
          }
        } finally {
          conn.close()
        }
      }
    })(conn)
  }

  def rollbackTx(txId: String, conn: Option[Connection]): Unit = {
    // noinspection RedundantDefaultArgument
    tryExecute(tableName, closeOnSuccessOrFailure = false, onExecutor = true) {
      conn: Connection => {
        connectionType match {
          case ConnectionType.Embedded =>
            val rgn = Misc.getRegionForTable(
              JdbcExtendedUtils.toUpperCase(tableName), true).asInstanceOf[LocalRegion]
            try {
              Misc.getGemFireCache.getCacheTransactionManager.rollback()
            } finally {
              val ds = rgn.getDiskStore
              if (ds != null) {
                ds.releaseDiskStoreReadLock()
              }
            }

          case _ =>
            logDebug(s"Going to rollback transaction $txId on server using $conn")
            var ps: PreparedStatement = null
            handleRollback(() => {
              ps = conn.prepareStatement(s"call sys.ROLLBACK_SNAPSHOT_TXID(?)")
              ps.setString(1, if (txId ne null) txId else "")
              ps.executeUpdate()
              logDebug(s"The transaction ID being rolled back is $txId")
              ps.close()
            }, () => {
              SmartConnectorHelper.snapshotTxIdForWrite.set(null)
              logDebug(s"Rolled back $txId the transaction on server ")
              if (!conn.isClosed) conn.close()
            })
        }
      }
    }(conn)
  }

  override def storeDelete(columnTableName: String, buffer: ByteBuffer, partitionId: Int,
      batchId: Long, compressionCodecId: Int, conn: Option[Connection]): Unit = {
    // check for task cancellation before further processing
    checkTaskCancellation()
    val value = new ColumnDeleteDelta(buffer, compressionCodecId, isCompressed = false)
    connectionType match {
      case ConnectionType.Embedded =>
        val region = Misc.getRegionForTable[ColumnFormatKey, ColumnFormatValue](
          columnTableName, true)
        val key = new ColumnFormatKey(batchId, partitionId,
          ColumnFormatEntry.DELETE_MASK_COL_INDEX)

        // check for full batch delete
        if (ColumnDelta.checkBatchDeleted(buffer)) {
          ColumnDelta.deleteBatch(key, region, columnTableName)
          return
        }
        region.put(key, value)

      case _ =>
        tryExecute(columnTableName, closeOnSuccessOrFailure = false,
          onExecutor = true) { connection =>

          // check for full batch delete
          if (ColumnDelta.checkBatchDeleted(buffer)) {
            val deleteStr = s"delete from ${quotedName(columnTableName)} where " +
                "uuid = ? and partitionId = ? and columnIndex = ?"
            val stmt = connection.prepareStatement(deleteStr)
            try {
              def addKeyToBatch(columnIndex: Int): Unit = {
                stmt.setLong(1, batchId)
                stmt.setInt(2, partitionId)
                stmt.setInt(3, columnIndex)
                stmt.addBatch()
              }
              // find the number of columns in the table
              val tableName = this.tableName
              val (schemaName, name) = tableName.indexOf('.') match {
                case -1 => (null, tableName)
                case index => (tableName.substring(0, index), tableName.substring(index + 1))
              }
              val rs = connection.getMetaData.getColumns(null, schemaName, name, "%")
              var numColumns = 0
              while (rs.next()) {
                numColumns += 1
              }
              rs.close()
              // add the stats rows
              addKeyToBatch(ColumnFormatEntry.STATROW_COL_INDEX)
              addKeyToBatch(ColumnFormatEntry.DELTA_STATROW_COL_INDEX)
              // add column values and deltas
              for (columnIndex <- 1 to numColumns) {
                addKeyToBatch(columnIndex)
                for (depth <- 0 until ColumnDelta.MAX_DEPTH) {
                  addKeyToBatch(ColumnDelta.deltaColumnIndex(
                    columnIndex - 1 /* zero based */ , depth))
                }
              }
              // lastly the delete delta row itself
              addKeyToBatch(ColumnFormatEntry.DELETE_MASK_COL_INDEX)
              stmt.executeBatch()
            } finally {
              stmt.close()
            }
            return
          }

          val deleteStr = getRowInsertOrPutStr(columnTableName, isPut = true)
          val stmt = connection.prepareStatement(deleteStr)
          var blob: ClientBlob = null
          try {
            stmt.setLong(1, batchId)
            stmt.setInt(2, partitionId)
            stmt.setInt(3, ColumnFormatEntry.DELETE_MASK_COL_INDEX)
            // wrap ColumnDelete to compress transparently in socket write if required
            blob = new ClientBlob(value)
            stmt.setBlob(4, blob)
            stmt.execute()
          } finally {
            // free the blob
            if (blob != null) {
              try {
                blob.free()
              } catch {
                case NonFatal(_) => // ignore
              }
            }
            stmt.close()
          }
        }(conn)
    }
  }

  def closeConnection(c: Option[Connection]): Unit = {
    c match {
      case Some(conn) if !conn.isClosed =>
        connectionType match {
          case ConnectionType.Embedded =>
            if (!conn.isInstanceOf[EmbedConnection]) {
              conn.commit()
              conn.close()
            }
          case _ =>
            // conn commit removed txState from the conn context.
            conn.commit()
            conn.close()
        }
      case _ => // Do nothing
    }
  }

  /**
   * Insert the base entry and n column entries in Snappy. Insert the base entry
   * in the end to ensure that the partial inserts of a cached batch are ignored
   * during iteration. We are not cleaning up the partial inserts of cached
   * batches for now.
   */
  private def doSnappyInsertOrPut(region: LocalRegion, batch: ColumnBatch,
      batchId: Long, partitionId: Int, maxDeltaRows: Int, compressionCodecId: Int): Unit = {
    val deltaUpdate = batch.deltaIndexes ne null
    val statRowIndex = if (deltaUpdate) ColumnFormatEntry.DELTA_STATROW_COL_INDEX
    else ColumnFormatEntry.STATROW_COL_INDEX
    var index = 1
    try {
      // add key-values pairs for each column (linked map to ensure stats row is last)
      val keyValues = new java.util.LinkedHashMap[ColumnFormatKey, ColumnFormatValue](
        batch.buffers.length + 1)
      batch.buffers.foreach { buffer =>
        val columnIndex = if (deltaUpdate) batch.deltaIndexes(index - 1) else index
        val key = new ColumnFormatKey(batchId, partitionId, columnIndex)
        val value = if (deltaUpdate) {
          new ColumnDelta(buffer, compressionCodecId, isCompressed = false)
        } else new ColumnFormatValue(buffer, compressionCodecId, isCompressed = false)
        keyValues.put(key, value)
        index += 1
      }
      // add the stats row
      val key = new ColumnFormatKey(batchId, partitionId, statRowIndex)
      val allocator = Misc.getGemFireCache.getBufferAllocator
      val statsBuffer = SharedUtils.createStatsBuffer(batch.statsData, allocator)
      val value = if (deltaUpdate) {
        new ColumnDelta(statsBuffer, compressionCodecId, isCompressed = false)
      } else new ColumnFormatValue(statsBuffer, compressionCodecId, isCompressed = false)
      keyValues.put(key, value)

      // do a putAll of the key-value map with create=true
      val startPut = CachePerfStats.getStatTime
      val putAllOp = region.newPutAllOperation(keyValues)
      if (putAllOp ne null) {
        putAllOp.getBaseEvent.setCreate(true)
        region.basicPutAll(keyValues, putAllOp, null)
      }
      region.getCachePerfStats.endPutAll(startPut)
    } catch {
      case NonFatal(e) =>
        // no explicit rollback needs to be done with snapshot
        logInfo(s"Region insert/put failed with exception $e")
        throw e
    }
  }

  /**
   * Insert the base entry and n column entries in Snappy. Insert the base entry
   * in the end to ensure that the partial inserts of a cached batch are ignored
   * during iteration. We are not cleaning up the partial inserts of cached
   * batches for now.
   */
  private def doGFXDInsertOrPut(columnTableName: String, batch: ColumnBatch,
      batchId: Long, partitionId: Int, maxDeltaRows: Int,
      compressionCodecId: Int): Connection => Unit = {
    {
      connection: Connection => {
        val deltaUpdate = batch.deltaIndexes ne null
        // we are using the same connection on which tx was started.
        val rowInsertStr = getRowInsertOrPutStr(columnTableName, deltaUpdate)
        val stmt = connection.prepareStatement(rowInsertStr)
        val statRowIndex = if (deltaUpdate) ColumnFormatEntry.DELTA_STATROW_COL_INDEX
        else ColumnFormatEntry.STATROW_COL_INDEX
        var index = 1
        var blobs: Array[ClientBlob] = null
        try {
          // add the columns
          blobs = batch.buffers.map(buffer => {
            val columnIndex = if (deltaUpdate) batch.deltaIndexes(index - 1) else index
            stmt.setLong(1, batchId)
            stmt.setInt(2, partitionId)
            stmt.setInt(3, columnIndex)
            // wrap in ColumnFormatValue to compress transparently in socket write if required
            val value = if (deltaUpdate) {
              new ColumnDelta(buffer, compressionCodecId, isCompressed = false)
            } else new ColumnFormatValue(buffer, compressionCodecId, isCompressed = false)
            val blob = new ClientBlob(value)
            stmt.setBlob(4, blob)
            index += 1
            stmt.addBatch()
            blob
          })
          // add the stat row
          stmt.setLong(1, batchId)
          stmt.setInt(2, partitionId)
          stmt.setInt(3, statRowIndex)
          val allocator = GemFireCacheImpl.getCurrentBufferAllocator
          val statsBuffer = SharedUtils.createStatsBuffer(batch.statsData, allocator)
          // wrap in ColumnFormatValue to compress transparently in socket write if required
          val value = if (deltaUpdate) {
            new ColumnDelta(statsBuffer, compressionCodecId, isCompressed = false)
          } else new ColumnFormatValue(statsBuffer, compressionCodecId, isCompressed = false)
          stmt.setBlob(4, new ClientBlob(value))
          stmt.addBatch()

          stmt.executeBatch()
          stmt.close()
        } catch {
          case NonFatal(e) =>
            // no explicit rollback needs to be done with snapshot
            logInfo(s"Connector insert/put failed with exception $e")
            throw e
        } finally {
          // free the blobs
          if (blobs != null) {
            for (blob <- blobs) {
              try {
                blob.free()
              } catch {
                case NonFatal(_) => // ignore
              }
            }
          }
        }
      }
    }
  }

  protected def getRowInsertOrPutStr(tableName: String, isPut: Boolean): String = {
    if (isPut) s"put into ${quotedName(tableName)} values (?, ?, ?, ?)"
    else s"insert into ${quotedName(tableName)} values (?, ?, ?, ?)"
  }

  override def getConnection(id: String, onExecutor: Boolean): Connection = {
    connectionType match {
      case ConnectionType.Embedded if onExecutor =>
        val currentCM = ContextService.getFactory.getCurrentContextManager
        if (currentCM ne null) {
          val conn = EmbedConnectionContext.getEmbedConnection(currentCM)
          if (conn ne null) return conn
        }
      case _ => // get pooled connection
    }

    val connProps = if (onExecutor) connProperties.executorConnProps
    else connProperties.connProps
    ConnectionPool.getPoolConnection(id, connProperties.dialect,
      connProperties.poolProps, connProps, connProperties.hikariCP)
  }

  override def getConnectedExternalStore(table: String,
      onExecutor: Boolean): ConnectedExternalStore =
    new JDBCSourceAsColumnarStore(connProperties, numPartitions, tableName, schema)
        with ConnectedExternalStore {

      @transient protected[this] override val connectedInstance: Connection =
        self.getConnection(table, onExecutor)

      override def withTable(tableName: String, numPartitions: Int): ExternalStore =
        throw new UnsupportedOperationException(s"withTable unexpected for ConnectedExternalStore")
    }

  override def getColumnBatchRDD(tableName: String,
      rowBuffer: String,
      projection: Array[Int],
      filters: Array[Expression],
      prunePartitions: () => Int,
      session: SparkSession,
      schema: StructType,
      delayRollover: Boolean): RDD[Any] = {
    val snappySession = session.asInstanceOf[SnappySession]
    connectionType match {
      case ConnectionType.Embedded =>
        new ColumnarStorePartitionedRDD(snappySession, tableName, projection,
          filters, (filters eq null) || filters.length == 0, prunePartitions, this)
      case _ =>
        // remove the url property from poolProps since that will be
        // partition-specific
        val poolProps = connProperties.poolProps -
            (if (connProperties.hikariCP) "jdbcUrl" else "url")

        val catalog = snappySession.externalCatalog
        val (rowSchema, rowTable) = JdbcExtendedUtils.getTableWithSchema(rowBuffer,
          conn = null, Some(snappySession))
        val relationInfo = catalog.getRelationInfo(rowSchema, rowTable, isRowTable = false)._1
        new SmartConnectorColumnRDD(snappySession, tableName, projection, filters,
          ConnectionProperties(connProperties.url,
            connProperties.driver, connProperties.dialect, poolProps,
            connProperties.connProps, connProperties.executorConnProps,
            connProperties.hikariCP), schema, store = this, allParts = relationInfo.partitions,
          prunePartitions, relationInfo.catalogSchemaVersion, delayRollover)
    }
  }

  private def doInsertOrPut(columnTableName: String, batch: ColumnBatch, batchId: Long,
      partitionId: Int, maxDeltaRows: Int, compressionCodecId: Int,
      conn: Option[Connection] = None): Unit = {
    // split the batch and put into row buffer if it is small
    if (maxDeltaRows > 0 && batch.numRows < math.min(maxDeltaRows,
      math.max(maxDeltaRows >>> 1, SystemProperties.SNAPPY_MIN_COLUMN_DELTA_ROWS))) {
      // noinspection RedundantDefaultArgument
      tryExecute(tableName, closeOnSuccessOrFailure = false /* batch.deltaIndexes ne null */ ,
        onExecutor = true)(doRowBufferPut(batch, partitionId))(conn)
    } else {
      connectionType match {
        case ConnectionType.Embedded =>
          val region = Misc.getRegionForTable(columnTableName, true)
              .asInstanceOf[PartitionedRegion]
          // create UUID if not present using the row buffer region because
          // all other callers (ColumnFormatEncoder, BucketRegion) use the same
          val uuid = if (BucketRegion.isValidUUID(batchId)) batchId
          else region.getColocatedWithRegion.newUUID(false)
          doSnappyInsertOrPut(region, batch, uuid, partitionId, maxDeltaRows, compressionCodecId)

        case _ =>
          // noinspection RedundantDefaultArgument
          tryExecute(tableName, closeOnSuccessOrFailure = false /* batch.deltaIndexes ne null */ ,
            onExecutor = true)(doGFXDInsertOrPut(columnTableName, batch, batchId, partitionId,
            maxDeltaRows, compressionCodecId))(conn)
      }
    }
  }

  private def doRowBufferPut(batch: ColumnBatch,
      partitionId: Int): Connection => Unit = {
    connection: Connection => {
      val gen = CodeGeneration.compileCode(
        tableName + ".columnTable.decompress", schema.fields, () => {
          val schemaAttrs = schema.toAttributes
          val tableScan = ColumnTableScan(schemaAttrs, dataRDD = null,
            otherRDDs = Nil, numBuckets = -1,
            partitionColumns = Nil, partitionColumnAliases = Nil,
            baseRelation = null, schema, allFilters = Nil, schemaAttrs,
            caseSensitive = true)
          val insertPlan = RowInsertExec(tableScan, putInto = true,
            Nil, Nil, numBuckets = -1, isPartitioned = false, schema,
            None, onExecutor = true, tableName, connProperties)
          // now generate the code with the help of WholeStageCodegenExec
          // this is only used for local code generation while its RDD
          // semantics and related methods are all ignored
          val (ctx, code) = ExternalStoreUtils.codeGenOnExecutor(
            WholeStageCodegenExec(insertPlan), insertPlan)
          val references = ctx.references
          // also push the index of connection reference at the end which
          // will be used below to update connection before execution
          references += insertPlan.connRef
          (code, references.toArray)
        })
      val refs = gen._2.clone()
      // set the connection object for current execution
      val connectionRef = refs(refs.length - 1).asInstanceOf[Int]
      refs(connectionRef) = connection
      // no harm in passing a references array with extra element at end
      val iter = gen._1.generate(refs).asInstanceOf[BufferedRowIterator]
      // put the single ColumnBatch in the iterator read by generated code
      iter.init(partitionId, Array(Iterator[Any](new ResultSetTraversal(
        conn = null, stmt = null, rs = null, context = null),
        ColumnBatchIterator(batch)).asInstanceOf[Iterator[InternalRow]]))
      // ignore the result which is the update count
      while (iter.hasNext) {
        iter.next()
      }
      // release the batch buffers
      batch.buffers.foreach(b => if (b ne null) BufferAllocator.releaseBuffer(b))
    }
  }

  private def getInProgressBucketSize(br: BucketRegion, shift: Int): Long =
    (br.getTotalBytes + br.getInProgressSize) >> shift

  // use the same saved connection for all operation
  private def getPartitionID(columnTableName: String,
      getBatchSizeInBytes: () => Long): (Int, Option[BucketRegion], Long) = {
    connectionType match {
      case ConnectionType.Embedded =>
        val region = Misc.getRegionForTable(columnTableName, true).asInstanceOf[LocalRegion]
        region match {
          case pr: PartitionedRegion =>
            pr.synchronized {
              val primaryBuckets = pr.getDataStore.getAllLocalPrimaryBucketRegions
              // if no local primary bucket, then select a random bucket
              if (primaryBuckets.isEmpty) {
                (Random.nextInt(pr.getTotalNumberOfBuckets), None, 0L)
              } else {
                // select the bucket with smallest size at this point
                val iterator = primaryBuckets.iterator()
                // for heap buffer, round off to nearest 8k to avoid tiny
                // size changes from effecting the minimum selection else
                // round off to 32 for off-heap where memory bytes has only
                // the entry+key overhead (but overflow bytes have data too)
                val shift = if (GemFireCacheImpl.hasNewOffHeap) 5 else 13
                assert(iterator.hasNext)
                var smallestBucket = iterator.next()
                var minBucketSize = getInProgressBucketSize(smallestBucket, shift)
                while (iterator.hasNext) {
                  val bucket = iterator.next()
                  val bucketSize = getInProgressBucketSize(bucket, shift)
                  if (bucketSize < minBucketSize ||
                      (bucketSize == minBucketSize && Random.nextBoolean())) {
                    smallestBucket = bucket
                    minBucketSize = bucketSize
                  }
                }
                val batchSize = getBatchSizeInBytes()
                // update the in-progress size of the chosen bucket
                smallestBucket.updateInProgressSize(batchSize)
                (smallestBucket.getId, Some(smallestBucket), batchSize)
              }
            }
          case _ => (-1, None, 0L)
        }
      // TODO: SW: for split mode, get connection to one of the
      // local servers and a bucket ID for only one of those
      case _ => (Random.nextInt(numPartitions), None, 0L)
    }
  }

  override def toString: String = s"ColumnarStore[$tableName, partitions=$numPartitions, " +
      s"connectionProperties=$connProperties, schema=$schema]"
}

final class ColumnarStorePartitionedRDD(
    @transient private val session: SnappySession,
    private var tableName: String,
    private var projection: Array[Int],
    @transient private[sql] val filters: Array[Expression],
    private[sql] var fullScan: Boolean,
    @(transient @param) partitionPruner: () => Int,
    @transient private val store: JDBCSourceAsColumnarStore)
    extends RDDKryo[Any](session.sparkContext, Nil) with KryoSerializable {

  private[this] var allPartitions: Array[Partition] = _
  private val evaluatePartitions: () => Array[Partition] = () => {
    val region = Misc.getRegionForTable(tableName, true)
    partitionPruner() match {
      case -1 if allPartitions != null =>
        allPartitions
      case -1 =>
        allPartitions = session.sessionState.getTablePartitions(
          region.asInstanceOf[PartitionedRegion])
        allPartitions
      case bucketId: Int =>
        if (!session.partitionPruning) {
          allPartitions = session.sessionState.getTablePartitions(
            region.asInstanceOf[PartitionedRegion])
          allPartitions
        } else {
          Utils.getPartitions(region, bucketId)
        }
    }
  }

  override def compute(part: Partition, context: TaskContext): Iterator[Any] = {

    Option(context).foreach(_.addTaskCompletionListener(_ => {
      val tx = TXManagerImpl.getCurrentSnapshotTXState
      if (tx != null /* && !(tx.asInstanceOf[TXStateProxy]).isClosed() */ ) {
        val cache = Misc.getGemFireCacheNoThrow
        if (cache ne null) {
          val txMgr = cache.getCacheTransactionManager
          txMgr.masqueradeAs(tx)
          txMgr.commit()
        }
      }
    }))

    // TODO: maybe we can start tx here
    // We can start different tx in each executor for first phase,
    // till global snapshot is available.
    // check if the tx is already running.
    // val txMgr = GemFireCacheImpl.getExisting.getCacheTransactionManager
    // txMgr.begin()
    // who will call commit.
    // val txId = txMgr.getTransactionId
    val bucketIds = part match {
      case p: MultiBucketExecutorPartition => p.buckets
      case _ => Collections.singleton(Int.box(part.index))
    }
    // val container = GemFireXDUtils.getGemFireContainer(tableName, true)
    // ColumnBatchIterator(container, bucketIds)
    val r = Misc.getRegionForTable(tableName, true).asInstanceOf[LocalRegion]
    ColumnBatchIterator(r, bucketIds, projection, fullScan, context)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[MultiBucketExecutorPartition].hostExecutorIds
  }

  override protected def getPartitions: Array[Partition] = {
    evaluatePartitions()
  }

  def getPartitionEvaluator: () => Array[Partition] = evaluatePartitions

  override def write(kryo: Kryo, output: Output): Unit = {
    super.write(kryo, output)
    output.writeString(tableName)
    output.writeInt(projection.length)
    output.writeInts(projection)
    output.writeBoolean(fullScan)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    super.read(kryo, input)
    tableName = input.readString()
    val numProjections = input.readInt
    projection = input.readInts(numProjections)
    fullScan = input.readBoolean()
  }
}

final class SmartConnectorColumnRDD(
    @transient private val session: SnappySession,
    private var tableName: String,
    private var projection: Array[Int],
    @transient private[sql] val filters: Array[Expression],
    private var connProperties: ConnectionProperties,
    private var schema: StructType,
    @transient private val store: ExternalStore,
    @transient private val allParts: Array[Partition],
    @(transient @param) partitionPruner: () => Int,
    private var catalogSchemaVersion: Long,
    private var delayRollover: Boolean)
    extends RDDKryo[Any](session.sparkContext, Nil) with KryoSerializable {

  private var serializedFilters: Array[Byte] = _

  private var preferHostName = SmartConnectorHelper.preferHostName(session)

  override def compute(split: Partition,
      context: TaskContext): Iterator[ByteBuffer] = {
    val helper = new SmartConnectorRDDHelper
    val part = split.asInstanceOf[SmartExecutorBucketPartition]
    val (conn, txId) = helper.getConnectionAndTXId(connProperties, part, preferHostName)
    logDebug(s"Scan for $tableName, Partition index = ${part.index}, bucketId = ${part.bucketId}")
    val partitionId = part.bucketId
    var itr: Iterator[ByteBuffer] = null
    try {
      // fetch all the column blobs pushing down the filters
      val (statement, rs) = helper.prepareScan(conn, txId,
        tableName, projection, serializedFilters, part, catalogSchemaVersion)
      itr = new ColumnBatchIteratorOnRS(conn, projection, statement, rs,
        context, partitionId)
    } finally {
      if (context ne null) {
        context.addTaskCompletionListener { _ =>
          logDebug(s"The txid going to be committed is $txId " + tableName)

          // if ((txId ne null) && !txId.equals("null")) {
          val ps = conn.prepareStatement(s"call sys.COMMIT_SNAPSHOT_TXID(?,?)")
          ps.setString(1, if (txId ne null) txId else "")
          ps.setString(2, if (delayRollover) tableName else "")
          ps.executeUpdate()
          logDebug(s"The txid being committed is $txId")
          ps.close()
          SmartConnectorHelper.snapshotTxIdForRead.set(null)
          logDebug(s"closed connection for task from listener $partitionId")
          try {
            conn.close()
            logDebug("closed connection for task " + context.partitionId())
          } catch {
            case NonFatal(e) => logWarning("Exception closing connection", e)
          }
          // }
        }
      }
    }
    itr
  }

  private def serializeFilters(filters: Array[Expression]): Array[Byte] = {
    // serialize the filters
    if ((filters ne null) && filters.length > 0) {
      // ship as source Filters which is public API for multiple version compatibility
      val srcFilters = filters.flatMap(translateToFilter)
      if (srcFilters.length > 0) {
        KryoSerializerPool.serialize((kryo, out) => kryo.writeObject(out, srcFilters))
      } else null
    } else null
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[SmartExecutorBucketPartition].hostList.map(_._1)
  }

  def getPartitionEvaluator: () => Array[Partition] = () => partitionPruner() match {
    case -1 => allParts
    case bucketId =>
      val part = allParts(bucketId).asInstanceOf[SmartExecutorBucketPartition]
      Array(new SmartExecutorBucketPartition(0, bucketId, part.hostList))
  }

  override def getPartitions: Array[Partition] = {
    // evaluate the filters at this point since they can change in every execution
    // (updated values in ParamLiteral will take care of updating filters)
    serializedFilters = serializeFilters(filters)
    val parts = getPartitionEvaluator()
    logDebug(s"$toString.getPartitions: $tableName partitions ${parts.mkString("; ")}")
    parts
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    super.write(kryo, output)

    output.writeString(tableName)
    output.writeVarInt(projection.length, true)
    output.writeInts(projection, true)
    val filterLen = if (serializedFilters ne null) serializedFilters.length else 0
    output.writeVarInt(filterLen, true)
    if (filterLen > 0) {
      output.writeBytes(serializedFilters, 0, filterLen)
    }
    ConnectionPropertiesSerializer.write(kryo, output, connProperties)
    StructTypeSerializer.write(kryo, output, schema)
    output.writeVarLong(catalogSchemaVersion, false)
    output.writeBoolean(delayRollover)
    output.writeBoolean(preferHostName)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    super.read(kryo, input)

    tableName = input.readString()
    val numColumns = input.readVarInt(true)
    projection = input.readInts(numColumns, true)
    val filterLen = input.readVarInt(true)
    serializedFilters = if (filterLen > 0) input.readBytes(filterLen) else null
    connProperties = ConnectionPropertiesSerializer.read(kryo, input)
    schema = StructTypeSerializer.read(kryo, input, c = null)
    catalogSchemaVersion = input.readVarLong(false)
    delayRollover = input.readBoolean()
    preferHostName = input.readBoolean()
  }
}

class SmartConnectorRowRDD(_session: SnappySession,
    _tableName: String,
    _isPartitioned: Boolean,
    _columns: Array[String],
    _connProperties: ConnectionProperties,
    _filters: Array[Expression],
    _partEval: () => Array[Partition],
    _partitionPruner: () => Int = () => -1,
    private var catalogSchemaVersion: Long,
    _commitTx: Boolean, _delayRollover: Boolean)
    extends RowFormatScanRDD(_session, _tableName, _isPartitioned, _columns,
      pushProjections = true, useResultSet = true, _connProperties,
    _filters, _partEval, _partitionPruner, _commitTx, _delayRollover,
    projection = Array.emptyIntArray, None) {

  private var preferHostName = SmartConnectorHelper.preferHostName(session)

  override def commitTxBeforeTaskCompletion(conn: Option[Connection],
      context: TaskContext): Unit = {
    Option(context).foreach(_.addTaskCompletionListener(_ => {
      val txId = SmartConnectorHelper.snapshotTxIdForRead.get match {
        case null => null
        case p => p._1
      }
      logDebug(s"The txid going to be committed is $txId " + tableName)
      // if ((txId ne null) && !txId.equals("null")) {
        val ps = conn.get.prepareStatement(s"call sys.COMMIT_SNAPSHOT_TXID(?,?)")
        ps.setString(1, if (txId  ne null) txId else "")
        ps.setString(2, if (delayRollover) tableName else "")
        ps.executeUpdate()
        logDebug(s"The txid being committed is $txId")
        ps.close()
        SmartConnectorHelper.snapshotTxIdForRead.set(null)
      // }
    }))
  }

  override def computeResultSet(
      thePart: Partition, context: TaskContext): (Connection, Statement, ResultSet) = {
    val helper = new SmartConnectorRDDHelper
    val (conn, txId) = helper.getConnectionAndTXId(connProperties,
      thePart.asInstanceOf[SmartExecutorBucketPartition], preferHostName)
    if (context ne null) {
      context.addTaskCompletionListener { _ =>
        try {
          val statement = conn.createStatement()
          statement match {
            case stmt: ClientStatement => stmt.getConnection.setCommonStatementAttributes(null)
          }
          statement.close()
        } catch {
          case NonFatal(e) => logWarning("Exception resetting commonStatementAttributes", e)
        }
        try {
          conn.close()
          logDebug("closed connection for task " + context.partitionId())
        } catch {
          case NonFatal(e) => logWarning("Exception closing connection", e)
        }
      }
    }
    val bucketPartition = thePart.asInstanceOf[SmartExecutorBucketPartition]
    logDebug(s"Scanning row buffer for $tableName,partId=${bucketPartition.index}," +
        s" bucketId = ${bucketPartition.bucketId}")
    val statement = conn.createStatement()
    val thriftConn = statement match {
      case clientStmt: ClientStatement =>
        val clientConn = clientStmt.getConnection
        if (isPartitioned) {
          clientConn.setCommonStatementAttributes(ClientStatement.setLocalExecutionBucketIds(
            new StatementAttrs(), Collections.singleton(Int.box(bucketPartition.bucketId)),
            tableName, true).setCatalogVersion(catalogSchemaVersion))
        } else {
          clientConn.setCommonStatementAttributes(
            new StatementAttrs().setCatalogVersion(catalogSchemaVersion))
        }
        clientConn
      case _ => null
    }
    if (isPartitioned && (thriftConn eq null)) {
      val ps = conn.prepareStatement("call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION(?, ?, ?)")
      ps.setString(1, tableName)
      val bucketString = bucketPartition.bucketId.toString
      ps.setString(2, bucketString)
      ps.setLong(3, catalogSchemaVersion)
      ps.executeUpdate()
      ps.close()
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

    if (thriftConn ne null) {
      stmt.asInstanceOf[ClientPreparedStatement].setSnapshotTransactionId(txId)
    } else if (txId != null) {
      if (!txId.isEmpty) {
        statement.execute(
          s"call sys.USE_SNAPSHOT_TXID('$txId')")
      }
    }

    val rs = stmt.executeQuery()

    // get the txid which was used to take the snapshot.
    if (!commitTx) {
      val getTXIdAndHostUrl = conn.prepareStatement("values sys.GET_SNAPSHOT_TXID_AND_HOSTURL(?)")
      getTXIdAndHostUrl.setBoolean(1, delayRollover)
      val rs = getTXIdAndHostUrl.executeQuery()
      rs.next()
      val txIdAndHostUrl = SmartConnectorHelper.getTxIdAndHostUrl(
        rs.getString(1), preferHostName)
      rs.close()
      getTXIdAndHostUrl.close()
      SmartConnectorHelper.snapshotTxIdForRead.set(txIdAndHostUrl)
      logDebug(s"The snapshot tx id is $txIdAndHostUrl and tablename is $tableName")
    }
    logDebug(s"The previous snapshot tx id is $txId and tablename is $tableName")
    (conn, stmt, rs)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[SmartExecutorBucketPartition].hostList.map(_._1)
  }

  override def getPartitions: Array[Partition] = {
    // evaluate the filter clause at this point since it can change in every execution
    // (updated values in ParamLiteral will take care of updating filters)
    evaluateWhereClause()
    val parts = partitionEvaluator()
    if(parts.length == _partEval().length){
      return getPartitionEvaluator
    }
    logDebug(s"$toString.getPartitions: $tableName partitions ${parts.mkString("; ")}")
    parts
  }

  def getPartitionEvaluator: Array[Partition] = {
    partitionPruner() match {
      case -1 => _partEval()
      case bucketId =>
        val part = _partEval()(bucketId).asInstanceOf[SmartExecutorBucketPartition]
        Array(new SmartExecutorBucketPartition(0, bucketId, part.hostList))
    }
  }

  def getSQLStatement(resolvedTableName: String,
      requiredColumns: Array[String], partitionId: Int): String = {
    "select " + requiredColumns.mkString(", ") + " from " + quotedName(resolvedTableName)
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    super.write(kryo, output)
    output.writeVarLong(catalogSchemaVersion, false)
    output.writeBoolean(preferHostName)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    super.read(kryo, input)
    catalogSchemaVersion = input.readVarLong(false)
    preferHostName = input.readBoolean()
  }
}

class SnapshotConnectionListener(store: JDBCSourceAsColumnarStore,
    delayRollover: Boolean) extends TaskCompletionListener {
  private val connAndTxId: Array[_ <: Object] = store.beginTx(delayRollover)
  private var isSuccess = false

  override def onTaskCompletion(context: TaskContext): Unit = {
    val txId = connAndTxId(1).asInstanceOf[String]
    val conn = Option(connAndTxId(0).asInstanceOf[Connection])
    if (connAndTxId(1) ne null) {
      if (success()) {
        store.commitTx(txId, delayRollover, conn)
      }
      else {
        store.rollbackTx(txId, conn)
      }
    }
    store.closeConnection(conn)
  }

  def success(): Boolean = {
    isSuccess
  }

  def setSuccess(): Unit = {
    isSuccess = true
  }

  def getConn: Connection = connAndTxId(0).asInstanceOf[Connection]
}
