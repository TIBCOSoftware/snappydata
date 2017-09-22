/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
import java.sql.{Connection, ResultSet, Statement}

import scala.annotation.meta.param
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.NonFatal

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.gemstone.gemfire.internal.cache.{BucketRegion, CachePerfStats, GemFireCacheImpl, LocalRegion, PartitionedRegion, TXManagerImpl}
import com.gemstone.gemfire.internal.shared.BufferAllocator
import com.gemstone.gemfire.internal.shared.unsafe.UnsafeHolder
import com.pivotal.gemfirexd.internal.engine.{GfxdConstants, Misc}
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextService
import com.pivotal.gemfirexd.internal.impl.jdbc.{EmbedConnection, EmbedConnectionContext}
import io.snappydata.impl.SparkConnectorRDDHelper
import io.snappydata.thrift.internal.ClientBlob

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.{ConnectionPropertiesSerializer, StructTypeSerializer}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{DynamicReplacableConstant, ParamLiteral}
import org.apache.spark.sql.collection._
import org.apache.spark.sql.execution.columnar._
import org.apache.spark.sql.execution.columnar.encoding.ColumnDeleteDelta
import org.apache.spark.sql.execution.row.{ResultSetTraversal, RowFormatScanRDD, RowInsertExec}
import org.apache.spark.sql.execution.{BufferedRowIterator, ConnectionPool, RDDKryo, WholeStageCodegenExec}
import org.apache.spark.sql.hive.ConnectorCatalog
import org.apache.spark.sql.sources.{ConnectionProperties, Filter}
import org.apache.spark.sql.store.{CodeGeneration, StoreUtils}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SnappyContext, SnappySession, SparkSession, ThinClientConnectorMode}
import org.apache.spark.util.TaskCompletionListener
import org.apache.spark.{Logging, Partition, TaskContext}

/**
 * Column Store implementation for GemFireXD.
 */
class JDBCSourceAsColumnarStore(private var _connProperties: ConnectionProperties,
    var numPartitions: Int, private var _tableName: String, var schema: StructType)
    extends ExternalStore with KryoSerializable with Logging {

  self =>

  override final def tableName: String = _tableName

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

  override def storeColumnBatch(columnTableName: String, batch: ColumnBatch,
      partitionId: Int, batchId: Long, maxDeltaRows: Int,
      conn: Option[Connection]): Unit = {
    if (partitionId >= 0) {
      doInsertOrPut(columnTableName, batch, batchId, partitionId, maxDeltaRows, conn)
    } else {
      val (bucketId, br, batchSize) = getPartitionID(columnTableName,
        () => batch.buffers.foldLeft(0L)(_ + _.capacity()))
      try {
        doInsertOrPut(columnTableName, batch, batchId, bucketId, maxDeltaRows, conn)
      } finally br match {
        case None =>
        case Some(bucket) => bucket.updateInProgressSize(-batchSize)
      }
    }
  }

  // begin should decide the connection which will be used by insert/commit/rollback
  def beginTx(): Array[_ <: Object] = {
    val conn = self.getConnection(tableName, onExecutor = true)

    assert(!conn.isClosed)
    tryExecute(tableName, closeOnSuccessOrFailure = false, onExecutor = true) {
      (conn: Connection) => {
        connectionType match {
          case ConnectionType.Embedded =>
            val txMgr = Misc.getGemFireCache.getCacheTransactionManager
            if (TXManagerImpl.snapshotTxState.get() == null && (txMgr.getTXState == null)) {
              txMgr.begin(com.gemstone.gemfire.cache.IsolationLevel.SNAPSHOT, null)
              Array(conn, txMgr.getTransactionId.stringFormat())
            } else {
              Array(conn, null)
            }
          case _ =>
            val txId = SparkConnectorRDDHelper.snapshotTxIdForWrite.get
            if (txId == null) {
              logDebug(s"Going to start the transaction on server on conn $conn ")
              val startAndGetSnapshotTXId = conn.prepareCall(s"call sys.START_SNAPSHOT_TXID (?)")
              startAndGetSnapshotTXId.registerOutParameter(1, java.sql.Types.VARCHAR)
              startAndGetSnapshotTXId.execute()
              val txid: String = startAndGetSnapshotTXId.getString(1)
              startAndGetSnapshotTXId.close()
              SparkConnectorRDDHelper.snapshotTxIdForWrite.set(txid)
              logDebug(s"The snapshot tx id is $txid and tablename is $tableName")
              Array(conn, txid)
            } else {
              logDebug(s"Going to use the transaction $txId on server on conn $conn ")
              // it should always be not null.
              if (!txId.equals("null")) {
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

  def commitTx(txId: String, conn: Option[Connection]): Unit = {
    // noinspection RedundantDefaultArgument
    tryExecute(tableName, closeOnSuccessOrFailure = true, onExecutor = true) {
      (conn: Connection) => {
        connectionType match {
          case ConnectionType.Embedded =>
            // if(SparkConnectorRDDHelper.snapshotTxIdForRead.get)
            Misc.getGemFireCache.getCacheTransactionManager.commit()
          case _ =>
            logDebug(s"Going to commit $txId the transaction on server conn is $conn")
            val ps = conn.prepareStatement(s"call sys.COMMIT_SNAPSHOT_TXID(?)")
            ps.setString(1, if (txId == null) "null" else txId)
            try {
              ps.executeUpdate()
              logDebug(s"The txid being committed is $txId")
            }
            finally {
              ps.close()
              SparkConnectorRDDHelper.snapshotTxIdForWrite.set(null)
              logDebug(s"Committed $txId the transaction on server ")
            }
        }
      }
    }(conn)
  }


  def rollbackTx(txId: String, conn: Option[Connection]): Unit = {
    // noinspection RedundantDefaultArgument
    tryExecute(tableName, closeOnSuccessOrFailure = true, onExecutor = true) {
      (conn: Connection) => {
        connectionType match {
          case ConnectionType.Embedded =>
            Misc.getGemFireCache.getCacheTransactionManager.rollback()
          case _ =>
            logDebug(s"Going to rollback $txId the transaction on server on wconn $conn ")
            val ps = conn.prepareStatement(s"call sys.ROLLBACK_SNAPSHOT_TXID(?)")
            ps.setString(1, if (txId == null) "null" else txId)
            try {
              ps.executeUpdate()
              logDebug(s"The txid being rolledback is $txId")
            }
            finally {
              ps.close()
              SparkConnectorRDDHelper.snapshotTxIdForWrite.set(null)
              logDebug(s"Rolled back $txId the transaction on server ")
            }
        }
      }
    }(conn)
  }

  override def storeDelete(columnTableName: String, buffer: ByteBuffer,
      statsData: Array[Byte], partitionId: Int,
      batchId: Long, conn: Option[Connection]): Unit = {
    val allocator = GemFireCacheImpl.getCurrentBufferAllocator
    val statsBuffer = createStatsBuffer(statsData, allocator)
    connectionType match {
      case ConnectionType.Embedded =>
        val region = Misc.getRegionForTable[ColumnFormatKey, ColumnFormatValue](
          columnTableName, true)
        var key = new ColumnFormatKey(batchId, partitionId,
          ColumnFormatEntry.DELETE_MASK_COL_INDEX)

        // check for full batch delete
        if (ColumnDelta.checkBatchDeleted(buffer)) {
          ColumnDelta.deleteBatch(key, region, columnTableName, forUpdate = false)
          return
        }

        val keyValues = new java.util.HashMap[ColumnFormatKey, ColumnFormatValue](2)
        val value = new ColumnDeleteDelta(buffer)
        keyValues.put(key, value)

        // add the stats row
        key = new ColumnFormatKey(batchId, partitionId,
          ColumnFormatEntry.DELTA_STATROW_COL_INDEX)
        val statsValue = new ColumnDelta(statsBuffer)
        keyValues.put(key, statsValue)

        region.putAll(keyValues)

      case _ =>
        tryExecute(columnTableName, closeOnSuccessOrFailure = false,
          onExecutor = true) { connection =>

          // check for full batch delete
          if (ColumnDelta.checkBatchDeleted(buffer)) {
            val deleteStr = s"delete from $columnTableName where " +
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
            blob = new ClientBlob(buffer, true)
            stmt.setBlob(4, blob)
            stmt.addBatch()

            // add the stats row
            stmt.setLong(1, batchId)
            stmt.setInt(2, partitionId)
            stmt.setInt(3, ColumnFormatEntry.DELTA_STATROW_COL_INDEX)
            blob = new ClientBlob(statsBuffer, true)
            stmt.setBlob(4, blob)
            stmt.addBatch()

            stmt.executeBatch()
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
            // it should always be not null.
            // get clears the state from connection
            // the tx would have been committed earlier
            // or it will be committed later
            val txId = SparkConnectorRDDHelper.snapshotTxIdForWrite.get
            if (txId != null && !txId.equals("null")) {
              val statement = conn.prepareStatement("values sys.GET_SNAPSHOT_TXID()")
              statement.executeQuery()
              statement.close()
            }
            // conn commit removed txState from the conn context.
            conn.commit()
            conn.close()
        }
      case _ => // Do nothing
    }
  }

  private def createStatsBuffer(statsData: Array[Byte],
      allocator: BufferAllocator): ByteBuffer = {
    // need to create a copy since underlying Array[Byte] can be re-used
    val statsLen = statsData.length
    val statsBuffer = allocator.allocateForStorage(statsLen)
    statsBuffer.put(statsData, 0, statsLen)
    statsBuffer.rewind()
    statsBuffer
  }

  /**
   * Insert the base entry and n column entries in Snappy. Insert the base entry
   * in the end to ensure that the partial inserts of a cached batch are ignored
   * during iteration. We are not cleaning up the partial inserts of cached
   * batches for now.
   */
  private def doSnappyInsertOrPut(region: LocalRegion, batch: ColumnBatch,
      batchId: Long, partitionId: Int, maxDeltaRows: Int): Unit = {
    val deltaUpdate = batch.deltaIndexes ne null
    val statRowIndex = if (deltaUpdate) ColumnFormatEntry.DELTA_STATROW_COL_INDEX
    else ColumnFormatEntry.STATROW_COL_INDEX
    var index = 1
    try {
      // add key-values pairs for each column
      val keyValues = new java.util.HashMap[ColumnFormatKey, ColumnFormatValue](
        batch.buffers.length + 1)
      batch.buffers.foreach { buffer =>
        val columnIndex = if (deltaUpdate) batch.deltaIndexes(index - 1) else index
        val key = new ColumnFormatKey(batchId, partitionId, columnIndex)
        val value = if (deltaUpdate) new ColumnDelta(buffer) else new ColumnFormatValue(buffer)
        keyValues.put(key, value)
        index += 1
      }
      // add the stats row
      val key = new ColumnFormatKey(batchId, partitionId, statRowIndex)
      val allocator = Misc.getGemFireCache.getBufferAllocator
      val statsBuffer = createStatsBuffer(batch.statsData, allocator)
      val value = if (deltaUpdate) new ColumnDelta(statsBuffer)
      else new ColumnFormatValue(statsBuffer)
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
      batchId: Long, partitionId: Int, maxDeltaRows: Int): (Connection => Unit) = {
    {
      (connection: Connection) => {
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
            val blob = new ClientBlob(buffer, true)
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
          val statsBuffer = createStatsBuffer(batch.statsData, allocator)
          stmt.setBlob(4, new ClientBlob(statsBuffer, true))
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
    if (isPut) s"put into $tableName values (?, ?, ?, ?)"
    else s"insert into $tableName values (?, ?, ?, ?)"
  }

  override def getConnection(id: String, onExecutor: Boolean): Connection = {
    connectionType match {
      case ConnectionType.Embedded =>
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
    }

  override def getColumnBatchRDD(tableName: String,
      rowBuffer: String,
      requiredColumns: Array[String],
      prunePartitions: => Int,
      session: SparkSession,
      schema: StructType): RDD[Any] = {
    val snappySession = session.asInstanceOf[SnappySession]
    connectionType match {
      case ConnectionType.Embedded =>
        new ColumnarStorePartitionedRDD(snappySession,
          tableName, prunePartitions, this)
      case _ =>
        // remove the url property from poolProps since that will be
        // partition-specific
        val poolProps = connProperties.poolProps -
            (if (connProperties.hikariCP) "jdbcUrl" else "url")

        val (parts, embdClusterRelDestroyVersion) =
          SnappyContext.getClusterMode(session.sparkContext) match {
          case ThinClientConnectorMode(_, _) =>
            val catalog = snappySession.sessionCatalog.asInstanceOf[ConnectorCatalog]
            val relInfo = catalog.getCachedRelationInfo(catalog.newQualifiedTableName(rowBuffer))
            (relInfo.partitions, relInfo.embdClusterRelDestroyVersion)
          case _ =>
            (Array.empty[Partition], -1)
        }

        new SmartConnectorColumnRDD(snappySession,
          tableName, requiredColumns, ConnectionProperties(connProperties.url,
            connProperties.driver, connProperties.dialect, poolProps,
            connProperties.connProps, connProperties.executorConnProps,
            connProperties.hikariCP), schema, this, parts, embdClusterRelDestroyVersion)
    }
  }

  private def doInsertOrPut(columnTableName: String, batch: ColumnBatch, batchId: Long,
      partitionId: Int, maxDeltaRows: Int, conn: Option[Connection] = None): Unit = {
    // split the batch and put into row buffer if it is small
    if (maxDeltaRows > 0 && batch.numRows < math.max(maxDeltaRows / 10,
      GfxdConstants.SNAPPY_MIN_COLUMN_DELTA_ROWS)) {
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
          doSnappyInsertOrPut(region, batch, uuid, partitionId, maxDeltaRows)

        case _ =>
          // noinspection RedundantDefaultArgument
          tryExecute(tableName, closeOnSuccessOrFailure = false /* batch.deltaIndexes ne null */ ,
            onExecutor = true)(doGFXDInsertOrPut(columnTableName, batch, batchId, partitionId,
            maxDeltaRows))(conn)
      }
    }
  }

  private def doRowBufferPut(batch: ColumnBatch,
      partitionId: Int): (Connection => Unit) = {
    (connection: Connection) => {
      val gen = CodeGeneration.compileCode(
        tableName + ".COLUMN_TABLE.DECOMPRESS", schema.fields, () => {
          val schemaAttrs = schema.toAttributes
          val tableScan = ColumnTableScan(schemaAttrs, dataRDD = null,
            otherRDDs = Seq.empty, numBuckets = -1,
            partitionColumns = Seq.empty, partitionColumnAliases = Seq.empty,
            baseRelation = null, schema, allFilters = Seq.empty, schemaAttrs,
            caseSensitive = true)
          val insertPlan = RowInsertExec(tableScan, putInto = true,
            Seq.empty, Seq.empty, numBuckets = -1, isPartitioned = false, schema,
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
      batch.buffers.foreach(UnsafeHolder.releaseIfDirectBuffer)
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
}


final class ColumnarStorePartitionedRDD(
    @transient private val session: SnappySession,
    private var tableName: String,
    @(transient @param) partitionPruner: => Int,
    @transient private val store: JDBCSourceAsColumnarStore)
    extends RDDKryo[Any](session.sparkContext, Nil) with KryoSerializable {

  private[this] var allPartitions: Array[Partition] = _
  private val evaluatePartitions: () => Array[Partition] = () => {
    val region = Misc.getRegionForTable(tableName, true)
    partitionPruner match {
      case -1 if allPartitions != null =>
        allPartitions
      case -1 =>
        allPartitions = session.sessionState.getTablePartitions(
          region.asInstanceOf[PartitionedRegion])
        allPartitions
      case bucketId: Int =>
        if (java.lang.Boolean.getBoolean("DISABLE_PARTITION_PRUNING")) {
          allPartitions = session.sessionState.getTablePartitions(
            region.asInstanceOf[PartitionedRegion])
          allPartitions
        } else {
          val pr = region.asInstanceOf[PartitionedRegion]
          val distMembers = StoreUtils.getBucketOwnersForRead(bucketId, pr)
          val prefNodes = distMembers.collect {
            case m if SnappyContext.containsBlockId(m.toString) =>
              Utils.getHostExecutorId(SnappyContext.getBlockId(
                m.toString).get.blockId)
          }
          Array(new MultiBucketExecutorPartition(0, ArrayBuffer(bucketId),
            pr.getTotalNumberOfBuckets, prefNodes.toSeq))
        }
    }
  }

  override def compute(part: Partition, context: TaskContext): Iterator[Any] = {

    Option(context).foreach(_.addTaskCompletionListener(_ => {
      val tx = TXManagerImpl.snapshotTxState.get()
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
      case _ => java.util.Collections.singleton(Int.box(part.index))
    }
    // val container = GemFireXDUtils.getGemFireContainer(tableName, true)
    // ColumnBatchIterator(container, bucketIds)
    val r = Misc.getRegionForTable(tableName, true).asInstanceOf[LocalRegion]
    ColumnBatchIterator(r, bucketIds, context)
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
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    super.read(kryo, input)
    tableName = input.readString()
  }
}

final class SmartConnectorColumnRDD(
    @transient private val session: SnappySession,
    private var tableName: String,
    private var requiredColumns: Array[String],
    private var connProperties: ConnectionProperties,
    private val schema: StructType,
    @transient private val store: ExternalStore,
    val parts: Array[Partition],
    val relDestroyVersion: Int = -1)
    extends RDDKryo[Any](session.sparkContext, Nil)
        with KryoSerializable {

  override def compute(split: Partition,
      context: TaskContext): Iterator[ByteBuffer] = {
    val helper = new SparkConnectorRDDHelper
    val conn: Connection = helper.getConnection(connProperties, split)

    val partitionId = split.index
    val (fetchStatsQuery, fetchColQuery) = helper.getSQLStatement(tableName,
      partitionId, requiredColumns.map(_.replace(store.columnPrefix, "")), schema)
    // fetch the stats
    val (statement, rs, txId) = helper.executeQuery(conn, tableName, split,
      fetchStatsQuery, relDestroyVersion)
    val itr = new ColumnBatchIteratorOnRS(conn, requiredColumns, statement, rs,
      context, partitionId, fetchColQuery)

    if (context ne null) {
      context.addTaskCompletionListener { _ =>
        logDebug(s"The txid going to be committed is $txId " + tableName)

        // if ((txId ne null) && !txId.equals("null")) {
        val ps = conn.prepareStatement(s"call sys.COMMIT_SNAPSHOT_TXID(?)")
        ps.setString(1, if (txId == null) "null" else txId)
        ps.executeUpdate()
        logDebug(s"The txid being committed is $txId")
        ps.close()
        SparkConnectorRDDHelper.snapshotTxIdForRead.set(null)
        logDebug(s"closed connection for task from listener $partitionId")
        try {
          conn.commit()
          conn.close()
          logDebug("closed connection for task " + context.partitionId())
        } catch {
          case NonFatal(e) => logWarning("Exception closing connection", e)
        }
        // }
      }
    }
    itr
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[ExecutorMultiBucketLocalShellPartition]
        .hostList.map(_._1.asInstanceOf[String])
  }

  override def getPartitions: Array[Partition] = {
    if (parts != null && parts.length > 0) {
      return parts
    }
    SparkConnectorRDDHelper.getPartitions(tableName)
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    super.write(kryo, output)

    output.writeString(tableName)
    output.writeVarInt(requiredColumns.length, true)
    for (column <- requiredColumns) {
      output.writeString(column)
    }
    ConnectionPropertiesSerializer.write(kryo, output, connProperties)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    super.read(kryo, input)

    tableName = input.readString()
    val numColumns = input.readVarInt(true)
    requiredColumns = Array.fill(numColumns)(input.readString())
    connProperties = ConnectionPropertiesSerializer.read(kryo, input)

  }
}

class SmartConnectorRowRDD(_session: SnappySession,
    _tableName: String,
    _isPartitioned: Boolean,
    _columns: Array[String],
    _connProperties: ConnectionProperties,
    _filters: Array[Filter] = Array.empty[Filter],
    _partEval: () => Array[Partition] = () => Array.empty[Partition],
    _relDestroyVersion: Int = -1,
    _commitTx: Boolean)
    extends RowFormatScanRDD(_session, _tableName, _isPartitioned, _columns,
      pushProjections = true, useResultSet = true, _connProperties,
    _filters, _partEval, _commitTx) {


  override def commitTxBeforeTaskCompletion(conn: Option[Connection],
      context: TaskContext): Unit = {
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => {
      val txId = SparkConnectorRDDHelper.snapshotTxIdForRead.get
      logDebug(s"The txid going to be committed is $txId " + tableName)
      // if ((txId ne null) && !txId.equals("null")) {
        val ps = conn.get.prepareStatement(s"call sys.COMMIT_SNAPSHOT_TXID(?)")
        ps.setString(1, if (txId == null) "null" else txId)
        ps.executeUpdate()
        logDebug(s"The txid being committed is $txId")
        ps.close()
        SparkConnectorRDDHelper.snapshotTxIdForRead.set(null)
      // }
    }))
  }

  override def computeResultSet(
      thePart: Partition, context: TaskContext): (Connection, Statement, ResultSet) = {
    val helper = new SparkConnectorRDDHelper
    val conn: Connection = helper.getConnection(
      connProperties, thePart)
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
        s"call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION(?, ?, ${_relDestroyVersion})")
      ps.setString(1, tableName)
      val partition = thePart.asInstanceOf[ExecutorMultiBucketLocalShellPartition]
      val bucketString = partition.buckets.mkString(",")
      ps.setString(2, bucketString)
      ps.executeUpdate()
      ps.close()
    }
    val sqlText = s"SELECT $columnList FROM $tableName$filterWhereClause"

    val args = filterWhereArgs
    val stmt = conn.prepareStatement(sqlText)
    if (args ne null) {
      ExternalStoreUtils.setStatementParameters(stmt, args.map {
        case pl: ParamLiteral => pl.convertedLiteral
        case l : DynamicReplacableConstant => l.convertedLiteral
        case v => v
      })
    }
    val fetchSize = connProperties.executorConnProps.getProperty("fetchSize")
    if (fetchSize ne null) {
      stmt.setFetchSize(fetchSize.toInt)
    }

    val txId = SparkConnectorRDDHelper.snapshotTxIdForRead.get
    if (txId != null) {
      if (!txId.equals("null")) {
        val statement = conn.createStatement()
        statement.execute(
          s"call sys.USE_SNAPSHOT_TXID('$txId')")
      }
    }

    val rs = stmt.executeQuery()

    // get the txid which was used to take the snapshot.
    if (!_commitTx) {
      val getSnapshotTXId = conn.prepareStatement("values sys.GET_SNAPSHOT_TXID()")
      val rs = getSnapshotTXId.executeQuery()
      rs.next()
      val txId = rs.getString(1)
      rs.close()
      getSnapshotTXId.close()
      SparkConnectorRDDHelper.snapshotTxIdForRead.set(txId)
      logDebug(s"The snapshot tx id is $txId and tablename is $tableName")
    }
    logDebug(s"The previous snapshot tx id is $txId and tablename is $tableName")
    (conn, stmt, rs)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[ExecutorMultiBucketLocalShellPartition]
        .hostList.map(_._1.asInstanceOf[String])
  }

  override def getPartitions: Array[Partition] = {
    // use incoming partitions if provided (e.g. for collocated tables)
    val parts = partitionEvaluator()
    if (parts != null && parts.length > 0) {
      return parts
    }
    val conn = ExternalStoreUtils.getConnection(tableName, connProperties,
      forExecutor = true)
    try {
      SparkConnectorRDDHelper.getPartitions(tableName)
    } finally {
      conn.commit()
      conn.close()
    }
  }

  def getSQLStatement(resolvedTableName: String,
      requiredColumns: Array[String], partitionId: Int): String = {
    "select " + requiredColumns.mkString(", ") + " from " + resolvedTableName
  }

}

class SnapshotConnectionListener(store: JDBCSourceAsColumnarStore) extends TaskCompletionListener {
  val connAndTxId: Array[_ <: Object] = store.beginTx()
  var isSuccess = false

  override def onTaskCompletion(context: TaskContext): Unit = {
    val txId = connAndTxId(1).asInstanceOf[String]
    val conn = connAndTxId(0).asInstanceOf[Connection]
    if (connAndTxId(1) != null) {
      if (success()) {
        store.commitTx(txId, Some(conn))
      }
      else {
        store.rollbackTx(txId, Some(conn))
      }
    }
    store.closeConnection(Some(conn))
  }

  def success(): Boolean = {
    isSuccess
  }

  def setSuccess(): Unit = {
    isSuccess = true
  }

  def getConn: Connection = connAndTxId(0).asInstanceOf[Connection]
}
