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

package org.apache.spark.sql.execution.columnar.impl

import java.nio.ByteBuffer
import java.sql.{Connection, ResultSet, Statement}
import java.util.Collections

import scala.annotation.meta.param
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.NonFatal

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.gemstone.gemfire.internal.cache._
import com.gemstone.gemfire.internal.shared.{BufferAllocator, SystemProperties}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextService
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnectionContext
import io.snappydata.impl.SmartConnectorRDDHelper
import io.snappydata.sql.catalog.SmartConnectorHelper
import io.snappydata.thrift.internal.ClientBlob

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.{ConnectionPropertiesSerializer, KryoSerializerPool, StructTypeSerializer}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.collection._
import org.apache.spark.sql.execution.columnar.ConnectionType.ConnectionType
import org.apache.spark.sql.execution.columnar._
import org.apache.spark.sql.execution.columnar.encoding.ColumnDeleteDelta
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.row.{ResultSetTraversal, RowFormatScanRDD, RowInsertExec}
import org.apache.spark.sql.execution.sources.StoreDataSourceStrategy.translateToFilter
import org.apache.spark.sql.execution.{BufferedRowIterator, RDDKryo, SnapshotConnectionListener, WholeStageCodegenExec}
import org.apache.spark.sql.sources.JdbcExtendedUtils.quotedName
import org.apache.spark.sql.sources.{ConnectionProperties, JdbcExtendedUtils}
import org.apache.spark.sql.store.CodeGeneration
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SnappySession, SparkSession}
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

  lazy val connectionType: ConnectionType = ExternalStoreUtils.getConnectionType(
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

  override def storeColumnBatch(columnTableName: String, batch: ColumnBatch, partitionId: Int,
      batchId: Long, maxDeltaRows: Int, compressionCodecId: Int, changesToDeltaBuffer: SQLMetric,
      numColumnBatches: SQLMetric, changesToColumnStore: SQLMetric,
      listener: SnapshotConnectionListener): Unit = {
    // check for task cancellation before further processing
    checkTaskCancellation()
    logDebug(s"Storing column batch having ${batch.numRows} rows, partitionId = $partitionId")
    if (partitionId >= 0) {
      doInsertOrPut(columnTableName, batch, batchId, partitionId, maxDeltaRows,
        compressionCodecId, changesToDeltaBuffer, numColumnBatches, changesToColumnStore, listener)
    } else {
      val (bucketId, br, batchSize) = getPartitionID(columnTableName,
        () => batch.buffers.foldLeft(0L)(_ + _.capacity()))
      try {
        doInsertOrPut(columnTableName, batch, batchId, bucketId, maxDeltaRows, compressionCodecId,
          changesToDeltaBuffer, numColumnBatches, changesToColumnStore, listener)
      } finally br match {
        case None =>
        case Some(bucket) => bucket.updateInProgressSize(-batchSize)
      }
    }
  }

  override def storeDelete(columnTableName: String, buffer: ByteBuffer, partitionId: Int,
      batchId: Long, compressionCodecId: Int, connection: Connection): Unit = {
    // check for task cancellation before further processing
    checkTaskCancellation()
    val value = new ColumnDeleteDelta(buffer, compressionCodecId, isCompressed = false)
    connectionType match {
      case ConnectionType.Embedded =>
        val region = Misc.getRegionForTable[ColumnFormatKey, ColumnFormatValue](
          columnTableName, true).asInstanceOf[PartitionedRegion]
        val key = new ColumnFormatKey(batchId, partitionId,
          ColumnFormatEntry.DELETE_MASK_COL_INDEX)

        // check for full batch delete
        if (ColumnCompactor.checkBatchDeleted(buffer)) {
          ColumnDelta.deleteBatch(key, region, key.getNumColumnsInTable(columnTableName))
          return
        }
        region.put(key, value)

      case _ =>
        // check for full batch delete
        if (ColumnCompactor.checkBatchDeleted(buffer)) {
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
            addKeyToBatch(ColumnFormatEntry.DELTA_STATROW_COL_INDEX) // for old persisted data
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
          stmt.executeUpdate()
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
    }
  }

  /**
   * Insert the base entry and n column entries in Snappy. Insert the base entry
   * in the end to ensure that the partial inserts of a cached batch are ignored
   * during iteration. We are not cleaning up the partial inserts of cached
   * batches for now.
   */
  private def doSnappyInsertOrPut(region: LocalRegion, batch: ColumnBatch,
      batchId: Long, partitionId: Int, compressionCodecId: Int): Unit = {
    val deltaUpdate = batch.deltaIndexes ne null
    val statRowIndex = ColumnFormatEntry.STATROW_COL_INDEX
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
        val op = if (deltaUpdate) "put" else "insert"
        logError(s"Column store region ${region.getFullPath} $op failed with exception", e)
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
      batchId: Long, partitionId: Int, compressionCodecId: Int,
      listener: SnapshotConnectionListener): Unit = {
    val deltaUpdate = batch.deltaIndexes ne null
    // we are using the same connection on which tx was started.
    val rowInsertStr = getRowInsertOrPutStr(columnTableName, deltaUpdate)
    val stmt = listener.connection.prepareStatement(rowInsertStr)
    val statRowIndex = ColumnFormatEntry.STATROW_COL_INDEX
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

  protected def getRowInsertOrPutStr(tableName: String, isPut: Boolean): String = {
    if (isPut) s"put into ${quotedName(tableName)} values (?, ?, ?, ?)"
    else s"insert into ${quotedName(tableName)} values (?, ?, ?, ?)"
  }

  override def getExistingConnection(context: Option[TaskContext]): Option[Connection] = {
    connectionType match {
      case ConnectionType.Embedded =>
        val currentCM = ContextService.getFactory.getCurrentContextManager
        if (currentCM ne null) {
          val conn = EmbedConnectionContext.getEmbedConnection(currentCM)
          if (conn ne null) return Some(conn)
        }
      case _ => // get pooled connection
    }
    val ctx = if (context.isEmpty) null else context.get // avoid another closure by orNull
    SnapshotConnectionListener.getExisting(ctx) match {
      case Some(listener) => Some(listener.connection)
      case _ => None
    }
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
        new ColumnarStorePartitionedRDD(snappySession, tableName, rowBuffer, projection, filters,
          (filters eq null) || filters.length == 0, prunePartitions, connProperties, delayRollover)
      case _ =>
        // remove the url property from poolProps since that will be
        // partition-specific
        val poolProps = connProperties.poolProps -
            (if (connProperties.hikariCP) "jdbcUrl" else "url")

        val catalog = snappySession.externalCatalog
        val (rowSchema, rowTable) = JdbcExtendedUtils.getTableWithSchema(rowBuffer,
          conn = null, Some(snappySession))
        val relationInfo = catalog.getRelationInfo(rowSchema, rowTable, isRowTable = false)._1
        new SmartConnectorColumnRDD(snappySession, tableName, rowBuffer, projection, filters,
          ConnectionProperties(connProperties.url,
            connProperties.driver, connProperties.dialect, poolProps,
            connProperties.connProps, connProperties.executorConnProps,
            connProperties.hikariCP), schema, allParts = relationInfo.partitions,
          prunePartitions, relationInfo.catalogSchemaVersion, delayRollover)
    }
  }

  private def doInsertOrPut(columnTableName: String, batch: ColumnBatch, batchId: Long,
      partitionId: Int, maxDeltaRows: Int, compressionCodecId: Int,
      changesToDeltaBuffer: SQLMetric, numColumnBatches: SQLMetric,
      changesToColumnStore: SQLMetric, listener: SnapshotConnectionListener): Unit = {
    // split the batch and put into row buffer if it is small
    if (maxDeltaRows > 0 && batch.numRows < math.min(maxDeltaRows,
      math.max(maxDeltaRows >>> 1, SystemProperties.SNAPPY_MIN_COLUMN_DELTA_ROWS))) {
      logDebug(s"Splitting column batch with ${batch.numRows} rows, " +
          s"partitionId = $partitionId into delta buffer")
      doRowBufferPut(batch, partitionId)
      if (changesToDeltaBuffer ne null) changesToDeltaBuffer.add(batch.numRows)
    } else {
      connectionType match {
        case ConnectionType.Embedded =>
          val region = Misc.getRegionForTable(columnTableName, true)
              .asInstanceOf[PartitionedRegion]
          // create UUID if not present using the row buffer region because
          // all other callers (ColumnFormatEncoder, BucketRegion) use the same
          val uuid = if (BucketRegion.isValidUUID(batchId)) batchId
          else region.getColocatedWithRegion.newUUID(
            !BucketRegion.ALLOW_COLUMN_STORE_UUID_OVERWRITE_ON_OVERFLOW)
          // set the batchId for the calls from ColumnBatchCreator (used by rollover)
          if (numColumnBatches eq null) BucketRegion.lastGeneratedBatchId.set(uuid)
          doSnappyInsertOrPut(region, batch, uuid, partitionId, compressionCodecId)

        case _ =>
          doGFXDInsertOrPut(columnTableName, batch, batchId, partitionId,
            compressionCodecId, listener)
      }
      if (numColumnBatches ne null) numColumnBatches.add(1L)
      if (changesToColumnStore ne null) changesToColumnStore.add(batch.numRows)
    }
  }

  private def doRowBufferPut(batch: ColumnBatch, partitionId: Int): Unit = {
    try {
      val gen = CodeGeneration.compileCode(
        CodeGeneration.createDeltaKey(tableName), schema.fields, () => {
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
          (code, references.toArray)
        })
      val iter = gen._1.generate(gen._2).asInstanceOf[BufferedRowIterator]
      // put the single ColumnBatch in the iterator read by generated code
      iter.init(partitionId, Array(Iterator[Any](new LazyIterator(() => new ResultSetTraversal(
        conn = null, stmt = null, rs = null, context = null,
        java.util.Collections.emptySet[Integer]())),
        ColumnBatchIterator(batch)).asInstanceOf[Iterator[InternalRow]]))
      // ignore the result which is the update count
      while (iter.hasNext) {
        iter.next()
      }
    } finally {
      // release the batch buffers
      batch.buffers.foreach(b => if (b ne null) BufferAllocator.releaseBuffer(b))
    }
  }

  // round off to nearest 8k to avoid tiny size changes from effecting the minimum selection
  private def getInProgressBucketSize(br: BucketRegion): Long =
    (br.getTotalBytes + br.getInProgressSize) >> 13L

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
                assert(iterator.hasNext)
                val smallestBuckets = new ArrayBuffer[BucketRegion](4)
                smallestBuckets += iterator.next()
                var minBucketSize = getInProgressBucketSize(smallestBuckets(0))
                while (iterator.hasNext) {
                  val bucket = iterator.next()
                  val bucketSize = getInProgressBucketSize(bucket)
                  if (bucketSize < minBucketSize) {
                    smallestBuckets.clear()
                    smallestBuckets += bucket
                    minBucketSize = bucketSize
                  } else if (bucketSize == minBucketSize) {
                    smallestBuckets += bucket
                  }
                }
                val batchSize = getBatchSizeInBytes()
                // choose a random bucket among all the smallest ones
                val smallestBucket = smallestBuckets(Random.nextInt(smallestBuckets.length))
                // update the in-progress size of the chosen bucket
                smallestBucket.updateInProgressSize(batchSize)
                (smallestBucket.getId, Some(smallestBucket), batchSize)
              }
            }
          case _ => (-1, None, 0L)
        }
      // TODO: SW: for split mode, get connection to one of the
      // local servers and a bucket ID for only one of those (or use the plan's partitionIndex?)
      case _ => (Random.nextInt(numPartitions), None, 0L)
    }
  }

  override def toString: String = s"ColumnarStore[$tableName, partitions=$numPartitions, " +
      s"connectionProperties=$connProperties, schema=$schema]"
}

final class ColumnarStorePartitionedRDD(
    @transient private val session: SnappySession,
    private[this] var tableName: String,
    private[this] var rowBufferTable: String,
    private[this] var projection: Array[Int],
    @transient val filters: Array[Expression],
    private[this] var fullScan: Boolean,
    @(transient @param) partitionPruner: () => Int,
    private[this] var connProperties: ConnectionProperties,
    private[this] var delayRollover: Boolean)
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
    val bucketIds = part match {
      case p: MultiBucketExecutorPartition => p.buckets
      case _ => Collections.singleton(Int.box(part.index))
    }
    SnapshotConnectionListener(context, startSnapshotTx = true, ConnectionType.Embedded,
      rowBufferTable, lockDiskStore = None, connProperties, delayRollover, Left(-1L))
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
    output.writeString(rowBufferTable)
    output.writeInt(projection.length)
    output.writeInts(projection)
    output.writeBoolean(fullScan)
    ConnectionPropertiesSerializer.write(kryo, output, connProperties)
    output.writeBoolean(delayRollover)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    super.read(kryo, input)
    tableName = input.readString()
    rowBufferTable = input.readString()
    val numProjections = input.readInt
    projection = input.readInts(numProjections)
    fullScan = input.readBoolean()
    connProperties = ConnectionPropertiesSerializer.read(kryo, input)
    delayRollover = input.readBoolean()
  }
}

final class SmartConnectorColumnRDD(
    @transient private val session: SnappySession,
    private[this] var tableName: String,
    private[this] var rowBufferTable: String,
    private[this] var projection: Array[Int],
    @transient private[sql] val filters: Array[Expression],
    private[this] var connProperties: ConnectionProperties,
    private[this] var schema: StructType,
    @transient private val allParts: Array[Partition],
    @(transient @param) partitionPruner: () => Int,
    private[this] var catalogVersion: Long,
    private[this] var delayRollover: Boolean)
    extends RDDKryo[Any](session.sparkContext, Nil) with KryoSerializable {

  private var serializedFilters: Array[Byte] = _

  private var preferHostName = SmartConnectorHelper.preferHostName(session)

  override def compute(split: Partition,
      context: TaskContext): Iterator[ByteBuffer] = {
    val helper = new SmartConnectorRDDHelper
    val part = split.asInstanceOf[SmartExecutorBucketPartition]
    val listener = SnapshotConnectionListener(context, startSnapshotTx = true, ConnectionType.Net,
      rowBufferTable, lockDiskStore = None, connProperties, delayRollover,
      Right(() => helper.createConnection(connProperties, part.hostList, preferHostName,
        forSnapshot = true) -> catalogVersion))
    val conn = listener.connection
    logDebug(s"Scan for $tableName, Partition index = ${part.index}, bucketId = ${part.bucketId}")
    val partitionId = part.bucketId
    // fetch all the column blobs pushing down the filters
    val (statement, rs) = helper.prepareScan(conn, tableName, projection, serializedFilters, part)
    new ColumnBatchIteratorOnRS(conn, projection, statement, rs, context, partitionId)
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
    output.writeString(rowBufferTable)
    output.writeVarInt(projection.length, true)
    output.writeInts(projection, true)
    val filterLen = if (serializedFilters ne null) serializedFilters.length else 0
    output.writeVarInt(filterLen, true)
    if (filterLen > 0) {
      output.writeBytes(serializedFilters, 0, filterLen)
    }
    ConnectionPropertiesSerializer.write(kryo, output, connProperties)
    StructTypeSerializer.write(kryo, output, schema)
    output.writeVarLong(catalogVersion, false)
    output.writeBoolean(delayRollover)
    output.writeBoolean(preferHostName)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    super.read(kryo, input)

    tableName = input.readString()
    rowBufferTable = input.readString()
    val numColumns = input.readVarInt(true)
    projection = input.readInts(numColumns, true)
    val filterLen = input.readVarInt(true)
    serializedFilters = if (filterLen > 0) input.readBytes(filterLen) else null
    connProperties = ConnectionPropertiesSerializer.read(kryo, input)
    schema = StructTypeSerializer.read(kryo, input, c = null)
    catalogVersion = input.readVarLong(false)
    delayRollover = input.readBoolean()
    preferHostName = input.readBoolean()
  }
}

class SmartConnectorRowRDD(_session: SnappySession,
    _tableName: String,
    _isPartitioned: Boolean,
    _columns: Array[String],
    _isDeltaBuffer: Boolean,
    _connProperties: ConnectionProperties,
    _filters: Array[Expression],
    _partEval: () => Array[Partition],
    _partitionPruner: () => Int = () => -1,
    private[this] var catalogSchemaVersion: Long,
    _delayRollover: Boolean)
    extends RowFormatScanRDD(_session, _tableName, _isPartitioned, _columns,
      pushProjections = true, useResultSet = true, _isDeltaBuffer, _connProperties,
    _filters, _partEval, _partitionPruner, _delayRollover,
    projection = Array.emptyIntArray, None) {

  private var preferHostName = SmartConnectorHelper.preferHostName(session)

  override protected def connectionType: ConnectionType = ConnectionType.Net

  override protected def createConnection(part: Partition): (Connection, Long) = {
    val helper = new SmartConnectorRDDHelper
    helper.createConnection(connProperties, part.asInstanceOf[SmartExecutorBucketPartition]
        .hostList, preferHostName, isDeltaBuffer) -> catalogSchemaVersion
  }

  override def computeResultSet(thePart: Partition,
      listener: SnapshotConnectionListener): (Statement, ResultSet) = {
    val bucketPartition = thePart.asInstanceOf[SmartExecutorBucketPartition]
    val conn = listener.connection
    if (isPartitioned) {
      SmartConnectorHelper.withPartitionAttrs(conn, tableName, bucketPartition.bucketId)(
        computeResultSet(bucketPartition, conn))
    } else computeResultSet(bucketPartition, conn)
  }

  private def computeResultSet(bucketPartition: SmartExecutorBucketPartition,
      conn: Connection): (Statement, ResultSet) = {
    logDebug(s"Scanning row buffer for $tableName,partId=${bucketPartition.index}," +
        s" bucketId = ${bucketPartition.bucketId}")
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
    (stmt, rs)
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
