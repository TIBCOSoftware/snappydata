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
package org.apache.spark.sql.execution.columnar.impl

import java.sql.{Connection, ResultSet, Statement}
import java.util.UUID

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.gemstone.gemfire.internal.cache.{AbstractRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.impl.SparkShellRDDHelper

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.ConnectionPropertiesSerializer
import org.apache.spark.sql.collection._
import org.apache.spark.sql.execution.RDDKryo
import org.apache.spark.sql.execution.columnar._
import org.apache.spark.sql.execution.row.RowFormatScanRDD
import org.apache.spark.sql.sources.{ConnectionProperties, Filter}
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.{SnappySession, SparkSession}
import org.apache.spark.{Partition, TaskContext}

/**
 * Column Store implementation for GemFireXD.
 */
class JDBCSourceAsColumnarStore(_connProperties: ConnectionProperties,
    _numPartitions: Int)
    extends JDBCSourceAsStore(_connProperties, _numPartitions) {

  self =>

  override def getConnectedExternalStore(tableName: String,
      onExecutor: Boolean): ConnectedExternalStore =
    new JDBCSourceAsColumnarStore(_connProperties, _numPartitions)
        with ConnectedExternalStore {
      protected[this] override val connectedInstance: Connection =
        self.getConnection(tableName, onExecutor)
    }

  override def getCachedBatchRDD(tableName: String, requiredColumns: Array[String],
      session: SparkSession): RDD[CachedBatch] = {
    val snappySession = session.asInstanceOf[SnappySession]
    connectionType match {
      case ConnectionType.Embedded =>
        new ColumnarStorePartitionedRDD(snappySession,
          tableName, this).asInstanceOf[RDD[CachedBatch]]
      case _ =>
        // remove the url property from poolProps since that will be
        // partition-specific
        val poolProps = _connProperties.poolProps -
            (if (_connProperties.hikariCP) "jdbcUrl" else "url")
        new SparkShellCachedBatchRDD(snappySession,
          tableName, requiredColumns, ConnectionProperties(_connProperties.url,
            _connProperties.driver, _connProperties.dialect, poolProps,
            _connProperties.connProps, _connProperties.executorConnProps,
            _connProperties.hikariCP), this)
    }
  }

  override protected def doInsert(tableName: String, batch: CachedBatch,
      batchId: UUID, partitionId: Int): (Connection => Any) = {
    {
      (connection: Connection) => {
        super.doInsert(tableName, batch, batchId, partitionId)(connection)
      }
    }
  }

  override protected def getPartitionID(tableName: String,
      partitionId: Int = -1): Int = {
    val connection = getConnection(tableName, onExecutor = true)
    try {
      connectionType match {
        case ConnectionType.Embedded =>
          val resolvedName = ExternalStoreUtils.lookupName(tableName,
            connection.getSchema)
          val region = Misc.getRegionForTable(resolvedName, true)
          region.asInstanceOf[AbstractRegion] match {
            case pr: PartitionedRegion =>
              if (partitionId == -1) {
                val primaryBucketIds = pr.getDataStore.
                    getAllLocalPrimaryBucketIdArray
                // TODO: do load-balancing among partitions instead
                // of random selection
                val numPrimaries = primaryBucketIds.size()
                // if no local primary bucket, then select some other
                if (numPrimaries > 0) {
                  primaryBucketIds.getQuick(rand.nextInt(numPrimaries))
                } else {
                  rand.nextInt(pr.getTotalNumberOfBuckets)
                }
              } else {
                partitionId
              }
            case _ => partitionId
          }
        // TODO: SW: for split mode, get connection to one of the
        // local servers and a bucket ID for only one of those
        case _ => rand.nextInt(_numPartitions)
      }
    } finally {
      connection.close()
    }
  }
}

final class ColumnarStorePartitionedRDD(
    @transient private val session: SnappySession,
    private var tableName: String,
    @transient private val store: JDBCSourceAsColumnarStore)
    extends RDDKryo[Any](session.sparkContext, Nil) with KryoSerializable {

  override def compute(part: Partition, context: TaskContext): Iterator[Any] = {
    val container = GemFireXDUtils.getGemFireContainer(tableName, true)
    val bucketIds = part match {
      case p: MultiBucketExecutorPartition => p.buckets
      case _ => java.util.Collections.singleton(Int.box(part.index))
    }
    if (container.isOffHeap) new OffHeapLobsIteratorOnScan(container, bucketIds)
    else new ByteArraysIteratorOnScan(container, bucketIds)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[MultiBucketExecutorPartition].hostExecutorIds
  }

  override protected def getPartitions: Array[Partition] = {
    store.tryExecute(tableName, conn => {
      val resolvedName = ExternalStoreUtils.lookupName(tableName,
        conn.getSchema)
      val region = Misc.getRegionForTable(resolvedName, true)
      session.sessionState.getTablePartitions(
        region.asInstanceOf[PartitionedRegion])
    })
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    super.write(kryo, output)
    output.writeString(tableName)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    super.read(kryo, input)
    tableName = input.readString()
  }
}

final class SparkShellCachedBatchRDD(
    @transient private val session: SnappySession,
    private var tableName: String,
    private var requiredColumns: Array[String],
    private var connProperties: ConnectionProperties,
    @transient private val store: ExternalStore)
    extends RDDKryo[CachedBatch](session.sparkContext, Nil)
        with KryoSerializable {

  override def compute(split: Partition,
      context: TaskContext): Iterator[CachedBatch] = {
    val helper = new SparkShellRDDHelper
    val conn: Connection = helper.getConnection(connProperties, split)
    val query: String = helper.getSQLStatement(ExternalStoreUtils.lookupName(
      tableName, conn.getSchema), requiredColumns, split.index)
    val (statement, rs) = helper.executeQuery(conn, tableName, split, query)
    new CachedBatchIteratorOnRS(conn, requiredColumns, statement, rs, context)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[ExecutorMultiBucketLocalShellPartition]
        .hostList.map(_._1.asInstanceOf[String])
  }

  override def getPartitions: Array[Partition] = {
    store.tryExecute(tableName, SparkShellRDDHelper.getPartitions(tableName, _))
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

class SparkShellRowRDD(_session: SnappySession,
    _tableName: String,
    _isPartitioned: Boolean,
    _columns: Array[String],
    _connProperties: ConnectionProperties,
    _filters: Array[Filter] = Array.empty[Filter],
    _parts: Array[Partition] = Array.empty[Partition])
    extends RowFormatScanRDD(_session, _tableName, _isPartitioned, _columns,
      pushProjections = true, useResultSet = true, _connProperties,
      _filters, _parts) {

  override def computeResultSet(
      thePart: Partition): (Connection, Statement, ResultSet) = {
    val helper = new SparkShellRDDHelper
    val conn: Connection = helper.getConnection(
      connProperties, thePart)
    val resolvedName = StoreUtils.lookupName(tableName, conn.getSchema)

    if (isPartitioned) {
      val ps = conn.prepareStatement(
        "call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION(?, ?)")
      ps.setString(1, resolvedName)
      val partition = thePart.asInstanceOf[ExecutorMultiBucketLocalShellPartition]
      val bucketString = partition.buckets.mkString(",")
      ps.setString(2, bucketString)
      ps.executeUpdate()
      ps.close()
    }
    val sqlText = s"SELECT $columnList FROM $resolvedName$filterWhereClause"

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
    (conn, stmt, rs)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[ExecutorMultiBucketLocalShellPartition]
        .hostList.map(_._1.asInstanceOf[String])
  }

  override def getPartitions: Array[Partition] = {
    // use incoming partitions if provided (e.g. for collocated tables)
    if (parts != null && parts.length > 0) {
      return parts
    }
    val conn = ExternalStoreUtils.getConnection(tableName, connProperties,
      forExecutor = true)
    try {
      SparkShellRDDHelper.getPartitions(tableName, conn)
    } finally {
      conn.close()
    }
  }

  def getSQLStatement(resolvedTableName: String,
      requiredColumns: Array[String], partitionId: Int): String = {
    "select " + requiredColumns.mkString(", ") + " from " + resolvedTableName
  }
}
