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

import java.util.Collections

import scala.collection.JavaConverters._

import com.gemstone.gemfire.internal.cache.{BucketRegion, EntryEventImpl, ExternalTableMetaData, TXManagerImpl, TXStateInterface}
import com.gemstone.gemfire.internal.snappy.memory.MemoryManagerStats
import com.gemstone.gemfire.internal.snappy.{CallbackFactoryProvider, StoreCallbacks, UMMMemoryTracker}
import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.store.{AbstractCompactExecRow, GemFireContainer}
import com.pivotal.gemfirexd.internal.engine.ui.SnappyRegionStats
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection
import com.pivotal.gemfirexd.internal.snappy.LeadNodeSmartConnectorOpContext
import io.snappydata.SnappyTableStatsProviderService

import org.apache.spark.memory.{MemoryManagerCallback, MemoryMode}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogFunction, FunctionResource, JarResource}
import org.apache.spark.sql.catalyst.expressions.SortDirection
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.{ColumnBatchCreator, ExternalStore}
import org.apache.spark.sql.hive.{ExternalTableType, SnappyStoreHiveCatalog}
import org.apache.spark.sql.store.StoreHashFunction
import org.apache.spark.sql.types._
import org.apache.spark.{Logging, SparkContext}

object StoreCallbacksImpl extends StoreCallbacks with Logging with Serializable {

  private val partitioner = new StoreHashFunction

  override def registerTypes(): Unit = {
    // register the column key and value types
    ColumnFormatEntry.registerTypes()
  }

  override def createColumnBatch(region: BucketRegion, batchID: Long,
      bucketID: Int): java.util.Set[AnyRef] = {
    val pr = region.getPartitionedRegion
    val container = pr.getUserAttribute.asInstanceOf[GemFireContainer]
    val catalogEntry: ExternalTableMetaData = container.fetchHiveMetaData(false)

    if (catalogEntry != null) {
      // LCC should be available assuming insert is already being done
      // via a proper connection
      var conn: EmbedConnection = null
      var contextSet: Boolean = false
      var txStateSet: Boolean = false
      try {
        var lcc: LanguageConnectionContext = Misc.getLanguageConnectionContext
        if (lcc == null) {
          conn = GemFireXDUtils.getTSSConnection(true, true, false)
          conn.getTR.setupContextStack()
          contextSet = true
          lcc = conn.getLanguageConnectionContext
          if (lcc == null) {
            Misc.getGemFireCache.getCancelCriterion.checkCancelInProgress(null)
          }
        }
        val row: AbstractCompactExecRow = container.newTemplateRow()
            .asInstanceOf[AbstractCompactExecRow]
        val tc = lcc.getTransactionExecute.asInstanceOf[GemFireTransaction]
        lcc.setExecuteLocally(Collections.singleton(bucketID), pr, false, null)
        try {
          val state: TXStateInterface = TXManagerImpl.getCurrentTXState
          if (tc.getCurrentTXStateProxy == null && state != null) {
            tc.setActiveTXState(state, true)
            txStateSet = true
          }
          val sc = lcc.getTransactionExecute.openScan(
            container.getId.getContainerId, false, 0,
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_NOLOCK /* not used */ ,
            null, null, 0, null, null, 0, null)

          val dependents = if (catalogEntry.dependents != null) {
            val tables = Misc.getMemStore.getAllContainers.asScala.
                map(x => (x.getSchemaName + "." + x.getTableName, x.fetchHiveMetaData(false)))
            catalogEntry.dependents.toSeq.map(x => tables.find(x == _._1).get._2)
          } else {
            Seq.empty
          }

          val tableName = container.getQualifiedTableName
          // add weightage column for sample tables if required
          var schema = catalogEntry.schema.asInstanceOf[StructType]
          if (catalogEntry.tableType == ExternalTableType.Sample.name &&
              schema(schema.length - 1).name != Utils.WEIGHTAGE_COLUMN_NAME) {
            schema = schema.add(Utils.WEIGHTAGE_COLUMN_NAME,
              LongType, nullable = false)
          }
          val batchCreator = new ColumnBatchCreator(pr,
            ColumnFormatRelation.columnBatchTableName(tableName), schema,
            catalogEntry.externalStore.asInstanceOf[ExternalStore],
            catalogEntry.compressionCodec)
          batchCreator.createAndStoreBatch(sc, row,
            batchID, bucketID, dependents)
        } finally {
          lcc.clearExecuteLocally()
          if (txStateSet) tc.clearActiveTXState(false, true)
        }
      } catch {
        case e: Throwable => throw e
      } finally {
        if (contextSet) {
          conn.getTR.restoreContextStack()
        }
      }
    } else {
      java.util.Collections.emptySet[AnyRef]()
    }
  }

  override def invokeColumnStorePutCallbacks(bucket: BucketRegion,
      events: Array[EntryEventImpl]): Unit = {
    val container = bucket.getPartitionedRegion.getUserAttribute
        .asInstanceOf[GemFireContainer]
    if ((container ne null) && container.isObjectStore) {
      container.getRowEncoder.afterColumnStorePuts(bucket, events)
    }
  }

  def getInternalTableSchemas: java.util.List[String] = {
    val schemas = new java.util.ArrayList[String](1)
    schemas.add(SnappyStoreHiveCatalog.HIVE_METASTORE)
    schemas
  }

  override def isColumnTable(qualifiedName: String): Boolean =
    ColumnFormatRelation.isColumnTable(qualifiedName)

  override def getHashCodeSnappy(dvd: scala.Any, numPartitions: Int): Int = {
    partitioner.computeHash(dvd, numPartitions)
  }

  override def getHashCodeSnappy(dvds: scala.Array[Object],
      numPartitions: Int): Int = {
    partitioner.computeHash(dvds, numPartitions)
  }

  override def columnBatchTableName(table: String): String = {
    ColumnFormatRelation.columnBatchTableName(table)
  }

  override def registerRelationDestroyForHiveStore(): Unit = {
    SnappyStoreHiveCatalog.registerRelationDestroy()
  }

  def getSnappyTableStats: AnyRef = {
    val c = SnappyTableStatsProviderService.getService
        .getTableSizeStats.values.asJavaCollection
    val list: java.util.List[SnappyRegionStats] = new java.util.ArrayList(c.size())
    list.addAll(c)
    list
  }

  override def performConnectorOp(ctx: Object): Unit = {

    val context = ctx.asInstanceOf[LeadNodeSmartConnectorOpContext]

    val session = SnappyContext(null: SparkContext).snappySession
    if (context.getUserName != null && !context.getUserName.isEmpty) {
      session.conf.set(Attribute.USERNAME_ATTR, context.getUserName)
      session.conf.set(Attribute.PASSWORD_ATTR, context.getAuthToken)
    }

    context.getType match {
      case LeadNodeSmartConnectorOpContext.OpType.CREATE_TABLE =>

        val tableIdent = context.getTableIdentifier
        val userSpecifiedJsonSchema = Option(context.getUserSpecifiedJsonSchema)
        val userSpecifiedSchema = if (userSpecifiedJsonSchema.isDefined) {
          Option(DataType.fromJson(userSpecifiedJsonSchema.get).asInstanceOf[StructType])
        } else {
          None
        }
        val schemaDDL = Option(context.getSchemaDDL)
        val provider = context.getProvider
        val mode = SmartConnectorHelper.deserialize(context.getMode).asInstanceOf[SaveMode]
        val options = SmartConnectorHelper
            .deserialize(context.getOptions).asInstanceOf[Map[String, String]]
        val isBuiltIn = context.getIsBuiltIn

        logDebug(s"StoreCallbacksImpl.performConnectorOp creating table $tableIdent")
        session.createTable(session.sessionCatalog.newQualifiedTableName(tableIdent),
          provider, userSpecifiedSchema, schemaDDL, mode, options, isBuiltIn)

      case LeadNodeSmartConnectorOpContext.OpType.DROP_TABLE =>
        val tableIdent = context.getTableIdentifier
        val ifExists = context.getIfExists

        logDebug(s"StoreCallbacksImpl.performConnectorOp dropping table $tableIdent")
        session.dropTable(session.sessionCatalog.newQualifiedTableName(tableIdent), ifExists)

      case LeadNodeSmartConnectorOpContext.OpType.CREATE_INDEX =>
        val tableIdent = context.getTableIdentifier
        val indexIdent = context.getIndexIdentifier
        val indexColumns = SmartConnectorHelper
            .deserialize(context.getIndexColumns).asInstanceOf[Map[String, Option[SortDirection]]]
        val options = SmartConnectorHelper
            .deserialize(context.getOptions).asInstanceOf[Map[String, String]]

        logDebug(s"StoreCallbacksImpl.performConnectorOp creating index $indexIdent")
        session.createIndex(
          session.sessionCatalog.newQualifiedTableName(indexIdent),
          session.sessionCatalog.newQualifiedTableName(tableIdent),
          indexColumns, options)

      case LeadNodeSmartConnectorOpContext.OpType.DROP_INDEX =>
        val indexIdent = context.getIndexIdentifier
        val ifExists = context.getIfExists

        logDebug(s"StoreCallbacksImpl.performConnectorOp dropping index $indexIdent")
        session.dropIndex(session.sessionCatalog.newQualifiedTableName(indexIdent), ifExists)

      case LeadNodeSmartConnectorOpContext.OpType.CREATE_UDF =>
        val db = context.getDb
        val className = context.getClassName
        val functionName = context.getFunctionName
        val jarURI = context.getjarURI()
        val resources: Seq[FunctionResource] = Seq(FunctionResource(JarResource, jarURI))

        logDebug(s"StoreCallbacksImpl.performConnectorOp creating udf $functionName")
        val functionDefinition = CatalogFunction(new FunctionIdentifier(
          functionName, Option(db)), className, resources)
        session.sharedState.externalCatalog.createFunction(db, functionDefinition)

      case LeadNodeSmartConnectorOpContext.OpType.DROP_UDF =>
        val db = context.getDb
        val functionName = context.getFunctionName

        logDebug(s"StoreCallbacksImpl.performConnectorOp dropping udf $functionName")
        session.sharedState.externalCatalog.dropFunction(db, functionName)

      case LeadNodeSmartConnectorOpContext.OpType.ALTER_TABLE =>
        val tableName = context.getTableIdentifier
        val addOrDropCol = context.getAddOrDropCol
        val columnName = context.getColumnName
        val columnDataType = context.getColumnDataType
        val columnNullable = context.getColumnNullable
        logDebug(s"StoreCallbacksImpl.performConnectorOp alter table ")
        session.alterTable(tableName, addOrDropCol, StructField(columnName,
          CatalystSqlParser.parseDataType(columnDataType), columnNullable))
        SnappySession.clearAllCache()
      case _ =>
        throw new AnalysisException("StoreCallbacksImpl.performConnectorOp unknown option")
    }

  }

  override def getLastIndexOfRow(o: Object): Int = {
    val r = o.asInstanceOf[Row]
    if (r != null) {
      r.getInt(r.length - 1)
    } else {
      -1
    }
  }

  override def acquireStorageMemory(objectName: String, numBytes: Long,
      buffer: UMMMemoryTracker, shouldEvict: Boolean, offHeap: Boolean): Boolean = {
    val mode = if (offHeap) MemoryMode.OFF_HEAP else MemoryMode.ON_HEAP
    if (numBytes > 0) {
      return MemoryManagerCallback.memoryManager.acquireStorageMemoryForObject(objectName,
        MemoryManagerCallback.storageBlockId, numBytes, mode, buffer, shouldEvict)
    } else if (numBytes < 0) {
      MemoryManagerCallback.memoryManager.releaseStorageMemoryForObject(
        objectName, -numBytes, mode)
    }
    true
  }

  override def releaseStorageMemory(objectName: String, numBytes: Long,
      offHeap: Boolean): Unit = {
    val mode = if (offHeap) MemoryMode.OFF_HEAP else MemoryMode.ON_HEAP
    MemoryManagerCallback.memoryManager.
        releaseStorageMemoryForObject(objectName, numBytes, mode)
  }

  override def dropStorageMemory(objectName: String, ignoreBytes: Long): Unit =
    // off-heap will be cleared via ManagedDirectBufferAllocator
    MemoryManagerCallback.memoryManager.
      dropStorageMemoryForObject(objectName, MemoryMode.ON_HEAP, ignoreBytes)

  override def resetMemoryManager(): Unit = MemoryManagerCallback.resetMemoryManager()

  override def isSnappyStore: Boolean = true

  override def getStoragePoolUsedMemory(offHeap: Boolean): Long =
    MemoryManagerCallback.memoryManager.getStoragePoolMemoryUsed(
      if (offHeap) MemoryMode.OFF_HEAP else MemoryMode.ON_HEAP)

  override def getStoragePoolSize(offHeap: Boolean): Long =
    MemoryManagerCallback.memoryManager.getStoragePoolSize(
      if (offHeap) MemoryMode.OFF_HEAP else MemoryMode.ON_HEAP)

  override def getExecutionPoolUsedMemory(offHeap: Boolean): Long =
    MemoryManagerCallback.memoryManager.getExecutionPoolUsedMemory(
      if (offHeap) MemoryMode.OFF_HEAP else MemoryMode.ON_HEAP)

  override def getExecutionPoolSize(offHeap: Boolean): Long =
    MemoryManagerCallback.memoryManager.getExecutionPoolSize(
      if (offHeap) MemoryMode.OFF_HEAP else MemoryMode.ON_HEAP)

  override def getOffHeapMemory(objectName: String): Long =
    MemoryManagerCallback.memoryManager.getOffHeapMemory(objectName)

  override def hasOffHeap: Boolean =
    MemoryManagerCallback.memoryManager.hasOffHeap

  override def logMemoryStats(): Unit =
    MemoryManagerCallback.memoryManager.logStats()

  override def shouldStopRecovery(): Boolean =
    MemoryManagerCallback.memoryManager.shouldStopRecovery()

  override def initMemoryStats(stats: MemoryManagerStats): Unit =
    MemoryManagerCallback.memoryManager.initMemoryStats(stats)
}

trait StoreCallback extends Serializable {
  CallbackFactoryProvider.setStoreCallbacks(StoreCallbacksImpl)
}
