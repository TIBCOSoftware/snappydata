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

import java.net.URLClassLoader
import java.sql.SQLException
import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.cache.{EntryDestroyedException, RegionDestroyedException}
import com.gemstone.gemfire.internal.cache.lru.LRUEntry
import com.gemstone.gemfire.internal.cache.persistence.query.CloseableIterator
import com.gemstone.gemfire.internal.cache.{BucketRegion, EntryEventImpl, ExternalTableMetaData, LocalRegion, TXManagerImpl, TXStateInterface}
import com.gemstone.gemfire.internal.shared.{FetchRequest, SystemProperties}
import com.gemstone.gemfire.internal.snappy.memory.MemoryManagerStats
import com.gemstone.gemfire.internal.snappy.{CallbackFactoryProvider, ColumnTableEntry, StoreCallbacks, UMMMemoryTracker}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.store.{AbstractCompactExecRow, GemFireContainer}
import com.pivotal.gemfirexd.internal.engine.ui.SnappyRegionStats
import com.pivotal.gemfirexd.internal.iapi.error.{PublicAPI, StandardException}
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController
import com.pivotal.gemfirexd.internal.iapi.util.IdUtil
import com.pivotal.gemfirexd.internal.impl.jdbc.{EmbedConnection, Util}
import com.pivotal.gemfirexd.internal.impl.sql.execute.PrivilegeInfo
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import io.snappydata.SnappyTableStatsProviderService
import io.snappydata.sql.catalog.{CatalogObjectType, SnappyExternalCatalog}

import org.apache.spark.Logging
import org.apache.spark.memory.{MemoryManagerCallback, MemoryMode}
import org.apache.spark.serializer.KryoSerializerPool
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodeGenerator, CodegenContext}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, Literal, TokenLiteral, UnsafeRow}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, expressions}
import org.apache.spark.sql.collection.{SharedUtils, ToolsCallbackInit, Utils}
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.columnar.encoding.ColumnStatsSchema
import org.apache.spark.sql.execution.columnar.{ColumnBatchCreator, ColumnBatchIterator, ColumnTableScan, ExternalStore, ExternalStoreUtils}
import org.apache.spark.sql.hive.SnappyHiveExternalCatalog
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.store.{CodeGeneration, StoreHashFunction}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

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

          // find the dependent indexes
          val dependents = catalogEntry.dependents
          // noinspection EmptyCheck
          val indexes = if ((dependents ne null) && dependents.length != 0) {
            val catalog = Misc.getMemStoreBooting.getExistingExternalCatalog
            dependents.toSeq.flatMap { dep =>
              val (depSchema, depTable) = SnappyExternalCatalog.getTableWithSchema(
                dep, container.getSchemaName)
              val metadata = catalog.getCatalogTableMetadata(depSchema, depTable)
              if ((metadata ne null) && metadata.tableType == CatalogObjectType.Index.toString) {
                Some(metadata)
              } else None
            }
          } else Nil

          val tableName = container.getQualifiedTableName
          // add weightage column for sample tables if required
          var schema = catalogEntry.schema.asInstanceOf[StructType]
          if (catalogEntry.tableType == CatalogObjectType.Sample.toString &&
              !schema(schema.length - 1).name.equalsIgnoreCase(Utils.WEIGHTAGE_COLUMN_NAME)) {
            schema = schema.add(Utils.WEIGHTAGE_COLUMN_NAME,
              LongType, nullable = false)
          }
          val batchCreator = new ColumnBatchCreator(pr, tableName,
            ColumnFormatRelation.columnBatchTableName(tableName), schema,
            catalogEntry.externalStore.asInstanceOf[ExternalStore],
            catalogEntry.compressionCodec)
          batchCreator.createAndStoreBatch(sc, row,
            batchID, bucketID, indexes)
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
    schemas.add(SystemProperties.SNAPPY_HIVE_METASTORE)
    schemas
  }

  override def isColumnTable(qualifiedName: String): Boolean =
    ColumnFormatRelation.isColumnTable(qualifiedName)

  override def skipEvictionForEntry(entry: LRUEntry): Boolean = {
    // skip eviction of stats rows (SNAP-2102)
    entry.getRawKey match {
      case k: ColumnFormatKey => k.columnIndex == ColumnFormatEntry.STATROW_COL_INDEX
      case _ => false
    }
  }

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

  @throws(classOf[SQLException])
  override def columnTableScan(columnTable: String,
      projection: Array[Int], serializedFilters: Array[Byte],
      bucketIds: java.util.Set[Integer]): CloseableIterator[ColumnTableEntry] = {
    // deserialize the filters
    val batchFilters = if ((serializedFilters ne null) && serializedFilters.length > 0) {
      KryoSerializerPool.deserialize(serializedFilters, 0, serializedFilters.length,
        (kryo, in) => kryo.readObject(in, classOf[Array[Filter]])).toSeq
    } else null
    val (region, schemaAttrs, batchFilterExprs) = try {
      val lr = Misc.getRegionForTable(columnTable, true).asInstanceOf[LocalRegion]
      val metadata = ExternalStoreUtils.getExternalTableMetaData(columnTable,
        lr.getUserAttribute.asInstanceOf[GemFireContainer], checkColumnStore = true)
      val schema = metadata.schema.asInstanceOf[StructType].toAttributes
      val filterExprs = if (batchFilters ne null) {
        batchFilters.map(f => translateFilter(f, schema))
      } else null
      (lr, schema, filterExprs)
    } catch {
      case ae: AnalysisException =>
        throw PublicAPI.wrapStandardException(StandardException.newException(
          SQLState.LANG_SYNTAX_OR_ANALYSIS_EXCEPTION, ae, ae.getMessage))
      case e@(_: IllegalStateException | _: RegionDestroyedException) =>
        throw PublicAPI.wrapStandardException(StandardException.newException(
          SQLState.LANG_TABLE_NOT_FOUND, GemFireContainer.getRowBufferTableName(columnTable), e))
    }

    val ctx = new CodegenContext
    val rowClass = classOf[UnsafeRow].getName
    // create the code snippet for applying the filters
    val numRows = ctx.freshName("numRows")
    ctx.addMutableState("int", numRows, "")
    val filterFunction = ColumnTableScan.generateStatPredicate(ctx, isColumnTable = true,
      schemaAttrs, batchFilterExprs, numRows, metricTerm = null, metricAdd = null)
    val filterPredicate = if (filterFunction.isEmpty) null
    else {
      val codeComment = ctx.registerComment(
        s"""Code for connector push down for $columnTable;
          projection=${projection.mkString(", ")}; filters=${batchFilters.mkString(", ")}""")
      val source =
        s"""
          public Object generate(Object[] references) {
            return new GeneratedTableIterator(references);
          }

          $codeComment
          final class GeneratedTableIterator implements ${classOf[StatsPredicate].getName} {

            private Object[] references;
            ${ctx.declareMutableStates()}

            public GeneratedTableIterator(Object[] references) {
              this.references = references;
              ${ctx.initMutableStates()}
              ${ctx.initPartition()}
            }

            ${ctx.declareAddedFunctions()}

            public boolean check($rowClass statsRow, boolean isLastStatsRow, boolean isDelta) {
              // TODO: don't have the update count for delta row (only insert count)
              // so adding the delta "insert" count to full count read in previous call
              $numRows += statsRow.getInt(${ColumnStatsSchema.COUNT_INDEX_IN_SCHEMA});
              return $filterFunction(statsRow, $numRows, isLastStatsRow, isDelta);
            }
         }
      """
      // try to compile, helpful for debug
      val cleanedSource = CodeFormatter.stripOverlappingComments(
        new CodeAndComment(CodeFormatter.stripExtraNewLines(source),
          ctx.getPlaceHolderToComments()))

      CodeGeneration.logDebug(s"\n${CodeFormatter.format(cleanedSource)}")

      val clazz = CodeGenerator.compile(cleanedSource)
      clazz.generate(ctx.references.toArray).asInstanceOf[StatsPredicate]
    }
    val batchIterator = ColumnBatchIterator(region, bucketIds, projection,
      fullScan = (batchFilters eq null) || batchFilters.isEmpty, context = null)
    val numColumnsInStatBlob = ColumnStatsSchema.numStatsColumns(schemaAttrs.length)

    // noinspection TypeAnnotation
    val entriesIter = new Iterator[ArrayBuffer[ColumnTableEntry]] {
      private var numColumns = (projection.length + 1) << 1

      // iterator will remain one step ahead to skip over filtered/deleted batches
      moveNext()

      private def moveNext(): Unit = {
        batchIterator.moveNext()
        while (batchIterator.currentVal ne null) {
          if (batchIterator.currentVal.remaining() == 0) batchIterator.moveNext()
          else if (filterPredicate ne null) {
            // first check the full stats
            val statsRow = SharedUtils.toUnsafeRow(batchIterator.currentVal, numColumnsInStatBlob)
            val deltaStatsRow = SharedUtils.toUnsafeRow(batchIterator.getCurrentDeltaStats,
              numColumnsInStatBlob)
            // check the delta stats after full stats (null columns will be treated as failure
            // which is what is required since it means that only full stats check should be done)
            if (filterPredicate.check(statsRow, deltaStatsRow eq null, isDelta = false) ||
                ((deltaStatsRow ne null) && filterPredicate.check(deltaStatsRow,
                  isLastStatsRow = true, isDelta = true))) {
              return
            }
            batchIterator.moveNext()
          }
          else return
        }
      }

      override def hasNext: Boolean = batchIterator.currentVal ne null

      override def next(): ArrayBuffer[ColumnTableEntry] = {
        val entries = new ArrayBuffer[ColumnTableEntry](numColumns)
        val uuid = batchIterator.getCurrentBatchId
        val bucketId = batchIterator.getCurrentBucketId
        // first add the stats rows and delete bitmask to batchIterator
        addColumnValue(batchIterator.getCurrentStatsColumn, ColumnFormatEntry.STATROW_COL_INDEX,
          uuid, bucketId, entries, throwIfMissing = true)
        addColumnValue(ColumnFormatEntry.DELTA_STATROW_COL_INDEX, uuid, bucketId,
          entries, throwIfMissing = false)
        addColumnValue(ColumnFormatEntry.DELETE_MASK_COL_INDEX, uuid, bucketId,
          entries, throwIfMissing = false)
        // force add all the projected columns and corresponding deltas, if present
        var i = 0
        while (i < projection.length) {
          val columnPosition = projection(i)
          val deltaPosition = ColumnDelta.deltaColumnIndex(columnPosition - 1, 0)
          addColumnValue(columnPosition, uuid, bucketId, entries, throwIfMissing = true)
          addColumnValue(deltaPosition, uuid, bucketId, entries, throwIfMissing = false)
          addColumnValue(deltaPosition - 1, uuid, bucketId, entries, throwIfMissing = false)
          i += 1
        }
        numColumns = entries.size
        moveNext()
        entries
      }

      private def addColumnValue(columnPosition: Int, uuid: Long, bucketId: Int,
          entries: ArrayBuffer[ColumnTableEntry], throwIfMissing: Boolean): Unit = {
        val value = batchIterator.itr.getBucketEntriesIterator
            .asInstanceOf[ClusteredColumnIterator].getColumnValue(columnPosition)
        addColumnValue(value, columnPosition, uuid, bucketId, entries, throwIfMissing)
      }

      private def addColumnValue(value: AnyRef, columnPosition: Int, uuid: Long, bucketId: Int,
          entries: ArrayBuffer[ColumnTableEntry], throwIfMissing: Boolean): Unit = {
        if (value ne null) {
          val columnValue = value.asInstanceOf[ColumnFormatValue].getValueRetain(
            FetchRequest.ORIGINAL)
          if (columnValue.size() > 0) {
            entries += new ColumnTableEntry(uuid, bucketId, columnPosition, columnValue)
            return
          }
        }
        if (throwIfMissing) {
          // empty buffer indicates value removed from region
          val ede = new EntryDestroyedException(s"Iteration on column=$columnPosition " +
              s"partition=$bucketId batchUUID=$uuid failed due to missing value")
          throw PublicAPI.wrapStandardException(StandardException.newException(
            SQLState.DATA_UNEXPECTED_EXCEPTION, ede))
        }
      }
    }
    new CloseableIterator[ColumnTableEntry] {
      private val iter = entriesIter.flatten

      override def hasNext: Boolean = iter.hasNext

      override def next(): ColumnTableEntry = iter.next()

      override def close(): Unit = batchIterator.close()
    }
  }

  private def attr(a: String, schema: Seq[AttributeReference]): AttributeReference = {
    // filter passed should have same case as in schema and not be qualified which
    // should be true since these have been created from resolved Expression by sender
    schema.find(_.name == a) match {
      case Some(attr) => attr
      case _ => throw Utils.analysisException(s"Could not find $a in ${schema.mkString(", ")}")
    }
  }

  /**
   * Translate a data source [[Filter]] into Catalyst [[Expression]].
   */
  private[sql] def translateFilter(filter: Filter,
      schema: Seq[AttributeReference]): Expression = filter match {
    case sources.EqualTo(a, v) =>
      expressions.EqualTo(attr(a, schema), TokenLiteral.newToken(v))
    case sources.EqualNullSafe(a, v) =>
      expressions.EqualNullSafe(attr(a, schema), TokenLiteral.newToken(v))

    case sources.GreaterThan(a, v) =>
      expressions.GreaterThan(attr(a, schema), TokenLiteral.newToken(v))
    case sources.LessThan(a, v) =>
      expressions.LessThan(attr(a, schema), TokenLiteral.newToken(v))

    case sources.GreaterThanOrEqual(a, v) =>
      expressions.GreaterThanOrEqual(attr(a, schema), TokenLiteral.newToken(v))
    case sources.LessThanOrEqual(a, v) =>
      expressions.LessThanOrEqual(attr(a, schema), TokenLiteral.newToken(v))

    case sources.In(a, list) =>
      val set = if (list.length > 0) {
        val l = Literal(list(0))
        val toCatalyst = CatalystTypeConverters.createToCatalystConverter(l.dataType)
        list.map(v => new TokenLiteral(toCatalyst(v), l.dataType)).toVector
      } else Vector.empty
      expressions.DynamicInSet(attr(a, schema), set)

    case sources.IsNull(a) => expressions.IsNull(attr(a, schema))
    case sources.IsNotNull(a) => expressions.IsNotNull(attr(a, schema))

    case sources.And(left, right) =>
      expressions.And(translateFilter(left, schema), translateFilter(right, schema))
    case sources.Or(left, right) =>
      expressions.Or(translateFilter(left, schema), translateFilter(right, schema))
    case sources.Not(child) => expressions.Not(translateFilter(child, schema))

    case sources.StringStartsWith(a, v) =>
      expressions.StartsWith(attr(a, schema),
        new TokenLiteral(UTF8String.fromString(v), StringType))
    case sources.StringEndsWith(a, v) =>
      expressions.EndsWith(attr(a, schema),
        new TokenLiteral(UTF8String.fromString(v), StringType))
    case sources.StringContains(a, v) =>
      expressions.Contains(attr(a, schema),
        new TokenLiteral(UTF8String.fromString(v), StringType))

    case _ => throw new IllegalStateException(s"translateFilter: unexpected filter = $filter")
  }

  override def registerCatalogSchemaChange(): Unit = {
    val catalog = SnappyHiveExternalCatalog.getInstance
    if (catalog ne null) catalog.registerCatalogSchemaChange(Nil)
  }

  def getSnappyTableStats: AnyRef = {
    val c = SnappyTableStatsProviderService.getService
        .refreshAndGetTableSizeStats.values.asJavaCollection
    val list: java.util.List[SnappyRegionStats] = new java.util.ArrayList(c.size())
    list.addAll(c)
    list
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

  override def waitForRuntimeManager(maxWaitMillis: Long): Unit = {
    val memoryManager = MemoryManagerCallback.memoryManager
    if (memoryManager.bootManager) {
      val endWait = System.currentTimeMillis() + math.max(10, maxWaitMillis)
      do {
        var interrupt: InterruptedException = null
        try {
          Thread.sleep(10)
        } catch {
          case ie: InterruptedException => interrupt = ie
        }
        val cache = Misc.getGemFireCacheNoThrow
        if (cache ne null) {
          cache.getCancelCriterion.checkCancelInProgress(interrupt)
          if (interrupt ne null) Thread.currentThread().interrupt()
        }
      } while (MemoryManagerCallback.memoryManager.bootManager &&
          System.currentTimeMillis() < endWait)
    }
  }

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

  override def clearConnectionPools(): Unit = {
    ConnectionPool.clear()
  }

  override def getLeadClassLoader: URLClassLoader =
    ToolsCallbackInit.toolsCallback.getLeadClassLoader

  override def clearSessionCache(onlyQueryPlanCache: Boolean = false): Unit = {
    SnappySession.clearAllCache(onlyQueryPlanCache)
  }

  override def refreshPolicies(ldapGroup: String): Unit = {
    SnappyHiveExternalCatalog.getExistingInstance.refreshPolicies(ldapGroup)
  }

  override def checkSchemaPermission(schemaName: String, currentUser: String): String = {
    val ms = Misc.getMemStoreBootingNoThrow
    val userId = IdUtil.getUserAuthorizationId(currentUser)
    if (ms ne null) {
      var conn: EmbedConnection = null
      if (ms.isSnappyStore && Misc.isSecurityEnabled) {
        var contextSet = false
        try {
          val dd = ms.getDatabase.getDataDictionary
          conn = GemFireXDUtils.getTSSConnection(false, true, false)
          conn.getTR.setupContextStack()
          contextSet = true
          val schema = Utils.toUpperCase(schemaName)
          val sd = dd.getSchemaDescriptor(
            schema, conn.getLanguageConnection.getTransactionExecute, false)
          if (sd eq null) {
            if (schema.equalsIgnoreCase(userId) ||
                schema.equalsIgnoreCase(userId.replace('-', '_'))) {
              if (ms.tableCreationAllowed()) return userId
              throw StandardException.newException(SQLState.AUTH_NO_ACCESS_NOT_OWNER,
                schema, schema)
            } else {
              throw StandardException.newException(SQLState.LANG_SCHEMA_DOES_NOT_EXIST, schema)
            }
          }
          PrivilegeInfo.checkOwnership(userId, sd, sd, dd)
          sd.getAuthorizationId
        } catch {
          case se: StandardException => throw Util.generateCsSQLException(se)
        } finally {
          if (contextSet) conn.getTR.restoreContextStack()
        }
      } else userId
    } else userId
  }
}

trait StoreCallback extends Serializable {
  CallbackFactoryProvider.setStoreCallbacks(StoreCallbacksImpl)
}

/**
 * The type of the generated class used by column stats check for a column batch.
 * Since there can be up-to two stats rows (full stats and delta stats), this has
 * an additional argument for the same to determine whether to update metrics or not.
 */
trait StatsPredicate {
  def check(row: UnsafeRow, isLastStatsRow: Boolean, isDelta: Boolean): Boolean
}
