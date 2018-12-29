/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

package io.snappydata

import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.function.{BiFunction, Predicate, Function => JFunction}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.control.NonFatal

import com.gemstone.gemfire.CancelException
import com.gemstone.gemfire.cache.execute.FunctionService
import com.gemstone.gemfire.cache.{IsolationLevel, LockTimeoutException}
import com.gemstone.gemfire.i18n.LogWriterI18n
import com.gemstone.gemfire.internal.SystemTimer
import com.gemstone.gemfire.internal.cache._
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdListResultCollector.ListResultCollectorValue
import com.pivotal.gemfirexd.internal.engine.distributed.{GfxdListResultCollector, GfxdMessage}
import com.pivotal.gemfirexd.internal.engine.locks.GfxdLockSet
import com.pivotal.gemfirexd.internal.engine.sql.execute.MemberStatisticsMessage
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer
import com.pivotal.gemfirexd.internal.engine.ui._
import io.snappydata.sql.catalog.CatalogObjectType
import org.eclipse.collections.impl.map.mutable.UnifiedMap
import org.eclipse.collections.impl.set.mutable.UnifiedSet

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.impl.{ColumnDelta, ColumnFormatEntry, ColumnFormatKey, ColumnFormatValue, RemoteEntriesIterator}
import org.apache.spark.sql.execution.columnar.{ColumnBatchIterator, ColumnInsertExec, ColumnTableScan, ExternalStore, ExternalStoreUtils}
import org.apache.spark.sql.execution.row.ResultSetTraversal
import org.apache.spark.sql.execution.{BufferedRowIterator, WholeStageCodegenExec}
import org.apache.spark.sql.store.CodeGeneration
import org.apache.spark.sql.{SnappyContext, ThinClientConnectorMode}

/*
* Object that encapsulates the actual stats provider service. Stats provider service
* will either be SnappyEmbeddedTableStatsProviderService or SnappyThinConnectorTableStatsProvider
 */
object SnappyTableStatsProviderService {
  // var that points to the actual stats provider service
  private var statsProviderService: TableStatsProviderService = _

  def start(sc: SparkContext, url: String): Unit = {
    SnappyContext.getClusterMode(sc) match {
      case ThinClientConnectorMode(_, _) =>
        statsProviderService = SnappyThinConnectorTableStatsProvider
      case _ =>
        statsProviderService = SnappyEmbeddedTableStatsProviderService
    }
    statsProviderService.start(sc, url)
  }

  def stop(): Unit = {
    val service = statsProviderService
    if (service ne null) {
      service.stop()
    }
  }

  def getService: TableStatsProviderService = {
    val service = statsProviderService
    if (service eq null) {
      throw new IllegalStateException("SnappyTableStatsProviderService not started")
    }
    service
  }

  // only for testing
  var TEST_SUSPEND_CACHE_INVALIDATION = false
}

object SnappyEmbeddedTableStatsProviderService extends TableStatsProviderService {

  override def start(sc: SparkContext, url: String): Unit = {
    if (!doRun) {
      this.synchronized {
        if (!doRun) {
          doRun = true
          Misc.getGemFireCache.getCCPTimer.schedule(
            new SystemTimer.SystemTimerTask {
              private val logger: LogWriterI18n = Misc.getGemFireCache.getLoggerI18n

              override def run2(): Unit = {
                try {
                  if (doRun) {
                    aggregateStats()
                  }
                } catch {
                  case _: CancelException => // ignore
                  case e: Exception => if (e.getMessage != null && !e.getMessage.contains(
                    "com.gemstone.gemfire.cache.CacheClosedException")) {
                    logger.warning(e)
                  } else {
                    logger.error(e)
                  }
                }
              }

              override def getLoggerI18n: LogWriterI18n = {
                logger
              }
            }, delayMillis, delayMillis)
        }
      }
    }
  }

  override def fillAggregatedMemberStatsOnDemand(): Unit = {

    try {
      val existingMembers = membersInfo.keys.toArray
      val collector = new GfxdListResultCollector(null, true)
      val msg = new MemberStatisticsMessage(collector)

      msg.executeFunction()

      val memStats = collector.getResult

      val itr = memStats.iterator()

      val members = new UnifiedMap[String, MemberStatistics](8)
      while (itr.hasNext) {
        val o = itr.next().asInstanceOf[ListResultCollectorValue]
        val memMap = o.resultOfSingleExecution.asInstanceOf[java.util.HashMap[String, Any]]

        val dssUUID = memMap.get("diskStoreUUID").asInstanceOf[java.util.UUID]
        val id = memMap.get("id").toString

        var memberStats: MemberStatistics = {
          if (dssUUID != null && membersInfo.contains(dssUUID.toString)) {
            membersInfo(dssUUID.toString)
          } else if (membersInfo.contains(id)) {
            membersInfo(id)
          } else {
            null
          }
        }

        if (memberStats == null) {
          memberStats = new MemberStatistics(memMap)
          if (dssUUID != null) {
            members.put(dssUUID.toString, memberStats)
          }
        } else {
          memberStats.updateMemberStatistics(memMap)
          if (dssUUID != null) {
            members.put(dssUUID.toString, memberStats)
          }
        }

        memberStats.setStatus("Running")
      }
      membersInfo ++= mapAsScalaMapConverter(members).asScala
      // mark members no longer running as stopped
      existingMembers.filterNot(members.containsKey).foreach(m =>
        membersInfo(m).setStatus("Stopped"))

      // update cluster level stats
      ClusterStatistics.getInstance().updateClusterStatistics(membersInfo.asJava)

    } catch {
      case NonFatal(e) => logWarning(e.getMessage, e)
    }
  }

  override def getStatsFromAllServers(sc: Option[SparkContext] = None): (Seq[SnappyRegionStats],
      Seq[SnappyIndexStats], Seq[SnappyExternalTableStats]) = {
    var result: Seq[SnappyRegionStatsCollectorResult] = Nil
    val dataServers = GfxdMessage.getAllDataStores
    var resultObtained: Boolean = false
    try {
      if (dataServers != null && dataServers.size() > 0) {
        result = FunctionService.onMembers(dataServers)
            // .withCollector(new GfxdListResultCollector())
            .execute(SnappyRegionStatsCollectorFunction.ID).getResult(5, TimeUnit.SECONDS).
            asInstanceOf[java.util.ArrayList[SnappyRegionStatsCollectorResult]]
            .asScala
        resultObtained = true
      }
    }
    catch {
      case NonFatal(e) =>
        log.warn("Exception occurred while collecting Table Statistics: " + e.getMessage)
        log.debug(e.getMessage, e)
    }

    val hiveTables = Misc.getMemStore.getExternalCatalog.getCatalogTables.asScala

    val externalTables: mutable.Buffer[SnappyExternalTableStats] = {
      try {
        // External Tables
        hiveTables.collect {
          case table if table.tableType == CatalogObjectType.External.toString =>
            new SnappyExternalTableStats(table.entityName, table.tableType, table.schema,
              table.shortProvider, table.externalStore, table.dataSourcePath, table.driverClass)
        }
      } catch {
        case NonFatal(e) =>
          log.warn("Exception occurred while collecting External Table Statistics: " + e.getMessage)
          log.debug(e.getMessage, e)
          mutable.Buffer.empty[SnappyExternalTableStats]
      }
    }

    if (resultObtained) {
      // Return updated tableSizeInfo
      // Map to hold hive table type against table names as keys
      val tableTypesMap: mutable.HashMap[String, String] = mutable.HashMap.empty[String, String]
      hiveTables.foreach(ht => {
        val key = ht.schema.toString + "." + ht.entityName
        tableTypesMap.put(key, ht.tableType)
      })

      val regionStats = result.flatMap(_.getRegionStats.asScala).map(rs => {
        val tableName = rs.getTableName
        try tableTypesMap.get(tableName) match {
          case Some(t) if CatalogObjectType.isColumnTable(CatalogObjectType.withName(
            Utils.toUpperCase(t))) => rs.setColumnTable(true)
          case _ => rs.setColumnTable(false)
        } catch {
          case _: Exception => rs.setColumnTable(false)
        }
        rs
      })

      // Return updated details
      (regionStats, result.flatMap(_.getIndexStats.asScala), externalTables)
    } else {
      // Return last successfully updated tableSizeInfo
      (tableSizeInfo.values.toSeq, result.flatMap(_.getIndexStats.asScala), externalTables)
    }
  }

  type PRIterator = PartitionedRegion#PRLocalScanIterator

  /**
   * Allows pulling stats rows efficiently if required. For the corner case
   * of bucket moving away while iterating other buckets.
   */
  private val createRemoteIterator = new BiFunction[java.lang.Integer, PRIterator,
      java.util.Iterator[RegionEntry]] {
    override def apply(bucketId: Integer, iter: PRIterator): java.util.Iterator[RegionEntry] = {
      new RemoteEntriesIterator(bucketId, Array.emptyIntArray, iter.getPartitionedRegion, null)
    }
  }

  private def handleTransaction(cache: GemFireCacheImpl, tx: TXStateProxy,
      context: TXManagerImpl.TXContext, success: Boolean): Unit = {
    if (tx ne null) {
      val txManager = cache.getCacheTransactionManager
      if (success) {
        txManager.commit(tx, null, TXManagerImpl.FULL_COMMIT, context, false)
        if (cache.getRvvSnapshotTestHook ne null) {
          cache.notifyRvvTestHook()
          cache.waitOnRvvSnapshotTestHook()
        }
      } else {
        txManager.rollback(tx, null, false)
      }
    }
  }

  def publishColumnTableRowCountStats(): Unit = {
    val cache = Misc.getGemFireCache
    val regions = cache.getApplicationRegions.iterator()
    while (regions.hasNext) {
      val region = regions.next()
      val container = region.getUserAttribute.asInstanceOf[GemFireContainer]
      if ((container ne null) && region.getDataPolicy.withPartitioning()) {
        val pr = region.asInstanceOf[PartitionedRegion]
        val columnMeta = if (container.isColumnStore) container.fetchHiveMetaData(false) else null
        if ((columnMeta ne null) && pr.getLocalMaxMemory > 0) {
          val numColumnsInTable = Utils.getTableSchema(columnMeta).length
          // Resetting PR numRows in cached batch as this will be calculated every time.
          var rowsInColumnBatch = 0L
          var offHeapSize = 0L
          if (container ne null) {
            // This deliberately collects uncommitted entries so that bulk insert
            // progress can be monitored on dashboard. However, the pre-created transaction
            // is used to check for committed entries in case there are multiple column
            // batches that need to be merged
            val itr = new pr.PRLocalScanIterator(false /* primaryOnly */ , null /* no TX */ ,
              null /* not required since includeValues is false */ ,
              createRemoteIterator, false /* forUpdate */ , false /* includeValues */)
            val maxDeltaRows = pr.getColumnMaxDeltaRows
            var smallBucketRegion: BucketRegion = null
            val smallBatchBuckets = new UnifiedSet[BucketRegion](2)
            // using direct region operations
            while (itr.hasNext) {
              val re = itr.next().asInstanceOf[RegionEntry]
              if (!re.isDestroyedOrRemoved) {
                val key = re.getRawKey.asInstanceOf[ColumnFormatKey]
                val bucketRegion = itr.getHostedBucketRegion
                if (bucketRegion.getBucketAdvisor.isPrimary) {
                  val batchRowCount = key.getColumnBatchRowCount(bucketRegion, re,
                    numColumnsInTable)
                  rowsInColumnBatch += batchRowCount
                  // check if bucket has multiple small batches
                  if (key.getColumnIndex == ColumnFormatEntry.STATROW_COL_INDEX &&
                      batchRowCount < maxDeltaRows) {
                    if (bucketRegion eq smallBucketRegion) smallBatchBuckets.add(bucketRegion)
                    else smallBucketRegion = bucketRegion
                  }
                }
                re._getValue() match {
                  case v: ColumnFormatValue => offHeapSize += v.getOffHeapSizeInBytes
                  case _ =>
                }
              }
            }
            itr.close()
            // submit a task to merge small batches if required
            if (smallBatchBuckets.size() > 0) {
              mergeSmallColumnBatches(pr, container, columnMeta, smallBatchBuckets.asScala)
            }
          }
          val stats = pr.getPrStats
          stats.setPRNumRowsInColumnBatches(rowsInColumnBatch)
          stats.setOffHeapSizeInBytes(offHeapSize)
        } else if (container.isRowBuffer && pr.getLocalMaxMemory > 0) {
          rolloverTasks.computeIfAbsent(pr, rolloverRowBuffersTask)
        }
      }
    }
  }

  // Ensure max one background task per table
  private val rolloverTasks = new ConcurrentHashMap[PartitionedRegion, Future[Unit]]()
  private val mergeTasks = new ConcurrentHashMap[PartitionedRegion, Future[Unit]]()

  private def minSizeForRollover(pr: PartitionedRegion): Int =
    math.max(pr.getColumnMaxDeltaRows >>> 3, pr.getColumnMinDeltaRows)

  /**
   * Check if row buffers are large and have not been touched for a while
   * then roll it over into the column table
   */
  // noinspection TypeAnnotation
  private[this] val rolloverRowBuffersTask = new JFunction[PartitionedRegion, Future[Unit]] {

    private def testBucket(br: BucketRegion, maxDeltaRows: Int, minModTime: Long): Boolean = {
      val bucketSize = br.getRegionSize
      bucketSize >= maxDeltaRows || (br.getLastModifiedTime <= minModTime &&
          bucketSize >= minSizeForRollover(br.getPartitionedRegion))
    }

    override def apply(pr: PartitionedRegion): Future[Unit] = {
      val localPrimaries = pr.getDataStore.getAllLocalPrimaryBucketRegions
      if ((localPrimaries ne null) && localPrimaries.size() > 0) {
        val maxDeltaRows = try {
          pr.getColumnMaxDeltaRows
        } catch {
          case NonFatal(_) => return null
        }
        val minModTime = pr.getCache.cacheTimeMillis() - delayMillis
        // minimize object creation in usual case with explicit iteration (rather than asScala)
        var rolloverBuckets: UnifiedSet[BucketRegion] = null
        val iter = localPrimaries.iterator()
        while (iter.hasNext) {
          val br = iter.next()
          if (testBucket(br, maxDeltaRows, minModTime) && !br.isLockededForMaintenance) {
            if (rolloverBuckets eq null) rolloverBuckets = new UnifiedSet[BucketRegion]()
            rolloverBuckets.add(br)
          }
        }
        // enqueue a job to roll over required row buffers into column table
        // (each bucket will perform a last minute check before rollover inside lock)
        if ((rolloverBuckets ne null) && rolloverBuckets.size() > 0) {
          implicit val executionContext = Utils.executionContext(pr.getGemFireCache)
          Future {
            try {
              val doRollover = new Predicate[BucketRegion] {
                override def test(br: BucketRegion): Boolean =
                  testBucket(br, maxDeltaRows, minModTime)
              }
              rolloverBuckets.asScala.foreach(bucket => Utils.withExceptionHandling(
                bucket.createAndInsertColumnBatch(null, true,
                  GfxdLockSet.MAX_LOCKWAIT_VAL, doRollover)))
            } finally {
              rolloverTasks.remove(pr)
            }
          }
        } else null
      } else null
    }
  }

  /**
   * Merge multiple column batches that are small in size in a bucket.
   * These can get created due to a small "tail" in bulk imports (large enough
   * to exceed minimal size that would have pushed them into row buffers),
   * or a time-based flush that tolerates small sized column batches due to
   * [[rolloverRowBuffersTask]] or a forced flush of even smaller size for sample tables.
   *
   * The ColumnBatchIterator is passed the stats row entry. Rest all the columns, including
   * delta/delete are looked up by the iterator (see ColumnFormatStatsIterator.getColumnValue)
   * when the generated code asks for them. Hence this will be same as iterating batches in
   * ColumnTableScan that will return merged entries with deltas/deletes applied.
   * The ColumnInsert is tied to output of this hence will create a combined merged batch.
   */
  private def mergeSmallColumnBatches(pr: PartitionedRegion, container: GemFireContainer,
      metaData: ExternalTableMetaData, smallBatchBuckets: mutable.Set[BucketRegion]): Unit = {
    mergeTasks.computeIfAbsent(pr, new JFunction[PartitionedRegion, Future[Unit]] {
      override def apply(pr: PartitionedRegion): Future[Unit] = {
        val cache = pr.getGemFireCache
        implicit val executionContext: ExecutionContext = Utils.executionContext(cache)
        Future(Utils.withExceptionHandling({
          val tableName = container.getQualifiedTableName
          val schema = Utils.getTableSchema(metaData)
          val maxDeltaRows = pr.getColumnMaxDeltaRows
          val compileKey = tableName.concat(".MERGE_SMALL_BATCHES")
          val gen = CodeGeneration.compileCode(compileKey, schema.fields, () => {
            val schemaAttrs = Utils.schemaAttributes(schema)
            val tableScan = ColumnTableScan(schemaAttrs, dataRDD = null,
              otherRDDs = Nil, numBuckets = -1, partitionColumns = Nil,
              partitionColumnAliases = Nil, baseRelation = null, schema, allFilters = Nil,
              schemaAttrs, caseSensitive = true)
            // zero delta row size to avoid going through rolloverRowBuffers again
            val insertPlan = ColumnInsertExec(tableScan, Nil, Nil,
              numBuckets = -1, isPartitioned = false, None,
              (pr.getColumnBatchSize, 0, metaData.compressionCodec),
              tableName, onExecutor = true, schema,
              metaData.externalStore.asInstanceOf[ExternalStore], useMemberVariables = false)
            // now generate the code with the help of WholeStageCodegenExec
            // this is only used for local code generation while its RDD semantics
            // and related methods are all ignored
            val (ctx, code) = ExternalStoreUtils.codeGenOnExecutor(
              WholeStageCodegenExec(insertPlan), insertPlan)
            val references = ctx.references
            // also push the index of batchId reference at the end which can be
            // used by caller to update the reference objects before execution
            references += insertPlan.getBatchIdRef
            (code, references.toArray)
          })
          val references = gen._2.clone()
          // full projection for the iterators
          val numColumns = schema.length
          val projection = (1 to numColumns).toArray
          val lockOwner = Thread.currentThread()
          var success = false
          var locked = false
          var tx: TXStateProxy = null
          var context: TXManagerImpl.TXContext = null
          logInfo(s"Found small batches in ${pr.getName}: " +
              smallBatchBuckets.map(_.getId).mkString(", "))
          // for each bucket, create an iterator to scan and insert the result batches;
          // a separate iterator is required because one ColumnInsertExec assumes a single batchId
          for (br <- smallBatchBuckets) try {
            success = false
            locked = false
            tx = null
            // lock the row buffer bucket for maintenance operations
            Thread.`yield`() // prefer foreground operations
            locked = br.lockForMaintenance(true, GfxdLockSet.MAX_LOCKWAIT_VAL, lockOwner)
            if (!locked) {
              throw new LockTimeoutException(
                s"Failed to lock ${br.getFullPath} for maintenance merge operation")
            }
            // start a new transaction for each bucket
            tx = if (cache.snapshotEnabled) {
              context = TXManagerImpl.getOrCreateTXContext()
              cache.getCacheTransactionManager.beginTX(context,
                IsolationLevel.SNAPSHOT, null, null)
            } else null
            // find the committed entries with small batches under the transaction
            val bucketId = br.getId
            val itr = new pr.PRLocalScanIterator(Collections.singleton(bucketId),
              tx.getTXStateForRead, false /* forUpdate */ , false /* includeValues */ ,
              false /* fetchRemote */)
            val entries = new mutable.ArrayBuffer[RegionEntry](2)
            while (itr.hasNext) {
              val re = itr.next().asInstanceOf[RegionEntry]
              if (!re.isDestroyedOrRemoved) {
                val key = re.getRawKey.asInstanceOf[ColumnFormatKey]
                val batchRowCount = key.getColumnBatchRowCount(itr.getHostedBucketRegion,
                  re, schema.length)
                // check if bucket has multiple small batches
                if (key.getColumnIndex == ColumnFormatEntry.STATROW_COL_INDEX &&
                    batchRowCount < maxDeltaRows) {
                  entries += re
                }
              }
            }
            itr.close()
            if (entries.length > 1) {
              // update the bucketId as per the current bucket
              val batchIdRef = references(references.length - 1).asInstanceOf[Int]
              references(batchIdRef + 1) = bucketId
              val keys = entries.map(_.getRawKey.asInstanceOf[ColumnFormatKey])
              logInfo(s"Merging batches for ${pr.getName}:$bucketId :: $keys")
              // no harm in passing a references array with an extra element at end
              val iter = gen._1.generate(references).asInstanceOf[BufferedRowIterator]
              // use the entries already determined for the iterator read by generated code
              val batchIter = ColumnBatchIterator(br, entries.iterator, projection, context = null)
              iter.init(bucketId, Array(Iterator[Any](new ResultSetTraversal(
                conn = null, stmt = null, rs = null, context = null), batchIter)
                  .asInstanceOf[Iterator[InternalRow]]))
              while (iter.hasNext) {
                iter.next() // ignore result which is number of inserted rows
              }
              // now delete the keys that have been inserted above
              logInfo(s"Deleting merged batches for ${pr.getName}:$bucketId :: $keys")
              keys.foreach(ColumnDelta.deleteBatch(_, pr, numColumns))
            }
            success = true
          } catch {
            case le: LockTimeoutException => logWarning(le.getMessage)
            case t: Throwable => Utils.logAndThrowException(t)
          } finally {
            handleTransaction(cache, tx, context, success)
            if (locked) {
              br.unlockAfterMaintenance(true, lockOwner)
            }
          }
        }, () => {
          mergeTasks.remove(pr)
        }))
      }
    })
  }
}
