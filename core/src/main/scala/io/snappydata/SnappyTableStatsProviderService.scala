/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.control.NonFatal

import com.gemstone.gemfire.cache.IsolationLevel
import com.gemstone.gemfire.cache.execute.FunctionService
import com.gemstone.gemfire.i18n.LogWriterI18n
import com.gemstone.gemfire.internal.SystemTimer
import com.gemstone.gemfire.internal.cache._
import com.gemstone.gemfire.{CancelException, SystemFailure}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdListResultCollector.ListResultCollectorValue
import com.pivotal.gemfirexd.internal.engine.distributed.{GfxdListResultCollector, GfxdMessage}
import com.pivotal.gemfirexd.internal.engine.sql.execute.MemberStatisticsMessage
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer
import com.pivotal.gemfirexd.internal.engine.ui._
import io.snappydata.collection.ObjectObjectHashMap

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.impl.{ColumnDelta, ColumnFormatEntry, ColumnFormatKey, ColumnFormatValue, RemoteEntriesIterator}
import org.apache.spark.sql.execution.columnar.{ColumnBatchIterator, ColumnInsertExec, ColumnTableScan, ExternalStore, ExternalStoreUtils}
import org.apache.spark.sql.execution.row.ResultSetTraversal
import org.apache.spark.sql.execution.{BufferedRowIterator, WholeStageCodegenExec}
import org.apache.spark.sql.store.CodeGeneration
import org.apache.spark.sql.types.StructType
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

  var suspendCacheInvalidation = false
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

      val members = ObjectObjectHashMap.withExpectedSize[String,
          MemberStatistics](8)
      while (itr.hasNext) {
        val o = itr.next().asInstanceOf[ListResultCollectorValue]
        val memMap = o.resultOfSingleExecution.asInstanceOf[java.util.HashMap[String, Any]]

        val dssUUID = memMap.get("diskStoreUUID").asInstanceOf[java.util.UUID]
        val id = memMap.get("id").toString

        var memberStats: MemberStatistics = {
          if(dssUUID != null && membersInfo.contains(dssUUID.toString)) {
            membersInfo(dssUUID.toString)
          } else if(membersInfo.contains(id)) {
            membersInfo(id)
          } else {
            null
          }
        }

        if (memberStats == null) {
          memberStats = new MemberStatistics(memMap)
          if (dssUUID != null) {
            members.justPut(dssUUID.toString, memberStats)
          } else {
            members.justPut(id, memberStats)
          }
        } else {
          memberStats.updateMemberStatistics(memMap)
          members.justPut(dssUUID.toString, memberStats)
        }

        memberStats.setStatus("Running")
      }
      membersInfo ++= members.asScala
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
    var externalTables: Seq[SnappyExternalTableStats] = Nil
    val dataServers = GfxdMessage.getAllDataStores
    try {
      if (dataServers != null && dataServers.size() > 0) {
        result = FunctionService.onMembers(dataServers)
            // .withCollector(new GfxdListResultCollector())
            .execute(SnappyRegionStatsCollectorFunction.ID).getResult(5, TimeUnit.SECONDS).
            asInstanceOf[java.util.ArrayList[SnappyRegionStatsCollectorResult]]
            .asScala
      }
    }
    catch {
      case NonFatal(e) => log.warn(e.getMessage, e)
    }

    try {
      // External Tables
      val hiveTables: java.util.List[ExternalTableMetaData] =
        Misc.getMemStore.getExternalCatalog.getHiveTables(true)
      externalTables = hiveTables.asScala.collect {
        case table if table.tableType.equalsIgnoreCase("EXTERNAL") =>
          new SnappyExternalTableStats(table.entityName, table.tableType, table.shortProvider,
            table.externalStore, table.dataSourcePath, table.driverClass)
      }
    }
    catch {
      case NonFatal(e) => log.warn(e.getMessage, e)
    }

    val tableStats = result.flatMap(_.getRegionStats.asScala)
    if (tableStats.isEmpty) {
      // Return last updated tableSizeInfo
      (tableSizeInfo.values.toSeq, result.flatMap(_.getIndexStats.asScala), externalTables)
    } else {
      // Return updated tableSizeInfo
      (tableStats, result.flatMap(_.getIndexStats.asScala), externalTables)
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

  private def handleException(t: Throwable): Unit = t match {
    case e: Error if SystemFailure.isJVMFailureError(e) =>
      SystemFailure.initiateFailure(e)
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw e
    case _ =>
      // Whenever you catch Error or Throwable, you must also
      // check for fatal JVM error (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure()
      logWarning(t.getMessage, t)
      throw t
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

  private def withExceptionHandling(f: => Unit, doFinally: () => Unit = null): Unit = {
    try {
      f
    } catch {
      case t: Throwable => handleException(t)
    } finally {
      if (doFinally ne null) doFinally()
    }
  }

  def publishColumnTableRowCountStats(): Unit = {
    def asSerializable[C](c: C) = c.asInstanceOf[C with Serializable]

    val cache = Misc.getGemFireCache
    val regions = asSerializable(cache.getApplicationRegions.asScala)
    for (region: LocalRegion <- regions) {
      if (region.getDataPolicy.withPartitioning()) {
        val pr = region.asInstanceOf[PartitionedRegion]
        val container = pr.getUserAttribute.asInstanceOf[GemFireContainer]
        val isColumnTable = container.isColumnStore
        if (isColumnTable && pr.getLocalMaxMemory > 0) {
          val metaData = container.fetchHiveMetaData(false)
          val schema = metaData.schema.asInstanceOf[StructType]
          val numColumnsInTable = schema.length
          // Resetting PR numRows in cached batch as this will be calculated every time.
          var rowsInColumnBatch = 0L
          var offHeapSize = 0L
          if (container ne null) {
            // This deliberately collects uncommitted entries so that bulk insert
            // progress can be monitored on dashboard. However, the pre-created transaction
            // is used to check for committed entries in case there are multiple column
            // batches that need to be merged
            val itr = new pr.PRLocalScanIterator(true /* primaryOnly */ , null /* no TX */ ,
              null /* not required since includeValues is false */ ,
              createRemoteIterator, false /* forUpdate */ , false /* includeValues */)
            val maxDeltaRows = pr.getColumnMaxDeltaRows
            var smallBucketRegion: BucketRegion = null
            val smallBatchBuckets = new mutable.ArrayBuffer[BucketRegion](2)
            // using direct region operations
            while (itr.hasNext) {
              val re = itr.next().asInstanceOf[RegionEntry]
              if (!re.isDestroyedOrRemoved) {
                val key = re.getRawKey.asInstanceOf[ColumnFormatKey]
                val batchRowCount = key.getColumnBatchRowCount(itr, re, numColumnsInTable)
                rowsInColumnBatch += batchRowCount
                // check if bucket has multiple small batches
                if (key.getColumnIndex == ColumnFormatEntry.STATROW_COL_INDEX &&
                    batchRowCount < maxDeltaRows) {
                  val br = itr.getHostedBucketRegion
                  if (br eq smallBucketRegion) smallBatchBuckets += br
                  else smallBucketRegion = br
                }
                re._getValue() match {
                  case v: ColumnFormatValue => offHeapSize += v.getOffHeapSizeInBytes
                  case _ =>
                }
              }
            }
            itr.close()
            // submit a task to merge small batches if required
            if (smallBatchBuckets.nonEmpty) {
              mergeSmallColumnBatches(pr, container, metaData, smallBatchBuckets)
            }
          }
          val stats = pr.getPrStats
          stats.setPRNumRowsInColumnBatches(rowsInColumnBatch)
          stats.setOffHeapSizeInBytes(offHeapSize)
        } else if (!isColumnTable && pr.getLocalMaxMemory > 0 && container.isRowBuffer) {
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
  private val rolloverRowBuffersTask = new JFunction[PartitionedRegion, Future[Unit]] {
    override def apply(pr: PartitionedRegion): Future[Unit] = {
      val localPrimaries = pr.getDataStore.getAllLocalPrimaryBucketRegions
      if ((localPrimaries ne null) && localPrimaries.size() > 0) {
        val doRollover = new Predicate[BucketRegion] {
          private val minModTime = pr.getCache.cacheTimeMillis() - delayMillis

          override def test(br: BucketRegion): Boolean = {
            br.getLastModifiedTime <= minModTime && br.getRegionSize >= minSizeForRollover(pr)
          }
        }
        val rolloverBuckets = localPrimaries.asScala.filter(
          br => doRollover.test(br) && !br.columnBatchFlushLock.isWriteLocked)
        // enqueue a job to roll over required row buffers into column table
        // (each bucket will perform a last minute check before rollover inside lock)
        if (rolloverBuckets.nonEmpty) {
          implicit val executionContext = Utils.executionContext(pr.getGemFireCache)
          Future {
            try {
              rolloverBuckets.foreach(bucket => withExceptionHandling(
                bucket.createAndInsertColumnBatch(null, true, doRollover)))
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
   */
  private def mergeSmallColumnBatches(pr: PartitionedRegion, container: GemFireContainer,
      metaData: ExternalTableMetaData, smallBatches: mutable.ArrayBuffer[BucketRegion]): Unit = {
    mergeTasks.computeIfAbsent(pr, new JFunction[PartitionedRegion, Future[Unit]] {
      override def apply(pr: PartitionedRegion): Future[Unit] = {
        logInfo(s"Found small batches in ${pr.getName}: ${smallBatches.map(_.getId)}")
        val cache = pr.getGemFireCache
        implicit val executionContext = Utils.executionContext(cache)
        Future(withExceptionHandling({
          val tableName = container.getQualifiedTableName
          val schema = metaData.schema.asInstanceOf[StructType]
          val maxDeltaRows = pr.getColumnMaxDeltaRows
          val compileKey = tableName.concat(".MERGE_SMALL_BATCHES")
          val gen = CodeGeneration.compileCode(compileKey, schema.fields, () => {
            val schemaAttrs = Utils.schemaAttributes(schema)
            val tableScan = ColumnTableScan(schemaAttrs, dataRDD = null,
              otherRDDs = Nil, numBuckets = -1, partitionColumns = Nil,
              partitionColumnAliases = Nil, baseRelation = null, schema, allFilters = Nil,
              schemaAttrs, caseSensitive = true)
            // reduce min delta row size to avoid going through rolloverRowBuffers again
            val insertPlan = ColumnInsertExec(tableScan, Nil, Nil,
              numBuckets = -1, isPartitioned = false, None,
              (pr.getColumnBatchSize, minSizeForRollover(pr), metaData.compressionCodec),
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
          var success = false
          var tx: TXStateProxy = null
          var context: TXManagerImpl.TXContext = null
          // for each bucket, create an iterator to scan and insert the result batches;
          // a separate iterator is required because one ColumnInsertExec assumes a single batchId
          for (br <- smallBatches) try {
            success = false
            // start a new transaction for each bucket
            tx = null
            tx = if (cache.snapshotEnabled) {
              context = TXManagerImpl.getOrCreateTXContext()
              cache.getCacheTransactionManager.beginTX(context, IsolationLevel.SNAPSHOT, null, null)
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
                val batchRowCount = key.getColumnBatchRowCount(itr, re, schema.length)
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
            case t: Throwable => handleException(t)
          } finally {
            handleTransaction(cache, tx, context, success)
          }
        }, () => mergeTasks.remove(pr)))
      }
    })
  }
}
