/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
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

import java.util.function.BiFunction

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.implicitConversions
import scala.util.control.Breaks._
import scala.util.control.NonFatal

import com.gemstone.gemfire.cache.execute.FunctionService
import com.gemstone.gemfire.i18n.LogWriterI18n
import com.gemstone.gemfire.internal.SystemTimer
import com.gemstone.gemfire.internal.cache.{AbstractRegionEntry, PartitionedRegion, RegionEntry}
import com.gemstone.gemfire.{CancelException, SystemFailure}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdListResultCollector.ListResultCollectorValue
import com.pivotal.gemfirexd.internal.engine.distributed.{GfxdListResultCollector, GfxdMessage}
import com.pivotal.gemfirexd.internal.engine.sql.execute.MemberStatisticsMessage
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer
import com.pivotal.gemfirexd.internal.engine.ui._
import io.snappydata.Constant._
import io.snappydata.sql.catalog.CatalogObjectType
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap

import org.apache.spark.SparkContext
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.CommonUtils
import org.apache.spark.sql.execution.columnar.impl.{ColumnFormatKey, ColumnFormatRelation, ColumnFormatValue, RemoteEntriesIterator}
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
          val delay = sc.getConf.getLong(PROPERTY_PREFIX +
              "calcTableSizeInterval", DEFAULT_CALC_TABLE_SIZE_SERVICE_INTERVAL)
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
            },
            delay, delay)
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

      val members = new Object2ObjectOpenHashMap[String, MemberStatistics](8)
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

  private[this] val regionStatsFuture =
    new RecurringFuture[Seq[SnappyRegionStatsCollectorResult]](() => {
      val dataServers = GfxdMessage.getAllDataStores
      if (dataServers != null && dataServers.size() > 0) {
        FunctionService.onMembers(dataServers)
            // .withCollector(new GfxdListResultCollector())
            .execute(SnappyRegionStatsCollectorFunction.ID).getResult()
            .asInstanceOf[java.util.ArrayList[SnappyRegionStatsCollectorResult]]
            .asScala
      } else Nil
    })(CommonUtils.waiterExecutionContext)

  override def getStatsFromAllServers(sc: Option[SparkContext] = None): (Seq[SnappyRegionStats],
      Seq[SnappyIndexStats], Seq[SnappyExternalTableStats]) = {
    var resultObtained: Boolean = false
    logDebug(s"Updating table statistics from all members. Waiting for $waitDuration")
    val result = try {
      regionStatsFuture.result(waitDuration) match {
        case Some(stats) => resultObtained = true; stats
        case None => // ignore timeout exception and return current map
          logWarning("Obtaining updated Table Statistics is taking longer than expected time.")
          Nil
      }
    } catch {
      case t if !SystemFailure.isJVMFailureError(t) =>
        logWarning("Unexpected exception when updating Table Statistics", t)
        Nil
    }

    // handle the case of a rare race condition where external catalog is still being initialized
    val hiveTables = Misc.getMemStore.getExternalCatalog match {
      case null => Nil
      case catalog => catalog.getCatalogTables.asScala
    }

    val externalTables: Seq[SnappyExternalTableStats] = {
      try {
        // External Tables
        hiveTables.collect {
          case table if table.tableType == CatalogObjectType.External.toString =>
            new SnappyExternalTableStats(table.entityName, table.tableType, table.schema,
              table.shortProvider, table.externalStore, table.dataSourcePath, table.driverClass)
        }
      }
      catch {
        case NonFatal(e) =>
          log.warn("Exception occurred while collecting External Table Statistics: " + e.getMessage)
          log.debug(e.getMessage, e)
          mutable.Buffer.empty[SnappyExternalTableStats]
      }
    }

    if (resultObtained) {
      // Return updated tableSizeInfo
      // Map to hold hive table type against table names as keys
      val tableTypes = hiveTables.map(ht =>
        Utils.toUpperCase(s"${ht.schema}.${ht.entityName}") -> ht.tableType).toMap
      val regionStats = result.flatMap(_.getRegionStats.asScala).map(rs => {
        val tableRegion = Misc.getRegionForTable(rs.getTableName, false)
        if (tableRegion != null && tableRegion.isInstanceOf[PartitionedRegion]) {
          val tablePrRegion = tableRegion.asInstanceOf[PartitionedRegion]
          val PrRegRedProvider = tablePrRegion.getRedundancyProvider
          if (PrRegRedProvider != null) {
            rs.setRedundancyImpaired(PrRegRedProvider.isRedundancyImpaired)
            rs.setRedundancy(tablePrRegion.getRedundantCopies)
          }

          val numBuckets = tablePrRegion.getPartitionAttributes.getTotalNumBuckets
          breakable {
            for (i <- 0 until numBuckets) {
              val idm = tablePrRegion.getNodeForBucketRead(i)
              if (idm == null) {
                rs.setAnyBucketLost(true)
                break
              }
            }
          }
        }

        try tableTypes.get(Utils.toUpperCase(rs.getTableName)) match {
          case Some(t) if CatalogObjectType.isColumnTable(CatalogObjectType.withName(
            Utils.toUpperCase(t))) => rs.setColumnTable(true)
          case _ => rs.setColumnTable(false)
        } catch {
          case _: Exception => rs.setColumnTable(false)
        }
        rs
      })

      // Return updated details
      (regionStats,
          result.flatMap(_.getIndexStats.asScala),
          externalTables)
    } else {
      // Return last successfully updated tableSizeInfo
      (tableSizeInfo.values.toSeq,
          result.flatMap(_.getIndexStats.asScala),
          externalTables)
    }
  }

  type PRIterator = PartitionedRegion#PRLocalScanIterator

  /**
   * Allows pulling stats rows efficiently if required. For the corner case
   * of bucket moving away while iterating other buckets.
   */
  private val createRemoteIterator = new BiFunction[java.lang.Integer, PRIterator,
      java.util.Iterator[RegionEntry]] {
    override def apply(bucketId: Integer,
        iter: PRIterator): java.util.Iterator[RegionEntry] = {
      new RemoteEntriesIterator(bucketId, Array.emptyIntArray,
        iter.getPartitionedRegion, null)
    }
  }

  def publishColumnTableRowCountStats(): Unit = {
    val regions = Misc.getGemFireCache.getApplicationRegions.asScala
    for (region <- regions) {
      if (region.getDataPolicy.withPartitioning()) {
        val table = Misc.getFullTableNameFromRegionPath(region.getFullPath)
        val pr = region.asInstanceOf[PartitionedRegion]
        val container = pr.getUserAttribute.asInstanceOf[GemFireContainer]
        if (ColumnFormatRelation.isColumnTable(table) &&
            pr.getLocalMaxMemory > 0) {
          var numColumnsInTable = -1
          // Resetting PR numRows in cached batch as this will be calculated every time.
          var rowsInColumnBatch = 0L
          var offHeapSize = 0L
          if (container ne null) {
            // TODO: SW: this should avoid iteration and use BucketRegion to get the sizes
            val itr = new pr.PRLocalScanIterator(false /* primaryOnly */ , null /* no TX */ ,
              null /* not required since includeValues is false */ ,
              createRemoteIterator, false /* forUpdate */ , false /* includeValues */)
            // using direct region operations
            while (itr.hasNext) {
              val re = itr.next().asInstanceOf[AbstractRegionEntry]
              val key = re.getRawKey.asInstanceOf[ColumnFormatKey]
              val bucketRegion = itr.getHostedBucketRegion
              if (bucketRegion.getBucketAdvisor.isPrimary) {
                if (numColumnsInTable < 0) {
                  numColumnsInTable = key.getNumColumnsInTable(table)
                }
                rowsInColumnBatch += key.getColumnBatchRowCount(bucketRegion, re,
                  numColumnsInTable)
              }
              re._getValue() match {
                case v: ColumnFormatValue => offHeapSize += v.getOffHeapSizeInBytes
                case _ =>
              }
            }
          }
          val stats = pr.getPrStats
          stats.setPRNumRowsInColumnBatches(rowsInColumnBatch)
          stats.setOffHeapSizeInBytes(offHeapSize)
        }
      }
    }
  }
}
