/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

import com.gemstone.gemfire.CancelException
import com.gemstone.gemfire.cache.DataPolicy
import com.gemstone.gemfire.cache.execute.FunctionService
import com.gemstone.gemfire.i18n.LogWriterI18n
import com.gemstone.gemfire.internal.SystemTimer
import com.gemstone.gemfire.internal.cache.execute.InternalRegionFunctionContext
import com.gemstone.gemfire.internal.cache.{LocalRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdListResultCollector.ListResultCollectorValue
import com.pivotal.gemfirexd.internal.engine.distributed.{GfxdListResultCollector, GfxdMessage}
import com.pivotal.gemfirexd.internal.engine.sql.execute.MemberStatisticsMessage
import com.pivotal.gemfirexd.internal.engine.store.{CompactCompositeKey, GemFireContainer}
import com.pivotal.gemfirexd.internal.engine.ui.{SnappyIndexStats, SnappyRegionStats, SnappyRegionStatsCollectorFunction, SnappyRegionStatsCollectorResult}
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation
import io.snappydata.Constant._
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.encoding.ColumnStatsSchema
import org.apache.spark.sql.execution.columnar.{JDBCAppendableRelation, JDBCSourceAsStore}
import org.apache.spark.sql.{SnappyContext, SnappySession}
import org.apache.spark.unsafe.Platform
import org.apache.spark.{Logging, SparkContext}

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.language.implicitConversions

object SnappyTableStatsProviderService extends Logging {

  @volatile
  private var tableSizeInfo = Map.empty[String, SnappyRegionStats]
  private val membersInfo = TrieMap.empty[String, mutable.Map[String, Any]]

  private var _snc: Option[SnappyContext] = None

  private def snc: SnappyContext = synchronized {
    _snc.getOrElse {
      val context = SnappyContext()
      _snc = Option(context)
      context
    }
  }

  @volatile private var doRun: Boolean = false
  @volatile private var running: Boolean = false


  def start(sc: SparkContext): Unit = {
    val delay = sc.getConf.getLong(Constant.SPARK_SNAPPY_PREFIX +
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
            case e: Exception => if (!e.getMessage.contains(
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

  private def aggregateStats(): Unit = synchronized {
    try {
      if (doRun) {
        val prevTableSizeInfo = tableSizeInfo
        running = true
        try {
          val (tableStats, indexStats) = getAggregatedStatsOnDemand
          tableSizeInfo = tableStats
          // get members details
          fillAggregatedMemberStatsOnDemand()
        } finally {
          running = false
          notifyAll()
        }
        // check if there has been a substantial change in table
        // stats, and clear the plan cache if so
        if (prevTableSizeInfo.size != tableSizeInfo.size) {
          SnappySession.clearAllCache(onlyQueryPlanCache = true)
        } else {
          val prevTotalRows = prevTableSizeInfo.values.map(_.getRowCount).sum
          val newTotalRows = tableSizeInfo.values.map(_.getRowCount).sum
          if (math.abs(newTotalRows - prevTotalRows) > 0.1 * prevTotalRows) {
            SnappySession.clearAllCache(onlyQueryPlanCache = true)
          }
        }
      }
    } catch {
      case _: CancelException => // ignore
      case e: Exception => if (!e.getMessage.contains(
        "com.gemstone.gemfire.cache.CacheClosedException")) {
        logWarning(e.getMessage, e)
      } else {
        logError(e.getMessage, e)
      }
    }
  }

  def fillAggregatedMemberStatsOnDemand(): Unit = {

    val existingMembers = membersInfo.keys.toArray
    val collector = new GfxdListResultCollector(null, true)
    val msg = new MemberStatisticsMessage(collector)

    msg.executeFunction()

    val memStats = collector.getResult

    val itr = memStats.iterator()

    val members = mutable.Map.empty[String, mutable.Map[String, Any]]
    while (itr.hasNext) {
      val o = itr.next().asInstanceOf[ListResultCollectorValue]
      val memMap = o.resultOfSingleExecution.asInstanceOf[java.util.HashMap[String, Any]]
      val map = mutable.HashMap.empty[String, Any]
      val keyItr = memMap.keySet().iterator()

      while (keyItr.hasNext) {
        val key = keyItr.next()
        map.put(key, memMap.get(key))
      }
      map.put("status", "Running")

      val dssUUID = memMap.get("diskStoreUUID").asInstanceOf[java.util.UUID]
      if (dssUUID != null) {
        members.put(dssUUID.toString, map)
      } else {
        members.put(memMap.get("id").asInstanceOf[String], map)
      }
    }
    membersInfo ++= members
    // mark members no longer running as stopped
    existingMembers.filterNot(members.contains).foreach(m =>
      membersInfo(m).put("status", "Stopped"))
  }

  def getMembersStatsFromService: mutable.Map[String, mutable.Map[String, Any]] = {
    membersInfo
  }


  def stop(): Unit = {
    doRun = false
    // wait for it to end for sometime
    synchronized {
      if (running) wait(20000)
    }
    _snc = None
  }


  def getTableStatsFromService(
      fullyQualifiedTableName: String): Option[SnappyRegionStats] = {
    val tableSizes = this.tableSizeInfo
    if (tableSizes.isEmpty || !tableSizes.contains(fullyQualifiedTableName)) {
      // force run
      aggregateStats()
    }
    tableSizeInfo.get(fullyQualifiedTableName)
  }

  def publishColumnTableRowCountStats(): Unit = {
    def asSerializable[C](c: C) = c.asInstanceOf[C with Serializable]

    val regions = asSerializable(Misc.getGemFireCache.getApplicationRegions.asScala)
    for (region: LocalRegion <- regions) {
      if (region.getDataPolicy == DataPolicy.PARTITION ||
          region.getDataPolicy == DataPolicy.PERSISTENT_PARTITION) {
        val table = Misc.getFullTableNameFromRegionPath(region.getFullPath)
        val pr = region.asInstanceOf[PartitionedRegion]
        val container = pr.getUserAttribute.asInstanceOf[GemFireContainer]
        if (table.startsWith(Constant.INTERNAL_SCHEMA_NAME) &&
            table.endsWith(Constant.SHADOW_TABLE_SUFFIX)) {
          if (container != null) {
            val bufferTable = JDBCAppendableRelation.getTableName(table)
            val numColumnsInBaseTbl = (Misc.getMemStore.getAllContainers.asScala.find(c => {
              c.getQualifiedTableName.equalsIgnoreCase(bufferTable)}).get.getNumColumns) - 1

            val numColumnsInStatBlob = numColumnsInBaseTbl * ColumnStatsSchema.NUM_STATS_PER_COLUMN
            val itr = pr.localEntriesIterator(null.asInstanceOf[InternalRegionFunctionContext],
              true, false, true, null).asInstanceOf[PartitionedRegion#PRLocalScanIterator]
            // Resetting PR Numrows in cached batch as this will be calculated every time.
            // TODO: Decrement count using deleted rows bitset in case of deletes in columntable
            var rowsInColumnBatch: Long = 0
            while (itr.hasNext) {
              val rl = itr.next().asInstanceOf[RowLocation]
              val key = rl.getKeyCopy.asInstanceOf[CompactCompositeKey]
              val x = key.getKeyColumn(2).getInt
              val currentBucketRegion = itr.getHostedBucketRegion
              if (x == JDBCSourceAsStore.STATROW_COL_INDEX) {
                currentBucketRegion.get(key) match {
                  case currentVal: Array[Array[Byte]] =>
                    val rowFormatter = container.
                        getRowFormatter(currentVal(0))
                    val statBytes = rowFormatter.getLob(currentVal, 4)
                    val unsafeRow = new UnsafeRow(numColumnsInStatBlob)
                    unsafeRow.pointTo(statBytes, Platform.BYTE_ARRAY_OFFSET,
                      statBytes.length)
                    rowsInColumnBatch += unsafeRow.getInt(
                      ColumnStatsSchema.COUNT_INDEX_IN_SCHEMA);
                  case _ =>
                }
              }
            }
            pr.getPrStats.setPRNumRowsInColumnBatches(rowsInColumnBatch)
          }
        }
      }
    }
  }

  def getAggregatedStatsOnDemand: (Map[String, SnappyRegionStats],
    Map[String, SnappyIndexStats]) = {
    val snc = this.snc
    if (snc == null) return (Map.empty, Map.empty)
    val (tableStats, indexStats) = getStatsFromAllServers

    val aggregatedStats = scala.collection.mutable.Map[String, SnappyRegionStats]()
    val aggregatedStatsIndex = scala.collection.mutable.Map[String, SnappyIndexStats]()
    if (!doRun) return (Map.empty, Map.empty)
    // val samples = getSampleTableList(snc)
    tableStats.foreach { stat =>
      aggregatedStats.get(stat.getRegionName) match {
        case Some(oldRecord) =>
          aggregatedStats.put(stat.getRegionName, oldRecord.getCombinedStats(stat))
        case None =>
          aggregatedStats.put(stat.getRegionName, stat)
      }
    }

    indexStats.foreach { stat =>
      aggregatedStatsIndex.put(stat.getIndexName, stat)
    }
    (Utils.immutableMap(aggregatedStats), Utils.immutableMap(aggregatedStatsIndex))
  }

  /*
  private def getSampleTableList(snc: SnappyContext): Seq[String] = {
    try {
      snc.sessionState.catalog
          .getDataSourceTables(Seq(ExternalTableType.Sample)).map(_.toString())
    } catch {
      case tnfe: org.apache.spark.sql.TableNotFoundException =>
        Seq.empty[String]
    }
  }
  */

  private def getStatsFromAllServers: (Seq[SnappyRegionStats], Seq[SnappyIndexStats]) = {
    var result = new java.util.ArrayList[SnappyRegionStatsCollectorResult]().asScala
    val dataServers = GfxdMessage.getAllDataStores
    if( dataServers != null && dataServers.size() > 0 ){
      result = FunctionService.onMembers(dataServers)
        .withCollector(new GfxdListResultCollector())
        .execute(SnappyRegionStatsCollectorFunction.ID).getResult().
        asInstanceOf[java.util.ArrayList[SnappyRegionStatsCollectorResult]]
        .asScala
    }
    (result.flatMap(_.getRegionStats.asScala), result.flatMap(_.getIndexStats.asScala))
  }

}
