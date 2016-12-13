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

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import com.gemstone.gemfire.CancelException
import com.gemstone.gemfire.cache.DataPolicy
import com.gemstone.gemfire.cache.execute.FunctionService
import com.gemstone.gemfire.i18n.LogWriterI18n
import com.gemstone.gemfire.internal.SystemTimer
import com.gemstone.gemfire.internal.cache.execute.InternalRegionFunctionContext
import com.gemstone.gemfire.internal.cache.{LocalRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.{GfxdListResultCollector, GfxdMessage}
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer
import com.pivotal.gemfirexd.internal.engine.ui.{SnappyRegionStats, SnappyRegionStatsCollectorFunction, SnappyRegionStatsCollectorResult}
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation
import io.snappydata.Constant._

import org.apache.spark.sql.{SnappyContext, SnappySession}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.{Logging, SparkContext}

object SnappyTableStatsProviderService extends Logging {

  @volatile private var tableSizeInfo = Map[String, SnappyRegionStats]()

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
              val prevTableSizeInfo = tableSizeInfo
              running = true
              try {
                tableSizeInfo = getAggregatedTableStatsOnDemand
              } finally synchronized {
                running = false
                notifyAll()
              }
              // check if there has been a substantial change in table
              // stats, and clear the plan cache if so
              if (prevTableSizeInfo.size != tableSizeInfo.size) {
                SnappySession.clearAllCache()
              } else {
                val prevTotalRows = prevTableSizeInfo.values.map(_.getRowCount).sum
                val newTotalRows = tableSizeInfo.values.map(_.getRowCount).sum
                if (math.abs(newTotalRows - prevTotalRows) > 0.1 * prevTotalRows) {
                  SnappySession.clearAllCache()
                }
              }
            }
          } catch {
            case _: CancelException => // ignore
            case e: Exception => if (!e.getMessage.contains(
              "com.gemstone.gemfire.cache.CacheClosedException")) {
              logger.warning(e)
            }
          }
        }

        override def getLoggerI18n: LogWriterI18n = {
          logger
        }
      },
      delay, delay)
  }

  def stop(): Unit = {
    doRun = false
    // wait for it to end for sometime
    synchronized {
      if (running) wait(20000)
    }
    _snc = None
  }

  def getTableStatsFromService(fullyQualifiedTableName: String):
  Option[SnappyRegionStats] = {
    if (tableSizeInfo == null || !tableSizeInfo.contains(fullyQualifiedTableName)) {
      None
    } else tableSizeInfo.get(fullyQualifiedTableName)
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
            val colPos = container.getTableDescriptor.getColumnDescriptor("NUMROWS").getPosition
            val itr = pr.localEntriesIterator(null.asInstanceOf[InternalRegionFunctionContext],
              true, false, true, null).asInstanceOf[PartitionedRegion#PRLocalScanIterator]
            while (itr.hasNext) {
              pr.getPrStats.incPRNumRowsInCachedBatches(itr.next().asInstanceOf[RowLocation]
                  .getRow(container).getColumn(colPos).getInt)
            }
          }
        }
      }
    }
  }

  def getAggregatedTableStatsOnDemand: Map[String, SnappyRegionStats] = {
    val snc = this.snc
    if (snc == null) return Map.empty
    val serverStats = getTableStatsFromAllServers
    val aggregatedStats = scala.collection.mutable.Map[String, SnappyRegionStats]()
    if (!doRun) return Map.empty
    // val samples = getSampleTableList(snc)
    serverStats.foreach { stat =>
      aggregatedStats.get(stat.getRegionName) match {
        case Some(oldRecord) =>
          aggregatedStats.put(stat.getRegionName, oldRecord.getCombinedStats(stat))
        case None =>
          aggregatedStats.put(stat.getRegionName, stat)
      }
    }
    Utils.immutableMap(aggregatedStats)
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

  private def getTableStatsFromAllServers: Seq[SnappyRegionStats] = {
    val result = FunctionService.onMembers(GfxdMessage.getAllDataStores)
        .withCollector(new GfxdListResultCollector())
        .execute(SnappyRegionStatsCollectorFunction.ID).getResult().
        asInstanceOf[java.util.ArrayList[SnappyRegionStatsCollectorResult]]
        .asScala

    result.flatMap(_.getRegionStats.asScala)
  }

}
