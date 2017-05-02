package io.snappydata

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.language.implicitConversions

import com.gemstone.gemfire.CancelException
import com.gemstone.gemfire.cache.DataPolicy
import com.gemstone.gemfire.cache.execute.FunctionService
import com.gemstone.gemfire.i18n.LogWriterI18n
import com.gemstone.gemfire.internal.SystemTimer
import com.gemstone.gemfire.internal.cache.execute.InternalRegionFunctionContext
import com.gemstone.gemfire.internal.cache.{PartitionedRegion, LocalRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.{GfxdMessage, GfxdListResultCollector}
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdListResultCollector.ListResultCollectorValue
import com.pivotal.gemfirexd.internal.engine.sql.execute.MemberStatisticsMessage
import com.pivotal.gemfirexd.internal.engine.store.{CompactCompositeKey, GemFireContainer}
import com.pivotal.gemfirexd.internal.engine.ui.{SnappyRegionStatsCollectorFunction, SnappyRegionStatsCollectorResult, SnappyIndexStats, SnappyRegionStats}
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation
import io.snappydata.Constant._

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.{JDBCSourceAsStore, JDBCAppendableRelation}
import org.apache.spark.sql.execution.columnar.encoding.ColumnStatsSchema
import org.apache.spark.unsafe.Platform
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.sql.{SnappySession, SnappyContext}

trait TableStatsProviderService extends Logging {

  @volatile
  private var tableSizeInfo = Map.empty[String, SnappyRegionStats]
  protected val membersInfo = TrieMap.empty[String, mutable.Map[String, Any]]

  private var _snc: Option[SnappyContext] = None

  protected def snc: SnappyContext = synchronized {
    _snc.getOrElse {
      val context = SnappyContext()
      _snc = Option(context)
      context
    }
  }

  @volatile protected var doRun: Boolean = false
  @volatile private var running: Boolean = false

  def start(sc: SparkContext): Unit
  def start(sc: SparkContext, url: String): Unit

  protected def aggregateStats(): Unit = synchronized {
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

  def getTableSizeStats(): Map[String, SnappyRegionStats] = {
    val tableSizes = this.tableSizeInfo
    if (tableSizes.isEmpty) {
      // force run
      aggregateStats()
    }
    tableSizeInfo
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

  def getStatsFromAllServers: (Seq[SnappyRegionStats], Seq[SnappyIndexStats])
}
