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

package io.snappydata

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.language.implicitConversions

import com.gemstone.gemfire.CancelException
import com.pivotal.gemfirexd.internal.engine.ui.{SnappyIndexStats, SnappyRegionStats}

import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.{SnappyContext, SnappySession}
import org.apache.spark.{Logging, SparkContext}

import scala.util.control.Breaks._

trait TableStatsProviderService extends Logging {

  @volatile
  private var tableSizeInfo = Map.empty[String, SnappyRegionStats]
  @volatile
  private var indexesInfo = Map.empty[String, SnappyIndexStats]
  protected val membersInfo: TrieMap[String, mutable.Map[String, Any]] =
    TrieMap.empty[String, mutable.Map[String, Any]]

  private var _snc: Option[SnappyContext] = None

  protected def snc: SnappyContext = synchronized {
    _snc.getOrElse {
      val context = SnappyContext()
      _snc = Option(context)
      context
    }
  }

  var memberStatsUpdater: Option[Thread] = None

  protected def getMemberStatsUpdater: Thread = synchronized {

    if (memberStatsUpdater.isEmpty ||
        memberStatsUpdater.get.getState == Thread.State.TERMINATED) {
      memberStatsUpdater = {
        val th = new Thread(new Runnable() {
          def run() {
            // update membersInfo
            fillAggregatedMemberStatsOnDemand()
          }
        });
        Option(th)
      }
    }
    memberStatsUpdater.get
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
          indexesInfo = indexStats // populating indexes stats

          // Commenting this call to avoid periodic refresh of members stats
          // get members details
          // fillAggregatedMemberStatsOnDemand()

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

  def getMembersStatsOnDemand: mutable.Map[String, mutable.Map[String, Any]] = {
    // fillAggregatedMemberStatsOnDemand()
    // membersInfo

    var infoToBeReturned: mutable.Map[String, mutable.Map[String, Any]] =
      TrieMap.empty[String, mutable.Map[String, Any]]
    val prevMembersInfo = membersInfo.synchronized{membersInfo}
    val waitTime: Int = 500;

    // get member stats updater thread
    val msUpdater = getMemberStatsUpdater;
    if (!msUpdater.isAlive) {
      // start updater thread to update members stats
      msUpdater.start();
    }

    val endTimeMillis = System.currentTimeMillis() + 5000;
    if (msUpdater.isAlive) {
      breakable {
        while (msUpdater.isAlive) {
          if (System.currentTimeMillis() > endTimeMillis) {
            logWarning("Obtaining updated Members Statistics is taking longer than expected time..")
            // infoToBeReturned = prevMembersInfo
            break
          }
          try {
            // Wait
            logInfo("Obtaining updated Members Statistics in progress.." +
                "Waiting for " + waitTime + " ms")
            Thread.sleep(waitTime)
          } catch {
            case e: InterruptedException => logWarning("InterruptedException", e)
          }
        }
      }

      if (msUpdater.getState == Thread.State.TERMINATED) {
        // Thread is terminated so assigning updated member info
        infoToBeReturned = membersInfo
      } else {
        // Thread is still running so assigning last updated member info
        logWarning("Setting last updated member statistics snapshot..")
        infoToBeReturned = prevMembersInfo
      }
    } else {
      // Thread is terminated so assigning updated member info
      infoToBeReturned = membersInfo
    }

    infoToBeReturned
  }

  def stop(): Unit = {
    doRun = false
    // wait for it to end for sometime
    synchronized {
      if (running) wait(20000)
    }
    _snc = None
  }

  def getIndexesStatsFromService: Map[String, SnappyIndexStats] = {
    // TODO: [SachinK] This code is commented to avoid forced refresh of stats
    // on every call (as indexesInfo could be empty).
    /*
    val indexStats = this.indexesInfo
    if (indexStats.isEmpty) {
      // force run
      aggregateStats()
    }
    */
    indexesInfo
  }

  def getTableSizeStats: Map[String, SnappyRegionStats] = {
    // TODO: [SachinK] Below conditional check can be commented to avoid
    // forced refresh of stats on every call (tableSizeInfo could be empty).
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
    val (tableStats, indexStats) = getStatsFromAllServers()

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

  def getStatsFromAllServers(sc: Option[SparkContext] = None): (Seq[SnappyRegionStats],
      Seq[SnappyIndexStats])
}
