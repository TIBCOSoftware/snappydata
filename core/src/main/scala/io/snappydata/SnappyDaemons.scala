/*
 * Changes for SnappyData additions and modifications.
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

import java.sql.Connection
import java.util.concurrent.{ScheduledExecutorService, ScheduledThreadPoolExecutor, TimeUnit}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag

import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.snappydata.Constant._

import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation
import org.apache.spark.sql.sources.ConnectionProperties
import org.apache.spark.{Logging, SparkContext}


object SnappyAnalyticsService extends Logging {
  @volatile
  private var tableStats =
    TrieMap[String, MemoryAnalytics]()
  private var analyticsExecutor: ScheduledExecutorService = null
  private var connProperties: ConnectionProperties = null
  val defaultStats = new MemoryAnalytics(0, 0, 0, 0, 0)
  private final val ZERO = 0

  def start(sc: SparkContext): Unit = {
    val delayInMillisconds = sc.getConf.getOption("spark.snappy.analyticsService.interval")
        .getOrElse(DEFAULT_ANALYTICS_SERVICE_INTERVAL).toString
    connProperties =
        ExternalStoreUtils.validateAndGetAllProps(sc, mutable.Map.empty[String, String])
    if (analyticsExecutor == null || analyticsExecutor.isShutdown) {
      analyticsExecutor = newDaemonSingleThreadScheduledExecutor("SnappyAnalyticsService")
      analyticsExecutor.scheduleWithFixedDelay(
        getTotalMemoryUsagePerTable, ZERO,
        delayInMillisconds.toLong, TimeUnit.MILLISECONDS)
    }
  }


  def stop: Unit = {
    if (!analyticsExecutor.isShutdown) {
      analyticsExecutor.shutdown()
      analyticsExecutor.awaitTermination(DEFAULT_ANALYTICS_SERVICE_INTERVAL, TimeUnit.MILLISECONDS)
    }
  }

  private def getTotalMemoryUsagePerTable = new Runnable {
    override def run(): Unit = {
      tryExecute(conn => getMemoryAnalyticsdetails(conn))
    }
  }

  private def getMemoryAnalyticsdetails(conn: Connection): Unit = {
    val currentTableStats = TrieMap[String, MemoryAnalytics]()
    val stmt = "select TABLE_NAME," +
        " SUM(ENTRY_SIZE), " +
        "SUM(KEY_SIZE), " +
        "SUM(VALUE_SIZE)," +
        "SUM(VALUE_SIZE_OFFHEAP)," +
        "SUM(TOTAL_SIZE)" +
        "from SYS.MEMORYANALYTICS " +
        "WHERE table_name not like 'HIVE_METASTORE%'  group by TABLE_NAME"
    val rs = conn.prepareStatement(stmt).executeQuery()
    while (rs.next()) {
      val name = rs.getString(1)
      val entrySize = convertToBytes(rs.getString(2))
      val keySize = convertToBytes(rs.getString(3))
      val valueSize = convertToBytes(rs.getString(4))
      val valueSizeOffHeap = convertToBytes(rs.getString(5))
      val totalSize = convertToBytes(rs.getString(6))
      currentTableStats.put(name,
        new MemoryAnalytics(entrySize, keySize, valueSize, valueSizeOffHeap, totalSize))
    }
    tableStats = currentTableStats
  }

  private def convertToBytes(value: String): Long = {
    if (value == null) ZERO else (value.toDouble * 1024).toLong
  }

  private def getRowBufferName(columnStoreName: String): String = {
    DEFAULT_SCHEMA +
        (columnStoreName.replace(SHADOW_TABLE_SUFFIX, "").
            replace(INTERNAL_SCHEMA_NAME, "")).trim
  }

  private def isColumnTable(tablename: String): Boolean =
    tablename.startsWith(INTERNAL_SCHEMA_NAME) && tablename.endsWith(SHADOW_TABLE_SUFFIX)


  def getTableSize(tableName: String, isColumnTable: Boolean = false): Long = {
    val currentTableStats = tableStats
    if (currentTableStats == null || !currentTableStats.contains(tableName)) {
      defaultStats.valueSize
    }
    else {
      if (isColumnTable) {
        currentTableStats.get(ColumnFormatRelation.cachedBatchTableName(tableName))
            .getOrElse(defaultStats).valueSize
        +currentTableStats.get(tableName).get.valueSize
      } else {
        currentTableStats.get(tableName).get.valueSize
      }
    }
  }

  def getUIInfo: Seq[UIAnalytics] = {
    val currentTableStats = tableStats
    if (currentTableStats == null) {
      return Seq.empty
    }
    val internalColumnTables = currentTableStats.filter(entry => isColumnTable(entry._1))

    var columnTableNames = scala.collection.mutable.Seq[String]()

    (internalColumnTables.map(entry => {
      val rowBuffer = getRowBufferName(entry._1)
      val rowBufferSize = currentTableStats.get(rowBuffer).get.totalSize
      columnTableNames = columnTableNames.+:(rowBuffer).+:(entry._1)
      new UIAnalytics(rowBuffer, rowBufferSize, entry._2.totalSize, true)
    })
        ++ currentTableStats.filter(
      entry => {
        !columnTableNames.contains(entry._1)
      }).map(rowEntry =>
      new UIAnalytics(rowEntry._1, rowEntry._2.totalSize, 0, false))).toSeq
  }

  final def tryExecute[T: ClassTag](f: Connection => T,
      closeOnSuccess: Boolean = true): T = {
    val conn = ConnectionPool.getPoolConnection("SYS.MEMORYANALYTICS",
      connProperties.dialect, connProperties.poolProps, connProperties.connProps,
      connProperties.hikariCP)
    var isClosed = false
    try {
      f(conn)
    } catch {
      case t: Throwable =>
        conn.close()
        isClosed = true
        throw t
    } finally {
      if (closeOnSuccess && !isClosed) {
        conn.close()
      }
    }
  }

  private def newDaemonSingleThreadScheduledExecutor(threadName: String)
  : ScheduledExecutorService = {
    val threadFactory =
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()
    val executor = new ScheduledThreadPoolExecutor(1, threadFactory)
    executor.setRemoveOnCancelPolicy(true)
    executor
  }
}


object SnappyDaemons {

  def start(sc: SparkContext): Unit = {
    SnappyAnalyticsService.start(sc)
  }

  def stop: Unit = {
    SnappyAnalyticsService.stop
  }
}

case class MemoryAnalytics(entrySize: Long, keySize: Long,
    valueSize: Long, valueSizeOffHeap: Long, totalSize: Long)

case class UIAnalytics(tableName: String, rowBufferSize: Long,
    columnBufferSize: Long, isColumnTable: Boolean)