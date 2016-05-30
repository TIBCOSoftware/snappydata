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

import java.sql.Connection
import java.util.ArrayList

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag

import com.gemstone.gemfire.cache.execute.FunctionService
import com.gemstone.gemfire.i18n.LogWriterI18n
import com.gemstone.gemfire.internal.SystemTimer
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.{RegionSizeCalculatorFunction, GfxdListResultCollector, GfxdMessage}
import io.snappydata.Constant._

import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation
import org.apache.spark.{Logging, SparkContext}
import java.util.HashMap
import org.apache.spark.sql.collection.Utils

object StoreTableValueSizeProviderService extends Logging {
  @volatile
  private var tableSizeInfo = Map[String, Long]()
  private var timer: SystemTimer = null

  def start(sc: SparkContext): Unit = {
    val delay =
      sc.getConf.getOption("spark.snappy.calcTableSizeInterval")
          .getOrElse(DEFAULT_CALC_TABLE_SIZE_SERVICE_INTERVAL).toString.toLong
    Misc.getGemFireCache.getCCPTimer().schedule(calculateTableSizeTask(sc), delay, delay)
  }

  def calculateTableSizeTask(sc: SparkContext): SystemTimer.SystemTimerTask = {
    new SystemTimer.SystemTimerTask {
      override def run2(): Unit = {
        val currentTableSizeInfo = mutable.HashMap[String, Long]()

        val result =
          FunctionService.onMembers(GfxdMessage.getAllDataStores())
              .withCollector(new GfxdListResultCollector())
              .execute(RegionSizeCalculatorFunction.ID).getResult()

        result.asInstanceOf[ArrayList[HashMap[String, Long]]]
            .asScala.foreach(_.asScala.foreach(row => {
          currentTableSizeInfo += (row._1 ->
              currentTableSizeInfo.get(row._1).map(value => value + row._2).getOrElse(row._2))
        }))

        tableSizeInfo = Utils.immutableMap(currentTableSizeInfo)
      }

      override def getLoggerI18n: LogWriterI18n = {
        Misc.getGemFireCache.getLoggerI18n
      }
    }
  }

  def getTableSize(tableName: String,
      isColumnTable: Boolean = false): Option[Long] = {
    val currentTableSizeInfo = tableSizeInfo
    if (currentTableSizeInfo == null) {
      None
    } else currentTableSizeInfo.get(tableName) match {
      case v if isColumnTable =>
        val size: Long = v.getOrElse(0)
        currentTableSizeInfo.get(ColumnFormatRelation.cachedBatchTableName(tableName)).
            map(value => value + size)
      case v => v
    }
  }
}

object StoreTableSizeProvider {

  private val memoryAnalyticsDefault = MemoryAnalytics(0, 0)

  def getTableSizes: Seq[UIAnalytics] = {
    val currentTableStats = tryExecute(conn => getMemoryAnalyticsDetails(conn))
    if (currentTableStats == null) {
      return Seq.empty
    }
    currentTableStats.filter(entry => !isColumnTable(entry._1)).map(details => {
      val maForRowBuffer = details._2
      val columnTableName = ColumnFormatRelation.cachedBatchTableName(details._1)
      val maForColumn = currentTableStats.
          getOrElse(columnTableName, memoryAnalyticsDefault)
      val isColumnTable = currentTableStats.contains(columnTableName)
      new UIAnalytics(details._1, maForRowBuffer.totalSize, maForRowBuffer.totalRows,
        maForColumn.totalSize, maForColumn.totalRows, isColumnTable)
    }).toSeq
  }

  private def getMemoryAnalyticsDetails(
      conn: Connection): mutable.Map[String, MemoryAnalytics] = {
    val currentTableStats = mutable.Map[String, MemoryAnalytics]()
    val stmt = "select TABLE_NAME," +
        " SUM(TOTAL_SIZE) ," +
        " SUM(NUM_ROWS) " +
        " from SYS.MEMORYANALYTICS " +
        "WHERE table_name not like 'HIVE_METASTORE%'  group by TABLE_NAME"
    val rs = conn.prepareStatement(stmt).executeQuery()
    while (rs.next()) {
      val name = rs.getString(1)
      val totalSize = convertToBytes(rs.getString(2))
      val totalRows = rs.getString(3).toLong
      currentTableStats.put(name, MemoryAnalytics(totalSize , totalRows))
    }
    currentTableStats
  }

  private def convertToBytes(value: String): Long = {
    if (value == null) 0 else (value.toDouble * 1024).toLong
  }


  private def getRowBufferName(columnStoreName: String): String = {
    columnStoreName.replace(SHADOW_TABLE_SUFFIX, "").
        replace(INTERNAL_SCHEMA_NAME, "").
        replaceFirst(".", "").
        replaceFirst("__", ".")
  }

  private def isColumnTable(tablename: String): Boolean =
    tablename.startsWith(INTERNAL_SCHEMA_NAME) && tablename.endsWith(SHADOW_TABLE_SUFFIX)


  final def tryExecute[T: ClassTag](f: Connection => T,
      closeOnSuccess: Boolean = true): T = {

    val connProperties = ExternalStoreUtils.validateAndGetAllProps(SnappyContext.globalSparkContext
      , mutable.Map.empty[String, String])
    val getConnection: () => Connection =
      ExternalStoreUtils.getConnector("SYS.MEMORYANALYTICS", connProperties, false)

    val conn: Connection = getConnection()

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
}

case class MemoryAnalytics(totalSize: Long, totalRows: Long)
case class UIAnalytics(tableName: String, rowBufferSize: Long, rowBufferCount: Long,
    columnBufferSize: Long, columnBufferCount: Long , isColumnTable: Boolean)
