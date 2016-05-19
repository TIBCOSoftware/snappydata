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
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation
import org.apache.spark.{Logging, SparkContext}
import java.util.HashMap

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
          FunctionService.onMembers(GfxdMessage.getAllGfxdServers())
              .withCollector(new GfxdListResultCollector())
              .execute(RegionSizeCalculatorFunction.ID).getResult()

        result.asInstanceOf[ArrayList[HashMap[String, Long]]]
            .asScala.foreach(_.asScala.foreach(row => {
          var totalSize: Long = row._2
          if (currentTableSizeInfo.contains(row._1)) {
            totalSize = currentTableSizeInfo.get(row._1).get + row._2
          }
          currentTableSizeInfo.put(row._1, totalSize)
        }))

          tableSizeInfo = currentTableSizeInfo.toMap
      }

      override def getLoggerI18n: LogWriterI18n = {
        return Misc.getGemFireCache.getLoggerI18n
      }
    }
  }

  def getTableSize(tableName: String, isColumnTable: Boolean = false):
  Option[Long] = {
    val currentTableSizeInfo = tableSizeInfo
    if (currentTableSizeInfo == null || !currentTableSizeInfo.contains(tableName)) {
      None
    }
    else {
      if (isColumnTable) {
        val optStat = currentTableSizeInfo.get(ColumnFormatRelation.cachedBatchTableName(tableName))
        if (optStat.isDefined) {
          Some(optStat.get + currentTableSizeInfo.get(tableName).get)
        } else {
          None
        }
      } else {
        Some(currentTableSizeInfo.get(tableName).get)
      }
    }
  }

}

object StoreTableSizeProvider {

  def getTableSizes: Seq[UIAnalytics] = {
    val currentTableStats = tryExecute(conn => getMemoryAnalyticsDetails(conn))
    if (currentTableStats == null) {
      return Seq.empty
    }
    val internalColumnTables = currentTableStats.filter(entry => isColumnTable(entry._1))

    var columnTableNames = scala.collection.mutable.Seq[String]()

    (internalColumnTables.map(entry => {
      val rowBuffer = getRowBufferName(entry._1)
      val rowBufferSize = currentTableStats.get(rowBuffer).get
      columnTableNames = columnTableNames.+:(rowBuffer).+:(entry._1)
      new UIAnalytics(rowBuffer, rowBufferSize, entry._2, true)
    })
        ++ currentTableStats.filter(
      entry => {
        !columnTableNames.contains(entry._1)
      }).map(rowEntry =>
      new UIAnalytics(rowEntry._1, rowEntry._2, 0, false))).toSeq
  }


  private def getMemoryAnalyticsDetails(conn: Connection): mutable.Map[String, Long] = {
    val currentTableStats = mutable.Map[String, Long]()
    val stmt = "select TABLE_NAME," +
        "SUM(TOTAL_SIZE)" +
        "from SYS.MEMORYANALYTICS " +
        "WHERE table_name not like 'HIVE_METASTORE%'  group by TABLE_NAME"
    val rs = conn.prepareStatement(stmt).executeQuery()
    while (rs.next()) {
      val name = rs.getString(1)
      val totalSize = convertToBytes(rs.getString(2))
      currentTableStats.put(name, totalSize)
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

}

case class UIAnalytics(tableName: String, rowBufferSize: Long,
    columnBufferSize: Long, isColumnTable: Boolean)