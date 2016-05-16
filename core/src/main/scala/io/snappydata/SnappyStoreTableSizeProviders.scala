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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag

import com.gemstone.gemfire.i18n.LogWriterI18n
import com.gemstone.gemfire.internal.SystemTimer
import com.gemstone.gemfire.internal.cache.PartitionedRegion
import com.pivotal.gemfirexd.internal.engine.Misc

import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation
import io.snappydata.Constant._

object StoreTableValueSizeProviderService extends Logging {
  @volatile
  private var tableSizeInfo = Map[String, Long]()
  private var timer: SystemTimer = null


  def start(sc: SparkContext): Unit = {
    val delay =
      sc.getConf.getOption("spark.snappy.calcTableSizeInterval")
          .getOrElse(DEFAULT_CALC_TABLE_SIZE_SERVICE_INTERVAL).toString.toLong
    timer = new SystemTimer(Misc.getGemFireCache.getDistributedSystem,
      true, Misc.getGemFireCache.getLoggerI18n)
    timer.schedule(calculateTableSizeTask, 0, delay)
  }

  def stop: Unit = {
    if (timer != null) {
      timer.cancel()
    }
  }

  def calculateTableSizeTask: SystemTimer.SystemTimerTask = {
    new SystemTimer.SystemTimerTask {
      override def run2(): Unit = {
        val sc = SnappyContext.globalSparkContext
        val snc = SnappyContext.getOrCreate(sc)
        val sizeInfoPerRegionAcc = sc.accumulableCollection(mutable.ArrayBuffer[TableSizeInfo]())
        val currentTableSizeInfo = mutable.HashMap[String, Long]()
        Utils.mapExecutors(snc, () => {
          Misc.getGemFireCache.getPartitionedRegions.asScala.foreach(region => {
            val valueSizeOfRegion = getSizeForAllPrimaryBucketsOfRegion(region)
            sizeInfoPerRegionAcc += new TableSizeInfo(
              region.getFullPath.replaceFirst("/" , "").replaceAll("/" , "."), valueSizeOfRegion)
          })
          Iterator.empty
        }).count()

        sizeInfoPerRegionAcc.value.map(tableInfo => {
          var totalSize: Long = 0
          if (currentTableSizeInfo.contains(tableInfo.tableName)) {
            totalSize = currentTableSizeInfo.get(tableInfo.tableName).get + tableInfo.totalSize
          }
          else {
            totalSize = tableInfo.totalSize
          }
          currentTableSizeInfo.put(tableInfo.tableName, totalSize)
        })

        tableSizeInfo = currentTableSizeInfo.toMap
      }

      override def getLoggerI18n: LogWriterI18n = {
        return Misc.getGemFireCache.getLoggerI18n
      }
    }
  }

  def getSizeForAllPrimaryBucketsOfRegion(region: PartitionedRegion): Long = {
    var sizeOfRegion: Long = 0
    region.getDataStore.getAllLocalPrimaryBucketRegions.asScala.foreach(bucketRegion =>
      sizeOfRegion = sizeOfRegion + bucketRegion.getTotalBytes)
    sizeOfRegion
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



  private def getMemoryAnalyticsDetails(conn: Connection): mutable.Map[String , Long] = {
    val currentTableStats = mutable.Map[String, Long]()
    val stmt = "select TABLE_NAME," +
        "SUM(TOTAL_SIZE)" +
        "from SYS.MEMORYANALYTICS " +
        "WHERE table_name not like 'HIVE_METASTORE%'  group by TABLE_NAME"
    val rs = conn.prepareStatement(stmt).executeQuery()
    while (rs.next()) {
      val name = rs.getString(1)
      val totalSize = convertToBytes(rs.getString(2))
      currentTableStats.put(name , totalSize)
    }
    currentTableStats
  }

  private def convertToBytes(value: String): Long = {
    if (value == null) 0 else (value.toDouble * 1024).toLong
  }


  private def getRowBufferName(columnStoreName: String): String = {
    (columnStoreName.replace(SHADOW_TABLE_SUFFIX, "").
        replace(INTERNAL_SCHEMA_NAME, "")).trim
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


case class TableSizeInfo(tableName: String, totalSize: Long)
case class UIAnalytics(tableName: String, rowBufferSize: Long,
    columnBufferSize: Long, isColumnTable: Boolean)