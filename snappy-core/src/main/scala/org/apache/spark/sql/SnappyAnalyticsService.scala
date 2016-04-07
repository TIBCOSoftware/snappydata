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

package org.apache.spark.sql

import java.util.concurrent.TimeUnit

import scala.collection.mutable

import io.snappydata.Constant._

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.sources.ConnectionProperties
import org.apache.spark.util.ThreadUtils


object SnappyAnalyticsService {
	private var tableStats = scala.collection.mutable.Map[String, MemoryAnalytics]()
	private val analyticsExecutor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
		"SnappyAnalyticsService")
	private var connProperties: ConnectionProperties = null
	private val readWriteLock = new LockUtils.ReadWriteLock()

	def start(sc: SparkContext): Unit = {
		connProperties =
				ExternalStoreUtils.validateAndGetAllProps(sc, mutable.Map.empty[String, String])
		val delayInMillisconds = sc.getConf.getOption("spark.snappy.analyticsService.interval")
				.getOrElse(DEFAULT_ANALYTICS_SERVICE_INTERVAL).toString
		analyticsExecutor.scheduleWithFixedDelay(
			getTotalMemoryUsagePerTable, DEFAULT_ANALYTICS_SERVICE_INTERVAL,
			delayInMillisconds.toLong, TimeUnit.MILLISECONDS)
	}

	def stop: Unit = {
		if (!analyticsExecutor.isShutdown) {
			analyticsExecutor.shutdown()
		}
	}

	private def getTotalMemoryUsagePerTable = new Runnable {
		override def run(): Unit = {
			val currentTableStats = scala.collection.mutable.Map[String, MemoryAnalytics]()
			val stmt = "select TABLE_NAME," +
					" SUM(ENTRY_SIZE), " +
					"SUM(KEY_SIZE), " +
					"SUM(VALUE_SIZE)," +
					"SUM(VALUE_SIZE_OFFHEAP)," +
					"SUM(TOTAL_SIZE)" +
					"from SYS.MEMORYANALYTICS " +
					"WHERE table_name not like 'HIVE_METASTORE%'  group by TABLE_NAME"
			val connection = ConnectionPool.getPoolConnection("SYS.MEMORYANALYTICS",
				connProperties.dialect, connProperties.poolProps, connProperties.connProps,
				connProperties.hikariCP)
			val rs = connection.prepareStatement(stmt).executeQuery()
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

			readWriteLock.executeInWriteLock(tableStats = currentTableStats)
			rs.close()
			connection.close()
		}
	}

	private def convertToBytes(value: String): Long = (value.toDouble * 1024).toLong

	private def getRowBufferName(columnStoreName: String): String = {
		DEFAULT_SCHEMA +
				(columnStoreName.replace(SHADOW_TABLE_SUFFIX, "").
						replace(INTERNAL_SCHEMA_NAME, "")).trim
	}

	private def isColumnTable(tablename: String): Boolean =
		tablename.startsWith(INTERNAL_SCHEMA_NAME) && tablename.endsWith(SHADOW_TABLE_SUFFIX)


	private def getValue(tableName: String, isColumnTable: Boolean = false): Long = {
		val defaultStats = new MemoryAnalytics(0, 0, 0, 0, 0)
		if (tableStats.contains(tableName)) {
			if (isColumnTable) {
				tableStats.get(cachedBatchTableName(tableName)).getOrElse(defaultStats).valueSize
				tableStats.get(tableName).get.valueSize
			} else {
				tableStats.get(tableName).get.valueSize
			}
		} else defaultStats.valueSize
	}

	private def getUIInfo: Seq[UIAnalytics] = {
		val internalColumnTables = tableStats.filter(entry => isColumnTable(entry._1))
		(internalColumnTables.map(entry => {
			val rowBuffer = getRowBufferName(entry._1)
			val rowBufferSize = tableStats.get(rowBuffer).get.totalSize
			tableStats.remove(rowBuffer)
			tableStats.remove(entry._1)
			new UIAnalytics(rowBuffer, rowBufferSize, entry._2.totalSize, true)
		}).
				++(tableStats.map(entry =>
					new UIAnalytics(entry._1, entry._2.totalSize, 0, false)))).toSeq
	}

	def getValueSize(tableName: String, isColumnTable: Boolean = false): Long = {
		readWriteLock.executeInReadLock(getValue(tableName, isColumnTable))
	}


	def getUIDetails: Seq[UIAnalytics] = readWriteLock.executeInReadLock(getUIInfo)

	private def cachedBatchTableName(table: String) = {
		val tableName = if (table.indexOf('.') > 0) {
			table.replace(".", "__")
		} else {
			table
		}
		INTERNAL_SCHEMA_NAME + "." + tableName + SHADOW_TABLE_SUFFIX
	}

}

case class MemoryAnalytics(entrySize: Long, keySize: Long,
		valueSize: Long, valueSizeOffHeap: Long, totalSize: Long)

case class UIAnalytics(tableName: String, rowBufferSize: Long,
		columnBufferSize: Long, isColumnTable: Boolean)