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

package org.apache.spark.sql.sources

import java.util.concurrent.TimeUnit

import scala.collection.mutable

import io.snappydata.Constant._

import org.apache.spark.SparkContext
import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.util.ThreadUtils


/**
	* get TableSizes for all the updated tables from Snappy Store
	*/

object SnappyAnalyticsService  {
	private val sc = SnappyContext.globalSparkContext
	private val tableStats = collection.mutable.Map[String, Long]()
	private val analyticsExecutor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
		"SnappyAnalyticsService")
	private var connProperties: ConnectionProperties = null


	def start(sc: SparkContext): Unit = {
		connProperties =
				ExternalStoreUtils.validateAndGetAllProps(sc, mutable.Map.empty[String, String])
		val delayInMillisconds = sc.getConf.getOption("spark.snappy.analytics.service.interval")
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
			val stmt = "select table_name , total_size from sys.memoryanalytics where " +
					" table_name not like 'HIVE_METASTORE.%' "
			val connection = ConnectionPool.getPoolConnection("sys.memoryanalytics",
				connProperties.dialect, connProperties.poolProps, connProperties.connProps,
				connProperties.hikariCP)
			val rs = connection.prepareStatement(stmt).executeQuery()
			while (rs.next()) {
				val name = rs.getString(1)
				val size = rs.getString(2)
				tableStats.put(name, (size.toDouble * 1024).toLong)
			}
			rs.close()
			connection.close()
		}
	}

	private def getRowBufferName(columnStoreName: String): String = {
		DEFAULT_SCHEMA +
				(columnStoreName.replace(SHADOW_TABLE_SUFFIX, "").
						replace(INTERNAL_SCHEMA_NAME, "")).trim
	}

	def getTableSize(tableName: String): Long = {
		tableStats.get(tableName).getOrElse(0)
	}

	def getTableStats: collection.Map[String, Long] = {
		val currentStats = tableStats.clone()
		val internalColumnTables = currentStats.filter(entry =>
			entry._1.startsWith(INTERNAL_SCHEMA_NAME) && entry._1.endsWith(SHADOW_TABLE_SUFFIX))
		// Only show the size of the internal column store against the column table name
		internalColumnTables.foreach(entry => {
			currentStats.put(getRowBufferName(entry._1), entry._2)
			currentStats.remove(entry._1)
		})
		currentStats
	}
}
