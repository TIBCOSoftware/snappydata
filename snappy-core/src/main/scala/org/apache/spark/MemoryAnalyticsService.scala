package org.apache.spark.sql.sources

import java.sql.DriverManager
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import io.snappydata.Constant._

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.util.ThreadUtils


/**
	* get TableSizes for all the updated tables from Snappy Store
	*/

object MemoryAnalyticsService {
	private var memoryAnalyticsExecutor: ScheduledExecutorService = null
	private val tableSizeStats = collection.mutable.Map[String, String]()

	def start(sc: SparkContext): Unit = {
		val delayInMillisconds = sc.getConf.getOption("snappy.MemoryAnalyticsService.interval")
				.getOrElse(DEFAULT_ANALYTICS_SERVICE_INTERVAL).toString
		memoryAnalyticsExecutor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
			"MemoryAnalyticsService")
		memoryAnalyticsExecutor.scheduleWithFixedDelay(
			getTotalMemoryUsagePerTable, 0, delayInMillisconds.toLong, TimeUnit.MILLISECONDS)
	}


	def stop(): Unit = {
		if (!memoryAnalyticsExecutor.isShutdown) {
			memoryAnalyticsExecutor.shutdown()
		}

	}

	private val getTotalMemoryUsagePerTable = new Runnable {
		override def run(): Unit = {
			val stmt = "select table_name , total_size from sys.memoryanalytics where " +
					" table_name not like 'HIVE_METASTORE.%' "
			DriverRegistry.register(JDBC_EMBEDDED_DRIVER)
			val conn = DriverManager.getConnection(DEFAULT_EMBEDDED_URL)
			val rs = conn.prepareStatement(stmt).executeQuery()
			while (rs.next()) {
				val name = rs.getString(1)
				val size = rs.getString(2)
				tableSizeStats.put(name, size)
			}

			rs.close()
			conn.close()
		}
	}

	def getTableSizeStatsInBytes(tableName: String): Long = {
		val sizeInMB = tableSizeStats.get(tableName).getOrElse(0)
		(sizeInMB.toString.toDouble * 1024 * 1024).toLong
	}

	def getAllStats : collection.Map[String, String] = {
		tableSizeStats
	}
}