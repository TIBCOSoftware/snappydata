package org.apache.spark.sql

import java.sql.DriverManager

import io.snappydata.{Constant, SnappyFunSuite}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.sql.columntable.ColumnFormatRelation

class SnappyAnalyticsServiceTest extends SnappyFunSuite
		with BeforeAndAfter
		with BeforeAndAfterAll {

	val columnTableName = "COLUMNTABLE"
	val rowTableName = "ROWTABLE"
	after {
		snc.dropTable(columnTableName, ifExists = true)
		snc.dropTable(rowTableName, ifExists = true)
	}

	test("Test Stats for Row Table") {
		val dataDF = getDF

		snc.sql(s"Create Table $rowTableName (a INT, b INT, c INT)")
		dataDF.write.format("row").mode(SaveMode.Append).saveAsTable(s"$rowTableName")

		val result = snc.sql(s"SELECT * FROM $rowTableName")
		assert(result.collect.length == 5)

		val analytics = queryMemoryAnalytics(s"APP.$rowTableName")

		val mValueSize = SnappyAnalyticsService.getValueSize(s"APP.$rowTableName")
		val mTotalSize = SnappyAnalyticsService.getUIDetails
				.filter(_.tableName.equals(s"APP.$rowTableName")).head.rowBufferSize

		assert(analytics.valueSize == mValueSize)
		assert(analytics.totalSize == mTotalSize)


		snc.sql(s"drop table $rowTableName")
	}

	test("Test Stats for Column Table") {
		val dataDF = getDF
		snc.createTable(columnTableName, "column", dataDF.schema, Map.empty[String, String])
		dataDF.write.format("column").mode(SaveMode.Append).saveAsTable(s"$columnTableName")

		val result = snc.sql("SELECT * FROM " + columnTableName)
		val r = result.collect
		assert(r.length == 5)

		val analyticsRowBuffer = queryMemoryAnalytics(s"APP.$columnTableName")
		val analyticsColumnBuffer = queryMemoryAnalytics(
			ColumnFormatRelation.cachedBatchTableName(columnTableName))
		val mValueSize = SnappyAnalyticsService.getValueSize(s"APP.$columnTableName")
		val uiDetails = SnappyAnalyticsService.getUIDetails
				.filter(_.tableName.equals(s"APP.$columnTableName")).head

		assert(mValueSize == analyticsColumnBuffer.valueSize + analyticsRowBuffer.valueSize)
		assert(uiDetails.rowBufferSize == analyticsRowBuffer.totalSize)
		assert(uiDetails.columnBufferSize == analyticsColumnBuffer.totalSize)
	}

	private def getDF = {
		val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
		val rdd = sc.parallelize(data, data.length).
				map(s => new io.snappydata.core.Data(s(0), s(1), s(2)))
		snc.createDataFrame(rdd)
	}

	private def queryMemoryAnalytics(tableName: String): MemoryAnalytics = {
		val query = "SELECT  SUM(VALUE_SIZE) , SUM(TOTAL_SIZE) FROM SYS.MEMORYANALYTICS" +
				s" WHERE TABLE_NAME = '$tableName'"
		var valueSize: Long = 0
		var totalSize: Long = 0
		val conn = DriverManager.getConnection(Constant.DEFAULT_EMBEDDED_URL)
		val rs = conn.createStatement().executeQuery(query)
		if (rs.next()) {
			valueSize = (rs.getString(1).toDouble * 1024).toLong
			totalSize = (rs.getString(2).toDouble * 1024).toLong
		}

		return new MemoryAnalytics(0, 0, valueSize, 0, totalSize)
	}
}
