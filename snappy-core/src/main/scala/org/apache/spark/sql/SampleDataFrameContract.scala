package org.apache.spark.sql

import org.apache.spark.sql.SampleDataFrameContract.ErrorRow
import org.apache.spark.sql.collection.MultiColumnOpenHashMap
import org.apache.spark.sql.sources.StatCounter

import scala.collection.mutable


trait SampleDataFrameContract {


  def registerSampleTable(tableName: String): Unit

  def errorStats(columnName: String,
                 groupBy: Set[String] = Set.empty): MultiColumnOpenHashMap[StatCounter]

  def errorEstimateAverage(columnName: String, confidence: Double,
                           groupByColumns: Set[String] = Set.empty): mutable.Map[Row, ErrorRow]
}

object SampleDataFrameContract {
  final type ErrorRow = (Double, Double, Double, Double)
}
