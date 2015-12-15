package org.apache.spark.sql

import org.apache.spark.sql.SampleDataFrameContract.ErrorRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.collection.MultiColumnOpenHashMap

import org.apache.spark.sql.sources.StatCounter

import scala.collection.mutable

class SampleDataFrame(@transient override val sqlContext: SnappyContext,
                          @transient override val logicalPlan: LogicalPlan
                          )
  extends DataFrame(sqlContext, logicalPlan) with Serializable {

  @transient var implementor: SampleDataFrameContract = createSampleDataFrameContract



  def registerSampleTable(tableName: String): Unit =
    implementor.registerSampleTable(tableName)

  override def registerTempTable(tableName: String): Unit =
    registerSampleTable(tableName)

  def errorStats(columnName: String,
                 groupBy: Set[String] = Set.empty): MultiColumnOpenHashMap[StatCounter] =
    implementor.errorStats(columnName, groupBy)

  def errorEstimateAverage(columnName: String, confidence: Double,
                           groupByColumns: Set[String] = Set.empty): mutable.Map[Row, ErrorRow] =
  implementor.errorEstimateAverage(columnName, confidence, groupByColumns)

  private def createSampleDataFrameContract =  sqlContext.aqpContext.createSampleDataFrameContract(sqlContext,
    this, logicalPlan)


}