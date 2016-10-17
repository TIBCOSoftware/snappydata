/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, SinglePartition}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.collection.ToolsCallbackInit
import org.apache.spark.sql.execution.columnar.impl.BaseColumnFormatRelation
import org.apache.spark.sql.execution.columnar.{ColumnTableScan, ConnectionType}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.row.RowFormatRelation
import org.apache.spark.sql.sources.{StatsPredicate, Filter, BaseRelation, PrunedUnsafeFilteredScan, SamplingRelation}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.dsl.expressions._

/** Physical plan node for scanning data from an DataSource scan RDD.
 * If user knows that the data is partitioned or replicated across
 * all nodes this SparkPla can be used to avoid expensive shuffle
 * and Broadcast joins. This plan overrides outputPartitioning and
 * make it inline with the partitioning of the underlying DataSource */
private[sql] abstract class PartitionedPhysicalScan(
    output: Seq[Attribute],
    numPartitions: Int,
    numBuckets: Int,
    partitionColumns: Seq[Expression],
    @transient override val relation: BaseRelation,
    requestedColumns: Seq[AttributeReference],
    pushedFilters: Seq[Filter],
    allFilters: Seq[Expression],
    schemaAttributes: Seq[AttributeReference],
    scanBuilder: (Seq[Attribute], Seq[Filter], StatsPredicate) =>
        (RDD[Any], Seq[RDD[InternalRow]]),
    // not used currently (if need to use then get from relation.table)
    override val metastoreTableIdentifier: Option[TableIdentifier] = None)
    extends DataSourceScanExec with CodegenSupport {

  val cachedBatchStatsSchema = relation.asInstanceOf[BaseColumnFormatRelation].
      getCachedBatchStatsSchema(schemaAttributes)

  private def statsFor(a: Attribute) = cachedBatchStatsSchema.forAttribute(a)

  // Returned filter predicate should return false iff it is impossible for the input expression
  // to evaluate to `true' based on statistics collected about this partition batch.
  @transient val buildFilter: PartialFunction[Expression, Expression] = {
    case And(lhs: Expression, rhs: Expression)
      if buildFilter.isDefinedAt(lhs) || buildFilter.isDefinedAt(rhs) =>
      (buildFilter.lift(lhs) ++ buildFilter.lift(rhs)).reduce(_ && _)

    case Or(lhs: Expression, rhs: Expression)
      if buildFilter.isDefinedAt(lhs) && buildFilter.isDefinedAt(rhs) =>
      buildFilter(lhs) || buildFilter(rhs)

    case EqualTo(a: AttributeReference, l: Literal) =>
      statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound
    case EqualTo(l: Literal, a: AttributeReference) =>
      statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound

    case LessThan(a: AttributeReference, l: Literal) => statsFor(a).lowerBound < l
    case LessThan(l: Literal, a: AttributeReference) => l < statsFor(a).upperBound

    case LessThanOrEqual(a: AttributeReference, l: Literal) => statsFor(a).lowerBound <= l
    case LessThanOrEqual(l: Literal, a: AttributeReference) => l <= statsFor(a).upperBound

    case GreaterThan(a: AttributeReference, l: Literal) => l < statsFor(a).upperBound
    case GreaterThan(l: Literal, a: AttributeReference) => statsFor(a).lowerBound < l

    case GreaterThanOrEqual(a: AttributeReference, l: Literal) => l <= statsFor(a).upperBound
    case GreaterThanOrEqual(l: Literal, a: AttributeReference) => statsFor(a).lowerBound <= l

    case IsNull(a: Attribute) => statsFor(a).nullCount > 0
    case IsNotNull(a: Attribute) => statsFor(a).count - statsFor(a).nullCount > 0
  }

  val partitionFilters: Seq[Expression] = {
    allFilters.flatMap { p =>
      val filter = buildFilter.lift(p)
      val boundFilter =
        filter.map(
          BindReferences.bindReference(
            _,
            cachedBatchStatsSchema.schema,
            allowFailures = true))

      boundFilter.foreach(_ =>
        filter.foreach(f => logInfo(s"Predicate $p generates partition filter: $f")))

      // If the filter can't be resolved then we are missing required statistics.
      boundFilter.filter(_.resolved)
    }
  }

  val (dataRDD, otherRDDs) = if (this.isInstanceOf[ColumnTableScan]) {
    scanBuilder(
      requestedColumns, pushedFilters, new StatsPredicate(newPredicate,
        partitionFilters.reduceOption(And).getOrElse(Literal(true)),
        cachedBatchStatsSchema.schema))
  } else {
    scanBuilder(
      requestedColumns, pushedFilters, new StatsPredicate(newPredicate, null, null))
  }

  private val extraInformation = relation.toString

  override lazy val schema: StructType = StructType.fromAttributes(output)

  // RDD cast as RDD[InternalRow] below just to satisfy interfaces like
  // inputRDDs though its actually of CachedBatches, CompactExecRows, etc
  override val rdd: RDD[InternalRow] = dataRDD.asInstanceOf[RDD[InternalRow]]

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    rdd :: Nil
  }

  protected override def doExecute(): RDD[InternalRow] = {
    WholeStageCodegenExec(this).execute()
  }

  /** Specifies how data is partitioned across different nodes in the cluster. */
  override lazy val outputPartitioning: Partitioning = {
    if (numPartitions == 1) {
      SinglePartition
    } else if (partitionColumns.nonEmpty) {
      val callbacks = ToolsCallbackInit.toolsCallback
      if (callbacks != null) {
        callbacks.getOrderlessHashPartitioning(partitionColumns,
          numPartitions, numBuckets)
      } else {
        HashPartitioning(partitionColumns, numPartitions)
      }
    } else super.outputPartitioning
  }


  private[sql] override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def simpleString: String = "Partitioned Scan " + extraInformation +
      " , Requested Columns = " + output.mkString("[", ",", "]") +
      " partitionColumns = " + partitionColumns.mkString("[", ",", "]" +
      " numBuckets= " + numBuckets +
      " numPartitions= " + numPartitions)
}

private[sql] object PartitionedPhysicalScan {

  private[sql] val CT_NUMROWS_POSITION = 3
  private[sql] val CT_COLUMN_START = 5

  def createFromDataSource(
      output: Seq[Attribute],
      numPartitions: Int,
      numBuckets: Int,
      partitionColumns: Seq[Expression],
      relation: PartitionedDataSourceScan,
      requestedColumns: Seq[AttributeReference],
      pushedFilters: Seq[Filter],
      allFilters: Seq[Expression],
      schemaAttributes: Seq[AttributeReference],
      scanBuilder: (Seq[Attribute], Seq[Filter], StatsPredicate) =>
          (RDD[Any], Seq[RDD[InternalRow]])): PartitionedPhysicalScan =
    relation match {
      case r: BaseColumnFormatRelation =>
        ColumnTableScan(output, numPartitions, numBuckets,
          partitionColumns, relation, requestedColumns,
          pushedFilters, allFilters, schemaAttributes, scanBuilder)
      case r: SamplingRelation =>
        ColumnTableScan(output, numPartitions, numBuckets,
          partitionColumns, relation, requestedColumns,
          pushedFilters, allFilters, schemaAttributes, scanBuilder)
      case _: RowFormatRelation =>
        RowTableScan(output, numPartitions, numBuckets,
          partitionColumns, relation, requestedColumns,
          pushedFilters, allFilters, schemaAttributes, scanBuilder)
    }
}

trait PartitionedDataSourceScan extends PrunedUnsafeFilteredScan {

  def table: String

  def schema: StructType

  def numPartitions: Int

  def numBuckets: Int

  def partitionColumns: Seq[String]

  def connectionType: ConnectionType.Value
}
