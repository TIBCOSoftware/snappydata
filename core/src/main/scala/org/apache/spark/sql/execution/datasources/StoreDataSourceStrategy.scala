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
package org.apache.spark.sql.execution.datasources

import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{InternalRow, expressions}
import org.apache.spark.sql.execution.columnar.ColumnTableScan
import org.apache.spark.sql.execution.columnar.impl.IndexColumnFormatRelation
import org.apache.spark.sql.execution.datasources.DataSourceStrategy._
import org.apache.spark.sql.execution.{DataSourceScanExec, PartitionedDataSourceScan, SparkPlan}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedUnsafeFilteredScan}
import org.apache.spark.sql.{AnalysisException, Strategy, execution}

/**
 * This strategy makes a PartitionedPhysicalRDD out of a PrunedFilterScan based datasource.
 * Mostly this is a copy of DataSourceStrategy of Spark. But it takes care of the underlying
 * partitions of the datasource.
 */
private[sql] object StoreDataSourceStrategy extends Strategy {

  def apply(plan: LogicalPlan): Seq[execution.SparkPlan] = plan match {
    case PhysicalOperation(projects, filters,
        l@LogicalRelation(t: PartitionedDataSourceScan, _, _)) =>
      pruneFilterProject(
        l,
        projects,
        filters,
        isPartitioned = true,
        t.numPartitions,
        t.numBuckets,
        t.partitionColumns,
        (a, f) => t.buildUnsafeScan(a.map(_.name).toArray, f)) :: Nil
    case PhysicalOperation(projects, filters,
    l@LogicalRelation(t: PrunedUnsafeFilteredScan, _, _)) =>
      pruneFilterProject(
        l,
        projects,
        filters,
        isPartitioned = false,
        0,
        0,
        Seq.empty[String],
        (a, f) => t.buildUnsafeScan(a.map(_.name).toArray, f)) :: Nil
    case _ => Nil
  }

  // Based on Public API.
  protected def pruneFilterProject(
      relation: LogicalRelation,
      projects: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      isPartitioned: Boolean,
      numPartition: Int,
      numBuckets: Int,
      partitionColumns: Seq[String],
      scanBuilder: (Seq[Attribute], Array[Filter]) =>
          (RDD[Any], Seq[RDD[InternalRow]])) = {
    pruneFilterProjectRaw(
      relation,
      projects,
      filterPredicates,
      isPartitioned,
      numPartition,
      numBuckets,
      partitionColumns,
      (requestedColumns, _, pushedFilters) => {
        scanBuilder(requestedColumns, pushedFilters.toArray)
      })
  }

  // Based on Catalyst expressions.
  protected def pruneFilterProjectRaw(
      relation: LogicalRelation,
      projects: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      isPartitioned: Boolean,
      numPartition: Int,
      numBuckets: Int,
      partitionColumns: Seq[String],
      scanBuilder: (Seq[Attribute], Seq[Expression], Seq[Filter]) =>
          (RDD[Any], Seq[RDD[InternalRow]])) = {

    val projectSet = AttributeSet(projects.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))

    val candidatePredicates = filterPredicates.map {
      _ transform {
        case a: AttributeReference =>
          relation.attributeMap(a) // Match original case of attributes.
      }
    }

    val (unhandledPredicates, pushedFilters) =
      selectFilters(relation.relation, candidatePredicates)

    // A set of column attributes that are only referenced by pushed down
    // filters. We can eliminate them from requested columns.
    val handledSet = {
      val handledPredicates = filterPredicates.filterNot(unhandledPredicates.contains)
      val unhandledSet = AttributeSet(unhandledPredicates.flatMap(_.references))
      AttributeSet(handledPredicates.flatMap(_.references)) --
          (projectSet ++ unhandledSet).map(relation.attributeMap)
    }

    // Combines all Catalyst filter `Expression`s that are either not
    // convertible to data source `Filter`s or cannot be handled by `relation`.
    val filterCondition = unhandledPredicates.reduceLeftOption(expressions.And)

    def applyFilter(fullScan: SparkPlan) = {
      filterCondition.map(execution.FilterExec(_, fullScan)).getOrElse(fullScan)
    }

    def applyProjection(allColScan: SparkPlan) = {
      execution.ProjectExec(projects, allColScan)
    }

    // Get the partition column attribute INFO from relation schema
    val sqlContext = relation.relation.sqlContext

    val joinedCols = partitionColumns.map(colName =>
      relation.resolveQuoted(colName, sqlContext.sessionState.analyzer.resolver)
          .getOrElse(throw new AnalysisException(
            s"""Cannot resolve column "$colName" among (${relation.output})""")))
    val metadata: Map[String, String] = if (numPartition > 0) {
      Map.empty[String, String]
    } else {
      val pairs = mutable.ArrayBuffer.empty[(String, String)]
      if (pushedFilters.nonEmpty) {
        pairs += (DataSourceScanExec.PUSHED_FILTERS ->
            pushedFilters.mkString("[", ", ", "]"))
      }
      pairs.toMap
    }

    val mappedProjects = projects.map(_.toAttribute)
    if (mappedProjects == projects &&
        projectSet.size == projects.size &&
        filterSet.subsetOf(projectSet)) {
      // When it is possible to just use column pruning to get the right projection and
      // when the columns of this projection are enough to evaluate all filter conditions,
      // just do a scan followed by a filter, with no extra project.
      val requestedColumns =
      projects.asInstanceOf[Seq[Attribute]] // Safe due to if above.
          .map(relation.attributeMap) // Match original case of attributes.
          // Don't request columns that are only referenced by pushed filters.
          .filterNot(handledSet.contains)

      def createDataSourceScan(dataSource: PartitionedDataSourceScan,
          dataRDD: RDD[Any], others: Seq[RDD[InternalRow]]) = {
        execution.PartitionedPhysicalScan.createFromDataSource(mappedProjects,
          numPartition,
          numBuckets,
          joinedCols,
          dataRDD,
          others,
          dataSource)
      }

      val scan = if (isPartitioned) {
        val (rdd, otherRDDs) = scanBuilder(requestedColumns,
          candidatePredicates, pushedFilters)
        createDataSourceScan(relation.relation.asInstanceOf[PartitionedDataSourceScan],
          rdd, otherRDDs)
      } else {
        execution.DataSourceScanExec.create(
          mappedProjects,
          scanBuilder(requestedColumns, candidatePredicates,
            pushedFilters)._1.asInstanceOf[RDD[InternalRow]],
          relation.relation, metadata, relation.metastoreTableIdentifier)
      }

      buildIndexScanIfRequired(relation.relation, scan,
        requestedColumns, pushedFilters,
        createDataSourceScan,
        applyFilter)
    } else {
      // Don't request columns that are only referenced by pushed filters.
      val requestedColumns = (projectSet ++ filterSet -- handledSet).map(
        relation.attributeMap).toSeq

      def createDataSourceScan(dataSource: PartitionedDataSourceScan,
          dataRDD: RDD[Any], others: Seq[RDD[InternalRow]]) = {
        execution.PartitionedPhysicalScan.createFromDataSource(requestedColumns,
          numPartition,
          numBuckets,
          joinedCols,
          dataRDD,
          others,
          dataSource)
      }

      val scan = if (isPartitioned) {
        val (rdd, otherRDDs) = scanBuilder(requestedColumns,
          candidatePredicates, pushedFilters)
        createDataSourceScan(relation.relation.asInstanceOf[PartitionedDataSourceScan],
          rdd, otherRDDs)
      } else {
        execution.DataSourceScanExec.create(
          requestedColumns,
          scanBuilder(requestedColumns, candidatePredicates,
            pushedFilters)._1.asInstanceOf[RDD[InternalRow]],
          relation.relation, metadata, relation.metastoreTableIdentifier)
      }

      buildIndexScanIfRequired(relation.relation, scan,
        requestedColumns, pushedFilters,
        createDataSourceScan,
        applyFilter, applyProjection)
    }
  }

  protected def buildIndexScanIfRequired(relation: BaseRelation,
      scan: SparkPlan,
      requestedColumns: Seq[Attribute], pushedFilters: Seq[Filter],
      createDataSourceScan: (PartitionedDataSourceScan, RDD[Any],
          Seq[RDD[InternalRow]]) => SparkPlan,
      applyFilter: (SparkPlan) => SparkPlan,
      applyProject: (SparkPlan) => SparkPlan = p => p) = {

    relation match {
      case ir: IndexColumnFormatRelation =>
        val (tableRelation, tableRowBufferRDD) = ir.buildBaseTableRowBufferScan(
          requestedColumns.map(_.name).toArray,
          pushedFilters.toArray)

        val tableRowBufferScan = applyProject(applyFilter(
          createDataSourceScan(tableRelation, tableRowBufferRDD, Nil)))


        val tableRowBufferExchange = execution.exchange.ShuffleExchange(scan.outputPartitioning,
          tableRowBufferScan).execute()

        if(scan.isInstanceOf[ColumnTableScan]) {
          val colScan = scan.asInstanceOf[ColumnTableScan].
              copy(otherRDDs = Seq(tableRowBufferExchange))
          colScan
        }
        else {
          throw new UnsupportedOperationException(
            s"${ir.table} IndexScan other than column format not supported")
        }

      case _ =>
        applyProject(applyFilter(scan))
    }

  }
}
