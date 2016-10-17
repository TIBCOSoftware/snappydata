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
import org.apache.spark.sql.execution.datasources.DataSourceStrategy._
import org.apache.spark.sql.execution.{DataSourceScanExec, PartitionedDataSourceScan}
import org.apache.spark.sql.sources.{Filter, PrunedUnsafeFilteredScan}
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
      numBuckets: Int,
      partitionColumns: Seq[String],
      scanBuilder: (Seq[Attribute], Array[Filter]) =>
          (RDD[Any], Seq[RDD[InternalRow]])) = {
    pruneFilterProjectRaw(
      relation,
      projects,
      filterPredicates,
      isPartitioned,
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

    // Get the partition column attribute INFO from relation schema
    val sqlContext = relation.relation.sqlContext

    val joinedCols = partitionColumns.map(colName =>
      relation.resolveQuoted(colName, sqlContext.sessionState.analyzer.resolver)
          .getOrElse(throw new AnalysisException(
            s"""Cannot resolve column "$colName" among (${relation.output})""")))
    val metadata: Map[String, String] = if (numBuckets > 0) {
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

      val scan = if (isPartitioned) {
        val (rdd, otherRDDs) = scanBuilder(requestedColumns,
          candidatePredicates, pushedFilters)
        execution.PartitionedPhysicalScan.createFromDataSource(
          mappedProjects,
          numBuckets,
          joinedCols,
          rdd,
          otherRDDs,
          relation.relation.asInstanceOf[PartitionedDataSourceScan])
      } else {
        execution.DataSourceScanExec.create(
          mappedProjects,
          scanBuilder(requestedColumns, candidatePredicates,
            pushedFilters)._1.asInstanceOf[RDD[InternalRow]],
          relation.relation, metadata, relation.metastoreTableIdentifier)
      }
      filterCondition.map(execution.FilterExec(_, scan)).getOrElse(scan)
    } else {
      // Don't request columns that are only referenced by pushed filters.
      val requestedColumns = (projectSet ++ filterSet -- handledSet).map(
        relation.attributeMap).toSeq

      val scan = if (isPartitioned) {
        val (rdd, otherRDDs) = scanBuilder(requestedColumns,
          candidatePredicates, pushedFilters)
        execution.PartitionedPhysicalScan.createFromDataSource(
          requestedColumns,
          numBuckets,
          joinedCols,
          rdd,
          otherRDDs,
          relation.relation.asInstanceOf[PartitionedDataSourceScan])
      } else {
        execution.DataSourceScanExec.create(
          requestedColumns,
          scanBuilder(requestedColumns, candidatePredicates,
            pushedFilters)._1.asInstanceOf[RDD[InternalRow]],
          relation.relation, metadata, relation.metastoreTableIdentifier)
      }
      execution.ProjectExec(projects,
        filterCondition.map(execution.FilterExec(_, scan)).getOrElse(scan))
    }
  }
}
