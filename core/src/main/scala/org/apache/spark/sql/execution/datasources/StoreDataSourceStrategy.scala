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
import org.apache.spark.sql.catalyst.CatalystTypeConverters.convertToScala
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, EmptyRow, Expression, Literal, NamedExpression, ParamLiteral}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, expressions}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy._
import org.apache.spark.sql.execution.{DataSourceScanExec, PartitionedDataSourceScan}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedUnsafeFilteredScan}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{AnalysisException, Strategy, execution, sources}
import org.apache.spark.unsafe.types.UTF8String

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
        t.numBuckets,
        t.partitionColumns,
        (a, f) => t.buildUnsafeScan(a.map(_.name).toArray, f)) :: Nil
    case PhysicalOperation(projects, filters,
    l@LogicalRelation(t: PrunedUnsafeFilteredScan, _, _)) =>
      pruneFilterProject(
        l,
        projects,
        filters,
        0,
        Seq.empty[String],
        (a, f) => t.buildUnsafeScan(a.map(_.name).toArray, f)) :: Nil
    case _ => Nil
  }

  private def pruneFilterProject(
      relation: LogicalRelation,
      projects: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      numBuckets: Int,
      partitionColumns: Seq[String],
      scanBuilder: (Seq[Attribute], Array[Filter]) =>
          (RDD[Any], Seq[RDD[InternalRow]])) = {
    pruneFilterProjectRaw(
      relation,
      projects,
      filterPredicates,
      numBuckets,
      partitionColumns,
      (requestedColumns, _, pushedFilters) => {
        scanBuilder(requestedColumns, pushedFilters.toArray)
      })
  }

  private def pruneFilterProjectRaw(
      relation: LogicalRelation,
      projects: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
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
    // check for joinedCols in projections
    val joinedAliases = if (projects.nonEmpty) {
      joinedCols.map(j => projects.collect {
        case a@Alias(child, _) if child.semanticEquals(j) => a.toAttribute
      })
    } else Seq.empty
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

      val (rdd, otherRDDs) = scanBuilder(requestedColumns,
        candidatePredicates, pushedFilters)
      val scan = relation.relation match {
        case partitionedRelation: PartitionedDataSourceScan =>
          execution.PartitionedPhysicalScan.createFromDataSource(
            mappedProjects,
            numBuckets,
            joinedCols,
            joinedAliases,
            rdd,
            otherRDDs,
            partitionedRelation,
            filterPredicates, // filter predicates for column batch screening
            relation.output,
            (requestedColumns, pushedFilters)
          )
        case baseRelation =>
          execution.DataSourceScanExec.create(
            mappedProjects,
            rdd.asInstanceOf[RDD[InternalRow]],
            baseRelation, metadata, relation.metastoreTableIdentifier)
      }
      filterCondition.map(execution.FilterExec(_, scan)).getOrElse(scan)
    } else {
      // Don't request columns that are only referenced by pushed filters.
      val requestedColumns = (projectSet ++ filterSet -- handledSet).map(
        relation.attributeMap).toSeq

      val (rdd, otherRDDs) = scanBuilder(requestedColumns,
        candidatePredicates, pushedFilters)
      val scan = relation.relation match {
        case partitionedRelation: PartitionedDataSourceScan =>
          execution.PartitionedPhysicalScan.createFromDataSource(
            requestedColumns,
            numBuckets,
            joinedCols,
            joinedAliases,
            rdd,
            otherRDDs,
            partitionedRelation,
            filterPredicates, // filter predicates for column batch screening
            relation.output,
            (requestedColumns, pushedFilters)
          )
        case baseRelation =>
          execution.DataSourceScanExec.create(
            requestedColumns,
            rdd.asInstanceOf[RDD[InternalRow]],
            baseRelation, metadata, relation.metastoreTableIdentifier)
      }
      execution.ProjectExec(projects,
        filterCondition.map(execution.FilterExec(_, scan)).getOrElse(scan))
    }
  }

  /**
   * Tries to translate a Catalyst [[Expression]] into data source [[Filter]].
   *
   * @return a `Some[Filter]` if the input [[Expression]] is convertible, otherwise a `None`.
   */
  protected[sql] def translateFilter(predicate: Expression): Option[Filter] = {
    predicate match {
      case expressions.EqualTo(a: Attribute, l@Literal(v, t)) =>
        Some(sources.EqualTo(a.name, l))
      case expressions.EqualTo(l@Literal(v, t), a: Attribute) =>
        Some(sources.EqualTo(a.name, l))

      case expressions.EqualNullSafe(a: Attribute, l@Literal(v, t)) =>
        Some(sources.EqualNullSafe(a.name, l))
      case expressions.EqualNullSafe(l@Literal(v, t), a: Attribute) =>
        Some(sources.EqualNullSafe(a.name, l))

      case expressions.GreaterThan(a: Attribute, Literal(v, t)) =>
        Some(sources.GreaterThan(a.name, convertToScala(v, t)))
      case expressions.GreaterThan(Literal(v, t), a: Attribute) =>
        Some(sources.LessThan(a.name, convertToScala(v, t)))

      case expressions.LessThan(a: Attribute, Literal(v, t)) =>
        Some(sources.LessThan(a.name, convertToScala(v, t)))
      case expressions.LessThan(Literal(v, t), a: Attribute) =>
        Some(sources.GreaterThan(a.name, convertToScala(v, t)))

      case expressions.GreaterThanOrEqual(a: Attribute, Literal(v, t)) =>
        Some(sources.GreaterThanOrEqual(a.name, convertToScala(v, t)))
      case expressions.GreaterThanOrEqual(Literal(v, t), a: Attribute) =>
        Some(sources.LessThanOrEqual(a.name, convertToScala(v, t)))

      case expressions.LessThanOrEqual(a: Attribute, Literal(v, t)) =>
        Some(sources.LessThanOrEqual(a.name, convertToScala(v, t)))
      case expressions.LessThanOrEqual(Literal(v, t), a: Attribute) =>
        Some(sources.GreaterThanOrEqual(a.name, convertToScala(v, t)))

      case expressions.InSet(a: Attribute, set) =>
        val toScala = CatalystTypeConverters.createToScalaConverter(a.dataType)
        Some(sources.In(a.name, set.toArray.map(toScala)))

      // Because we only convert In to InSet in Optimizer when there are more than certain
      // items. So it is possible we still get an In expression here that needs to be pushed
      // down.
      case expressions.In(a: Attribute, list) if !list.exists(!_.isInstanceOf[Literal]) =>
        val hSet = list.map(e => e.eval(EmptyRow))
        val toScala = CatalystTypeConverters.createToScalaConverter(a.dataType)
        Some(sources.In(a.name, hSet.toArray.map(toScala)))

      case expressions.IsNull(a: Attribute) =>
        Some(sources.IsNull(a.name))
      case expressions.IsNotNull(a: Attribute) =>
        Some(sources.IsNotNull(a.name))

      case expressions.And(left, right) =>
        (translateFilter(left) ++ translateFilter(right)).reduceOption(sources.And)

      case expressions.Or(left, right) =>
        for {
          leftFilter <- translateFilter(left)
          rightFilter <- translateFilter(right)
        } yield sources.Or(leftFilter, rightFilter)

      case expressions.Not(child) =>
        translateFilter(child).map(sources.Not)

      case expressions.StartsWith(a: Attribute, Literal(v: UTF8String, StringType)) =>
        Some(sources.StringStartsWith(a.name, v.toString))

      case expressions.EndsWith(a: Attribute, Literal(v: UTF8String, StringType)) =>
        Some(sources.StringEndsWith(a.name, v.toString))

      case expressions.Contains(a: Attribute, Literal(v: UTF8String, StringType)) =>
        Some(sources.StringContains(a.name, v.toString))

      case _ => None
    }
  }

  /**
   * Selects Catalyst predicate [[Expression]]s which are convertible into data source [[Filter]]s
   * and can be handled by `relation`.
   *
   * @return A pair of `Seq[Expression]` and `Seq[Filter]`. The first element contains all Catalyst
   *         predicate [[Expression]]s that are either not convertible or cannot be handled by
   *         `relation`. The second element contains all converted data source [[Filter]]s that
   *         will be pushed down to the data source.
   */
  protected[sql] def selectFilters(
      relation: BaseRelation,
      predicates: Seq[Expression]): (Seq[Expression], Seq[Filter]) = {

    // For conciseness, all Catalyst filter expressions of type `expressions.Expression` below are
    // called `predicate`s, while all data source filters of type `sources.Filter` are simply called
    // `filter`s.

    val translated: Seq[(Expression, Filter)] =
      for {
        predicate <- predicates
        filter <- translateFilter(predicate)
      } yield predicate -> filter

    // A map from original Catalyst expressions to corresponding translated data source filters.
    val translatedMap: Map[Expression, Filter] = translated.toMap

    // Catalyst predicate expressions that cannot be translated to data source filters.
    val unrecognizedPredicates = predicates.filterNot(translatedMap.contains)

    // Data source filters that cannot be handled by `relation`. The semantic of a unhandled filter
    // at here is that a data source may not be able to apply this filter to every row
    // of the underlying dataset.
    val unhandledFilters = relation.unhandledFilters(translatedMap.values.toArray).toSet

    val (unhandled, handled) = translated.partition {
      case (predicate, filter) =>
        unhandledFilters.contains(filter)
    }

    // Catalyst predicate expressions that can be translated to data source filters, but cannot be
    // handled by `relation`.
    val (unhandledPredicates, _) = unhandled.unzip

    // Translated data source filters that can be handled by `relation`
    val (_, handledFilters) = handled.unzip

    // translated contains all filters that have been converted to the public Filter interface.
    // We should always push them to the data source no matter whether the data source can apply
    // a filter to every row or not.
    val (_, translatedFilters) = translated.unzip

    (unrecognizedPredicates ++ unhandledPredicates, translatedFilters)
  }
}
