/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import scala.collection.mutable.ArrayBuffer

import io.snappydata.Constant

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.CatalystTypeConverters.convertToScala
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, expressions}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, Cast, EmptyRow, Expression, Literal, NamedExpression, ParamLiteral, SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.{InsertIntoDataSourceCommand, LogicalRelation}
import org.apache.spark.sql.{Row, Strategy, execution, sources}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

object SnappyDataSourceStrategy extends Strategy with Logging {
  def apply(plan: LogicalPlan): Seq[execution.SparkPlan] = plan match {
    case PhysicalOperation(projects, filters, l @ LogicalRelation(t: CatalystScan, _, _)) =>
      pruneFilterProjectRaw(
        l,
        projects,
        filters,
        (requestedColumns, allPredicates, _) =>
          toCatalystRDD(l, requestedColumns, t.buildScan(requestedColumns, allPredicates))) :: Nil

    case PhysicalOperation(projects, filters, l @ LogicalRelation(t: PrunedFilteredScan, _, _)) =>
      pruneFilterProject(
        l,
        projects,
        filters,
        (a, f) => toCatalystRDD(l, a, t.buildScan(a.map(_.name).toArray, f))) :: Nil

    case PhysicalOperation(projects, filters, l @ LogicalRelation(t: PrunedScan, _, _)) =>
      pruneFilterProject(
        l,
        projects,
        filters,
        (a, _) => toCatalystRDD(l, a, t.buildScan(a.map(_.name).toArray))) :: Nil

    case l @ LogicalRelation(baseRelation: TableScan, _, _) =>
      RowDataSourceScanExec(
        l.output,
        toCatalystRDD(l, baseRelation.buildScan()),
        baseRelation,
        UnknownPartitioning(0),
        Map.empty,
        None) :: Nil

    case i @ logical.InsertIntoTable(l @ LogicalRelation(t: InsertableRelation, _, _),
    part, query, overwrite, false) if part.isEmpty =>
      ExecutedCommandExec(InsertIntoDataSourceCommand(l, query, overwrite)) :: Nil

    case _ => Nil
  }

  // Get the bucket ID based on the bucketing values.
  // Restriction: Bucket pruning works iff the bucketing column has one and only one column.
  def getBucketId(bucketColumn: Attribute, numBuckets: Int, value: Any): Int = {
    val mutableRow = new SpecificInternalRow(Seq(bucketColumn.dataType))
    mutableRow(0) = Cast(Literal(value), bucketColumn.dataType).eval(null)
    val bucketIdGeneration = UnsafeProjection.create(
      HashPartitioning(bucketColumn :: Nil, numBuckets).partitionIdExpression :: Nil,
      bucketColumn :: Nil)

    bucketIdGeneration(mutableRow).getInt(0)
  }

  // Based on Public API.
  private def pruneFilterProject(
      relation: LogicalRelation,
      projects: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      scanBuilder: (Seq[Attribute], Array[Filter]) => RDD[InternalRow]) = {
    pruneFilterProjectRaw(
      relation,
      projects,
      filterPredicates,
      (requestedColumns, _, pushedFilters) => {
        scanBuilder(requestedColumns, pushedFilters.toArray)
      })
  }

  // Based on Catalyst expressions. The `scanBuilder` function accepts three arguments:
  //
  //  1. A `Seq[Attribute]`, containing all required column attributes. Used to handle relation
  //     traits that support column pruning (e.g. `PrunedScan` and `PrunedFilteredScan`).
  //
  //  2. A `Seq[Expression]`, containing all gathered Catalyst filter expressions, only used for
  //     `CatalystScan`.
  //
  //  3. A `Seq[Filter]`, containing all data source `Filter`s that are converted from (possibly a
  //     subset of) Catalyst filter expressions and can be handled by `relation`.  Used to handle
  //     relation traits (`CatalystScan` excluded) that support filter push-down (e.g.
  //     `PrunedFilteredScan` and `HadoopFsRelation`).
  //
  // Note that 2 and 3 shouldn't be used together.
  private def pruneFilterProjectRaw(
      relation: LogicalRelation,
      projects: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      scanBuilder: (Seq[Attribute], Seq[Expression], Seq[Filter]) => RDD[InternalRow]): SparkPlan = {

    val projectSet = AttributeSet(projects.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))

    val candidatePredicates = filterPredicates.map { _ transform {
      case a: AttributeReference => relation.attributeMap(a) // Match original case of attributes.
    }}

    val (unhandledPredicates, pushedFilters, handledFilters) =
      selectFilters(relation.relation, candidatePredicates)

    // A set of column attributes that are only referenced by pushed down filters.  We can eliminate
    // them from requested columns.
    val handledSet = {
      val handledPredicates = filterPredicates.filterNot(unhandledPredicates.contains)
      val unhandledSet = AttributeSet(unhandledPredicates.flatMap(_.references))
      AttributeSet(handledPredicates.flatMap(_.references)) --
          (projectSet ++ unhandledSet).map(relation.attributeMap)
    }

    // Combines all Catalyst filter `Expression`s that are either not convertible to data source
    // `Filter`s or cannot be handled by `relation`.
    val filterCondition = unhandledPredicates.reduceLeftOption(expressions.And)

    // These metadata values make scan plans uniquely identifiable for equality checking.
    // TODO(SPARK-17701) using strings for equality checking is brittle
    val metadata: Map[String, String] = {
      val pairs = ArrayBuffer.empty[(String, String)]

      // Mark filters which are handled by the underlying DataSource with an Astrisk
      if (pushedFilters.nonEmpty) {
        val markedFilters = for (filter <- pushedFilters) yield {
          if (handledFilters.contains(filter)) s"*$filter" else s"$filter"
        }
        pairs += ("PushedFilters" -> markedFilters.mkString("[", ", ", "]"))
      }
      pairs += ("ReadSchema" ->
          StructType.fromAttributes(projects.map(_.toAttribute)).catalogString)
      pairs.toMap
    }

    if (projects.map(_.toAttribute) == projects &&
        projectSet.size == projects.size &&
        filterSet.subsetOf(projectSet)) {
      // When it is possible to just use column pruning to get the right projection and
      // when the columns of this projection are enough to evaluate all filter conditions,
      // just do a scan followed by a filter, with no extra project.
      val requestedColumns = projects
          // Safe due to if above.
          .asInstanceOf[Seq[Attribute]]
          // Match original case of attributes.
          .map(relation.attributeMap)
          // Don't request columns that are only referenced by pushed filters.
          .filterNot(handledSet.contains)

      val scan = RowDataSourceScanExec(
        projects.map(_.toAttribute),
        scanBuilder(requestedColumns, candidatePredicates, pushedFilters),
        relation.relation, UnknownPartitioning(0), metadata,
        relation.catalogTable.map(_.identifier))
      filterCondition.map(execution.FilterExec(_, scan)).getOrElse(scan)
    } else {
      // Don't request columns that are only referenced by pushed filters.
      val requestedColumns =
        (projectSet ++ filterSet -- handledSet).map(relation.attributeMap).toSeq

      val scan = RowDataSourceScanExec(
        requestedColumns,
        scanBuilder(requestedColumns, candidatePredicates, pushedFilters),
        relation.relation, UnknownPartitioning(0), metadata,
        relation.catalogTable.map(_.identifier))
      execution.ProjectExec(
        projects, filterCondition.map(execution.FilterExec(_, scan)).getOrElse(scan))
    }
  }

  /**
   * Convert RDD of Row into RDD of InternalRow with objects in catalyst types
   */
  private[this] def toCatalystRDD(
      relation: LogicalRelation,
      output: Seq[Attribute],
      rdd: RDD[Row]): RDD[InternalRow] = {
    if (relation.relation.needConversion) {
      execution.RDDConversions.rowToRowRdd(rdd, output.map(_.dataType))
    } else {
      rdd.asInstanceOf[RDD[InternalRow]]
    }
  }

  /**
   * Convert RDD of Row into RDD of InternalRow with objects in catalyst types
   */
  private[this] def toCatalystRDD(relation: LogicalRelation, rdd: RDD[Row]): RDD[InternalRow] = {
    toCatalystRDD(relation, relation.output, rdd)
  }

  /**
   * Tries to translate a Catalyst [[Expression]] into data source [[Filter]].
   *
   * @return a `Some[Filter]` if the input [[Expression]] is convertible, otherwise a `None`.
   */
  protected[sql] def translateFilter(predicate: Expression, prmLitPushDown: Boolean):
  Option[Filter] = {
    predicate match {
      case expressions.EqualTo(a: Attribute, Literal(v, t)) =>
        Some(sources.EqualTo(a.name, convertToScala(v, t)))
      case expressions.EqualTo(a: Attribute, x: ParamLiteral) if prmLitPushDown =>
        Some(sources.EqualTo(a.name, x))

      case expressions.EqualTo(x: ParamLiteral, a: Attribute) if prmLitPushDown =>
        Some(sources.EqualTo(a.name, x))
      case expressions.EqualTo(Literal(v, t), a: Attribute) =>
        Some(sources.EqualTo(a.name, convertToScala(v, t)))

      case expressions.EqualNullSafe(a: Attribute, Literal(v, t)) =>
        Some(sources.EqualNullSafe(a.name, convertToScala(v, t)))
      case expressions.EqualNullSafe(a: Attribute, x: ParamLiteral) if prmLitPushDown =>
        Some(sources.EqualNullSafe(a.name, x))

      case expressions.EqualNullSafe(Literal(v, t), a: Attribute) =>
        Some(sources.EqualNullSafe(a.name, convertToScala(v, t)))
      case expressions.EqualNullSafe(x: ParamLiteral, a: Attribute) if prmLitPushDown =>
        Some(sources.EqualNullSafe(a.name, x))

      case expressions.GreaterThan(a: Attribute, Literal(v, t)) =>
        Some(sources.GreaterThan(a.name, convertToScala(v, t)))
      case expressions.GreaterThan(a: Attribute, x: ParamLiteral) if prmLitPushDown =>
        Some(sources.GreaterThan(a.name, x))

      case expressions.GreaterThan(Literal(v, t), a: Attribute) =>
        Some(sources.LessThan(a.name, convertToScala(v, t)))
      case expressions.GreaterThan(x: ParamLiteral, a: Attribute) if prmLitPushDown =>
        Some(sources.LessThan(a.name, x))

      case expressions.LessThan(a: Attribute, Literal(v, t)) =>
        Some(sources.LessThan(a.name, convertToScala(v, t)))
      case expressions.LessThan(a: Attribute, x: ParamLiteral) if prmLitPushDown =>
        Some(sources.LessThan(a.name, x))

      case expressions.LessThan(Literal(v, t), a: Attribute) =>
        Some(sources.GreaterThan(a.name, convertToScala(v, t)))

      case expressions.LessThan(x: ParamLiteral, a: Attribute) if prmLitPushDown =>
        Some(sources.GreaterThan(a.name, x))

      case expressions.GreaterThanOrEqual(a: Attribute, Literal(v, t)) =>
        Some(sources.GreaterThanOrEqual(a.name, convertToScala(v, t)))
      case expressions.GreaterThanOrEqual(a: Attribute, x: ParamLiteral) if prmLitPushDown =>
        Some(sources.GreaterThanOrEqual(a.name, x))

      case expressions.GreaterThanOrEqual(Literal(v, t), a: Attribute) =>
        Some(sources.LessThanOrEqual(a.name, convertToScala(v, t)))
      case expressions.GreaterThanOrEqual(x: ParamLiteral, a: Attribute) if prmLitPushDown =>
        Some(sources.LessThanOrEqual(a.name, x))

      case expressions.LessThanOrEqual(a: Attribute, Literal(v, t)) =>
        Some(sources.LessThanOrEqual(a.name, convertToScala(v, t)))
      case expressions.LessThanOrEqual(a: Attribute, x: ParamLiteral) if prmLitPushDown =>
        Some(sources.LessThanOrEqual(a.name, x))

      case expressions.LessThanOrEqual(Literal(v, t), a: Attribute) =>
        Some(sources.GreaterThanOrEqual(a.name, convertToScala(v, t)))
      case expressions.LessThanOrEqual(x: ParamLiteral, a: Attribute) if prmLitPushDown =>
        Some(sources.GreaterThanOrEqual(a.name, x))

      case expressions.InSet(a: Attribute, set) if !set.exists(_.isInstanceOf[ParamLiteral]) ||
          prmLitPushDown =>
        val toScala = CatalystTypeConverters.createToScalaConverter(a.dataType)
        // Some(sources.In(a.name, set.toArray.map(toScala)))
        Some(sources.In(a.name, set.toArray.map(_ match {
          case pl: ParamLiteral => pl
          case x => toScala(x)
        })))
      // Because we only convert In to InSet in Optimizer when there are more than certain
      // items. So it is possible we still get an In expression here that needs to be pushed
      // down.
      case expressions.In(a: Attribute, list) if (list.forall(_.isInstanceOf[Literal])
      || ( prmLitPushDown && list.exists(_.isInstanceOf[ParamLiteral]))) =>
        val hSet = list.map(e => e match {
          case x: ParamLiteral => x
          case _ => e.eval(EmptyRow)
        })
        val toScala = CatalystTypeConverters.createToScalaConverter(a.dataType)
        Some(sources.In(a.name, hSet.toArray.map(_ match {
          case pl: ParamLiteral => pl
          case x => toScala(x)
        })))

      case expressions.IsNull(a: Attribute) =>
        Some(sources.IsNull(a.name))
      case expressions.IsNotNull(a: Attribute) =>
        Some(sources.IsNotNull(a.name))

      case expressions.And(left, right) =>
        (translateFilter(left, prmLitPushDown) ++
            translateFilter(right, prmLitPushDown)).reduceOption(sources.And)

      case expressions.Or(left, right) =>
        for {
          leftFilter <- translateFilter(left, prmLitPushDown)
          rightFilter <- translateFilter(right, prmLitPushDown)
        } yield sources.Or(leftFilter, rightFilter)

      case expressions.Not(child) =>
        translateFilter(child, prmLitPushDown).map(sources.Not)

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
   * @return A triplet of `Seq[Expression]`, `Seq[Filter]`, and `Seq[Filter]` . The first element
   *         contains all Catalyst predicate [[Expression]]s that are either not convertible or
   *         cannot be handled by `relation`. The second element contains all converted data source
   *         [[Filter]]s that will be pushed down to the data source. The third element contains
   *         all [[Filter]]s that are completely filtered at the DataSource.
   */
  protected[sql] def selectFilters(
      relation: BaseRelation,
      predicates: Seq[Expression]): (Seq[Expression], Seq[Filter], Set[Filter]) = {

    // For conciseness, all Catalyst filter expressions of type `expressions.Expression` below are
    // called `predicate`s, while all data source filters of type `sources.Filter` are simply called
    // `filter`s.

    // A map from original Catalyst expressions to corresponding translated data source filters.
    // If a predicate is not in this map, it means it cannot be pushed down.
    val isGFERelation = relation.getClass.getSimpleName.equals(Constant.GEMFIRE_RELATION)
    val translatedMap: Map[Expression, Filter] = predicates.flatMap { p =>
      translateFilter(p, isGFERelation).map(f => p -> f)
    }.toMap

    val pushedFilters: Seq[Filter] = translatedMap.values.toSeq

    // Catalyst predicate expressions that cannot be converted to data source filters.
    val nonconvertiblePredicates = predicates.filterNot(translatedMap.contains)

    // Data source filters that cannot be handled by `relation`. An unhandled filter means
    // the data source cannot guarantee the rows returned can pass the filter.
    // As a result we must return it so Spark can plan an extra filter operator.
    val unhandledFilters = relation.unhandledFilters(translatedMap.values.toArray).toSet
    val unhandledPredicates = translatedMap.filter { case (p, f) =>
      unhandledFilters.contains(f)
    }.keys
    val handledFilters = pushedFilters.toSet -- unhandledFilters

    (nonconvertiblePredicates ++ unhandledPredicates, pushedFilters, handledFilters)
  }
}