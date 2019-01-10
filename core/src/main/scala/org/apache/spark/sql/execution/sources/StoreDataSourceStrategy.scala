/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
/*
 * PhysicalScan taken from Spark's PhysicalOperation having license as below.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.sources

import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, EmptyRow, Expression, NamedExpression, ParamLiteral, PredicateHelper, TokenLiteral}
import org.apache.spark.sql.catalyst.plans.logical.{BroadcastHint, LogicalPlan, Project, Filter => LFilter}
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, analysis, expressions}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.{PartitionedDataSourceScan, RowDataSourceScanExec}
import org.apache.spark.sql.sources.{Filter, PrunedUnsafeFilteredScan}
import org.apache.spark.sql.{AnalysisException, SnappySession, SparkSession, Strategy, execution, sources}

/**
 * This strategy makes a PartitionedPhysicalRDD out of a PrunedFilterScan based datasource.
 * Mostly this is a copy of DataSourceStrategy of Spark. But it takes care of the underlying
 * partitions of the datasource.
 */
private[sql] object StoreDataSourceStrategy extends Strategy {

  def apply(plan: LogicalPlan): Seq[execution.SparkPlan] = plan match {
    case PhysicalScan(projects, filters, scan) => scan match {
      case l@LogicalRelation(t: PartitionedDataSourceScan, _, _) =>
        pruneFilterProject(
          l,
          projects,
          filters,
          t.numBuckets,
          t.partitionColumns,
          (a, f) => t.buildUnsafeScan(a.map(_.name).toArray, f.toArray)) :: Nil
      case l@LogicalRelation(t: PrunedUnsafeFilteredScan, _, _) =>
        pruneFilterProject(
          l,
          projects,
          filters,
          0,
          Nil,
          (a, f) => t.buildUnsafeScan(a.map(_.name).toArray, f.toArray)) :: Nil
      case LogicalRelation(_, _, _) =>
        var foundParamLiteral = false
        val tp = plan.transformAllExpressions {
          case pl: ParamLiteral =>
            foundParamLiteral = true
            pl.asLiteral
        }
        // replace ParamLiteral with TokenLiteral for external data sources so Spark's
        // translateToFilter can push down required filters
        if (foundParamLiteral) {
          planLater(tp) :: Nil
        } else {
          Nil
        }
      case _ => Nil
    }
    case _ => Nil
  }

  private def pruneFilterProject(
      relation: LogicalRelation,
      projects: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      numBuckets: Int,
      partitionColumns: Seq[String],
      scanBuilder: (Seq[Attribute], Seq[Expression]) => (RDD[Any], Seq[RDD[InternalRow]])) = {

    var allDeterministic = true
    val projectSet = AttributeSet(projects.flatMap { p =>
      if (allDeterministic && !p.deterministic) allDeterministic = false
      p.references
    })
    val filterSet = AttributeSet(filterPredicates.flatMap { f =>
      if (allDeterministic && !f.deterministic) allDeterministic = false
      f.references
    })

    if (!allDeterministic) {
      // keep one-to-one mapping between partitions and buckets for non-deterministic
      // expressions like spark_partition_id()
      SparkSession.getActiveSession match {
        case Some(session: SnappySession) => session.linkPartitionsToBuckets(flag = true)
        case _ =>
      }
    }

    val candidatePredicates = filterPredicates.map {
      _ transform {
        case a: AttributeReference =>
          relation.attributeMap(a) // Match original case of attributes.
      }
    }

    val unhandledPredicates = relation.relation.asInstanceOf[PrunedUnsafeFilteredScan]
        .unhandledFilters(candidatePredicates)

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

    // use case-insensitive resolution since partitioning columns during
    // creation could be using the same as opposed to during scan
    val joinedCols = partitionColumns.map(colName =>
      relation.resolveQuoted(colName, analysis.caseInsensitiveResolution)
          .getOrElse(throw new AnalysisException(
            s"""Cannot resolve column "$colName" among (${relation.output})""")))
    // check for joinedCols in projections
    val joinedAliases = if (projects.nonEmpty) {
      joinedCols.map(j => projects.collect {
        case a@Alias(child, _) if child.semanticEquals(j) => a.toAttribute
      })
    } else Nil

    def getMetadata: Map[String, String] = if (numBuckets > 0) {
      Map.empty[String, String]
    } else {
      val pushedFilters = candidatePredicates.flatMap(translateToFilter)
      val pairs = mutable.ArrayBuffer.empty[(String, String)]
      if (pushedFilters.nonEmpty) {
        pairs += ("PushedFilters" ->
            pushedFilters.mkString("[", ", ", "]"))
      }
      pairs.toMap
    }

    val mappedProjects = projects.map(_.toAttribute)
    val projectOnlyAttributes = mappedProjects == projects
    if (projectOnlyAttributes &&
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

      val (rdd, otherRDDs) = scanBuilder(requestedColumns, candidatePredicates)
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
            (requestedColumns, candidatePredicates)
          )
        case baseRelation =>
          RowDataSourceScanExec(
            mappedProjects,
            scanBuilder(requestedColumns, candidatePredicates)._1.asInstanceOf[RDD[InternalRow]],
            baseRelation, UnknownPartitioning(0), getMetadata,
            relation.catalogTable.map(_.identifier))
      }
      filterCondition.map(execution.FilterExec(_, scan)).getOrElse(scan)
    } else {
      // Don't request columns that are only referenced by pushed filters.
      val requestedColumns = (projectSet ++ filterSet -- handledSet).map(
        relation.attributeMap).toSeq

      val (rdd, otherRDDs) = scanBuilder(requestedColumns, candidatePredicates)
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
            (requestedColumns, candidatePredicates)
          )
        case baseRelation =>
          RowDataSourceScanExec(
            mappedProjects,
            scanBuilder(requestedColumns, candidatePredicates)._1.asInstanceOf[RDD[InternalRow]],
            baseRelation, UnknownPartitioning(0), getMetadata,
            relation.catalogTable.map(_.identifier))
      }
      if (projectOnlyAttributes || allDeterministic || filterCondition.isEmpty) {
        execution.ProjectExec(projects,
          filterCondition.map(execution.FilterExec(_, scan)).getOrElse(scan))
      } else {
        execution.FilterExec(filterCondition.get, execution.ProjectExec(projects, scan))
      }
    }
  }

  /**
   * Tries to translate a Catalyst [[Expression]] into data source [[Filter]].
   *
   * @return a `Some[Filter]` if the input [[Expression]] is convertible, otherwise a `None`.
   */
  protected[sql] def translateToFilter(predicate: Expression): Option[Filter] = {
    predicate match {
      case expressions.EqualTo(a: Attribute, TokenLiteral(v)) =>
        Some(sources.EqualTo(a.name, v))
      case expressions.EqualTo(TokenLiteral(v), a: Attribute) =>
        Some(sources.EqualTo(a.name, v))

      case expressions.EqualNullSafe(a: Attribute, TokenLiteral(v)) =>
        Some(sources.EqualNullSafe(a.name, v))
      case expressions.EqualNullSafe(TokenLiteral(v), a: Attribute) =>
        Some(sources.EqualNullSafe(a.name, v))

      case expressions.GreaterThan(a: Attribute, TokenLiteral(v)) =>
        Some(sources.GreaterThan(a.name, v))
      case expressions.GreaterThan(TokenLiteral(v), a: Attribute) =>
        Some(sources.LessThan(a.name, v))

      case expressions.LessThan(a: Attribute, TokenLiteral(v)) =>
        Some(sources.LessThan(a.name, v))
      case expressions.LessThan(TokenLiteral(v), a: Attribute) =>
        Some(sources.GreaterThan(a.name, v))

      case expressions.GreaterThanOrEqual(a: Attribute, TokenLiteral(v)) =>
        Some(sources.GreaterThanOrEqual(a.name, v))
      case expressions.GreaterThanOrEqual(TokenLiteral(v), a: Attribute) =>
        Some(sources.LessThanOrEqual(a.name, v))

      case expressions.LessThanOrEqual(a: Attribute, TokenLiteral(v)) =>
        Some(sources.LessThanOrEqual(a.name, v))
      case expressions.LessThanOrEqual(TokenLiteral(v), a: Attribute) =>
        Some(sources.GreaterThanOrEqual(a.name, v))

      case expressions.InSet(a: Attribute, set) =>
        val toScala = CatalystTypeConverters.createToScalaConverter(a.dataType)
        Some(sources.In(a.name, set.toArray.map(toScala)))

      case expressions.DynamicInSet(a: Attribute, set) =>
        val hSet = set.map(e => e.eval(EmptyRow))
        val toScala = CatalystTypeConverters.createToScalaConverter(a.dataType)
        Some(sources.In(a.name, hSet.toArray.map(toScala)))

      // Because we only convert In to InSet in Optimizer when there are more than certain
      // items. So it is possible we still get an In expression here that needs to be pushed down.
      case expressions.In(a: Attribute, list) if list.forall(TokenLiteral.isConstant) =>
        val hSet = list.map(e => e.eval(EmptyRow))
        val toScala = CatalystTypeConverters.createToScalaConverter(a.dataType)
        Some(sources.In(a.name, hSet.toArray.map(toScala)))

      case expressions.IsNull(a: Attribute) =>
        Some(sources.IsNull(a.name))

      case expressions.IsNotNull(a: Attribute) =>
        Some(sources.IsNotNull(a.name))

      case expressions.And(left, right) =>
        (translateToFilter(left) ++ translateToFilter(right)).reduceOption(sources.And)

      case expressions.Or(left, right) =>
        for {
          leftFilter <- translateToFilter(left)
          rightFilter <- translateToFilter(right)
        } yield sources.Or(leftFilter, rightFilter)

      case expressions.Not(child) =>
        translateToFilter(child).map(sources.Not)

      case expressions.StartsWith(a: Attribute, TokenLiteral(v)) =>
        Some(sources.StringStartsWith(a.name, v.toString))

      case expressions.EndsWith(a: Attribute, TokenLiteral(v)) =>
        Some(sources.StringEndsWith(a.name, v.toString))

      case expressions.Contains(a: Attribute, TokenLiteral(v)) =>
        Some(sources.StringContains(a.name, v.toString))

      case _ => None
    }
  }
}

/**
 * Taken from Spark's PhysicalOperation with the difference that non-deterministic
 * fields don't cause all columns of underlying table to be projected.
 */
/**
 * A pattern that matches any number of project or filter operations on top of another relational
 * operator.  All filter operators are collected and their conditions are broken up and returned
 * together with the top project operator.
 * [[org.apache.spark.sql.catalyst.expressions.Alias Aliases]] are in-lined/substituted if
 * necessary.
 */
object PhysicalScan extends PredicateHelper {
  type ReturnType = (Seq[NamedExpression], Seq[Expression], LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    val (fields, filters, child, _) = collectProjectsAndFilters(plan)
    Some((fields.getOrElse(child.output), filters, child))
  }

  /**
   * Collects all projects and filters, in-lining/substituting aliases if necessary.
   * Here are two examples for alias in-lining/substitution.
   * Before:
   * {{{
   *   SELECT c1 FROM (SELECT key AS c1 FROM t1) t2 WHERE c1 > 10
   *   SELECT c1 AS c2 FROM (SELECT key AS c1 FROM t1) t2 WHERE c1 > 10
   * }}}
   * After:
   * {{{
   *   SELECT key AS c1 FROM t1 WHERE key > 10
   *   SELECT key AS c2 FROM t1 WHERE key > 10
   * }}}
   */
  private def collectProjectsAndFilters(plan: LogicalPlan): (Option[Seq[NamedExpression]],
      Seq[Expression], LogicalPlan, Map[Attribute, Expression]) =
    plan match {
      case Project(fields, child) =>
        val (_, filters, other, aliases) = collectProjectsAndFilters(child)
        val substitutedFields = fields.map(substitute(aliases)).asInstanceOf[Seq[NamedExpression]]
        (Some(substitutedFields), filters, other, collectAliases(substitutedFields))

      case LFilter(condition, child) =>
        val (fields, filters, other, aliases) = collectProjectsAndFilters(child)
        val substitutedCondition = substitute(aliases)(condition)
        (fields, filters ++ splitConjunctivePredicates(substitutedCondition), other, aliases)

      case BroadcastHint(child) => collectProjectsAndFilters(child)

      case other => (None, Nil, other, Map.empty)
    }

  private def collectAliases(fields: Seq[Expression]): Map[Attribute, Expression] = fields.collect {
    case a@Alias(child, _) => a.toAttribute -> child
  }.toMap

  private def substitute(aliases: Map[Attribute, Expression])(expr: Expression): Expression = {
    expr.transform {
      case a@Alias(ref: AttributeReference, name) =>
        aliases.get(ref)
            .map(Alias(_, name)(a.exprId, a.qualifier, isGenerated = a.isGenerated))
            .getOrElse(a)

      case a: AttributeReference =>
        aliases.get(a)
            .map(Alias(_, a.name)(a.exprId, a.qualifier, isGenerated = a.isGenerated)).getOrElse(a)
    }
  }
}
