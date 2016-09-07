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

package org.apache.spark.sql.internal

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.internal.cache.{AbstractRegion, ColocationHelper, PartitionedRegion, PartitionedRegionHelper}
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.QueryHint

import org.apache.spark.sql.aqp.SnappyContextFunctions
import org.apache.spark.sql.catalyst.{CatalystConf, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, EliminateSubqueryAliases, NoSuchTableException, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.{CatalogRelation, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, BinaryComparison, BinaryOperator, Cast, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.optimizer.{Optimizer, PushPredicateThroughJoin}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Filter, InsertIntoTable, Join, LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.collection.MultiColumnOpenHashSet.ColumnHandler
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.{ExternalStoreUtils, JDBCAppendableRelation}
import org.apache.spark.sql.execution.columnar.impl.{BaseColumnFormatRelation, ColumnFormatRelation, IndexColumnFormatRelation}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.{QueryExecution, SparkOptimizer, SparkPlan, SparkPlanner, datasources}
import org.apache.spark.sql.hive.{QualifiedTableName, SnappyStoreHiveCatalog}
import org.apache.spark.sql.sources.{ParentRelation, _}
import org.apache.spark.sql.{AnalysisException, Column, SnappySession, SnappySqlParser, SnappyStrategies, Strategy}


class SnappySessionState(snappySession: SnappySession)
    extends SessionState(snappySession) {

  self =>

  @transient
  val contextFunctions: SnappyContextFunctions = new SnappyContextFunctions

  protected lazy val sharedState: SnappySharedState =
    snappySession.sharedState.asInstanceOf[SnappySharedState]

  override lazy val sqlParser: SnappySqlParser =
    contextFunctions.newSQLParser(this.snappySession)

  override lazy val analyzer: Analyzer = new Analyzer(catalog, conf) {
    override val extendedResolutionRules =
      new PreprocessTableInsertOrPut(conf) ::
          new FindDataSourceTable(snappySession) ::
          DataSourceAnalysis(conf) ::
          ResolveRelationsExtended ::
          ResolveIndex ::
          (if (conf.runSQLonFile) new ResolveDataSource(snappySession) :: Nil else Nil)

    override val extendedCheckRules = Seq(
      datasources.PreWriteCheck(conf, catalog), PrePutCheck)
  }

  /**
    * Replaces [[UnresolvedRelation]]s with concrete relations from the catalog.
    */
  object ResolveRelationsExtended extends Rule[LogicalPlan] with PredicateHelper {
    def getTable(u: UnresolvedRelation): LogicalPlan = {
      try {
        catalog.lookupRelation(u.tableIdentifier, u.alias)
      } catch {
        case _: NoSuchTableException =>
          u.failAnalysis(s"Table not found: ${u.tableName}")
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case i@PutIntoTable(u: UnresolvedRelation, _) =>
        i.copy(table = EliminateSubqueryAliases(getTable(u)))

    }
  }

  object ResolveIndex extends Rule[LogicalPlan] with PredicateHelper {

    def apply(plan: LogicalPlan): LogicalPlan = {

      val indexHint = QueryHint.WithIndex.toString
      val explicitIndexHint = snappySession.queryHints.collect {
        case (hint, value) if hint.startsWith(indexHint) =>
          (hint.substring(indexHint.length),
              catalog.lookupRelation(
                snappySession.getIndexTable(catalog.newQualifiedTableName(value))))
      }

      val newPlan = plan transformUp {
        case table@LogicalRelation(colRelation: ColumnFormatRelation, _, _)
          if explicitIndexHint.size > 0 => // if any index hint is provided, honor only that much.
          explicitIndexHint.get(colRelation.table) match {
            case Some(index) => applyRefreshTables(table, index).getOrElse(table)
            case _ => table
          }
        case subQuery@SubqueryAlias(alias, child)
          if explicitIndexHint.size > 0 => // if any index hint is provided, honor only that much.
          explicitIndexHint.get(alias) match {
            case Some(index) => applyRefreshTables(subQuery, index).getOrElse(subQuery)
            case _ => subQuery
          }
        case f@Filter(filterCondition, j@Join(left, right, Inner, joinCondition)) =>
          val newF = PushPredicateThroughJoin(f)
          val replacedF = ResolveIndex(newF)
          replacedF

        case j@Join(left, right, Inner, condition) if condition.isDefined =>
          val cols = splitConjunctivePredicates(condition.get)
          val leftIndexes = fetchIndexes2(left)
          val rightIndexes = fetchIndexes2(right)

          val colocatedEntities = leftIndexes.flatMap {
            case leftEntity@LogicalRelation(leftRelation: BaseColumnFormatRelation, _, _) =>
              val colocatedRightEntities = rightIndexes.filter {
                case t@LogicalRelation(rightRelation: BaseColumnFormatRelation, _, _) =>
                  isColocated(leftRelation, rightRelation)
                case _ => false
              }
              Seq.empty[LogicalPlan].zipAll(colocatedRightEntities,
                leftEntity, null.asInstanceOf[LogicalPlan])
            case _ => Seq.empty[(LogicalPlan, LogicalPlan)]
          }

          if (colocatedEntities.isEmpty) {
            logDebug("Nothing is colocated between the tables")
            return j
          }

          val joinRelationPairs = cols.map({
            case org.apache.spark.sql.catalyst.expressions.EqualTo(left, right) =>
              (unwrapReference(left).get, unwrapReference(right).get)
            case org.apache.spark.sql.catalyst.expressions.EqualNullSafe(left, right) =>
              (unwrapReference(left).get, unwrapReference(right).get)
          })

          val satisfyingJoinCond = colocatedEntities.zipWithIndex.map { case ((left, right), i) =>
            val partitioningColPairs = ExtractPartitioningColumns(left).zip(
              ExtractPartitioningColumns(right))

            if (matchColumnSets(partitioningColPairs, joinRelationPairs)) {
              (i, partitioningColPairs.length)
            } else {
              (-1, 0)
            }
          }.filter(v => v._1 >= 0 && v._2 > 0).sortBy(_._2)(Ordering[Int].reverse)

          if (satisfyingJoinCond.isEmpty) {
            logDebug("join condition insufficient for matching any colocation columns ")
            return j
          }

          val (leftReplacement, rightReplacement) =
            colocatedEntities(satisfyingJoinCond(0) match { case (idx, _) => idx })

          val newLeft = applyRefreshTables(left, leftReplacement)
          val newRight = applyRefreshTables(right, rightReplacement)

          val newJoin = {
            if (newLeft.nonEmpty && newRight.nonEmpty) {
              j.copy(left = newLeft.get, right = newRight.get)
            } else if (newLeft.nonEmpty) {
              j.copy(left = newLeft.get)
            } else if (newRight.nonEmpty) {
              j.copy(right = newRight.get)
            } else {
              j
            }
          }

          newJoin
        case f@Filter(cond, child) =>
          val tabToIdxReplacementMap = extractIndexTransformations(cond, child)
          tabToIdxReplacementMap.nonEmpty match {
            case true =>
              replaceTableWithIndex(tabToIdxReplacementMap, f)
            case _ =>
              f
          }
      }

      newPlan transformUp {
        case q: LogicalPlan =>
          q transformExpressionsUp {
            case a: AttributeReference =>
              q.resolveChildren(Seq(a.qualifier.getOrElse(""), a.name),
                analyzer.resolver).getOrElse(a)
          }
      }
    }

    private def replaceTableWithIndex(tabToIdxReplacementMap: Map[String, Option[LogicalPlan]],
        plan: LogicalPlan) = {
      plan transformUp {
        case l@LogicalRelation(b: ColumnFormatRelation, _, _) =>
          tabToIdxReplacementMap.find {
            case (t, Some(index)) =>
              b.table.indexOf(t) >= 0
            case (t, None) => false
          }.flatMap(_._2).getOrElse(l)

        case s@SubqueryAlias(alias, child@LogicalRelation(p: ParentRelation, _, _)) =>
          tabToIdxReplacementMap.withDefaultValue(None)(alias) match {
            case Some(i) => s.copy(child = i)
            case _ => s
          }
      }

    }

    private def extractIndexTransformations(cond: Expression, child: LogicalPlan) = {
      val tableToIndexes = fetchIndexes(child)
      val cols = splitConjunctivePredicates(cond)
      val columnGroups = segregateColsByTables(cols)

      getBestMatchingIndex(columnGroups, tableToIndexes)
    }

    private def isColocated(left: BaseColumnFormatRelation, right: BaseColumnFormatRelation) = {

      val leftRegion = Misc.getRegionForTable(left.resolvedName, true)
      val leftLeader = leftRegion.asInstanceOf[AbstractRegion] match {
        case pr: PartitionedRegion => ColocationHelper.getLeaderRegionName(pr)
      }

      val rightRegion = Misc.getRegionForTable(right.resolvedName, true)
      val rightLeader = rightRegion.asInstanceOf[AbstractRegion] match {
        case pr: PartitionedRegion => ColocationHelper.getLeaderRegionName(pr)
      }

      leftLeader.equals(rightLeader)
    }

    private def ExtractPartitioningColumns(plan: LogicalPlan) = plan match {
      case LogicalRelation(cf: BaseColumnFormatRelation, _, _) => cf.partitionColumns
    }

    private def matchColumnSets(partitioningColPairs: Seq[(String, String)],
        joinRelationPairs: Seq[(AttributeReference, AttributeReference)]): Boolean = {

      if (partitioningColPairs.length != joinRelationPairs.length) {
        return false
      }

      partitioningColPairs.filter({
        case (left, right) =>
          joinRelationPairs.exists {
            case (jleft, jright) =>
              left.equalsIgnoreCase(jleft.name) && right.equalsIgnoreCase(jright.name) ||
                  left.equalsIgnoreCase(jright.name) && right.equalsIgnoreCase(jleft.name)
          }
      }).length == partitioningColPairs.length

    }

    def applyRefreshTables(plan: LogicalPlan, replaceWith: LogicalPlan): Option[LogicalPlan] = {
      if (EliminateSubqueryAliases(plan) == replaceWith) {
        return None
      }

      plan match {
        case LogicalRelation(cf: ColumnFormatRelation, _, _) =>
          Some(replaceTableWithIndex(Map(cf.table -> Some(replaceWith)), plan))
        case SubqueryAlias(alias, _) =>
          Some(replaceTableWithIndex(Map(alias -> Some(replaceWith)), plan))
        case _ => None
      }
    }

    private def fetchIndexes(plan: LogicalPlan): mutable.HashMap[LogicalPlan, Seq[LogicalPlan]] =
      plan.children.foldLeft(mutable.HashMap.empty[LogicalPlan, Seq[LogicalPlan]]) {
        case (m, table) =>
          val newVal: Seq[LogicalPlan] = m.getOrElse(table,
            table match {
              case l@LogicalRelation(p: ParentRelation, _, _) =>
                val catalog = snappySession.sessionCatalog
                p.getDependents(catalog).map(idx =>
                  catalog.lookupRelation(catalog.newQualifiedTableName(idx)))
              case SubqueryAlias(alias, child@LogicalRelation(p: ParentRelation, _, _)) =>
                val catalog = snappySession.sessionCatalog
                p.getDependents(catalog).map(idx =>
                  catalog.lookupRelation(catalog.newQualifiedTableName(idx)))
              case _ => Nil
            }
          )
          // we are capturing same set of indexes for one or more
          // SubqueryAlias. This comes handy while matching columns
          // that is aliased for lets say self join.
          // select x.*, y.* from tab1 x, tab2 y where x.parentID = y.id
          m += (table -> newVal)
      }

    private def fetchIndexes2(plan: LogicalPlan): Seq[LogicalPlan] =
      EliminateSubqueryAliases(plan) match {
        case l@LogicalRelation(p: ParentRelation, _, _) =>
          val catalog = snappySession.sessionCatalog
          Seq(l) ++ p.getDependents(catalog).map(idx =>
            catalog.lookupRelation(catalog.newQualifiedTableName(idx)))
        case _ => Nil
      }

    private def unwrapReference(input: Expression) = input.references.collectFirst {
      case a: AttributeReference => a
    }

    private def segregateColsByTables(cols: Seq[Expression]) = cols.
        foldLeft(mutable.MutableList.empty[AttributeReference]) {
          case (m, col@BinaryComparison(left, _)) =>
            unwrapReference(left).foldLeft(m) {
              case (m, leftA: AttributeReference) =>
                m.find(_.fastEquals(leftA)) match {
                  case None => m += leftA
                  case _ => m
                }
            }
          case (m, _) => m
        }.groupBy(_.qualifier.get)

    private def getBestMatchingIndex(
        columnGroups: Map[String, mutable.MutableList[AttributeReference]],
        tableToIndexes: mutable.HashMap[LogicalPlan, Seq[LogicalPlan]]) = columnGroups.map {
      case (table, colList) =>
        val indexes = tableToIndexes.find { case (tableRelation, _) => tableRelation match {
          case l@LogicalRelation(b: ColumnFormatRelation, _, _) =>
            b.table.indexOf(table) >= 0
          case SubqueryAlias(alias, _) => alias.equals(table)
          case _ => false
        }
        }.map(_._2).getOrElse(Nil)

        val index = indexes.foldLeft(Option.empty[LogicalPlan]) {
          case (max, i) => i match {
            case l@LogicalRelation(idx: IndexColumnFormatRelation, _, _) =>
              idx.partitionColumns.filter(c => colList.exists(_.name.equalsIgnoreCase(c))) match {
                case predicatesMatched
                  if predicatesMatched.length == idx.partitionColumns.length =>
                  val existing = max.getOrElse(l)
                  existing match {
                    case LogicalRelation(eIdx: IndexColumnFormatRelation, _, _)
                      if (idx.partitionColumns.length > eIdx.partitionColumns.length) =>
                      Some(l)
                    case _ => Some(existing)
                  }
                case _ => max
              }
          }
        }

        if (index.isDefined) {
          logInfo(s"Picking up $index for $table")
        }

        (table, index)
    }.filter({ case (_, idx) => idx.isDefined })


  }

  /**
    * Internal catalog for managing table and database states.
    */
  override lazy val catalog = new SnappyStoreHiveCatalog(
    sharedState.externalCatalog,
    snappySession,
    functionResourceLoader,
    functionRegistry,
    conf,
    newHadoopConf())

  override def planner: SparkPlanner = new DefaultPlanner(snappySession, conf,
    experimentalMethods.extraStrategies)

  override def executePlan(plan: LogicalPlan): QueryExecution =
    contextFunctions.executePlan(snappySession, plan)
}

class DefaultPlanner(snappySession: SnappySession, conf: SQLConf,
    extraStrategies: Seq[Strategy])
    extends SparkPlanner(snappySession.sparkContext, conf, extraStrategies)
        with SnappyStrategies {

  val sampleSnappyCase: PartialFunction[LogicalPlan, Seq[SparkPlan]] = {
    case _ => Nil
  }

  // TODO temporary flag till we determine every thing works
  // fine with the optimizations
  val storeOptimization = conf.getConfString(
    "snappy.store.optimization", "true").toBoolean

  val storeOptimizedRules: Seq[Strategy] = if (storeOptimization) {
    Seq(StoreDataSourceStrategy, LocalJoinStrategies)
  }
  else {
    Nil
  }

  override def strategies: Seq[Strategy] =
    Seq(SnappyStrategies,
      StoreStrategy, StreamQueryStrategy) ++
        storeOptimizedRules ++
        super.strategies
}

private[sql] final class PreprocessTableInsertOrPut(conf: SQLConf)
    extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // Check for SchemaInsertableRelation first
    case i@InsertIntoTable(l@LogicalRelation(r: SchemaInsertableRelation,
    _, _), _, child, _, _) if l.resolved && child.resolved =>
      r.insertableRelation(child.output) match {
        case Some(ir) =>
          val relation = LogicalRelation(ir.asInstanceOf[BaseRelation],
            l.expectedOutputAttributes, l.metastoreTableIdentifier)
          castAndRenameChildOutput(i.copy(table = relation),
            relation.output, null, child)
        case None =>
          throw new AnalysisException(s"$l requires that the query in the " +
              "SELECT clause of the INSERT INTO/OVERWRITE statement " +
              "generates the same number of columns as its schema.")
      }

    // Check for PUT
    // Need to eliminate subqueries here. Unlike InsertIntoTable whose
    // subqueries have already been eliminated by special check in
    // ResolveRelations, no such special rule has been added for PUT
    case p@PutIntoTable(table, child) if table.resolved && child.resolved =>
      EliminateSubqueryAliases(table) match {
        case l@LogicalRelation(_: RowInsertableRelation, _, _) =>
          // First, make sure the data to be inserted have the same number of
          // fields with the schema of the relation.
          val expectedOutput = l.output
          if (expectedOutput.size != child.output.size) {
            throw new AnalysisException(s"$l requires that the query in the " +
                "SELECT clause of the PUT INTO statement " +
                "generates the same number of columns as its schema.")
          }
          castAndRenameChildOutput(p, expectedOutput, l, child)

        case _ => p
      }

    // other cases handled like in PreprocessTableInsertion
    case i@InsertIntoTable(table, partition, child, _, _)
      if table.resolved && child.resolved => table match {
      case relation: CatalogRelation =>
        val metadata = relation.catalogTable
        preprocess(i, metadata.identifier.quotedString,
          metadata.partitionColumnNames)
      case LogicalRelation(h: HadoopFsRelation, _, identifier) =>
        val tblName = identifier.map(_.quotedString).getOrElse("unknown")
        preprocess(i, tblName, h.partitionSchema.map(_.name))
      case LogicalRelation(_: InsertableRelation, _, identifier) =>
        val tblName = identifier.map(_.quotedString).getOrElse("unknown")
        preprocess(i, tblName, Nil)
      case other => i
    }
  }

  private def preprocess(
      insert: InsertIntoTable,
      tblName: String,
      partColNames: Seq[String]): InsertIntoTable = {

    val expectedColumns = insert.expectedColumns
    val child = insert.child
    if (expectedColumns.isDefined && expectedColumns.get.length != child.schema.length) {
      throw new AnalysisException(
        s"Cannot insert into table $tblName because the number of columns are different: " +
            s"need ${expectedColumns.get.length} columns, " +
            s"but query has ${child.schema.length} columns.")
    }

    if (insert.partition.nonEmpty) {
      // the query's partitioning must match the table's partitioning
      // this is set for queries like: insert into ... partition (one = "a", two = <expr>)
      val samePartitionColumns =
      if (conf.caseSensitiveAnalysis) {
        insert.partition.keySet == partColNames.toSet
      } else {
        insert.partition.keySet.map(_.toLowerCase) == partColNames.map(_.toLowerCase).toSet
      }
      if (!samePartitionColumns) {
        throw new AnalysisException(
          s"""
             |Requested partitioning does not match the table $tblName:
             |Requested partitions: ${insert.partition.keys.mkString(",")}
             |Table partitions: ${partColNames.mkString(",")}
           """.stripMargin)
      }
      expectedColumns.map(castAndRenameChildOutput(insert, _, null, child))
          .getOrElse(insert)
    } else {
      // All partition columns are dynamic because because the InsertIntoTable
      // command does not explicitly specify partitioning columns.
      expectedColumns.map(castAndRenameChildOutput(insert, _, null, child))
          .getOrElse(insert).copy(partition = partColNames.map(_ -> None).toMap)
    }
  }

  /**
    * If necessary, cast data types and rename fields to the expected
    * types and names.
    */
  // TODO: do we really need to rename?
  def castAndRenameChildOutput[T <: LogicalPlan](
      plan: T,
      expectedOutput: Seq[Attribute],
      newRelation: LogicalRelation,
      child: LogicalPlan): T = {
    val newChildOutput = expectedOutput.zip(child.output).map {
      case (expected, actual) =>
        if (expected.dataType.sameType(actual.dataType) &&
            expected.name == actual.name) {
          actual
        } else {
          Alias(Cast(actual, expected.dataType), expected.name)()
        }
    }

    if (newChildOutput == child.output) {
      plan match {
        case p: PutIntoTable => p.copy(table = newRelation).asInstanceOf[T]
        case i: InsertIntoTable => plan
      }
    } else plan match {
      case p: PutIntoTable => p.copy(table = newRelation,
        child = Project(newChildOutput, child)).asInstanceOf[T]
      case i: InsertIntoTable => i.copy(child = Project(newChildOutput,
        child)).asInstanceOf[T]
    }
  }
}

private[sql] case object PrePutCheck extends (LogicalPlan => Unit) {

  def apply(plan: LogicalPlan): Unit = {
    plan.foreach {
      case PutIntoTable(LogicalRelation(t: RowPutRelation, _, _), query) =>
        // Get all input data source relations of the query.
        val srcRelations = query.collect {
          case LogicalRelation(src: BaseRelation, _, _) => src
        }
        if (srcRelations.contains(t)) {
          throw Utils.analysisException(
            "Cannot put into table that is also being read from.")
        } else {
          // OK
        }

      case PutIntoTable(table, query) =>
        throw Utils.analysisException(s"$table does not allow puts.")

      case _ => // OK
    }
  }
}
