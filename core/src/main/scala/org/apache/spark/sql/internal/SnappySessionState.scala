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

package org.apache.spark.sql.internal

import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ArrayBuffer
import scala.reflect.{ClassTag, classTag}

import com.gemstone.gemfire.internal.cache.{CacheDistributionAdvisee, ColocationHelper, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore
import com.pivotal.gemfirexd.internal.impl.jdbc.Util
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import io.snappydata.Property.HashAggregateSize
import io.snappydata.sql.catalog.{CatalogObjectType, SnappyExternalCatalog}
import io.snappydata.{Constant, Property}

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry, TypedConfigBuilder}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.analysis.TypeCoercion.PromoteStrings
import org.apache.spark.sql.catalyst.analysis.{Analyzer, EliminateSubqueryAliases, NoSuchTableException, Star, UnresolvedAttribute, UnresolvedOrdinal, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, In, ScalarSubquery, _}
import org.apache.spark.sql.catalyst.optimizer.{Optimizer, ReorderJoin}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Filter => LogicalFilter, _}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.columnar.impl.IndexColumnFormatRelation
import org.apache.spark.sql.execution.command.{DDLUtils, RunnableCommand}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReuseExchange}
import org.apache.spark.sql.execution.sources.{PhysicalScan, StoreDataSourceStrategy}
import org.apache.spark.sql.internal.SQLConf.SQLConfigBuilder
import org.apache.spark.sql.policy.PolicyProperties
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.streaming.{LogicalDStreamPlan, StreamingQueryManager, WindowLogicalPlan}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.Duration
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{Partition, SparkConf}


class SnappySessionState(val snappySession: SnappySession)
    extends SessionState(snappySession) with SnappyStrategies {

  @transient
  val contextFunctions: SnappyContextFunctions = new SnappyContextFunctions

  val sampleSnappyCase: PartialFunction[LogicalPlan, Seq[SparkPlan]] = {
    case MarkerForCreateTableAsSelect(child) => PlanLater(child) :: Nil
    case BypassRowLevelSecurity(child) => PlanLater(child) :: Nil
    case _ => Nil
  }

  protected lazy val snappySharedState: SnappySharedState = snappySession.sharedState

  override lazy val streamingQueryManager: StreamingQueryManager = {
    // Disabling `SnappyAggregateStrategy` for streaming queries as it clashes with
    // `StatefulAggregationStrategy` which is applied by spark for streaming queries. This
    // implies that Snappydata aggregation optimisation will be turned off for any usage of
    // this session including non-streaming queries.

    HashAggregateSize.set(snappySession.sessionState.conf, "-1")
    new StreamingQueryManager(snappySession)
  }

  override lazy val sqlParser: SnappySqlParser =
    contextFunctions.newSQLParser(this.snappySession)

  private[sql] var disableStoreOptimizations: Boolean = false

  // Only Avoid rule PromoteStrings that remove ParamLiteral for its type being NullType
  // Rest all rules, even if redundant, are same as analyzer for maintainability reason
  lazy val analyzerPrepare: Analyzer = new Analyzer(catalog, conf) {

    def getStrategy(strategy: analyzer.Strategy): Strategy = strategy match {
      case analyzer.FixedPoint(_) => fixedPoint
      case _ => Once
    }

    override lazy val batches: Seq[Batch] = analyzer.batches.map {
      case batch if batch.name.equalsIgnoreCase("Resolution") =>
        Batch(batch.name, getStrategy(batch.strategy), batch.rules.filter(_ match {
          case PromoteStrings => if (sqlParser.sqlParser.questionMarkCounter > 0) {
            false
          } else {
            true
          }
          case _ => true
        }): _*)
      case batch => Batch(batch.name, getStrategy(batch.strategy), batch.rules: _*)
    }

    override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
      getExtendedResolutionRules(this)

    override val extendedCheckRules: Seq[LogicalPlan => Unit] = getExtendedCheckRules
  }

  def getExtendedResolutionRules(analyzer: Analyzer): Seq[Rule[LogicalPlan]] =
    AnalyzeCreateTable(snappySession) ::
        new PreprocessTable(this) ::
        ResolveRelationsExtended ::
        ResolveAliasInGroupBy ::
        new FindDataSourceTable(snappySession) ::
        DataSourceAnalysis(conf) ::
        AnalyzeMutableOperations(snappySession, analyzer) ::
        ResolveQueryHints(snappySession) ::
        RowLevelSecurity ::
        ExternalRelationLimitFetch ::
        (if (conf.runSQLonFile) new ResolveDataSource(snappySession) ::
            Nil else Nil)


  def getExtendedCheckRules: Seq[LogicalPlan => Unit] = {
    Seq(ConditionalPreWriteCheck(datasources.PreWriteCheck(conf, catalog)), PrePutCheck)
  }

  override lazy val analyzer: Analyzer = new Analyzer(catalog, conf) {

    override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
      getExtendedResolutionRules(this)

    override val extendedCheckRules: Seq[LogicalPlan => Unit] = getExtendedCheckRules
  }

  override lazy val optimizer: Optimizer = new SparkOptimizer(catalog, conf, experimentalMethods) {
    override def batches: Seq[Batch] = {
      implicit val ss: SnappySession = snappySession
      var insertedSnappyOpts = 0
      val modified = super.batches.map {
        case batch if batch.name.equalsIgnoreCase("Operator Optimizations") =>
          insertedSnappyOpts += 1
          val (left, right) = batch.rules.splitAt(batch.rules.indexOf(ReorderJoin))
          Batch(batch.name, batch.strategy, (left :+ ResolveIndex()) ++ right: _*)
        case b => b
      }

      if (insertedSnappyOpts != 1) {
        throw new AnalysisException("Snappy Optimizations not applied")
      }

      modified :+
          Batch("Streaming SQL Optimizers", Once, PushDownWindowLogicalPlan) :+
          Batch("Link buckets to RDD partitions", Once, new LinkPartitionsToBuckets) :+
          Batch("TokenizedLiteral Folding Optimization", Once, TokenizedLiteralFolding) :+
          Batch("Order join conditions ", Once, OrderJoinConditions)
    }
  }


  // copy of ConstantFolding that will turn a constant up/down cast into
  // a static value.
  object TokenizedLiteralFolding extends Rule[LogicalPlan] {

    def apply(plan: LogicalPlan): LogicalPlan = {
      val foldedLiterals = new ArrayBuffer[TokenizedLiteral](4)
      // TokenizedLiterals already marked as folded and must be reverted to that state
      val preFoldedLiterals = new ArrayBuffer[TokenizedLiteral](2)

      /**
       * Temporarily mark tokens as foldable to enable constant folding.
       * Uses transform instead of foreach for more comprehensive iteration through
       * entire expression tree using product iterator rather than only children.
       */
      def mark(e: Expression, foldable: Boolean = true): Expression = e transform {
        case p: TokenizedLiteral =>
          if (!foldable) {
            if (p.foldable) p.markFoldable(false)
          } else if (p.foldable) {
            if (!foldedLiterals.contains(p)) preFoldedLiterals += p
          } else {
            p.markFoldable(true)
            foldedLiterals += p
          }
          p
        // also mark linking for scalar/predicate subqueries and disable plan caching
        case s@(_: ScalarSubquery | _: PredicateSubquery) if foldable =>
          snappySession.linkPartitionsToBuckets(flag = true)
          snappySession.planCaching = false
          s
      }

      def unmarkAll(e: Expression): Expression = {
        // faster to iterate through collected literals rather than using transform again
        if (foldedLiterals.nonEmpty) {
          foldedLiterals.foreach(_.markFoldable(false))
          foldedLiterals.clear()
        }
        if (preFoldedLiterals.nonEmpty) {
          preFoldedLiterals.foreach(_.markFoldable(true))
          preFoldedLiterals.clear()
        }
        e
      }

      def foldExpression(e: Expression): DynamicFoldableExpression = {
        // lets mark child params foldable false so that nested expression doesn't
        // attempt to wrap
        DynamicFoldableExpression(mark(e, foldable = false))
      }

      plan transform {
        // transformDown for expression so that top-most node which is foldable gets
        // selected for wrapping by DynamicFoldableExpression and further sub-expressions
        // do not since foldExpression will reset inner ParamLiterals as non-foldable
        case q: LogicalPlan => q.mapExpressions(expr => unmarkAll(mark(expr).transformDown {
          // ignore leaf literals
          case l@(_: Literal | _: DynamicReplacableConstant) => l
          // Wrap expressions that are foldable.
          case e if e.foldable => foldExpression(e)
          // Like Spark's OptimizeIn but uses DynamicInSet to allow for tokenized literals
          // to be optimized too.
          case expr@In(v, l) if !disableStoreOptimizations =>
            val list = l.collect {
              case e@(_: Literal | _: DynamicReplacableConstant) => e
              case e if e.foldable => foldExpression(e)
            }
            if (list.length == l.length) {
              val newList = ExpressionSet(list).toVector
              // hash sets are faster that linear search for more than a couple of entries
              // for non-primitive types while keeping limit as default 10 for primitives
              val threshold = v.dataType match {
                case _: DecimalType => "2"
                case _: NumericType => "10"
                case _ => "2"
              }
              if (newList.size > conf.getConfString(
                SQLConf.OPTIMIZER_INSET_CONVERSION_THRESHOLD.key, threshold).toInt) {
                DynamicInSet(v, newList)
              } else if (newList.size < list.size) {
                expr.copy(list = newList)
              } else {
                // newList.length == list.length
                expr
              }
            } else expr
        }))
      }
    }
  }

  object PushDownWindowLogicalPlan extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = {
      var duration: Duration = null
      var slide: Option[Duration] = None
      var transformed: Boolean = false
      plan transformDown {
        case win@WindowLogicalPlan(d, s, child, false) =>
          child match {
            case LogicalRelation(_, _, _) |
                 LogicalDStreamPlan(_, _) => win
            case _ => duration = d
              slide = s
              transformed = true
              win.child
          }
        case c@(LogicalRelation(_, _, _) |
                LogicalDStreamPlan(_, _)) =>
          if (transformed) {
            transformed = false
            WindowLogicalPlan(duration, slide, c, transformed = true)
          } else c
      }
    }
  }

  /**
   * This rule sets the flag at query level to link the partitions to
   * be created for tables to be the same as number of buckets. This will avoid
   * exchange on one side of a non-collocated join in many cases.
   */
  final class LinkPartitionsToBuckets extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = {
      plan.foreach {
        case _ if Property.ForceLinkPartitionsToBuckets.get(conf) =>
          // always create one partition per bucket
          snappySession.linkPartitionsToBuckets(flag = true)
        case j: Join if !JoinStrategy.isReplicatedJoin(j) =>
          // disable for the entire query for consistency
          snappySession.linkPartitionsToBuckets(flag = true)
        case _: InsertIntoTable | _: TableMutationPlan |
             LogicalRelation(_: IndexColumnFormatRelation, _, _) =>
          // disable for inserts/puts to avoid exchanges and indexes to work correctly
          snappySession.linkPartitionsToBuckets(flag = true)
        case _ => // nothing for others
      }
      plan
    }
  }

  override lazy val conf: SnappyConf = new SnappyConf(snappySession)

  /**
   * The partition mapping selected for the lead partitioned region in
   * a collocated chain for current execution
   */
  private[spark] val leaderPartitions = new ConcurrentHashMap[PartitionedRegion,
      Array[Partition]](16, 0.7f, 1)

  @volatile private[sql] var enableExecutionCache: Boolean = _
  protected final lazy val executionCache =
    new ConcurrentHashMap[LogicalPlan, QueryExecution](4, 0.7f, 1)

  /**
   * Replaces [[UnresolvedRelation]]s with concrete relations from the catalog.
   */
  object ResolveRelationsExtended extends Rule[LogicalPlan] with PredicateHelper {
    def getTable(u: UnresolvedRelation): LogicalPlan = {
      try {
        catalog.lookupRelation(u.tableIdentifier, u.alias)
      } catch {
        case _: NoSuchTableException =>
          u.failAnalysis(s"Table not found: ${u.tableIdentifier.unquotedString}")
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case i@PutIntoTable(u: UnresolvedRelation, _) =>
        i.copy(table = EliminateSubqueryAliases(getTable(u)))
      case d@DMLExternalTable(_, u: UnresolvedRelation, _) =>
        d.copy(query = EliminateSubqueryAliases(getTable(u)))
    }
  }

  /**
   * Orders the join keys as per the  underlying partitioning keys ordering of the table.
   */
  object OrderJoinConditions extends Rule[LogicalPlan] with JoinQueryPlanning {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, otherCondition, left, right) =>
        prepareOrderedCondition(joinType, left, right, leftKeys, rightKeys, otherCondition)
    }

    def getPartCols(plan: LogicalPlan): Seq[NamedExpression] = {
      plan match {
        case PhysicalScan(_, _, child) => child match {
          case r@LogicalRelation(scan: PartitionedDataSourceScan, _, _) =>
            // send back numPartitions=1 for replicated table since collocated
            if (!scan.isPartitioned) return Nil
            val partCols = scan.partitionColumns.map(colName =>
              r.resolveQuoted(colName, analysis.caseInsensitiveResolution)
                  .getOrElse(throw new AnalysisException(
                    s"""Cannot resolve column "$colName" among (${r.output})""")))
            partCols
          case _ => Nil
        }
        case _ => Nil
      }
    }

    private def orderJoinKeys(left: LogicalPlan,
        right: LogicalPlan,
        leftKeys: Seq[Expression],
        rightKeys: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
      val leftPartCols = getPartCols(left)
      val rightPartCols = getPartCols(right)
      if (leftPartCols ne Nil) {
        val (keyOrder, allPartPresent) = getKeyOrder(left, leftKeys, leftPartCols)
        if (allPartPresent) {
          val leftOrderedKeys = keyOrder.zip(leftKeys).sortWith(_._1 < _._1).unzip._2
          val rightOrderedKeys = keyOrder.zip(rightKeys).sortWith(_._1 < _._1).unzip._2
          (leftOrderedKeys, rightOrderedKeys)
        } else {
          (leftKeys, rightKeys)
        }
      } else if (rightPartCols ne Nil) {
        val (keyOrder, allPartPresent) = getKeyOrder(right, rightKeys, rightPartCols)
        if (allPartPresent) {
          val leftOrderedKeys = keyOrder.zip(leftKeys).sortWith(_._1 < _._1).unzip._2
          val rightOrderedKeys = keyOrder.zip(rightKeys).sortWith(_._1 < _._1).unzip._2
          (leftOrderedKeys, rightOrderedKeys)
        } else {
          (leftKeys, rightKeys)
        }
      } else {
        (leftKeys, rightKeys)
      }
    }

    private def prepareOrderedCondition(joinType: JoinType,
        left: LogicalPlan,
        right: LogicalPlan,
        leftKeys: Seq[Expression],
        rightKeys: Seq[Expression],
        otherCondition: Option[Expression]): LogicalPlan = {
      val (leftOrderedKeys, rightOrderedKeys) = orderJoinKeys(left, right, leftKeys, rightKeys)
      val joinPairs = leftOrderedKeys.zip(rightOrderedKeys)
      val newJoin = joinPairs.map(EqualTo.tupled).reduceOption(And)
      val allConditions = (newJoin ++ otherCondition).reduceOption(And)
      Join(left, right, joinType, allConditions)
    }
  }

  object ResolveAliasInGroupBy extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case p if !p.childrenResolved => p
      case Aggregate(groups, aggs, child) if aggs.forall(_.resolved) &&
        groups.exists(_.isInstanceOf[UnresolvedAttribute]) =>
        val newGroups = groups.map {
          case u@UnresolvedAttribute(nameParts) if nameParts.length == 1 =>
            aggs.collectFirst {
              case Alias(exp, name) if name.equalsIgnoreCase(nameParts.head) =>
                exp
            }.getOrElse(u.failAnalysis("unresolved group by alias"))
        }
        Aggregate(newGroups, aggs, child)
      case o => o
    }
  }

  object RowLevelSecurity extends Rule[LogicalPlan] {
    // Y combinator
    val conditionEvaluator: (Expression => Boolean) => (Expression => Boolean) =
      (f: (Expression => Boolean)) =>
        (exp: Expression) => exp.eq(PolicyProperties.rlsAppliedCondition) ||
            (exp match {
              case And(left, _) => f(left)
              case EqualTo(l: Literal, r: Literal) =>
                l.value == r.value && l.value == PolicyProperties.rlsConditionStringUtf8
              case _ => false
            })


    def rlsConditionChecker(f: (Expression => Boolean) => (Expression => Boolean)):
    Expression => Boolean = f(rlsConditionChecker(f))(_: Expression)

    def apply(plan: LogicalPlan): LogicalPlan = {
      val memStore = GemFireStore.getBootingInstance
      if ((memStore eq null) || !memStore.isRLSEnabled) return plan

      plan match {
        case _: BypassRowLevelSecurity | _: Update | _: Delete |
             _: DeleteFromTable | _: PutIntoTable => plan

        // TODO: Asif: Bypass row level security filter apply if the command
        // is of type RunnableCommad. Later if it turns out any data operation
        // is happening via this command we need to handle it
        case _: RunnableCommand => plan
        case _ if !alreadyPolicyApplied(plan) => plan.transformUp {
          case lr@LogicalRelation(rlsRelation: RowLevelSecurityRelation, _, _) =>
            val policyFilter = snappySession.sessionState.catalog.
                getCombinedPolicyFilterForNativeTable(rlsRelation, Some(lr))
            policyFilter match {
              case Some(filter) => filter.copy(child = lr)
              case None => lr
            }

          case SubqueryAlias(name, LogicalFilter(condition, child), ti) => LogicalFilter(condition,
            SubqueryAlias(name, child, ti))

          case LogicalFilter(condition1, LogicalFilter(condition2, child)) =>
            if (rlsConditionChecker(conditionEvaluator)(condition1)) {
              if (rlsConditionChecker(conditionEvaluator)(condition2)) {
                LogicalFilter(condition1, child)
              } else {
                LogicalFilter(And(condition1, condition2), child)
              }
            } else {
              LogicalFilter(And(condition2, condition1), child)
            }
        }
        case _ => plan
      }
    }

    def alreadyPolicyApplied(plan: LogicalPlan): Boolean = {
      plan.collectFirst {
        case f: LogicalFilter => f
      }.exists(f => rlsConditionChecker(conditionEvaluator)(f.condition))
    }
  }

  object ExternalRelationLimitFetch extends Rule[LogicalPlan] {
    private val indexes = (0, 1, 2, 3, 4, 5)
    private val (create_tv_bool, filter_bool, agg_func_bool, extRelation_bool, allProjectionBool,
    alreadyProcessed_bool) = indexes

    def apply(plan: LogicalPlan): LogicalPlan = {
      val limit = limitExternalDataFetch(plan)
      if (limit > 0) {
        Limit(Literal(limit), plan)
      } else {
        plan
      }
    }

    def limitExternalDataFetch(plan: LogicalPlan): Int = {
      // if plan is pure select with or without limit , has GemFireRelation,
      // no Filter , no GroupBy, no Aggregate then apply rule and is not a CreateTable
      // or a CreateView
      // TODO: Deal with View

      val boolsArray = Array.ofDim[Boolean](indexes.productArity)
      // by default assume all projections are fetched
      boolsArray(allProjectionBool) = true
      var externalRelation: ApplyLimitOnExternalRelation = null
      plan.foreachUp {
        {
          case LogicalRelation(baseRelation: ApplyLimitOnExternalRelation, _, _) =>
            boolsArray(extRelation_bool) = true
            externalRelation = baseRelation

          case _: MarkerForCreateTableAsSelect => boolsArray(create_tv_bool) = true
          case _: Aggregate => boolsArray(agg_func_bool) = true
          case Project(projs, _) => if (!(boolsArray(extRelation_bool) &&
              ((projs.length == externalRelation.asInstanceOf[BaseRelation].schema.length &&
                  projs.zip(externalRelation.asInstanceOf[BaseRelation].schema).forall {
                    case (ne, sf) => ne.name.equalsIgnoreCase(sf.name)
                  })
                  || (projs.length == 1 && projs.head.isInstanceOf[Star])))) {
            boolsArray(allProjectionBool) = false
          }
          case _: GlobalLimit | _: LocalLimit => boolsArray(alreadyProcessed_bool) = true
          case _: org.apache.spark.sql.catalyst.plans.logical.Filter =>
            boolsArray(filter_bool) = true
          case _ =>
        }
      }

      if (boolsArray(extRelation_bool) && boolsArray(allProjectionBool) &&
          !(boolsArray(create_tv_bool) || boolsArray(filter_bool) ||
              boolsArray(agg_func_bool) || boolsArray(alreadyProcessed_bool))) {
        externalRelation.getLimit
      } else {
        -1
      }

    }
  }

  case class AnalyzeMutableOperations(sparkSession: SparkSession,
      analyzer: Analyzer) extends Rule[LogicalPlan] with PredicateHelper {

    private def getKeyAttributes(table: LogicalPlan, child: LogicalPlan,
        plan: LogicalPlan): (Seq[NamedExpression], LogicalPlan, LogicalRelation) = {
      var tableName = ""
      val keyColumns = table.collectFirst {
        case lr@LogicalRelation(mutable: MutableRelation, _, _) =>
          val ks = mutable.getKeyColumns
          if (ks.isEmpty) {
            val currentKey = snappySession.currentKey
            // if this is a row table, then fallback to direct execution
            mutable match {
              case _: UpdatableRelation if currentKey ne null =>
                return (Nil, DMLExternalTable(snappySession.tableIdentifier(
                  mutable.table), lr, currentKey.sqlText), lr)
              case _ =>
                throw new AnalysisException(
                  s"Empty key columns for update/delete on $mutable")
            }
          }
          tableName = mutable.table
          ks
      }.getOrElse(throw new AnalysisException(
        s"Update/Delete requires a MutableRelation but got $table"))
      // resolve key columns right away
      var mutablePlan: Option[LogicalRelation] = None
      val newChild = child.transformDown {
        case lr@LogicalRelation(mutable: MutableRelation, _, _)
          if mutable.table.equalsIgnoreCase(tableName) =>
          mutablePlan = Some(mutable.withKeyColumns(lr, keyColumns))
          mutablePlan.get
      }

      mutablePlan match {
        case Some(sourcePlan) =>
          val keyAttrs = keyColumns.map { name =>
            analysis.withPosition(sourcePlan) {
              sourcePlan.resolve(
                name.split('.'), analyzer.resolver).getOrElse(
                throw new AnalysisException(s"Could not resolve key column $name"))
            }
          }
          (keyAttrs, newChild, sourcePlan)
        case _ => throw new AnalysisException(
          s"Could not find any scan from the table '$tableName' to be updated in $plan")
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case c: DMLExternalTable if !c.query.resolved =>
        c.copy(query = analyzeQuery(c.query))

      case u@Update(table, child, keyColumns, updateCols, updateExprs)
        if keyColumns.isEmpty && u.resolved && child.resolved =>
        // add the key columns to the plan
        val (keyAttrs, newChild, relation) = getKeyAttributes(table, child, u)
        // if this is a row table with no PK, then fallback to direct execution
        if (keyAttrs.isEmpty) newChild
        else {
          // check that partitioning or key columns should not be updated
          val nonUpdatableColumns = (relation.relation.asInstanceOf[MutableRelation]
              .partitionColumns.map(Utils.toUpperCase) ++
              keyAttrs.map(k => Utils.toUpperCase(k.name))).toSet
          // resolve the columns being updated and cast the expressions if required
          val (updateAttrs, newUpdateExprs) = updateCols.zip(updateExprs).map { case (c, expr) =>
            val attr = analysis.withPosition(relation) {
              relation.resolve(
                c.name.split('.'), analyzer.resolver).getOrElse(
                throw new AnalysisException(s"Could not resolve update column ${c.name}"))
            }
            val colName = Utils.toUpperCase(c.name)
            if (nonUpdatableColumns.contains(colName)) {
              throw new AnalysisException("Cannot update partitioning/key column " +
                  s"of the table for $colName (among [${nonUpdatableColumns.mkString(", ")}])")
            }
            // cast the update expressions if required
            val newExpr = if (attr.dataType.sameType(expr.dataType)) {
              expr
            } else {
              // avoid unnecessary copy+cast when inserting DECIMAL types
              // into column table
              expr.dataType match {
                case _: DecimalType
                  if attr.dataType.isInstanceOf[DecimalType] => expr
                case _ => Alias(Cast(expr, attr.dataType), attr.name)()
              }
            }
            (attr, newExpr)
          }.unzip
          // collect all references and project on them to explicitly eliminate
          // any extra columns
          val allReferences = newChild.references ++ AttributeSet(updateAttrs) ++
              AttributeSet(newUpdateExprs.flatMap(_.references)) ++ AttributeSet(keyAttrs)
          u.copy(child = Project(newChild.output.filter(allReferences.contains), newChild),
            keyColumns = keyAttrs.map(_.toAttribute),
            updateColumns = updateAttrs.map(_.toAttribute), updateExpressions = newUpdateExprs)
        }

      case d@Delete(table, child, keyColumns) if keyColumns.isEmpty && child.resolved =>
        // add and project only the key columns
        val (keyAttrs, newChild, _) = getKeyAttributes(table, child, d)
        // if this is a row table with no PK, then fallback to direct execution
        if (keyAttrs.isEmpty) newChild
        else {
          d.copy(child = Project(keyAttrs, newChild),
            keyColumns = keyAttrs.map(_.toAttribute))
        }
      case d@DeleteFromTable(table, child) if table.resolved && child.resolved =>
        ColumnTableBulkOps.transformDeletePlan(sparkSession, d)
      case p@PutIntoTable(table, child) if table.resolved && child.resolved =>
        ColumnTableBulkOps.transformPutPlan(sparkSession, p)
    }

    private def analyzeQuery(query: LogicalPlan): LogicalPlan = {
      val qe = sparkSession.sessionState.executePlan(query)
      qe.assertAnalyzed()
      qe.analyzed
    }
  }

  /**
   * Internal catalog for managing table and database states.
   */
  override lazy val catalog: SnappySessionCatalog = {
    new SnappySessionCatalog(
      snappySharedState.getExternalCatalogInstance(snappySession),
      snappySession,
      snappySession.sharedState.globalTempViewManager,
      functionResourceLoader,
      functionRegistry,
      conf,
      newHadoopConf())
  }

  protected[sql] def queryPreparations(
      topLevel: Boolean): Seq[Rule[SparkPlan]] = Seq[Rule[SparkPlan]](
    python.ExtractPythonUDFs,
    TokenizeSubqueries(snappySession),
    EnsureRequirements(snappySession.sessionState.conf),
    OptimizeSortPlans,
    CollapseCollocatedPlans(snappySession),
    CollapseCodegenStages(snappySession.sessionState.conf),
    InsertCachedPlanFallback(snappySession, topLevel),
    ReuseExchange(snappySession.sessionState.conf))

  protected def newQueryExecution(plan: LogicalPlan): QueryExecution = {
    new QueryExecution(snappySession, plan) {

      override protected def preparations: Seq[Rule[SparkPlan]] = {
        snappySession.addContextObject(SnappySession.ExecutionKey,
          () => newQueryExecution(plan))
        queryPreparations(topLevel = true)
      }
    }
  }

  override final def executePlan(plan: LogicalPlan): QueryExecution = {
    initSnappyStrategies
    clearExecutionData()
    beforeExecutePlan(plan)
    val qe = newQueryExecution(plan)
    if (enableExecutionCache) executionCache.put(plan, qe)
    qe
  }

  private lazy val initSnappyStrategies: Unit = {
    val storeOptimizedRules: Seq[Strategy] =
      Seq(StoreDataSourceStrategy, SnappyAggregation, HashJoinStrategies)

    experimentalMethods.extraStrategies = experimentalMethods.extraStrategies ++
        Seq(SnappyStrategies, StoreStrategy, StreamQueryStrategy) ++ storeOptimizedRules
  }

  protected def beforeExecutePlan(plan: LogicalPlan): Unit = {
  }

  private[sql] def getExecution(plan: LogicalPlan): QueryExecution = executionCache.get(plan)

  private[sql] def clearExecutionCache(): Unit = executionCache.clear()

  private[spark] def prepareExecution(plan: SparkPlan): SparkPlan = {
    queryPreparations(topLevel = false).foldLeft(plan) {
      case (sp, rule) => rule.apply(sp)
    }
  }

  private[spark] def clearExecutionData(): Unit = {
    conf.refreshNumShufflePartitions()
    leaderPartitions.clear()
    snappySession.clearContext()
  }

  def getTablePartitions(region: PartitionedRegion): Array[Partition] = {
    val leaderRegion = ColocationHelper.getLeaderRegion(region)
    leaderPartitions.computeIfAbsent(leaderRegion,
      new java.util.function.Function[PartitionedRegion, Array[Partition]] {
        override def apply(pr: PartitionedRegion): Array[Partition] = {
          val linkPartitionsToBuckets = snappySession.hasLinkPartitionsToBuckets
          val preferPrimaries = snappySession.preferPrimaries
          if (linkPartitionsToBuckets || preferPrimaries) {
            // also set the default shuffle partitions for this execution
            // to minimize exchange
            snappySession.sessionState.conf.setExecutionShufflePartitions(
              region.getTotalNumberOfBuckets)
          }
          StoreUtils.getPartitionsPartitionedTable(snappySession, pr,
            linkPartitionsToBuckets, preferPrimaries)
        }
      })
  }

  def getTablePartitions(region: CacheDistributionAdvisee): Array[Partition] =
    StoreUtils.getPartitionsReplicatedTable(snappySession, region)
}

class SnappyConf(@transient val session: SnappySession)
    extends SQLConf with Serializable {

  /** Pool to be used for the execution of queries from this session */
  @volatile private[this] var schedulerPool: String = Property.SchedulerPool.defaultValue.get

  /** If shuffle partitions is set by [[setExecutionShufflePartitions]]. */
  @volatile private[this] var executionShufflePartitions: Int = _

  /**
   * Records the number of shuffle partitions to be used determined on runtime
   * from available cores on the system. A value <= 0 indicates that it was set
   * explicitly by user and should not use a dynamic value.
   */
  @volatile private[this] var dynamicShufflePartitions: Int = _

  SQLConf.SHUFFLE_PARTITIONS.defaultValue match {
    case Some(d) if (session ne null) && super.numShufflePartitions == d =>
      dynamicShufflePartitions = coreCountForShuffle
    case None if session ne null =>
      dynamicShufflePartitions = coreCountForShuffle
    case _ =>
      executionShufflePartitions = -1
      dynamicShufflePartitions = -1
  }

  private def coreCountForShuffle: Int = {
    val count = SnappyContext.totalCoreCount.get()
    if (count > 0 || (session eq null)) math.min(super.numShufflePartitions, count)
    else math.min(super.numShufflePartitions, session.sparkContext.defaultParallelism)
  }

  private def keyUpdateActions(key: String, value: Option[Any], doSet: Boolean): Unit = key match {
    // clear plan cache when some size related key that effects plans changes
    case SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key |
         Property.HashJoinSize.name |
         Property.HashAggregateSize.name |
         Property.ForceLinkPartitionsToBuckets.name => session.clearPlanCache()
    case SQLConf.SHUFFLE_PARTITIONS.key =>
      // stop dynamic determination of shuffle partitions
      if (doSet) {
        executionShufflePartitions = -1
        dynamicShufflePartitions = -1
      } else {
        dynamicShufflePartitions = coreCountForShuffle
      }
      session.clearPlanCache()
    case Property.SchedulerPool.name =>
      schedulerPool = value match {
        case None => Property.SchedulerPool.defaultValue.get
        case Some(pool: String) if session.sparkContext.getPoolForName(pool).isDefined => pool
        case Some(pool) => throw new IllegalArgumentException(s"Invalid Pool $pool")
      }

    case Property.PartitionPruning.name => value match {
      case Some(b) => session.partitionPruning = b.toString.toBoolean
      case None => session.partitionPruning = Property.PartitionPruning.defaultValue.get
    }
      session.clearPlanCache()

    case Property.PlanCaching.name =>
      value match {
        case Some(boolVal) =>
          if (boolVal.toString.toBoolean) {
            session.clearPlanCache()
          }
          session.planCaching = boolVal.toString.toBoolean
        case None => session.planCaching = Property.PlanCaching.defaultValue.get
      }

    case Property.PlanCachingAll.name =>
      value match {
        case Some(boolVal) =>
          val clearCache = !boolVal.toString.toBoolean
          if (clearCache) SnappySession.getPlanCache.asMap().clear()
        case None =>
      }

    case Property.Tokenize.name =>
      value match {
        case Some(boolVal) => session.tokenize = boolVal.toString.toBoolean
        case None => session.tokenize = Property.Tokenize.defaultValue.get
      }
      session.clearPlanCache()

    case Property.DisableHashJoin.name =>
      value match {
        case Some(boolVal) => session.disableHashJoin = boolVal.toString.toBoolean
        case None => session.disableHashJoin = Property.DisableHashJoin.defaultValue.get
      }
      session.clearPlanCache()

    case SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key => session.clearPlanCache()

    case Constant.TRIGGER_AUTHENTICATION => value match {
      case Some(boolVal) if boolVal.toString.toBoolean =>
        if ((Misc.getMemStoreBootingNoThrow ne null) && Misc.isSecurityEnabled) {
          SecurityUtils.checkCredentials(getConfString(
            com.pivotal.gemfirexd.Attribute.USERNAME_ATTR),
            getConfString(com.pivotal.gemfirexd.Attribute.PASSWORD_ATTR)) match {
            case None => // success
            case Some(failure) =>
              throw Util.generateCsSQLException(SQLState.NET_CONNECT_AUTH_FAILED, failure)
          }
        }
      case _ =>
    }

    case _ => // ignore others
  }

  private[sql] def refreshNumShufflePartitions(): Unit = synchronized {
    if (session ne null) {
      if (executionShufflePartitions != -1) {
        executionShufflePartitions = 0
      }
      if (dynamicShufflePartitions != -1) {
        dynamicShufflePartitions = coreCountForShuffle
      }
    }
  }

  private[sql] def setExecutionShufflePartitions(n: Int): Unit = synchronized {
    if (executionShufflePartitions != -1 && session != null) {
      executionShufflePartitions = math.max(n, executionShufflePartitions)
    }
  }

  override def numShufflePartitions: Int = {
    val partitions = this.executionShufflePartitions
    if (partitions > 0) partitions
    else {
      val partitions = this.dynamicShufflePartitions
      if (partitions > 0) partitions else super.numShufflePartitions
    }
  }

  def activeSchedulerPool: String = schedulerPool

  override def setConfString(key: String, value: String): Unit = {
    keyUpdateActions(key, Some(value), doSet = true)
    super.setConfString(key, value)
  }

  override def setConf[T](entry: ConfigEntry[T], value: T): Unit = {
    keyUpdateActions(entry.key, Some(value), doSet = true)
    require(entry != null, "entry cannot be null")
    require(value != null, s"value cannot be null for key: ${entry.key}")
    entry.defaultValue match {
      case Some(_) => super.setConf(entry, value)
      case None => super.setConf(entry.asInstanceOf[ConfigEntry[Option[T]]], Some(value))
    }
  }

  override def unsetConf(key: String): Unit = {
    keyUpdateActions(key, None, doSet = false)
    super.unsetConf(key)
  }

  override def unsetConf(entry: ConfigEntry[_]): Unit = {
    keyUpdateActions(entry.key, None, doSet = false)
    super.unsetConf(entry)
  }
}

class SQLConfigEntry private(private[sql] val entry: ConfigEntry[_]) {

  def key: String = entry.key

  def doc: String = entry.doc

  def isPublic: Boolean = entry.isPublic

  def defaultValue[T]: Option[T] = entry.defaultValue.asInstanceOf[Option[T]]

  def defaultValueString: String = entry.defaultValueString

  def valueConverter[T]: String => T =
    entry.asInstanceOf[ConfigEntry[T]].valueConverter

  def stringConverter[T]: T => String =
    entry.asInstanceOf[ConfigEntry[T]].stringConverter

  override def toString: String = entry.toString
}

object SQLConfigEntry {

  private def handleDefault[T](entry: TypedConfigBuilder[T],
      defaultValue: Option[T]): SQLConfigEntry = defaultValue match {
    case Some(v) => new SQLConfigEntry(entry.createWithDefault(v))
    case None => new SQLConfigEntry(entry.createOptional)
  }

  def sparkConf[T: ClassTag](key: String, doc: String, defaultValue: Option[T],
      isPublic: Boolean = true): SQLConfigEntry = {
    classTag[T] match {
      case ClassTag.Int => handleDefault[Int](ConfigBuilder(key)
          .doc(doc).intConf, defaultValue.asInstanceOf[Option[Int]])
      case ClassTag.Long => handleDefault[Long](ConfigBuilder(key)
          .doc(doc).longConf, defaultValue.asInstanceOf[Option[Long]])
      case ClassTag.Double => handleDefault[Double](ConfigBuilder(key)
          .doc(doc).doubleConf, defaultValue.asInstanceOf[Option[Double]])
      case ClassTag.Boolean => handleDefault[Boolean](ConfigBuilder(key)
          .doc(doc).booleanConf, defaultValue.asInstanceOf[Option[Boolean]])
      case c if c.runtimeClass == classOf[String] =>
        handleDefault[String](ConfigBuilder(key).doc(doc).stringConf,
          defaultValue.asInstanceOf[Option[String]])
      case c => throw new IllegalArgumentException(
        s"Unknown type of configuration key: $c")
    }
  }

  def apply[T: ClassTag](key: String, doc: String, defaultValue: Option[T],
      isPublic: Boolean = true): SQLConfigEntry = {
    classTag[T] match {
      case ClassTag.Int => handleDefault[Int](SQLConfigBuilder(key)
          .doc(doc).intConf, defaultValue.asInstanceOf[Option[Int]])
      case ClassTag.Long => handleDefault[Long](SQLConfigBuilder(key)
          .doc(doc).longConf, defaultValue.asInstanceOf[Option[Long]])
      case ClassTag.Double => handleDefault[Double](SQLConfigBuilder(key)
          .doc(doc).doubleConf, defaultValue.asInstanceOf[Option[Double]])
      case ClassTag.Boolean => handleDefault[Boolean](SQLConfigBuilder(key)
          .doc(doc).booleanConf, defaultValue.asInstanceOf[Option[Boolean]])
      case c if c.runtimeClass == classOf[String] =>
        handleDefault[String](SQLConfigBuilder(key).doc(doc).stringConf,
          defaultValue.asInstanceOf[Option[String]])
      case c => throw new IllegalArgumentException(
        s"Unknown type of configuration key: $c")
    }
  }
}

trait AltName[T] {

  def name: String

  def altName: String

  def configEntry: SQLConfigEntry

  def defaultValue: Option[T] = configEntry.defaultValue[T]

  def getOption(conf: SparkConf): Option[String] = if (altName == null) {
    conf.getOption(name)
  } else {
    conf.getOption(name) match {
      case s: Some[String] => // check if altName also present and fail if so
        if (conf.contains(altName)) {
          throw new IllegalArgumentException(
            s"Both $name and $altName configured. Only one should be set.")
        } else s
      case None => conf.getOption(altName)
    }
  }

  private def get(conf: SparkConf, name: String,
      defaultValue: String): T = {
    configEntry.entry.defaultValue match {
      case Some(_) => configEntry.valueConverter[T](
        conf.get(name, defaultValue))
      case None => configEntry.valueConverter[Option[T]](
        conf.get(name, defaultValue)).get
    }
  }

  def get(conf: SparkConf): T = if (altName == null) {
    get(conf, name, configEntry.defaultValueString)
  } else {
    if (conf.contains(name)) {
      if (!conf.contains(altName)) get(conf, name, configEntry.defaultValueString)
      else {
        throw new IllegalArgumentException(
          s"Both $name and $altName configured. Only one should be set.")
      }
    } else {
      get(conf, altName, configEntry.defaultValueString)
    }
  }

  def get(properties: Properties): T = {
    val propertyValue = getProperty(properties)
    if (propertyValue ne null) configEntry.valueConverter[T](propertyValue)
    else defaultValue.get
  }

  def getProperty(properties: Properties): String = if (altName == null) {
    properties.getProperty(name)
  } else {
    val v = properties.getProperty(name)
    if (v != null) {
      // check if altName also present and fail if so
      if (properties.getProperty(altName) != null) {
        throw new IllegalArgumentException(
          s"Both $name and $altName specified. Only one should be set.")
      }
      v
    } else properties.getProperty(altName)
  }

  def unapply(key: String): Boolean = name.equals(key) ||
      (altName != null && altName.equals(key))
}

trait SQLAltName[T] extends AltName[T] {

  private def get(conf: SQLConf, entry: SQLConfigEntry): T = {
    entry.defaultValue match {
      case Some(_) => conf.getConf(entry.entry.asInstanceOf[ConfigEntry[T]])
      case None => conf.getConf(entry.entry.asInstanceOf[ConfigEntry[Option[T]]]).get
    }
  }

  private def get(conf: SQLConf, name: String,
      defaultValue: String): T = {
    configEntry.entry.defaultValue match {
      case Some(_) => configEntry.valueConverter[T](
        conf.getConfString(name, defaultValue))
      case None => configEntry.valueConverter[Option[T]](
        conf.getConfString(name, defaultValue)).get
    }
  }

  def get(conf: SQLConf): T = if (altName == null) {
    get(conf, configEntry)
  } else {
    if (conf.contains(name)) {
      if (!conf.contains(altName)) get(conf, configEntry)
      else {
        throw new IllegalArgumentException(
          s"Both $name and $altName configured. Only one should be set.")
      }
    } else {
      get(conf, altName, configEntry.defaultValueString)
    }
  }

  def getOption(conf: SQLConf): Option[T] = if (altName == null) {
    if (conf.contains(name)) Some(get(conf, name, "<undefined>"))
    else defaultValue
  } else {
    if (conf.contains(name)) {
      if (!conf.contains(altName)) Some(get(conf, name, ""))
      else {
        throw new IllegalArgumentException(
          s"Both $name and $altName configured. Only one should be set.")
      }
    } else if (conf.contains(altName)) {
      Some(get(conf, altName, ""))
    } else defaultValue
  }

  def set(conf: SQLConf, value: T, useAltName: Boolean = false): Unit = {
    if (useAltName) {
      conf.setConfString(altName, configEntry.stringConverter(value))
    } else {
      conf.setConf[T](configEntry.entry.asInstanceOf[ConfigEntry[T]], value)
    }
  }

  def remove(conf: SQLConf, useAltName: Boolean = false): Unit = {
    conf.unsetConf(if (useAltName) altName else name)
  }
}

private[sql] final class PreprocessTable(state: SnappySessionState) extends Rule[LogicalPlan] {

  private def conf: SQLConf = state.conf

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {

    // Add dbtable property for create table. While other routes can add it in
    // SnappySession.createTable, the DataFrameWriter path needs to be handled here.
    case c@CreateTable(tableDesc, mode, queryOpt) if DDLUtils.isDatasourceTable(tableDesc) =>
      val tableIdent = state.catalog.resolveTableIdentifier(tableDesc.identifier)
      val provider = tableDesc.provider.get
      // treat saveAsTable with mode=Append as insertInto
      if (mode == SaveMode.Append && queryOpt.isDefined && state.catalog.tableExists(tableIdent)) {
        new Insert(table = UnresolvedRelation(tableDesc.identifier),
          partition = Map.empty, child = queryOpt.get,
          overwrite = OverwriteOptions(enabled = false), ifNotExists = false)
      } else if (SnappyContext.isBuiltInProvider(provider) ||
          CatalogObjectType.isGemFireProvider(provider)) {
        val tableName = tableIdent.unquotedString
        // dependent tables are stored as comma-separated so don't allow comma in table name
        if (tableName.indexOf(',') != -1) {
          throw new AnalysisException(s"Table '$tableName' cannot contain comma in its name")
        }
        var newOptions = tableDesc.storage.properties +
            (SnappyExternalCatalog.DBTABLE_PROPERTY -> tableName)
        if (CatalogObjectType.isColumnTable(SnappyContext.getProviderType(provider))) {
          // add default batchSize and maxDeltaRows options for column tables
          if (!newOptions.contains(ExternalStoreUtils.COLUMN_MAX_DELTA_ROWS)) {
            newOptions += (ExternalStoreUtils.COLUMN_MAX_DELTA_ROWS ->
                ExternalStoreUtils.defaultColumnMaxDeltaRows(state.snappySession).toString)
          }
          if (!newOptions.contains(ExternalStoreUtils.COLUMN_BATCH_SIZE)) {
            newOptions += (ExternalStoreUtils.COLUMN_BATCH_SIZE ->
                ExternalStoreUtils.defaultColumnBatchSize(state.snappySession).toString)
          }
        }
        c.copy(tableDesc.copy(storage = tableDesc.storage.copy(properties = newOptions)))
      } else c

    // Check for SchemaInsertableRelation first
    case i@InsertIntoTable(l@LogicalRelation(r: SchemaInsertableRelation,
    _, _), _, child, _, _) if l.resolved && child.resolved =>
      r.insertableRelation(child.output) match {
        case Some(ir) if r eq ir => i
        case Some(ir) =>
          val br = ir.asInstanceOf[BaseRelation]
          val relation = LogicalRelation(br, catalogTable = l.catalogTable)
          castAndRenameChildOutputForPut(i.copy(table = relation),
            relation.output, br, null, child)
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
        case l@LogicalRelation(ir: RowInsertableRelation, _, _) =>
          // First, make sure the data to be inserted have the same number of
          // fields with the schema of the relation.
          val expectedOutput = l.output
          if (expectedOutput.size != child.output.size) {
            throw new AnalysisException(s"$l requires that the query in the " +
                "SELECT clause of the PUT INTO statement " +
                "generates the same number of columns as its schema.")
          }
          castAndRenameChildOutputForPut(p, expectedOutput, ir, l, child)

        case _ => p
      }

    // Check for DELETE
    // Need to eliminate subqueries here. Unlike InsertIntoTable whose
    // subqueries have already been eliminated by special check in
    // ResolveRelations, no such special rule has been added for PUT
    case d@DeleteFromTable(table, child) if table.resolved && child.resolved =>
      EliminateSubqueryAliases(table) match {
        case l@LogicalRelation(dr: MutableRelation, _, _) =>

          val keyColumns = dr.getPrimaryKeyColumns
          val childOutput = keyColumns.map(col =>
            child.resolveQuoted(col, analysis.caseInsensitiveResolution) match {
              case Some(a: Attribute) => a
              case _ => throw new AnalysisException(s"$l requires that the query in the " +
                  "WHERE clause of the DELETE FROM statement " +
                  s"must have all the key column(s) ${keyColumns.mkString(",")} but found " +
                  s"${child.output.mkString(",")} instead.")
            })

          val expectedOutput = keyColumns.map(col =>
            l.resolveQuoted(col, analysis.caseInsensitiveResolution) match {
              case Some(a: Attribute) => a
              case _ => throw new AnalysisException(s"The target table must contain all the" +
                  s" key column(s) ${keyColumns.mkString(",")}. " +
                  s"Actual schema: ${l.output.mkString(",")}")
            })

          castAndRenameChildOutputForPut(d, expectedOutput, dr, l, Project(childOutput, child))

        case _ => d
      }

    // other cases handled like in PreprocessTableInsertion
    case i@InsertIntoTable(table, _, child, _, _)
      if table.resolved && child.resolved => PreprocessTableInsertion(conf).apply(i)
  }

  /**
   * If necessary, cast data types and rename fields to the expected
   * types and names.
   */
  def castAndRenameChildOutputForPut[T <: LogicalPlan](
      plan: T,
      expectedOutput: Seq[Attribute],
      relation: BaseRelation,
      newRelation: LogicalRelation,
      child: LogicalPlan): T = {
    val newChildOutput = expectedOutput.zip(child.output).map {
      case (expected, actual) =>
        if (expected.dataType.sameType(actual.dataType) &&
            expected.name == actual.name) {
          actual
        } else {
          // avoid unnecessary copy+cast when inserting DECIMAL types
          // into column table
          actual.dataType match {
            case _: DecimalType
              if expected.dataType.isInstanceOf[DecimalType] &&
                  relation.isInstanceOf[PlanInsertableRelation] => actual
            case _ => Alias(Cast(actual, expected.dataType), expected.name)()
          }
        }
    }

    if (newChildOutput == child.output) {
      plan match {
        case p: PutIntoTable => p.copy(table = newRelation).asInstanceOf[T]
        case d: DeleteFromTable => d.copy(table = newRelation).asInstanceOf[T]
        case _: InsertIntoTable => plan
      }
    } else plan match {
      case p: PutIntoTable => p.copy(table = newRelation,
        child = Project(newChildOutput, child)).asInstanceOf[T]
      case d: DeleteFromTable => d.copy(table = newRelation,
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
      case PutIntoTable(table, _) =>
        throw Utils.analysisException(s"$table does not allow puts.")
      case _ => // OK
    }
  }
}

private[sql] case class ConditionalPreWriteCheck(sparkPreWriteCheck: datasources.PreWriteCheck)
    extends (LogicalPlan => Unit) {
  def apply(plan: LogicalPlan): Unit = {
    plan match {
      case PutIntoColumnTable(_, _, _) => // Do nothing
      case _ => sparkPreWriteCheck.apply(plan)
    }
  }
}

/**
 * Deals with any escape characters in the LIKE pattern in optimization.
 * Does not deal with startsAndEndsWith equivalent of Spark's LikeSimplification
 * so 'a%b' kind of pattern with additional escaped chars will not be optimized.
 */
object LikeEscapeSimplification {

  private def addTokenizedLiteral(parser: SnappyParser, s: String): Expression = {
    if (parser ne null) parser.addTokenizedLiteral(UTF8String.fromString(s), StringType)
    else Literal(UTF8String.fromString(s), StringType)
  }

  def simplifyLike(parser: SnappyParser, expr: Expression,
      left: Expression, pattern: String): Expression = {
    val len_1 = pattern.length - 1
    if (len_1 == -1) return EqualTo(left, addTokenizedLiteral(parser, ""))
    val str = new StringBuilder(pattern.length)
    var wildCardStart = false
    var i = 0
    while (i < len_1) {
      pattern.charAt(i) match {
        case '\\' =>
          val c = pattern.charAt(i + 1)
          c match {
            case '_' | '%' | '\\' => // literal char
            case _ => return expr
          }
          str.append(c)
          // if next character is last one then it is literal
          if (i == len_1 - 1) {
            if (wildCardStart) return EndsWith(left, addTokenizedLiteral(parser, str.toString))
            else return EqualTo(left, addTokenizedLiteral(parser, str.toString))
          }
          i += 1
        case '%' if i == 0 => wildCardStart = true
        case '%' | '_' => return expr // wildcards in middle are left as is
        case c => str.append(c)
      }
      i += 1
    }
    pattern.charAt(len_1) match {
      case '%' =>
        if (wildCardStart) Contains(left, addTokenizedLiteral(parser, str.toString))
        else StartsWith(left, addTokenizedLiteral(parser, str.toString))
      case '_' | '\\' => expr
      case c =>
        str.append(c)
        if (wildCardStart) EndsWith(left, addTokenizedLiteral(parser, str.toString))
        else EqualTo(left, addTokenizedLiteral(parser, str.toString))
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case l@Like(left, Literal(pattern, StringType)) =>
      simplifyLike(null, l, left, pattern.toString)
  }
}

case class MarkerForCreateTableAsSelect(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class BypassRowLevelSecurity(child: LogicalFilter) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

/**
 * Wrap plan-specific query hints (like joinType). This extends Spark's BroadcastHint
 * so that filters/projections etc can be pushed below this by optimizer.
 */
class LogicalPlanWithHints(_child: LogicalPlan, val hints: Map[String, String])
    extends BroadcastHint(_child) {

  override def productArity: Int = 2

  override def productElement(n: Int): Any = n match {
    case 0 => child
    case 1 => hints
  }

  override def simpleString: String =
    s"LogicalPlanWithHints[hints = $hints; child = ${child.simpleString}]"
}
