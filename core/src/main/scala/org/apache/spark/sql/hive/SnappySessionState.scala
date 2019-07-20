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

package org.apache.spark.sql.hive

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.internal.cache.{CacheDistributionAdvisee, ColocationHelper, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore
import io.snappydata.Property
import io.snappydata.Property.HashAggregateSize

import org.apache.spark.Partition
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.analysis.TypeCoercion.{PromoteStrings, numericPrecedence}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, CleanupAliases, EliminateSubqueryAliases, EliminateUnions, NoSuchTableException, ResolveCreateNamedStruct, ResolveInlineTables, ResolveTableValuedFunctions, Star, SubstituteUnresolvedOrdinals, TimeWindowing, TypeCoercion, UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{And, BinaryArithmetic, EqualTo, In, ScalarSubquery, _}
import org.apache.spark.sql.catalyst.optimizer.{Optimizer, ReorderJoin}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Filter => LogicalFilter, _}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.columnar.impl.IndexColumnFormatRelation
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReuseExchange}
import org.apache.spark.sql.execution.sources.{PhysicalScan, StoreDataSourceStrategy}
import org.apache.spark.sql.internal._
import org.apache.spark.sql.policy.PolicyProperties
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.streaming.{LogicalDStreamPlan, StreamingQueryManager, WindowLogicalPlan}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.Duration


/**
 * Holds all session-specific state for a given [[SnappySession]].
 */
class SnappySessionState(val snappySession: SnappySession)
    extends SessionState(snappySession) with SnappyStrategies {

  @transient
  val contextFunctions: SnappyContextFunctions = new SnappyContextFunctions

  val sampleSnappyCase: PartialFunction[LogicalPlan, Seq[SparkPlan]] = {
    case MarkerForCreateTableAsSelect(child) => PlanLater(child) :: Nil
    case BypassRowLevelSecurity(child) => PlanLater(child) :: Nil
    case _ => Nil
  }

  protected[hive] val snappySharedState: SnappySharedState =
    snappySession.sharedState.asInstanceOf[SnappySharedState]

  override lazy val streamingQueryManager: StreamingQueryManager = {
    // Disabling `SnappyAggregateStrategy` for streaming queries as it clashes with
    // `StatefulAggregationStrategy` which is applied by spark for streaming queries. This
    // implies that Snappydata aggregation optimisation will be turned off for any usage of
    // this session including non-streaming queries.

    HashAggregateSize.set(conf, "-1")
    new StreamingQueryManager(snappySession)
  }

  private[sql] lazy val hiveState: HiveSessionState = {
    // switch the shared state to that of hive temporarily
    snappySession.setSharedState(snappySharedState.getOrCreateHiveSharedState)
    try {
      val state = new HiveSessionState(snappySession)
      snappySession.setSessionState(state)
      try {
        // initialize lazy members
        state.metadataHive
        state.catalog
        state
      } finally {
        snappySession.setSessionState(this)
      }
    } finally {
      snappySession.setSharedState(snappySharedState)
    }
  }

  /**
   * Execute a method switching the session and shared states in the session to external hive.
   * Rules, Strategies and catalog lookups into the external hive meta-store may need to switch
   * since session/shared states may be read from the session dynamically inside the body of
   * given function that will expect it to be the external hive ones.
   */
  private[sql] def withHiveState[T](f: => T): T = {
    snappySession.setSharedState(snappySharedState.getOrCreateHiveSharedState)
    snappySession.setSessionState(hiveState)
    try {
      f
    } finally {
      snappySession.setSessionState(this)
      snappySession.setSharedState(snappySharedState)
    }
  }

  private[sql] def hiveSessionCatalog: HiveSessionCatalog = hiveState.catalog

  override lazy val sqlParser: SnappySqlParser =
    contextFunctions.newSQLParser(this.snappySession)

  private[sql] var disableStoreOptimizations: Boolean = false

  def getExtendedResolutionRules(analyzer: Analyzer): Seq[Rule[LogicalPlan]] =
    new HiveConditionalRule(_.catalog.ParquetConversions, this) ::
        new HiveConditionalRule(_.catalog.OrcConversions, this) ::
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

  override lazy val analyzer: Analyzer = new SnappyAnalyzer(this) {

    override val extendedCheckRules: Seq[LogicalPlan => Unit] = getExtendedCheckRules

    override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
      getExtendedResolutionRules(this)
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
          case expr@In(v, l) if !disableStoreOptimizations && l.forall(e =>
            e.isInstanceOf[Literal] || e.isInstanceOf[DynamicReplacableConstant] || e.foldable) =>
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
    private def getTable(u: UnresolvedRelation): LogicalPlan = {
      try {
        catalog.lookupRelation(u.tableIdentifier, u.alias)
      } catch {
        case _: TableNotFoundException | _: NoSuchTableException =>
          u.failAnalysis(s"Table not found: ${u.tableIdentifier.unquotedString}")
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case i@PutIntoTable(u: UnresolvedRelation, _) =>
        i.copy(table = EliminateSubqueryAliases(getTable(u)))
      case d@DeleteFromTable(u: UnresolvedRelation, _) =>
        d.copy(table = EliminateSubqueryAliases(getTable(u)))
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
            }.getOrElse(u)
          case x => x
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
            val policyFilter = catalog.getCombinedPolicyFilterForNativeTable(rlsRelation, Some(lr))
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

  case class AnalyzeMutableOperations(session: SnappySession,
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
              .partitionColumns.map(Utils.toLowerCase) ++
              keyAttrs.map(k => Utils.toLowerCase(k.name))).toSet
          // resolve the columns being updated and cast the expressions if required
          val (updateAttrs, newUpdateExprs) = updateCols.zip(updateExprs).map { case (c, expr) =>
            val attr = analysis.withPosition(relation) {
              relation.resolve(
                c.name.split('.'), analyzer.resolver).getOrElse(
                throw new AnalysisException(s"Could not resolve update column ${c.name}"))
            }
            val colName = Utils.toLowerCase(c.name)
            if (nonUpdatableColumns.contains(colName)) {
              throw new AnalysisException("Cannot update partitioning/key column " +
                  s"of the table for $colName (among [${nonUpdatableColumns.mkString(", ")}])")
            }

            val newExpr = if (attr.dataType.sameType(expr.dataType)) {
              expr
            } else {
              def typesCompatible: Boolean = expr.dataType match {
                // allowing assignment of narrower numeric expression to wider decimal attribute
                case dt: NumericType if attr.dataType.isInstanceOf[DecimalType]
                    && attr.dataType.asInstanceOf[DecimalType].isWiderThan(dt) => true
                // allowing assignment of narrower numeric types to wider numeric types as far as
                // precision is not compromised
                case dt: NumericType if !attr.dataType.isInstanceOf[DecimalType]
                    && numericPrecedence.indexOf(dt) < numericPrecedence.indexOf(attr.dataType) =>
                  true
                // allowing assignment of null value
                case _: NullType => true
                // allowing assignment to a string type column for all datatypes
                case dt if attr.dataType.isInstanceOf[StringType] => true
                case dt => false
              }

              // avoid unnecessary copy+cast when inserting DECIMAL types into column table
              if (expr.dataType.isInstanceOf[DecimalType] && attr.dataType.isInstanceOf[DecimalType]
                  && attr.dataType.asInstanceOf[DecimalType].isWiderThan(expr.dataType)) {
                expr
              } else if (typesCompatible) {
                Alias(Cast(expr, attr.dataType), attr.name)()
              } else {
                val message = s"Data type of expression (${expr.dataType}) is not" +
                    s" compatible with the data type of attribute '${attr.name}' (${attr.dataType})"
                throw new AnalysisException(message)
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
        ColumnTableBulkOps.transformDeletePlan(session, d)
      case p@PutIntoTable(table, child) if table.resolved && child.resolved =>
        ColumnTableBulkOps.transformPutPlan(session, p)
    }

    private def analyzeQuery(query: LogicalPlan): LogicalPlan = {
      val qe = executePlan(query)
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
    EnsureRequirements(conf),
    OptimizeSortPlans,
    CollapseCollocatedPlans(snappySession),
    CollapseCodegenStages(conf),
    InsertCachedPlanFallback(snappySession, topLevel),
    ReuseExchange(conf))

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
        Seq(new HiveConditionalStrategy(_.HiveTableScans, this),
          new HiveConditionalStrategy(_.DataSinks, this),
          new HiveConditionalStrategy(_.Scripts, this),
          SnappyStrategies, StoreStrategy, StreamQueryStrategy) ++ storeOptimizedRules
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
            conf.setExecutionShufflePartitions(region.getTotalNumberOfBuckets)
          }
          StoreUtils.getPartitionsPartitionedTable(snappySession, pr,
            linkPartitionsToBuckets, preferPrimaries)
        }
      })
  }

  def getTablePartitions(region: CacheDistributionAdvisee): Array[Partition] =
    StoreUtils.getPartitionsReplicatedTable(snappySession, region)
}

class HiveConditionalRule(rule: HiveSessionState => Rule[LogicalPlan], state: SnappySessionState)
    extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Parquet/Orc conversion rules will indirectly read the session state from the session
    // so switch to it and restore at the end
    if (state.snappySession.enableHiveSupport) state.withHiveState {
      rule(state.hiveState)(plan)
    } else plan
  }
}

class HiveConditionalStrategy(strategy: HiveStrategies => Strategy, state: SnappySessionState)
    extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val session = state.snappySession
    // some strategies like DataSinks read the session state and expect it to be
    // HiveSessionState so switch it before invoking the strategy and restore at the end
    if (session.enableHiveSupport) state.withHiveState {
      strategy(state.hiveState.planner.asInstanceOf[HiveStrategies])(plan)
    } else Nil
  }
}


class SnappyAnalyzer(sessionState: SnappySessionState)
    extends Analyzer(sessionState.catalog, sessionState.conf) {

  // This list of rule is exact copy of org.apache.spark.sql.catalyst.analysis.Analyzer.batches
  // It is replicated to inject StringPromotionCheckForUpdate rule. Since Analyzer.batches is
  // declared as a lazy val, it can not be accessed using super keywork.
  private[sql] lazy val ruleBatches = Seq(
    Batch("Substitution", fixedPoint,
      CTESubstitution,
      WindowsSubstitution,
      EliminateUnions,
      new SubstituteUnresolvedOrdinals(sessionState.conf)),
    Batch("Resolution", fixedPoint,
      ResolveTableValuedFunctions ::
          ResolveRelations ::
          ResolveReferences ::
          ResolveCreateNamedStruct ::
          ResolveDeserializer ::
          ResolveNewInstance ::
          ResolveUpCast ::
          ResolveGroupingAnalytics ::
          ResolvePivot ::
          ResolveOrdinalInOrderByAndGroupBy ::
          ResolveMissingReferences ::
          ExtractGenerator ::
          ResolveGenerate ::
          ResolveFunctions ::
          ResolveAliases ::
          ResolveSubquery ::
          ResolveWindowOrder ::
          ResolveWindowFrame ::
          ResolveNaturalAndUsingJoin ::
          ExtractWindowExpressions ::
          GlobalAggregates ::
          ResolveAggregateFunctions ::
          TimeWindowing ::
          ResolveInlineTables ::
          TypeCoercion.typeCoercionRules ++
              extendedResolutionRules : _*),
    Batch("Nondeterministic", Once,
      PullOutNondeterministic),
    Batch("UDF", Once,
      HandleNullInputsForUDF),
    Batch("FixNullability", Once,
      FixNullability),
    Batch("Cleanup", fixedPoint,
      CleanupAliases)
  )

  override lazy val batches: Seq[Batch] = ruleBatches.map {
    case batch if batch.name.equalsIgnoreCase("Resolution") =>
      val rules = batch.rules.flatMap {
        case PromoteStrings =>
          StringPromotionCheckForUpdate.asInstanceOf[Rule[LogicalPlan]] :: SnappyPromoteStrings ::
              PromoteStrings :: Nil
        case r => r :: Nil
      }

      Batch(batch.name, batch.strategy, rules: _*)
    case batch => Batch(batch.name, batch.strategy, batch.rules: _*)
  }


  // This Rule fails an update query when type of Arithmetic operators doesn't match. This
  // need to be done because by default spark performs fail safe implicit type
  // conversion when type of two operands does't match and this can lead to null values getting
  // populated in the table.
  private object StringPromotionCheckForUpdate extends Rule[LogicalPlan] {

    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan match {
        case Update(table, child, keyColumns, updateColumns, updateExpressions) =>
          updateExpressions.foreach {
            case e if !e.childrenResolved => // do nothing
            case BinaryArithmetic(_@StringType(), _) | BinaryArithmetic(_, _@StringType()) =>
              throw new AnalysisException("Implicit type casting of string type to numeric" +
                  " type is not performed for update statements.")
            case _ => // do nothing
          }
          Update(table, child, keyColumns, updateColumns, updateExpressions)
        case _ => plan
      }
    }
  }

  /*
    SnappyPromoteStrings is applied before Spark's org.apache.spark.sql.catalyst.analysis.TypeCoercion.PromoteStrings rule.
    Spark PromoteStrings rule causes issues in prepared statements by replacing ParamLiteral
    with NULL in case of BinaryComparison with left node being StringType and right being
    ParamLiteral (or vice-versa) as by default ParamLiteral datatype is NullType. In such a case, this rule
    converts ParmaLiteral type to StringType to prevent it being replaced by NULL
   */
  object SnappyPromoteStrings extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan resolveExpressions {
        case e if !e.childrenResolved => e
        case p @ BinaryComparison(left @ StringType(), right @ QuestionMark(pos))
          if right.dataType == NullType =>
          p.makeCopy(Array(left,
            ParamLiteral(right.value, StringType, right.pos, execId = -1, tokenized = true)))
        case p @ BinaryComparison(left @ QuestionMark(pos), right @ StringType())
          if left.dataType == NullType =>
          p.makeCopy(Array(
            ParamLiteral(left.value, StringType, left.pos, execId = -1, tokenized = true),
            right))
      }
    }
  }
}

object QuestionMark {
  def unapply(p: ParamLiteral): Option[Int] = {
    if (p.pos == 0 && (p.dataType == NullType || p.dataType == StringType)) {
      p.value match {
        case r: Row => Some(r.getInt(0))
        case _ => None
      }
    } else None
  }
}
