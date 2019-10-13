/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

import org.apache.spark.Partition
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.analysis.TypeCoercion.{PromoteStrings, numericPrecedence}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, Star, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.{And, BinaryArithmetic, EqualTo, In, _}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Filter => LogicalFilter, _}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.columnar.impl.IndexColumnFormatRelation
import org.apache.spark.sql.execution.command.{ExecutedCommandExec, RunnableCommand}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.sources.{PhysicalScan, StoreDataSourceStrategy}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, HiveTableScanExec, InsertIntoHiveTable}
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
trait SnappySessionState extends SessionState with SnappyStrategies with SparkSupport {

  val snappySession: SnappySession

  def catalogBuilder(wrapped: Option[SnappySessionCatalog]): SessionCatalog

  def analyzerBuilder(): Analyzer

  def optimizerBuilder(): Optimizer

  val conf: SQLConf
  val sqlParser: ParserInterface
  val streamingQueryManager: StreamingQueryManager

  final def snappyConf: SnappyConf = conf.asInstanceOf[SnappyConf]

  final def snappySqlParser: SnappySqlParser = sqlParser.asInstanceOf[SnappySqlParser]

  private[sql] lazy val sampleSnappyCase: PartialFunction[LogicalPlan, Seq[SparkPlan]] =
    snappySession.contextFunctions.createSampleSnappyCase()

  private[sql] lazy val hiveSession: SparkSession = {
    // disable enableHiveSupport during initialization to avoid calls into SnappyConf
    val oldValue = snappySession.enableHiveSupport
    snappySession.enableHiveSupport = false
    snappySession.hiveInitializing = true
    val session = SnappyContext.newHiveSession()
    val hiveConf = session.sessionState.conf
    snappyConf.foreach(hiveConf.setConfString)
    hiveConf.setConfString(StaticSQLConf.CATALOG_IMPLEMENTATION.key, "hive")
    snappySession.enableHiveSupport = oldValue
    snappySession.hiveInitializing = false
    session
  }

  private[sql] lazy val hiveState: HiveSessionState =
    hiveSession.sessionState.asInstanceOf[HiveSessionState]

  /**
   * Execute a method switching the session and shared states in the session to external hive.
   * Rules, Strategies and catalog lookups into the external hive meta-store may need to switch
   * since session/shared states may be read from the session dynamically inside the body of
   * given function that will expect it to be the external hive ones.
   */
  private[sql] def withHiveSession[T](f: => T): T = {
    SparkSession.setActiveSession(hiveSession)
    try {
      f
    } finally {
      SparkSession.setActiveSession(snappySession)
    }
  }

  private[sql] var disableStoreOptimizations: Boolean = false

  override lazy val analyzer: Analyzer = analyzerBuilder()

  override lazy val optimizer: Optimizer = optimizerBuilder()

  protected[sql] def getExtendedCheckRules: Seq[LogicalPlan => Unit] = {
    Seq(ConditionalPreWriteCheck(internals.newPreWriteCheck(this)), PrePutCheck, HiveOnlyCheck)
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
        case s: SubqueryExpression if foldable =>
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
        case q: LogicalPlan => internals.mapExpressions(q, ex => unmarkAll(mark(ex).transformDown {
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
            case _: LogicalRelation | _: LogicalDStreamPlan => win
            case _ => duration = d
              slide = s
              transformed = true
              win.child
          }
        case c@(_: LogicalRelation | _: LogicalDStreamPlan) =>
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
  object LinkPartitionsToBuckets extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = {
      plan.foreach {
        case _ if Property.ForceLinkPartitionsToBuckets.get(conf) =>
          // always create one partition per bucket
          snappySession.linkPartitionsToBuckets(flag = true)
        case j: Join if !JoinStrategy.isReplicatedJoin(j) =>
          // disable for the entire query for consistency
          snappySession.linkPartitionsToBuckets(flag = true)
        case _: InsertIntoTable | _: TableMutationPlan =>
          // disable for inserts/puts to avoid exchanges and indexes to work correctly
          snappySession.linkPartitionsToBuckets(flag = true)
        case l: LogicalRelation if l.relation.isInstanceOf[IndexColumnFormatRelation] =>
          // disable for indexes
          snappySession.linkPartitionsToBuckets(flag = true)
        case _ => // nothing for others
      }
      plan
    }
  }

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
          case r: LogicalRelation if r.relation.isInstanceOf[PartitionedDataSourceScan] =>
            // send back numPartitions=1 for replicated table since collocated
            val scan = r.relation.asInstanceOf[PartitionedDataSourceScan]
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
    def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
      // pivot with '*' projection messes up references for some reason
      // in older versions of Spark
      case Project(projectList, p: Pivot)
        if projectList.length == 1 && projectList.head.isInstanceOf[Star] => p
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

      // add implicit grouping columns to pivot
      case p@Pivot(groupBy, pivotColumn, _, aggregates, child)
        if groupBy.isEmpty && pivotColumn.resolved && aggregates.forall(_.resolved) =>
        val pivotColAndAggRefs = pivotColumn.references ++ AttributeSet(aggregates)
        val groupByExprs = child.output.filterNot(pivotColAndAggRefs.contains)
        p.copy(groupByExprs = groupByExprs)

      case o => o
    }
  }

  object RowLevelSecurity extends Rule[LogicalPlan] {
    // noinspection ScalaUnnecessaryParentheses
    // Y combinator
    val conditionEvaluator: (Expression => Boolean) => Expression => Boolean =
    (f: Expression => Boolean) =>
      (exp: Expression) => exp.eq(PolicyProperties.rlsAppliedCondition) ||
          (exp match {
            case And(left, _) => f(left)
            case EqualTo(l: Literal, r: Literal) =>
              l.value == r.value && l.value == PolicyProperties.rlsConditionStringUtf8
            case _ => false
          })

    // noinspection ScalaUnnecessaryParentheses
    def rlsConditionChecker(f: (Expression => Boolean) =>
        Expression => Boolean): Expression => Boolean = f(rlsConditionChecker(f))(_: Expression)

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
          case lr: LogicalRelation if lr.relation.isInstanceOf[RowLevelSecurityRelation] =>
            val policyFilter = catalog.getCombinedPolicyFilterForNativeTable(
              lr.relation.asInstanceOf[RowLevelSecurityRelation], Some(lr))
            policyFilter match {
              case Some(filter) => filter.copy(child = lr)
              case None => lr
            }

          case a: SubqueryAlias if a.child.isInstanceOf[LogicalFilter] =>
            val lf = a.child.asInstanceOf[LogicalFilter]
            LogicalFilter(lf.condition, internals.newSubqueryAlias(a.alias, lf.child))

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
          case lr: LogicalRelation if lr.relation.isInstanceOf[ApplyLimitOnExternalRelation] =>
            boolsArray(extRelation_bool) = true
            externalRelation = lr.relation.asInstanceOf[ApplyLimitOnExternalRelation]

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
        case lr: LogicalRelation if lr.relation.isInstanceOf[MutableRelation] =>
          val mutable = lr.relation.asInstanceOf[MutableRelation]
          val ks = mutable.getKeyColumns
          if (ks.isEmpty) {
            val currentKey = snappySession.currentKey
            // if this is a row table, then fallback to direct execution
            mutable match {
              case _: UpdatableRelation if currentKey ne null =>
                return (Nil, DMLExternalTable(lr, currentKey.sqlText), lr)
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
        case lr: LogicalRelation if lr.relation.isInstanceOf[MutableRelation] &&
            lr.relation.asInstanceOf[MutableRelation].table.equalsIgnoreCase(tableName) =>
          val mutable = lr.relation.asInstanceOf[MutableRelation]
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
                case _ if attr.dataType.isInstanceOf[StringType] => true
                case _ => false
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
  }

  /**
   * Internal catalog for managing table and database states.
   */
  override lazy val catalog: SnappySessionCatalog =
    catalogBuilder(None).asInstanceOf[SnappySessionCatalog]

  lazy val wrapperCatalog: SnappySessionCatalog =
    catalogBuilder(Some(catalog)).asInstanceOf[SnappySessionCatalog]

  private def queryPreparations(topLevel: Boolean): Seq[Rule[SparkPlan]] =
    snappySession.contextFunctions.queryPreparations(topLevel)

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
    snappyConf.resetDefaults()
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
            snappyConf.setExecutionShufflePartitions(region.getTotalNumberOfBuckets)
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
    if (state.snappySession.enableHiveSupport) state.withHiveSession {
      rule(state.hiveState)(plan)
    } else plan
  }
}

class HiveConditionalStrategy(strategy: HiveStrategies => Strategy, state: SnappySessionState)
    extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    // some strategies like DataSinks read the session state and expect it to be
    // HiveSessionState so switch it before invoking the strategy and restore at the end
    if (state.snappySession.enableHiveSupport) state.withHiveSession {
      strategy(state.hiveState.planner.asInstanceOf[HiveStrategies])(plan)
    } else Nil
  }
}


trait SnappyAnalyzer extends Analyzer {

  def session: SnappySession

  val baseAnalyzerInstance: Analyzer

  override lazy val batches: Seq[Batch] = baseAnalyzerInstance.batches.map {
    case batch if batch.name.equalsIgnoreCase("Resolution") =>
      val rules = batch.rules.flatMap {
        case PromoteStrings =>
          StringPromotionCheckForUpdate.asInstanceOf[Rule[LogicalPlan]] :: SnappyPromoteStrings ::
              PromoteStrings :: Nil
        case r => r :: Nil
      }

      Batch(batch.name, batch.strategy.asInstanceOf[Strategy], rules: _*)
    case batch => Batch(batch.name, batch.strategy.asInstanceOf[Strategy], batch.rules: _*)
  }

  def baseExecute(plan: LogicalPlan): LogicalPlan = super.execute(plan)

  override def execute(plan: LogicalPlan): LogicalPlan =
    session.contextFunctions.executePlan(this, plan)

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
        case p@BinaryComparison(left@StringType(), right@QuestionMark(_))
          if right.dataType == NullType =>
          p.makeCopy(Array(left,
            ParamLiteral(right.value, StringType, right.pos, execId = -1, tokenized = true)))
        case p@BinaryComparison(left@QuestionMark(_), right@StringType())
          if left.dataType == NullType =>
          p.makeCopy(Array(
            ParamLiteral(left.value, StringType, left.pos, execId = -1, tokenized = true),
            right))
      }
    }
  }

}

/**
 * Rule to replace Spark's SortExec plans with an optimized SnappySortExec (in SMJ for now).
 * Also sets the "spark.task.cpus" property implicitly for file scans/writes.
 */
case class OptimizeSortAndFilePlans(conf: SnappyConf) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case join@joins.SortMergeJoinExec(_, _, _, _, _, sort@SortExec(_, _, child, _)) =>
      join.copy(right = SnappySortExec(sort, child))
    case s@(_: FileSourceScanExec | _: HiveTableScanExec | _: InsertIntoHiveTable |
            ExecutedCommandExec(_: InsertIntoHadoopFsRelationCommand |
                                _: CreateHiveTableAsSelectCommand)) =>
      conf.setDynamicCpusPerTask()
      s
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
