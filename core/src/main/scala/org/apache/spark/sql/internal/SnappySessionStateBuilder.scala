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

package org.apache.spark.sql.internal

import java.util.Locale

import com.gemstone.gemfire.internal.cache.{CacheDistributionAdvisee, ColocationHelper, PartitionedRegion}
import io.snappydata.Property
import org.apache.spark.Partition
import org.apache.spark.annotation.{Experimental, InterfaceStability}
import org.apache.spark.sql.aqp.SnappyContextFunctions
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.analysis.{Analyzer, CastSupport, EliminateSubqueryAliases, NoSuchTableException, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.UnresolvedCatalogRelation
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, AttributeSet, Cast, Contains, DynamicFoldableExpression, DynamicInSet, DynamicReplacableConstant, EndsWith, EqualTo, Expression, ExpressionSet, In, Like, Literal, NamedExpression, ParamLiteral, PredicateHelper, ScalarSubquery, StartsWith, TokenizedLiteral}
import org.apache.spark.sql.catalyst.optimizer.{Optimizer, ReorderJoin}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.columnar.impl.IndexColumnFormatRelation
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReuseExchange}
import org.apache.spark.sql.execution.sources.PhysicalScan
import org.apache.spark.sql.hive.{SnappyStoreHiveCatalog, _}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.streaming.{LogicalDStreamPlan, WindowLogicalPlan}
import org.apache.spark.sql.types.{DecimalType, NumericType, StringType, StructType}
import org.apache.spark.sql.{SnappyStrategies, Strategy, _}
import org.apache.spark.streaming.Duration
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ArrayBuffer

/**
  * Builder that produces a SnappyData-aware `SessionState`.
  */
@Experimental
@InterfaceStability.Unstable
class SnappySessionStateBuilder(sparkSession: SparkSession,
                                parentState: Option[SessionState] = None)
  extends BaseSessionStateBuilder(sparkSession, parentState) {
  override val session = sparkSession.asInstanceOf[SnappySession]
  /**
    * Function that produces a new instance of the `BaseSessionStateBuilder`. This is used by the
    * [[SessionState]]'s clone functionality. Make sure to override this when implementing your own
    * [[SessionStateBuilder]].
    */
  override protected def newBuilder: NewBuilder =
    new SnappySessionStateBuilder(_, _)

//  override protected def customPlanningStrategies: Seq[Strategy] = {
//    Seq(StoreStrategy, StreamQueryStrategy, StoreDataSourceStrategy,
//      SnappyAggregation, HashJoinStrategies)
//  }

  override protected def analyzer: Analyzer = new Analyzer(catalog, conf) {
    override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
        new PreprocessTableInsertOrPut(conf) +:
          new FindDataSourceTable(session) +:
          SnappyDataSourceAnalysis(conf) +:
          DataSourceAnalysis(conf) +:
          ResolveRelationsExtended +:
          AnalyzeMutableOperations(session, this) +:
          ResolveQueryHints(session) +:
          ResolveSQLOnFile(session) +:
           customResolutionRules

    override val extendedCheckRules: Seq[LogicalPlan => Unit] =
      PrePutCheck+:
        customCheckRules
  }

  override protected def planner: SparkPlanner =
    new SnappySparkPlanner(session, conf, experimentalMethods)

  override protected def optimizer: Optimizer = new SparkOptimizer(catalog, experimentalMethods) {
    override def batches: Seq[Batch] = {
      implicit val ss = session
      var insertedSnappyOpts = 0
      val modified = super.batches.map {
        case batch if batch.name
          .equalsIgnoreCase("Operator Optimization before Inferring Filters") =>
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

  private def externalCatalog: SnappyExternalCatalog =
    session.sharedState.externalCatalog.asInstanceOf[SnappyExternalCatalog]

  @transient
  val contextFunctions: SnappyContextFunctions = new SnappyContextFunctions

  protected lazy val snappySharedState: SnappySharedState = session.sharedState

  private[internal] lazy val metadataHive = snappySharedState.metadataHive().newSession()

  override lazy val sqlParser: ParserInterface = contextFunctions.newSQLParser(session)

  private[sql] var disableStoreOptimizations: Boolean = false

  override protected lazy val conf: SQLConf = new SnappyConf(session)

  /**
    * Create a [[SnappyStoreHiveCatalog]].
    */
  override protected lazy val catalog: SnappyStoreHiveCatalog = {
    val cat = SnappyContext.getClusterMode(session.sparkContext) match {
      case ThinClientConnectorMode(_, _) =>
        new SnappyConnectorCatalog(
          externalCatalog,
          session,
          metadataHive,
          session.sharedState.globalTempViewManager,
          functionRegistry,
          conf,
          SessionState.newHadoopConf(session.sparkContext.hadoopConfiguration, conf),
          sqlParser,
          resourceLoader)
      case _ =>
        new SnappyStoreHiveCatalog(
          externalCatalog,
          session,
          metadataHive,
          session.sharedState.globalTempViewManager,
          functionRegistry,
          conf,
          SessionState.newHadoopConf(session.sparkContext.hadoopConfiguration, conf),
          sqlParser,
          resourceLoader)
    }
    parentState.foreach(_.catalog.copyStateTo(cat))
    cat
  }

  // copy of ConstantFolding that will turn a constant up/down cast into
  // a static value.
  object TokenizedLiteralFolding extends Rule[LogicalPlan] {

    private def foldExpression(e: Expression): DynamicFoldableExpression = {
      // lets mark child params foldable false so that nested expression doesn't
      // attempt to wrap.
      e.foreach {
        case p: TokenizedLiteral => p.markFoldable(false)
        case _ =>
      }
      DynamicFoldableExpression(e)
    }

    def apply(plan: LogicalPlan): LogicalPlan = {
      val foldedLiterals = new ArrayBuffer[TokenizedLiteral](4)
      val newPlan = plan transformAllExpressions {
        case p: TokenizedLiteral =>
          if (!p.foldable) {
            p.markFoldable(true)
            foldedLiterals += p
          }
          p
        // also mark linking for scalar/predicate subqueries and disable plan caching
        case s@(_: ScalarSubquery /* | _: PredicateSubquery */) =>
          session.linkPartitionsToBuckets(flag = true)
          session.planCaching = false
          s
      } transform {
        case q: LogicalPlan => q transformExpressionsDown {
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
        }
      }
      for (l <- foldedLiterals) l.markFoldable(false)
      newPlan
    }
  }

  override def createQueryExecution: LogicalPlan => QueryExecution = { plan =>
    clearExecutionData()
    newQueryExecution(plan)
  }

    protected[sql] def queryPreparations(topLevel: Boolean): Seq[Rule[SparkPlan]] = Seq(
      python.ExtractPythonUDFs,
      PlanSubqueries(session),
      EnsureRequirements(session.sessionState.conf),
      CollapseCollocatedPlans(session),
      CollapseCodegenStages(session.sessionState.conf),
      InsertCachedPlanFallback(session, topLevel),
      ReuseExchange(session.sessionState.conf))

    protected def newQueryExecution(plan: LogicalPlan): QueryExecution = {
      new QueryExecution(session, plan) {

        session.addContextObject(SnappySession.ExecutionKey,
          () => newQueryExecution(plan))

        override protected def preparations: Seq[Rule[SparkPlan]] =
          queryPreparations(topLevel = true)
      }
    }

    private[spark] def prepareExecution(plan: SparkPlan): SparkPlan = {
      queryPreparations(topLevel = false).foldLeft(plan) {
        case (sp, rule) => rule.apply(sp)
      }
    }

    private[spark] def clearExecutionData(): Unit = {
      conf.asInstanceOf[SnappyConf].refreshNumShufflePartitions()
      session.leaderPartitions.clear()
      session.clearContext()
    }

  def getTablePartitions(region: PartitionedRegion): Array[Partition] = {
    val leaderRegion = ColocationHelper.getLeaderRegion(region)
    session.leaderPartitions.computeIfAbsent(leaderRegion,
      new java.util.function.Function[PartitionedRegion, Array[Partition]] {
        override def apply(pr: PartitionedRegion): Array[Partition] = {
          val linkPartitionsToBuckets = session.hasLinkPartitionsToBuckets
          val preferPrimaries = session.preferPrimaries
          if (linkPartitionsToBuckets || preferPrimaries) {
            // also set the default shuffle partitions for this execution
            // to minimize exchange
            session.sessionState.conf.asInstanceOf[SnappyConf]
              .setExecutionShufflePartitions(region.getTotalNumberOfBuckets)
          }
          StoreUtils.getPartitionsPartitionedTable(session, pr,
            linkPartitionsToBuckets, preferPrimaries)
        }
      })
  }

  def getTablePartitions(region: CacheDistributionAdvisee): Array[Partition] =
    StoreUtils.getPartitionsReplicatedTable(session, region)

  case class SnappyDataSourceAnalysis(conf: SQLConf) extends Rule[LogicalPlan] with CastSupport {
    override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case CreateTable(tableDesc, mode, None) =>
        val userSpecifiedSchema: Option[StructType] =
          if (tableDesc.schema.isEmpty) None else {
          Some(SparkSession.getActiveSession.get.asInstanceOf[SnappySession].normalizeSchema
          (tableDesc.schema))
        }
        val options = Map.empty[String, String] ++ tableDesc.storage.properties

        val optionsWithPath: Map[String, String] = if (tableDesc.storage.locationUri.isDefined) {
          options + ("path" -> tableDesc.storage.locationUri.get.getPath)
        } else options
        val (provider, isBuiltIn) = SnappyContext.getBuiltInProvider(tableDesc.provider.get)
          CreateMetastoreTableUsing(tableDesc.identifier, None, userSpecifiedSchema,
            None, provider, mode != SaveMode.ErrorIfExists, optionsWithPath, isBuiltIn)

      case CreateTable(tableDesc, mode, Some(query)) =>
        val userSpecifiedSchema = SparkSession.getActiveSession.get
          .asInstanceOf[SnappySession].normalizeSchema(query.schema)
        val options = Map.empty[String, String] ++ tableDesc.storage.properties
        val (provider, isBuiltIn) = SnappyContext.getBuiltInProvider(tableDesc.provider.get)
        CreateMetastoreTableUsingSelect(tableDesc.identifier, None,
          Some(userSpecifiedSchema), None, provider, tableDesc.partitionColumnNames.toArray,
          mode, options, query, isBuiltIn)

      case CreateTableUsing(tableIdent, baseTable, userSpecifiedSchema, schemaDDL,
      provider, allowExisting, options, isBuiltIn) =>
        CreateMetastoreTableUsing(tableIdent, baseTable,
          userSpecifiedSchema, schemaDDL, provider, allowExisting, options, isBuiltIn)

      case CreateTableUsingSelect(tableIdent, baseTable, userSpecifiedSchema, schemaDDL,
      provider, partitionColumns, mode, options, query, isBuiltIn) =>
        CreateMetastoreTableUsingSelect(tableIdent, baseTable,
          userSpecifiedSchema, schemaDDL, provider, partitionColumns, mode,
          options, query, isBuiltIn)

      case DropTableOrView(isView: Boolean, ifExists, tableIdent) =>
        DropTableOrViewCommand(isView, ifExists, tableIdent)

      case TruncateManagedTable(ifExists, tableIdent) =>
        TruncateManagedTableCommand(ifExists, tableIdent)

      case AlterTableAddColumn(tableIdent, addColumn) =>
        AlterTableAddColumnCommand(tableIdent, addColumn)

      case AlterTableDropColumn(tableIdent, column) =>
        AlterTableDropColumnCommand(tableIdent, column)

      case CreateIndex(indexName, baseTable, indexColumns, options) =>
        CreateIndexCommand(indexName, baseTable, indexColumns, options)

      case DropIndex(ifExists, indexName) => DropIndexCommand(indexName, ifExists)

      case SetSchema(schemaName) => SetSchemaCommand(schemaName)

      case d@DeployCommand(_, _, _, _) => d

      case d@DeployJarCommand(_, _) => d

      case d@UnDeployCommand(_) => d

      case l@ListPackageJarsCommand(_) => l

      case SnappyStreamingActions(action, batchInterval) =>
        SnappyStreamingActionsCommand(action, batchInterval)

      case d@DMLExternalTable(_, storeRelation: LogicalRelation, insertCommand) =>
        ExternalTableDMLCmd(storeRelation, insertCommand, d.output)

     case InsertIntoTable(l@LogicalRelation(p: PlanInsertableRelation,
     _, _, _), part, query, overwrite, false) =>
       SnappyInsertIntoTable(l, part, query, overwrite, false)
    }
  }
  /**
    * Replaces [[UnresolvedRelation]]s with concrete relations from the catalog.
    */
  object ResolveRelationsExtended extends Rule[LogicalPlan] with PredicateHelper {
    def getTable(u: UnresolvedRelation): LogicalPlan = {
      try {
        catalog.lookupRelation(u.tableIdentifier)
      } catch {
        case _: NoSuchTableException =>
          u.failAnalysis(s"Table not found: ${u.tableName}")
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
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
          case r@LogicalRelation(scan: PartitionedDataSourceScan, _, _, _) =>
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

  case class AnalyzeMutableOperations(sparkSession: SparkSession,
      analyzer: Analyzer) extends Rule[LogicalPlan] with PredicateHelper {

    private def getKeyAttributes(table: LogicalPlan,
        child: LogicalPlan,
        plan: LogicalPlan): (Seq[NamedExpression], LogicalPlan, LogicalRelation) = {
      var tableName = ""
      val keyColumns = table.collectFirst {
        case lr@LogicalRelation(mutable: MutableRelation, _, _, _) =>
          val ks = mutable.getKeyColumns
          if (ks.isEmpty) {
            val currentKey = session.currentKey
            // if this is a row table, then fallback to direct execution
            mutable match {
              case _: UpdatableRelation if currentKey ne null =>
                return (Nil, DMLExternalTable(catalog.newQualifiedTableName(
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
        case lr@LogicalRelation(mutable: MutableRelation, _, _, _)
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
          val allReferences = newChild.references ++
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
      case d@DeleteFromTable(_, child) if child.resolved =>
        ColumnTableBulkOps.transformDeletePlan(sparkSession, d)
      case p@PutIntoTable(_, child) if child.resolved =>
        ColumnTableBulkOps.transformPutPlan(sparkSession, p)
    }

    private def analyzeQuery(query: LogicalPlan): LogicalPlan = {
      val qe = sparkSession.sessionState.executePlan(query)
      qe.assertAnalyzed()
      qe.analyzed
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
          session.linkPartitionsToBuckets(flag = true)
        case j: Join if !JoinStrategy.isLocalJoin(j) =>
          // disable for the entire query for consistency
          session.linkPartitionsToBuckets(flag = true)
        case _: InsertIntoTable | _: TableMutationPlan |
             LogicalRelation(_: IndexColumnFormatRelation, _, _, _) =>
          // disable for inserts/puts to avoid exchanges and indexes to work correctly
          session.linkPartitionsToBuckets(flag = true)
        case _ => // nothing for others
      }
      plan
    }
  }

  /**
    * Replaces [[UnresolvedRelation]]s if the plan is for direct query on files.
    */
  case class ResolveSQLOnFile(session: SnappySession) extends Rule[LogicalPlan] {
    private def maybeSQLFile(u: UnresolvedRelation): Boolean = {
      session.sessionState.conf.runSQLonFile && u.tableIdentifier.database.isDefined
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
      case u: UnresolvedRelation if maybeSQLFile(u) =>
        try {
          val dataSource = DataSource(
            session,
            paths = u.tableIdentifier.table :: Nil,
            className = u.tableIdentifier.database.get)

          // `dataSource.providingClass` may throw ClassNotFoundException, then the outer try-catch
          // will catch it and return the original plan, so that the analyzer can report table not
          // found later.
          val isFileFormat = classOf[FileFormat].isAssignableFrom(dataSource.providingClass)
          if (!isFileFormat ||
            dataSource.className.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER) {
            throw new AnalysisException("Unsupported data source type for direct query on files: " +
              s"${u.tableIdentifier.database.get}")
          }
          LogicalRelation(dataSource.resolveRelation())
        } catch {
          case _: ClassNotFoundException => u
          case e: Exception =>
            // the provider is valid, but failed to create a logical plan
            u.failAnalysis(e.getMessage)
        }
    }
  }

}

private[sql] final class PreprocessTableInsertOrPut(conf: SQLConf)
    extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // Check for SchemaInsertableRelation first
    case i@InsertIntoTable(l@LogicalRelation(r: SchemaInsertableRelation,
    _, _, _), _, child, _, _) if l.resolved && child.resolved =>
      r.insertableRelation(child.output) match {
        case Some(ir) =>
          val br = ir.asInstanceOf[BaseRelation]
          val relation = LogicalRelation(br, l.catalogTable.get)
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
        case l@LogicalRelation(ir: RowInsertableRelation, _, _, _) =>
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
        case l@LogicalRelation(dr: DeletableRelation, _, _, _) =>
          def comp(a: Attribute, targetCol: String): Boolean = a match {
            case ref: AttributeReference => targetCol.equals(ref.name.toUpperCase)
          }

          val expectedOutput = l.output
          if (!child.output.forall(a => expectedOutput.exists(e => comp(a, e.name.toUpperCase)))) {
            throw new AnalysisException(s"$l requires that the query in the " +
                "WHERE clause of the DELETE FROM statement " +
                "generates the same column name(s) as in its schema but found " +
                s"${child.output.mkString(",")} instead.")
          }
          l match {
            case LogicalRelation(ps: PartitionedDataSourceScan, _, _, _) =>
              if (!ps.partitionColumns.forall(a => child.output.exists(e =>
                comp(e, a.toUpperCase)))) {
                throw new AnalysisException(s"${child.output.mkString(",")}" +
                    s" columns in the WHERE clause of the DELETE FROM statement must " +
                    s"have all the parititioning column(s) ${ps.partitionColumns.mkString(",")}.")
              }
            case _ =>
          }
          castAndRenameChildOutputForPut(d, expectedOutput, dr, l, child)

        case l@LogicalRelation(dr: MutableRelation, _, _, _) =>
          val expectedOutput = l.output
          if (child.output.length != expectedOutput.length) {
            throw new AnalysisException(s"$l requires that the query in the " +
                "WHERE clause of the DELETE FROM statement " +
                "generates the same number of column(s) as in its schema but found " +
                s"${child.output.mkString(",")} instead.")
          }
          castAndRenameChildOutputForPut(d, expectedOutput, dr, l, child)
        case _ => d
      }

    // other cases handled like in PreprocessTableInsertion
    case i@InsertIntoTable(table, _, child, _, _)
      if table.resolved && child.resolved => table match {
        case relation: UnresolvedCatalogRelation =>
        val metadata = relation.tableMeta
        preProcess(i, relation = null, metadata.identifier.quotedString,
          metadata.partitionColumnNames)
      case LogicalRelation(h: HadoopFsRelation, _, identifier, _) =>
        val tblName = identifier.map(_.identifier.quotedString).getOrElse("unknown")
        preProcess(i, h, tblName, h.partitionSchema.map(_.name))
      case LogicalRelation(ir: InsertableRelation, _, identifier, _) =>
        val tblName = identifier.map(_.identifier.quotedString).getOrElse("unknown")
        preProcess(i, ir, tblName, Nil)
      case _ => i
    }
  }

  private def preProcess(
      insert: InsertIntoTable,
      relation: BaseRelation,
      tblName: String,
      partColNames: Seq[String]): InsertIntoTable = {

    // val expectedColumns = insert

    val normalizedPartSpec = PartitioningUtils.normalizePartitionSpec(
      insert.partition, partColNames, tblName, conf.resolver)

    val expectedColumns = {
      val staticPartCols = normalizedPartSpec.filter(_._2.isDefined).keySet
      insert.table.output.filterNot(a => staticPartCols.contains(a.name))
    }

    if (expectedColumns.length != insert.query.schema.length) {
      throw new AnalysisException(
        s"Cannot insert into table $tblName because the number of columns are different: " +
            s"need ${expectedColumns.length} columns, " +
            s"but query has ${insert.query.schema.length} columns.")
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
      castAndRenameChildOutput(insert.copy(partition = normalizedPartSpec), expectedColumns)

//      expectedColumns.map(castAndRenameChildOutput(insert, _, relation, null,
//        child)).getOrElse(insert)
    } else {
      // All partition columns are dynamic because because the InsertIntoTable
      // command does not explicitly specify partitioning columns.
      castAndRenameChildOutput(insert, expectedColumns)
          .copy(partition = partColNames.map(_ -> None).toMap)
//      expectedColumns.map(castAndRenameChildOutput(insert, _, relation, null,
//        child)).getOrElse(insert).copy(partition = partColNames
//          .map(_ -> None).toMap)
    }
  }

  /**
   * If necessary, cast data types and rename fields to the expected
   * types and names.
   */
  // TODO: do we really need to rename?
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
      case i: InsertIntoTable => i.copy(query = Project(newChildOutput,
        child)).asInstanceOf[T]
    }
  }

  private def castAndRenameChildOutput(
      insert: InsertIntoTable,
      expectedOutput: Seq[Attribute]): InsertIntoTable = {
    val newChildOutput = expectedOutput.zip(insert.query.output).map {
      case (expected, actual) =>
        if (expected.dataType.sameType(actual.dataType) &&
            expected.name == actual.name &&
            expected.metadata == actual.metadata) {
          actual
        } else {
          // Renaming is needed for handling the following cases like
          // 1) Column names/types do not match, e.g., INSERT INTO TABLE tab1 SELECT 1, 2
          // 2) Target tables have column metadata
          Alias(Cast(actual, expected.dataType), expected.name)(
            explicitMetadata = Option(expected.metadata))
        }
    }

    if (newChildOutput == insert.query.output) insert
    else {
      insert.copy(query = Project(newChildOutput, insert.query))
    }
  }
}

class SnappySparkPlanner(val snappySession: SnappySession, conf: SQLConf,
                         experimentalMethods: ExperimentalMethods)
  extends SparkPlanner(snappySession.sparkContext, conf, experimentalMethods)
    with SnappyStrategies {

  val sampleSnappyCase: PartialFunction[LogicalPlan, Seq[SparkPlan]] = {
    case _ => Nil
  }

  private val storeOptimizedRules: Seq[Strategy] =
    Seq(SnappyStoreStrategy, SnappyAggregation, HashJoinStrategies)

  override def strategies: Seq[Strategy] =
    Seq(SnappyStrategies, SnappyStoreStrategy, StreamQueryStrategy) ++
      storeOptimizedRules ++
      super.strategies
}

// copy of ConstantFolding that will turn a constant up/down cast into
// a static value.
object ParamLiteralFolding extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case p: ParamLiteral => p.markFoldable(true)
      p
  } transform {
    case q: LogicalPlan => q transformExpressionsDown {
      // ignore leaf ParamLiteral & Literal
      case p: ParamLiteral => p
      case l: Literal => l
      // Wrap expressions that are foldable.
      case e if e.foldable =>
        // lets mark child params foldable false so that nested expression doesn't
        // attempt to wrap.
        e.foreach {
          case p: ParamLiteral => p.markFoldable(false)
          case _ =>
        }
        DynamicFoldableExpression(e)
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
          case LogicalRelation(_, _, _, _) |
               LogicalDStreamPlan(_, _) => win
          case _ => duration = d
            slide = s
            transformed = true
            win.child
        }
      case c@(LogicalRelation(_, _, _, _) |
              LogicalDStreamPlan(_, _)) =>
        if (transformed) {
          transformed = false
          WindowLogicalPlan(duration, slide, c, transformed = true)
        } else c
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

private[sql] case object PrePutCheck extends (LogicalPlan => Unit) {

  def apply(plan: LogicalPlan): Unit = {
    plan.foreach {
      case PutIntoTable(LogicalRelation(t: RowPutRelation, _, _, _), query) =>
        // Get all input data source relations of the query.
        val srcRelations = query.collect {
          case LogicalRelation(src: BaseRelation, _, _, _) => src
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
case class SnappyInsertIntoTable(table: LogicalPlan, partition: Map[String, Option[String]],
  query: LogicalPlan, overwrite: Boolean, ifPartitionNotExists: Boolean)
  extends LogicalPlan {
  override def children: Seq[LogicalPlan] = query :: Nil
  override def output: Seq[Attribute] = Seq.empty
  override lazy val resolved: Boolean = true
}

/**
 * Rule to "normalize" ParamLiterals for the case of aggregation expression being used
 * in projection. Specifically the ParamLiterals from aggregations need to be replaced
 * into projection so that latter can be resolved successfully in plan execution
 * because ParamLiterals will match expression only by position and not value at the
 * time of execution. This rule is useful only before plan caching after parsing.
 *
 * See Spark's PhysicalAggregation rule for more details.
 */
object ResolveAggregationExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case Aggregate(groupingExpressions, resultExpressions, child) =>
      // Replace any ParamLiterals in the original resultExpressions with any matching ones
      // in groupingExpressions matching on the value like a Literal rather than position.
      val newResultExpressions = resultExpressions.map { expr =>
        expr.transformDown {
          case e: AggregateExpression => e
          case expression =>
            groupingExpressions.collectFirst {
              case p: ParamLiteral if p.equals(expression) =>
                expression.asInstanceOf[ParamLiteral].tokenized = true
                p.tokenized = true
                p
              case e if e.semanticEquals(expression) =>
                // collect ParamLiterals from grouping expressions and apply
                // to result expressions in the same order
                val literals = new ArrayBuffer[ParamLiteral](2)
                e.transformDown {
                  case p: ParamLiteral => literals += p; p
                }
                if (literals.nonEmpty) {
                  val iter = literals.iterator
                  expression.transformDown {
                    case p: ParamLiteral =>
                      val newLiteral = iter.next()
                      assert(newLiteral.equals(p))
                      p.tokenized = true
                      newLiteral.tokenized = true
                      newLiteral
                  }
                } else expression
            } match {
              case Some(e) => e
              case _ => expression
            }
        }.asInstanceOf[NamedExpression]
      }
      Aggregate(groupingExpressions, newResultExpressions, child)
  }
}
