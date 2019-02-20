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

import java.lang.reflect.Field

import scala.collection.mutable

import io.snappydata.{HintName, QueryHint}

import org.apache.spark.SparkContext
import org.apache.spark.deploy.SparkSubmitUtils
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{UnresolvedRelation, UnresolvedSubqueryColumnAliases, UnresolvedTableValuedFunction}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStatistics, CatalogStorageFormat, CatalogTable, CatalogTableType, FunctionResource}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeGenerator, CodegenContext, GeneratedClass}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, CurrentRow, ExprId, Expression, ExpressionInfo, FrameType, Generator, NamedExpression, NullOrdering, SortDirection, SortOrder, SpecifiedWindowFrame, UnaryMinus, UnboundedFollowing, UnboundedPreceding}
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.execution.command.{ClearCacheCommand, CreateFunctionCommand, DescribeTableCommand}
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation, PreWriteCheck}
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.execution.ui.{SQLAppStatusListener, SQLAppStatusStore, SnappySQLAppListener}
import org.apache.spark.sql.execution.{CacheManager, SparkOptimizer, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{DataType, Metadata, StructType}
import org.apache.spark.storage.StorageLevel

/**
 * Implementation of [[SparkInternals]] for Spark 2.3.2.
 */
class Spark232Internals extends SparkInternals {

  private val codegenContextClassFunctions: Field = {
    val f = classOf[CodegenContext].getDeclaredField("classFunctions")
    f.setAccessible(true)
    f
  }

  override def version: String = "2.3.2"

  override def uncacheQuery(spark: SparkSession, plan: LogicalPlan, blocking: Boolean): Unit = {
    spark.sharedState.cacheManager.uncacheQuery(spark, plan, blocking)
  }

  override def mapExpressions(plan: LogicalPlan, f: Expression => Expression): LogicalPlan = {
    plan.mapExpressions(f)
  }

  override def registerFunction(session: SparkSession, name: FunctionIdentifier,
      info: ExpressionInfo, function: Seq[Expression] => Expression): Unit = {
    session.sessionState.functionRegistry.registerFunction(name, info, function)
  }

  override def addClassField(ctx: CodegenContext, javaType: String,
      varName: String, initFunc: String => String,
      forceInline: Boolean, useFreshName: Boolean): String = {
    ctx.addMutableState(javaType, varName, initFunc, forceInline, useFreshName)
  }

  override def addFunction(ctx: CodegenContext, funcName: String, funcCode: String,
      inlineToOuterClass: Boolean = false): String = {
    ctx.addNewFunction(funcName, funcCode, inlineToOuterClass)
  }

  override def isFunctionAddedToOuterClass(ctx: CodegenContext, funcName: String): Boolean = {
    codegenContextClassFunctions.get(ctx).asInstanceOf[
        mutable.Map[String, mutable.Map[String, String]]].get(ctx.outerClassName) match {
      case Some(m) => m.contains(funcName)
      case None => false
    }
  }

  override def splitExpressions(ctx: CodegenContext, expressions: Seq[String]): String = {
    ctx.splitExpressionsWithCurrentInputs(expressions)
  }

  override def resetCopyResult(ctx: CodegenContext): Unit = {}

  override def isPredicateSubquery(expr: Expression): Boolean = false

  override def copyPredicateSubquery(expr: Expression, newPlan: LogicalPlan,
      newExprId: ExprId): Expression = {
    throw new UnsupportedOperationException(
      "unexpected copyPredicateSubquery call in Spark 2.3 module")
  }

  override def newWholeStagePlan(plan: SparkPlan): WholeStageCodegenExec = {
    WholeStageCodegenExec(plan)(codegenStageId = 0)
  }

  override def newCaseInsensitiveMap(map: Map[String, String]): Map[String, String] = {
    CaseInsensitiveMap[String](map)
  }

  // TODO: SW: inhibit SQLTab attach in SharedState.statusStore and instead do it
  // here for embedded mode in the second call so that security policies are applied to the tab
  def createAndAttachSQLListener(sparkContext: SparkContext): Unit = {
    // SQLAppStatusListener is created in the constructor of SharedState that needs to be overridden
  }

  def createAndAttachSQLListener(state: SharedState): Unit = {
    // replace inside SQLAppStatusStore as well as change on the Spark ListenerBus
    val listenerField = classOf[SQLAppStatusStore].getDeclaredField("listener")
    listenerField.setAccessible(true)
    listenerField.get(state.statusStore).asInstanceOf[Option[SQLAppStatusListener]] match {
      case Some(_: SnappySQLAppListener) => // already changed
      case Some(_: SQLAppStatusListener) =>
        val newListener = new SnappySQLAppListener(state.sparkContext)
        // update on ListenerBus
        state.sparkContext.listenerBus.findListenersByClass[SQLAppStatusListener]().foreach(
          state.sparkContext.removeSparkListener)
        state.sparkContext.listenerBus.addToStatusQueue(newListener)
        listenerField.set(state.statusStore, newListener)
      case _ =>
    }
  }

  def clearSQLListener(): Unit = {
    // no global SQLListener in Spark 2.3
  }

  override def createViewSQL(session: SparkSession, plan: LogicalPlan,
      originalText: Option[String]): String = originalText match {
    case Some(viewSQL) => viewSQL
    case None =>
      throw new AnalysisException(
        "Cannot create a persisted VIEW from the Dataset API")
  }

  override def createView(desc: CatalogTable, output: Seq[Attribute],
      child: LogicalPlan): LogicalPlan = View(desc, output, child)

  override def newCreateFunctionCommand(schemaName: Option[String], functionName: String,
      className: String, resources: Seq[FunctionResource], isTemp: Boolean,
      ignoreIfExists: Boolean, replace: Boolean): LogicalPlan = {
    CreateFunctionCommand(schemaName, functionName, className, resources, isTemp,
      ignoreIfExists, replace)
  }

  override def newDescribeTableCommand(table: TableIdentifier,
      partitionSpec: Map[String, String], isExtended: Boolean): LogicalPlan = {
    DescribeTableCommand(table, partitionSpec, isExtended)
  }

  override def newClearCacheCommand(): LogicalPlan = ClearCacheCommand()

  override def resolveMavenCoordinates(coordinates: String, remoteRepos: Option[String],
      ivyPath: Option[String], exclusions: Seq[String]): String = {
    SparkSubmitUtils.resolveMavenCoordinates(coordinates,
      SparkSubmitUtils.buildIvySettings(remoteRepos, ivyPath), exclusions)
  }

  override def copyAttribute(attr: AttributeReference)(name: String,
      dataType: DataType, nullable: Boolean, metadata: Metadata): AttributeReference = {
    attr.copy(name = name, dataType = dataType, nullable = nullable, metadata = metadata)(
      exprId = attr.exprId, qualifier = attr.qualifier)
  }

  override def newInsertPlanWithCountOutput(table: LogicalPlan,
      partition: Map[String, Option[String]], child: LogicalPlan,
      overwrite: Boolean, ifNotExists: Boolean): LogicalPlan = {
    new Insert23(table, partition, child, overwrite, ifNotExists)
  }

  override def newGroupingSet(groupingSets: Seq[Seq[Expression]],
      groupByExprs: Seq[Expression], child: LogicalPlan,
      aggregations: Seq[NamedExpression]): LogicalPlan = {
    GroupingSets(groupingSets, groupByExprs, child, aggregations)
  }

  override def newUnresolvedRelation(tableIdentifier: TableIdentifier,
      alias: Option[String]): LogicalPlan = alias match {
    case None => UnresolvedRelation(tableIdentifier)
    case Some(a) => SubqueryAlias(a, UnresolvedRelation(tableIdentifier))
  }

  override def newSubqueryAlias(alias: String, child: LogicalPlan): SubqueryAlias = {
    SubqueryAlias(alias, child)
  }

  override def newUnresolvedColumnAliases(outputColumnNames: Seq[String],
      child: LogicalPlan): LogicalPlan = {
    if (outputColumnNames.isEmpty) child
    else UnresolvedSubqueryColumnAliases(outputColumnNames, child)
  }

  override def newSortOrder(child: Expression, direction: SortDirection,
      nullOrdering: NullOrdering): SortOrder = {
    SortOrder(child, direction, nullOrdering, Set.empty)
  }

  override def newRepartitionByExpression(partitionExpressions: Seq[Expression],
      numPartitions: Int, child: LogicalPlan): RepartitionByExpression = {
    RepartitionByExpression(partitionExpressions, child, numPartitions)
  }

  override def newUnresolvedTableValuedFunction(functionName: String,
      functionArgs: Seq[Expression], outputNames: Seq[String]): UnresolvedTableValuedFunction = {
    UnresolvedTableValuedFunction(functionName, functionArgs, outputNames)
  }

  override def newFrameBoundary(boundaryType: FrameBoundaryType.Type,
      num: Option[Expression]): Any = {
    boundaryType match {
      case FrameBoundaryType.UnboundedPreceding => UnboundedPreceding
      case FrameBoundaryType.ValuePreceding => UnaryMinus(num.get)
      case FrameBoundaryType.CurrentRow => CurrentRow
      case FrameBoundaryType.UnboundedFollowing => UnboundedFollowing
      case FrameBoundaryType.ValueFollowing => num.get
    }
  }

  override def newSpecifiedWindowFrame(frameType: FrameType, frameStart: Any,
      frameEnd: Any): SpecifiedWindowFrame = {
    SpecifiedWindowFrame(frameType, frameStart.asInstanceOf[Expression],
      frameEnd.asInstanceOf[Expression])
  }

  override def newLogicalPlanWithHints(child: LogicalPlan,
      hints: Map[QueryHint.Type, HintName.Type]): LogicalPlanWithHints = {
    new ResolvedPlanWithHints(child, hints)
  }

  override def isHintPlan(plan: LogicalPlan): Boolean = plan.isInstanceOf[ResolvedHint]

  override def getHints(plan: LogicalPlan): Map[QueryHint.Type, HintName.Type] = plan match {
    case p: ResolvedPlanWithHints => p.allHints
    case _: ResolvedHint =>
      // only broadcast supported
      Map(QueryHint.JoinType -> HintName.JoinType_Broadcast)
    case _ => Map.empty
  }

  override def isBroadcastable(plan: LogicalPlan): Boolean = {
    // Spark now uses the UnresolvedHint/ResolvedHint infrastructure and not a fixed flag
    false
  }

  override def newOneRowRelation(): LogicalPlan = OneRowRelation()

  override def newGeneratePlan(generator: Generator, outer: Boolean, qualifier: Option[String],
      generatorOutput: Seq[Attribute], child: LogicalPlan): LogicalPlan = {
    Generate(generator, unrequiredChildIndex = Nil, outer, qualifier, generatorOutput, child)
  }

  override def writeToDataSource(ds: DataSource, mode: SaveMode,
      data: Dataset[Row]): BaseRelation = {
    ds.writeAndRead(mode, data.planWithBarrier, data.planWithBarrier.output.map(_.name),
      data.queryExecution.executedPlan)
  }

  override def newLogicalRelation(relation: BaseRelation,
      expectedOutputAttributes: Option[Seq[AttributeReference]],
      catalogTable: Option[CatalogTable], isStreaming: Boolean): LogicalRelation = {
    val output = expectedOutputAttributes match {
      case None => relation.schema.toAttributes
      case Some(attrs) => attrs
    }
    LogicalRelation(relation, output, catalogTable, isStreaming)
  }

  // scalastyle:off

  override def newCatalogTable(identifier: TableIdentifier, tableType: CatalogTableType,
      storage: CatalogStorageFormat, schema: StructType, provider: Option[String],
      partitionColumnNames: Seq[String], bucketSpec: Option[BucketSpec],
      owner: String, createTime: Long, lastAccessTime: Long, properties: Map[String, String],
      stats: Option[(BigInt, Option[BigInt], Map[String, ColumnStat])],
      viewOriginalText: Option[String], viewText: Option[String],
      comment: Option[String], unsupportedFeatures: Seq[String],
      tracksPartitionsInCatalog: Boolean, schemaPreservesCase: Boolean,
      ignoredProperties: Map[String, String]): CatalogTable = {
    val statistics = stats match {
      case None => None
      case Some(s) => Some(CatalogStatistics(s._1, s._2, s._3))
    }
    CatalogTable(identifier, tableType, storage, schema, provider, partitionColumnNames,
      bucketSpec, owner, createTime, lastAccessTime, createVersion = "", properties, statistics,
      viewText, comment, unsupportedFeatures, tracksPartitionsInCatalog,
      schemaPreservesCase, ignoredProperties)
  }

  // scalastyle:on

  override def catalogTableViewOriginalText(catalogTable: CatalogTable): Option[String] = None

  override def catalogTableSchemaPreservesCase(catalogTable: CatalogTable): Boolean =
    catalogTable.schemaPreservesCase

  override def catalogTableIgnoredProperties(catalogTable: CatalogTable): Map[String, String] =
    catalogTable.ignoredProperties

  override def newCatalogTableWithViewOriginalText(catalogTable: CatalogTable,
      viewOriginalText: Option[String]): CatalogTable = catalogTable

  override def newShuffleExchange(newPartitioning: Partitioning, child: SparkPlan): Exchange = {
    ShuffleExchangeExec(newPartitioning, child)
  }

  override def getStatistics(plan: LogicalPlan): Statistics = plan.stats

  override def supportsPartial(aggregate: AggregateFunction): Boolean = true

  override def planAggregateWithoutPartial(groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression], resultExpressions: Seq[NamedExpression],
      planChild: () => SparkPlan): Seq[SparkPlan] = {
    throw new UnsupportedOperationException(
      "unexpected planAggregateWithoutPartial call in Spark 2.3 module")
  }

  override def compile(code: CodeAndComment): GeneratedClass = CodeGenerator.compile(code)._1

  override def newJSONOptions(parameters: Map[String, String],
      session: Option[SparkSession]): JSONOptions = session match {
    case None =>
      new JSONOptions(parameters,
        SQLConf.SESSION_LOCAL_TIMEZONE.defaultValue.get,
        SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD.defaultValue.get)
    case Some(sparkSession) =>
      new JSONOptions(parameters,
        sparkSession.sessionState.conf.sessionLocalTimeZone,
        sparkSession.sessionState.conf.columnNameOfCorruptRecord)
  }

  override def newSparkOptimizer(sessionState: SnappySessionState): SparkOptimizer = {
    new SparkOptimizer(sessionState.catalog, sessionState.experimentalMethods)
        with DefaultOptimizer {
      override def state: SnappySessionState = sessionState
    }
  }

  override def newPreWriteCheck(sessionState: SnappySessionState): LogicalPlan => Unit = {
    PreWriteCheck
  }

  override def newCacheManager(): CacheManager = new SnappyCacheManager
}

/**
 * Simple extension to CacheManager to enable clearing cached plan on cache create/drop.
 */
final class SnappyCacheManager extends CacheManager {

  override def cacheQuery(query: Dataset[_], tableName: Option[String],
      storageLevel: StorageLevel): Unit = {
    super.cacheQuery(query, tableName, storageLevel)
    // clear plan cache since cached representation can change existing plans
    query.sparkSession.asInstanceOf[SnappySession].clearPlanCache()
  }

  override def uncacheQuery(session: SparkSession, plan: LogicalPlan, blocking: Boolean): Unit = {
    super.uncacheQuery(session, plan, blocking)
    session.asInstanceOf[SnappySession].clearPlanCache()
  }

  override def recacheByPlan(session: SparkSession, plan: LogicalPlan): Unit = {
    super.recacheByPlan(session, plan)
    session.asInstanceOf[SnappySession].clearPlanCache()
  }

  override def recacheByPath(session: SparkSession, resourcePath: String): Unit = {
    super.recacheByPath(session, resourcePath)
    session.asInstanceOf[SnappySession].clearPlanCache()
  }
}
