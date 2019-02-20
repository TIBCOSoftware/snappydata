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
package org.apache.spark.sql

import io.snappydata.{HintName, QueryHint}

import org.apache.spark.sql.catalyst.analysis.UnresolvedTableValuedFunction
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogTableType, FunctionResource}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodegenContext, GeneratedClass}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ExprId, Expression, ExpressionInfo, FrameType, Generator, NamedExpression, NullOrdering, SortDirection, SortOrder, SpecifiedWindowFrame}
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan, RepartitionByExpression, Statistics, SubqueryAlias}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.{CacheManager, SparkOptimizer, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.internal.{LogicalPlanWithHints, SharedState, SnappySessionState}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{DataType, Metadata, StructType}
import org.apache.spark.{Logging, SparkContext}

/**
 * Common interface for Spark internal API used by the core module.
 *
 * Note that this interface only intends to achieve source-level
 * compatibility meaning that entire core module with the specific
 * implementation of this interface has to be re-compiled in entirety
 * for separate Spark versions and one cannot just combine core module
 * compiled for a Spark version with an implementation of this
 * interface for another Spark version.
 */
trait SparkInternals extends Logging {

  final val emptyFunc: String => String = _ => ""

  if (version != SparkSupport.DEFAULT_VERSION) {
    logInfo(s"SnappyData: loading support for Spark $version")
  }

  /**
   * Version of this implementation. This should always match
   * the result of SparkContext.version for current SparkContext.
   */
  def version: String

  /**
   * Remove any cached data of Dataset.persist for given plan.
   */
  def uncacheQuery(spark: SparkSession, plan: LogicalPlan, blocking: Boolean): Unit

  /**
   * Apply a mapping function on all expressions in the given logical plan
   * and return the updated plan.
   */
  def mapExpressions(plan: LogicalPlan, f: Expression => Expression): LogicalPlan

  /**
   * Register an inbuilt function in the session function registry.
   */
  def registerFunction(session: SparkSession, name: FunctionIdentifier,
      info: ExpressionInfo, function: Seq[Expression] => Expression): Unit

  /**
   * Add a mutable state variable to given [[CodegenContext]] and return the variable name.
   */
  def addClassField(ctx: CodegenContext, javaType: String,
      varName: String, initFunc: String => String = emptyFunc,
      forceInline: Boolean = false, useFreshName: Boolean = true): String

  /**
   * Adds a function to the generated class. In newer Spark versions, if the code for outer class
   * grows too large, the function will be inlined into a new private, inner class,
   * and a class-qualified name for the function will be returned.
   */
  def addFunction(ctx: CodegenContext, funcName: String, funcCode: String,
      inlineToOuterClass: Boolean = false): String

  /**
   * Returns true if a given function has already been added to the outer class.
   */
  def isFunctionAddedToOuterClass(ctx: CodegenContext, funcName: String): Boolean

  /**
   * Split the generated code for given expressions into multiple methods assuming
   * [[CodegenContext.INPUT_ROW]] has been used (else return inline expression code).
   */
  def splitExpressions(ctx: CodegenContext, expressions: Seq[String]): String

  /**
   * Reset CodegenContext's copyResult to false if required (skipped in newer Spark versions).
   */
  def resetCopyResult(ctx: CodegenContext): Unit

  /**
   * Check if the current expression is a predicate sub-query.
   */
  def isPredicateSubquery(expr: Expression): Boolean

  /**
   * Make a copy of given predicate sub-query with new plan and [[ExprId]].
   */
  def copyPredicateSubquery(expr: Expression, newPlan: LogicalPlan, newExprId: ExprId): Expression

  /**
   * Compile the given [[SparkPlan]] using whole-stage code generation and return
   * the generated code along with the [[CodegenContext]] use for code generation.
   */
  def newWholeStagePlan(plan: SparkPlan): WholeStageCodegenExec

  /**
   * Create a new immutable map whose keys are case-insensitive from a given map.
   */
  def newCaseInsensitiveMap(map: Map[String, String]): Map[String, String]

  /**
   * Create a new SQL listener with SnappyData extensions and attach to the SparkUI.
   * The extension provides handling of:
   * <p>
   * a) combining the two part execution with CachedDataFrame where first execution
   * does the caching ("prepare" phase) along with the actual execution while subsequent
   * executions only do the latter
   * <p>
   * b) shortens the SQL string to display properly in the UI (CachedDataFrame already
   * takes care of posting the SQL string rather than method name unlike Spark).
   * <p>
   * This variant is invoked before initialization of SharedState for Spark versions
   * where listener is attached independently of SharedState before latter is created.
   */
  def createAndAttachSQLListener(sparkContext: SparkContext): Unit

  /**
   * Create a new SQL listener with SnappyData extensions and attach to the SparkUI.
   * The extension provides handling of:
   * <p>
   * a) combining the two part execution with CachedDataFrame where first execution
   * does the caching ("prepare" phase) along with the actual execution while subsequent
   * executions only do the latter
   * <p>
   * b) shortens the SQL string to display properly in the UI (CachedDataFrame already
   * takes care of posting the SQL string rather than method name unlike Spark).
   * <p>
   * This variant is invoked after initialization of SharedState for Spark versions
   * where listener is attached as part of SharedState creation.
   */
  def createAndAttachSQLListener(state: SharedState): Unit

  /**
   * Clear any global SQL listener.
   */
  def clearSQLListener(): Unit

  /**
   * Create a SQL string appropriate for a persisted VIEW plan and storage in catalog
   * from a given [[LogicalPlan]] for the VIEW.
   */
  def createViewSQL(session: SparkSession, plan: LogicalPlan,
      originalText: Option[String]): String

  /**
   * Create a [[LogicalPlan]] for CREATE VIEW.
   */
  def createView(desc: CatalogTable, output: Seq[Attribute], child: LogicalPlan): LogicalPlan

  /**
   * Create a [[LogicalPlan]] for CREATE FUNCTION.
   */
  def newCreateFunctionCommand(schemaName: Option[String], functionName: String,
      className: String, resources: Seq[FunctionResource], isTemp: Boolean,
      ignoreIfExists: Boolean, replace: Boolean): LogicalPlan

  /**
   * Create a [[LogicalPlan]] for DESCRIBE TABLE.
   */
  def newDescribeTableCommand(table: TableIdentifier, partitionSpec: Map[String, String],
      isExtended: Boolean): LogicalPlan

  /**
   * Create a [[LogicalPlan]] for CLEAR CACHE.
   */
  def newClearCacheCommand(): LogicalPlan

  /**
   * Resolve Maven coordinates for a package, cache the jars and return the required CLASSPATH.
   */
  def resolveMavenCoordinates(coordinates: String, remoteRepos: Option[String],
      ivyPath: Option[String], exclusions: Seq[String]): String

  /**
   * Create a copy of [[AttributeReference]] with given new arguments.
   */
  def copyAttribute(attr: AttributeReference)(name: String = attr.name,
      dataType: DataType = attr.dataType, nullable: Boolean = attr.nullable,
      metadata: Metadata = attr.metadata): AttributeReference

  /**
   * Create a new INSERT plan that has a LONG count of rows as its output.
   */
  def newInsertPlanWithCountOutput(table: LogicalPlan, partition: Map[String, Option[String]],
      child: LogicalPlan, overwrite: Boolean, ifNotExists: Boolean): LogicalPlan

  /**
   * Create an expression for GROUPING SETS.
   */
  def newGroupingSet(groupingSets: Seq[Seq[Expression]], groupByExprs: Seq[Expression],
      child: LogicalPlan, aggregations: Seq[NamedExpression]): LogicalPlan

  /**
   * Create a new unresolved relation (Table/View/Alias).
   */
  def newUnresolvedRelation(tableIdentifier: TableIdentifier, alias: Option[String]): LogicalPlan

  /**
   * Create an alias for a sub-query.
   */
  def newSubqueryAlias(alias: String, child: LogicalPlan): SubqueryAlias

  /**
   * Create a plan for column aliases in a table/sub-query/...
   * Not supported by older Spark versions.
   */
  def newUnresolvedColumnAliases(outputColumnNames: Seq[String],
      child: LogicalPlan): LogicalPlan

  /**
   * Create a [[SortOrder]].
   */
  def newSortOrder(child: Expression, direction: SortDirection,
      nullOrdering: NullOrdering): SortOrder

  /**
   * Create a new [[LogicalPlan]] for REPARTITION.
   */
  def newRepartitionByExpression(partitionExpressions: Seq[Expression],
      numPartitions: Int, child: LogicalPlan): RepartitionByExpression

  /**
   * Create a new unresolved table value function.
   */
  def newUnresolvedTableValuedFunction(functionName: String, functionArgs: Seq[Expression],
      outputNames: Seq[String]): UnresolvedTableValuedFunction

  /**
   * Create a new frame boundary. This is a FrameBoundary is older Spark versions
   * while newer ones use an Expression instead.
   */
  def newFrameBoundary(boundaryType: FrameBoundaryType.Type,
      num: Option[Expression] = None): Any

  /**
   * Create a new [[SpecifiedWindowFrame]] given the [[FrameType]] and start/end frame
   * boundaries as returned by [[newFrameBoundary]].
   */
  def newSpecifiedWindowFrame(frameType: FrameType,
      frameStart: Any, frameEnd: Any): SpecifiedWindowFrame

  /**
   * Create a new wrapper [[LogicalPlan]] that encapsulates an arbitrary set of hints.
   */
  def newLogicalPlanWithHints(child: LogicalPlan,
      hints: Map[QueryHint.Type, HintName.Type]): LogicalPlanWithHints

  /**
   * Return true if the given LogicalPlan encapsulates a child plan with query hint(s).
   */
  def isHintPlan(plan: LogicalPlan): Boolean

  /**
   * If the given plan encapsulates query hints, then return the hint type and name pairs.
   */
  def getHints(plan: LogicalPlan): Map[QueryHint.Type, HintName.Type]

  /**
   * Return true if current plan has been explicitly marked for broadcast and false otherwise.
   */
  def isBroadcastable(plan: LogicalPlan): Boolean

  /**
   * Create a new OneRowRelation.
   */
  def newOneRowRelation(): LogicalPlan

  /**
   * Create a new [[LogicalPlan]] for GENERATE.
   */
  def newGeneratePlan(generator: Generator, outer: Boolean, qualifier: Option[String],
      generatorOutput: Seq[Attribute], child: LogicalPlan): LogicalPlan

  /**
   * Write a DataFrame to a DataSource.
   */
  def writeToDataSource(ds: DataSource, mode: SaveMode, data: Dataset[Row]): BaseRelation

  /**
   * Create a new [[LogicalRelation]].
   */
  def newLogicalRelation(relation: BaseRelation,
      expectedOutputAttributes: Option[Seq[AttributeReference]],
      catalogTable: Option[CatalogTable], isStreaming: Boolean): LogicalRelation

  // scalastyle:off

  def newCatalogTable(identifier: TableIdentifier, tableType: CatalogTableType,
      storage: CatalogStorageFormat, schema: StructType, provider: Option[String],
      partitionColumnNames: Seq[String], bucketSpec: Option[BucketSpec],
      owner: String, createTime: Long, lastAccessTime: Long, properties: Map[String, String],
      stats: Option[(BigInt, Option[BigInt], Map[String, ColumnStat])],
      viewOriginalText: Option[String], viewText: Option[String],
      comment: Option[String], unsupportedFeatures: Seq[String],
      tracksPartitionsInCatalog: Boolean, schemaPreservesCase: Boolean,
      ignoredProperties: Map[String, String]): CatalogTable

  // scalastyle:on

  def catalogTableViewOriginalText(catalogTable: CatalogTable): Option[String]

  def catalogTableSchemaPreservesCase(catalogTable: CatalogTable): Boolean

  def catalogTableIgnoredProperties(catalogTable: CatalogTable): Map[String, String]

  def newCatalogTableWithViewOriginalText(catalogTable: CatalogTable,
      viewOriginalText: Option[String]): CatalogTable

  /**
   * Create a new shuffle exchange plan.
   */
  def newShuffleExchange(newPartitioning: Partitioning, child: SparkPlan): Exchange

  /**
   * Get the [[Statistics]] for a given [[LogicalPlan]].
   */
  def getStatistics(plan: LogicalPlan): Statistics

  /**
   * Return true if the given [[AggregateFunction]] support partial result aggregation.
   */
  def supportsPartial(aggregate: AggregateFunction): Boolean

  /**
   * Create a physical [[SparkPlan]] for an [[AggregateFunction]] that does not support
   * partial result aggregation ([[supportsPartial]] is false).
   */
  def planAggregateWithoutPartial(groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression], planChild: () => SparkPlan): Seq[SparkPlan]

  /**
   * Compile given generated code assuming it results in an implemenation of [[GeneratedClass]].
   */
  def compile(code: CodeAndComment): GeneratedClass

  /**
   * Create a new [[JSONOptions]] object given the parameters.
   */
  def newJSONOptions(parameters: Map[String, String],
      session: Option[SparkSession]): JSONOptions

  /**
   * Create a new optimizer with extended rules for SnappyData.
   */
  def newSparkOptimizer(sessionState: SnappySessionState): SparkOptimizer

  /**
   * Return the Spark plan for check pre-conditions before a write operation.
   */
  def newPreWriteCheck(sessionState: SnappySessionState): LogicalPlan => Unit

  /**
   * Create a new SnappyData extended CacheManager to clear cached plans on cached data changes.
   */
  def newCacheManager(): CacheManager
}

/**
 * Enumeration for frame boundary type to provie a common way of expressing it due to
 * major change in frame boundary handling across Spark versions.
 */
object FrameBoundaryType extends Enumeration {
  type Type = Value

  val CurrentRow, UnboundedPreceding, UnboundedFollowing, ValuePreceding, ValueFollowing = Value
}
