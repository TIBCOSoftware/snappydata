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

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import io.snappydata.Property.HashAggregateSize
import io.snappydata.sql.catalog.SnappyExternalCatalog
import io.snappydata.sql.catalog.impl.SmartConnectorExternalCatalog
import io.snappydata.{HintName, QueryHint}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.deploy.SparkSubmitUtils
import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry, UnresolvedRelation, UnresolvedTableValuedFunction}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeGenerator, CodegenContext, GeneratedClass}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, CurrentRow, ExprId, Expression, ExpressionInfo, FrameBoundary, FrameType, Generator, Literal, NamedExpression, NullOrdering, PredicateSubquery, SortDirection, SortOrder, SpecifiedWindowFrame, UnboundedFollowing, UnboundedPreceding, ValueFollowing, ValuePreceding}
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow, SQLBuilder, TableIdentifier}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.{ClearCacheCommand, CreateFunctionCommand, DescribeTableCommand, RunnableCommand}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchange}
import org.apache.spark.sql.execution.ui.{SQLTab, SnappySQLListener}
import org.apache.spark.sql.hive.{HiveConditionalRule, SnappyAnalyzer, SnappyHiveCatalogBase, SnappyHiveExternalCatalog, SnappySessionState}
import org.apache.spark.sql.internal.SQLConf.SQLConfigBuilder
import org.apache.spark.sql.sources.{BaseRelation, Filter, ResolveQueryHints}
import org.apache.spark.sql.streaming.{LogicalDStreamPlan, StreamingQueryManager}
import org.apache.spark.sql.types.{DataType, Metadata, StructType}
import org.apache.spark.streaming.SnappyStreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext, SparkException}

/**
 * Implementation of [[SparkInternals]] for Spark 2.1.0.
 */
class Spark210Internals extends SparkInternals {

  override def version: String = "2.1.0"

  override def uncacheQuery(spark: SparkSession, plan: LogicalPlan, blocking: Boolean): Unit = {
    implicit val encoder: ExpressionEncoder[Row] = RowEncoder(plan.schema)
    spark.sharedState.cacheManager.uncacheQuery(Dataset(spark, plan), blocking)
  }

  /**
   * Apply a map function to each expression present in this query operator, and return a new
   * query operator based on the mapped expressions.
   *
   * Taken from the mapExpressions in Spark 2.1.1 and beyond.
   */
  override def mapExpressions(plan: LogicalPlan, f: Expression => Expression): LogicalPlan = {
    var changed = false

    @inline def transformExpression(e: Expression): Expression = {
      val newE = f(e)
      if (newE.fastEquals(e)) {
        e
      } else {
        changed = true
        newE
      }
    }

    def recursiveTransform(arg: Any): AnyRef = arg match {
      case e: Expression => transformExpression(e)
      case Some(e: Expression) => Some(transformExpression(e))
      case Some(seq: Traversable[_]) => Some(seq.map(recursiveTransform))
      case m: Map[_, _] => m
      case d: DataType => d // Avoid unpacking Structs
      case seq: Traversable[_] => seq.map(recursiveTransform)
      case other: AnyRef => other
      case null => null
    }

    /**
     * Efficient alternative to `productIterator.map(f).toArray`.
     */
    def mapProductIterator[B: ClassTag](f: Any => B): Array[B] = {
      val arr = Array.ofDim[B](plan.productArity)
      var i = 0
      while (i < arr.length) {
        arr(i) = f(plan.productElement(i))
        i += 1
      }
      arr
    }

    val newArgs = mapProductIterator(recursiveTransform)

    if (changed) plan.makeCopy(newArgs).asInstanceOf[plan.type] else plan
  }

  override def registerFunction(session: SparkSession, name: FunctionIdentifier,
      info: ExpressionInfo, function: Seq[Expression] => Expression): Unit = {
    session.sessionState.functionRegistry.registerFunction(name.unquotedString, info, function)
  }

  override def addClassField(ctx: CodegenContext, javaType: String,
      varName: String, initFunc: String => String,
      forceInline: Boolean, useFreshName: Boolean): String = {
    val variableName = if (useFreshName) ctx.freshName(varName) else varName
    ctx.addMutableState(javaType, variableName, initFunc(variableName))
    variableName
  }

  override def getInlinedClassFields(ctx: CodegenContext): (Seq[(String, String)], Seq[String]) = {
    ctx.mutableStates.map(t => t._1 -> t._2) -> ctx.mutableStates.map(_._3)
  }

  override def addFunction(ctx: CodegenContext, funcName: String, funcCode: String,
      inlineToOuterClass: Boolean = false): String = {
    ctx.addNewFunction(funcName, funcCode)
    funcName
  }

  override def isFunctionAddedToOuterClass(ctx: CodegenContext, funcName: String): Boolean = {
    ctx.addedFunctions.contains(funcName)
  }

  override def splitExpressions(ctx: CodegenContext, expressions: Seq[String]): String = {
    ctx.splitExpressions(ctx.INPUT_ROW, expressions)
  }

  override def resetCopyResult(ctx: CodegenContext): Unit = ctx.copyResult = false

  override def isPredicateSubquery(expr: Expression): Boolean =
    expr.isInstanceOf[PredicateSubquery]

  override def copyPredicateSubquery(expr: Expression, newPlan: LogicalPlan,
      newExprId: ExprId): Expression = {
    expr.asInstanceOf[PredicateSubquery].copy(plan = newPlan, exprId = newExprId)
  }

  override def newWholeStagePlan(plan: SparkPlan): WholeStageCodegenExec = {
    WholeStageCodegenExec(plan)
  }

  override def newCaseInsensitiveMap(map: Map[String, String]): Map[String, String] = {
    new CaseInsensitiveMap(map)
  }

  def createAndAttachSQLListener(sparkContext: SparkContext): Unit = {
    // if the call is done the second time, then attach in embedded mode
    // too since this is coming from ToolsCallbackImpl
    val (forceAttachUI, listener, old) = SparkSession.sqlListener.get() match {
      case l: SnappySQLListener => (true, l, null) // already set
      case l =>
        val listener = new SnappySQLListener(sparkContext.conf)
        if (SparkSession.sqlListener.compareAndSet(l, listener)) {
          sparkContext.listenerBus.addListener(listener)
          if (l ne null) sparkContext.listenerBus.removeListener(l)
        }
        (false, listener, l)
    }
    // embedded mode attaches SQLTab later via ToolsCallbackImpl that also
    // takes care of injecting any authentication module if configured
    sparkContext.ui match {
      case Some(ui) if forceAttachUI || !SnappyContext.getClusterMode(sparkContext)
          .isInstanceOf[SnappyEmbeddedMode] =>
        // clear the previous SQLTab, if any
        if (old ne null) {
          ui.getTabs.foreach {
            case tab: SQLTab =>
              ui.detachTab(tab)
              ui.removeStaticHandler("/static/sql")
            case _ =>
          }
        }
        new SQLTab(listener, ui)
      case _ =>
    }
  }

  def createAndAttachSQLListener(state: SharedState): Unit = {
    // check that SparkSession.sqlListener should be set correctly
    SparkSession.sqlListener.get() match {
      case _: SnappySQLListener =>
      case l =>
        throw new IllegalStateException(s"expected SnappySQLListener to be set but was $l")
    }
  }

  def clearSQLListener(): Unit = {
    SparkSession.sqlListener.set(null)
  }

  override def createViewSQL(session: SparkSession, plan: LogicalPlan,
      originalText: Option[String]): String = {
    val viewSQL = new SQLBuilder(plan).toSQL
    // Validate the view SQL - make sure we can parse it and analyze it.
    // If we cannot analyze the generated query, there is probably a bug in SQL generation.
    try {
      session.sql(viewSQL).queryExecution.assertAnalyzed()
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException(s"Failed to analyze the canonicalized SQL: $viewSQL", e)
    }
    viewSQL
  }

  override def createView(desc: CatalogTable, output: Seq[Attribute],
      child: LogicalPlan): LogicalPlan = child

  override def newCreateFunctionCommand(schemaName: Option[String], functionName: String,
      className: String, resources: Seq[FunctionResource], isTemp: Boolean,
      ignoreIfExists: Boolean, replace: Boolean): LogicalPlan = {
    if (ignoreIfExists) {
      throw new ParseException(s"CREATE FUNCTION does not support IF NOT EXISTS in Spark $version")
    }
    if (replace) {
      throw new ParseException(s"CREATE FUNCTION does not support REPLACE in Spark $version")
    }
    CreateFunctionCommand(schemaName, functionName, className, resources, isTemp)
  }

  override def newDescribeTableCommand(table: TableIdentifier,
      partitionSpec: Map[String, String], isExtended: Boolean,
      isFormatted: Boolean): RunnableCommand = {
    DescribeTableCommand(table, partitionSpec, isExtended, isFormatted)
  }

  override def newClearCacheCommand(): LogicalPlan = ClearCacheCommand

  override def resolveMavenCoordinates(coordinates: String, remoteRepos: Option[String],
      ivyPath: Option[String], exclusions: Seq[String]): String = {
    SparkSubmitUtils.resolveMavenCoordinates(coordinates, remoteRepos, ivyPath, exclusions)
  }

  override def copyAttribute(attr: AttributeReference)(name: String,
      dataType: DataType, nullable: Boolean, metadata: Metadata): AttributeReference = {
    attr.copy(name = name, dataType = dataType, nullable = nullable, metadata = metadata)(
      exprId = attr.exprId, qualifier = attr.qualifier, isGenerated = attr.isGenerated)
  }

  override def withNewChild(insert: InsertIntoTable, newChild: LogicalPlan): InsertIntoTable = {
    insert.copy(child = newChild)
  }

  override def newInsertPlanWithCountOutput(table: LogicalPlan,
      partition: Map[String, Option[String]], child: LogicalPlan,
      overwrite: Boolean, ifNotExists: Boolean): InsertIntoTable = {
    new Insert21(table, partition, child, OverwriteOptions(enabled = overwrite), ifNotExists)
  }

  override def getOverwriteOption(insert: InsertIntoTable): Boolean = insert.overwrite.enabled

  override def getIfNotExistsOption(insert: InsertIntoTable): Boolean = insert.ifNotExists

  override def newGroupingSet(groupingSets: Seq[Seq[Expression]],
      groupByExprs: Seq[Expression], child: LogicalPlan,
      aggregations: Seq[NamedExpression]): LogicalPlan = {
    val keyMap = groupByExprs.zipWithIndex.toMap
    val numExpressions = keyMap.size
    val mask = (1 << numExpressions) - 1
    val bitmasks: Seq[Int] = groupingSets.map(set => set.foldLeft(mask)((bitmap, col) => {
      if (!keyMap.contains(col)) {
        throw new ParseException(s"GROUPING SETS column '$col' does not appear in GROUP BY list")
      }
      bitmap & ~(1 << (numExpressions - 1 - keyMap(col)))
    }))
    GroupingSets(bitmasks, groupByExprs, child, aggregations)
  }

  override def newUnresolvedRelation(tableIdentifier: TableIdentifier,
      alias: Option[String]): LogicalPlan = {
    UnresolvedRelation(tableIdentifier, alias)
  }

  override def unresolvedRelationAlias(u: UnresolvedRelation): Option[String] = u.alias

  override def newSubqueryAlias(alias: String, child: LogicalPlan,
      view: Option[TableIdentifier]): SubqueryAlias = SubqueryAlias(alias, child, view)

  override def getViewFromAlias(q: SubqueryAlias): Option[TableIdentifier] = q.view

  override def newAlias(child: Expression, name: String,
      copyAlias: Option[NamedExpression]): Alias = {
    copyAlias match {
      case None => Alias(child, name)()
      case Some(a: Alias) =>
        Alias(child, name)(a.exprId, a.qualifier, a.explicitMetadata, a.isGenerated)
      case Some(a) => Alias(child, name)(a.exprId, a.qualifier, isGenerated = a.isGenerated)
    }
  }

  override def newUnresolvedColumnAliases(outputColumnNames: Seq[String],
      child: LogicalPlan): LogicalPlan = {
    if (outputColumnNames.isEmpty) child
    else {
      throw new ParseException(s"Aliases ($outputColumnNames) for column names " +
          s"of a sub-plan not supported in Spark $version")
    }
  }

  override def newSortOrder(child: Expression, direction: SortDirection,
      nullOrdering: NullOrdering): SortOrder = {
    SortOrder(child, direction, nullOrdering)
  }

  override def newRepartitionByExpression(partitionExpressions: Seq[Expression],
      numPartitions: Int, child: LogicalPlan): RepartitionByExpression = {
    RepartitionByExpression(partitionExpressions, child, Some(numPartitions))
  }

  override def newUnresolvedTableValuedFunction(functionName: String,
      functionArgs: Seq[Expression], outputNames: Seq[String]): UnresolvedTableValuedFunction = {
    if (outputNames.nonEmpty) {
      throw new ParseException(s"Aliases ($outputNames) for table value function " +
          s"'$functionName' not supported in Spark $version")
    }
    UnresolvedTableValuedFunction(functionName, functionArgs)
  }

  private def boundaryInt(boundaryType: FrameBoundaryType.Type,
      num: Option[Expression]): Int = num match {
    case Some(l: Literal) => l.value.toString.toInt
    case _ => throw new ParseException(
      s"Expression ($num) in frame boundary ($boundaryType) not supported in Spark $version")
  }

  override def newFrameBoundary(boundaryType: FrameBoundaryType.Type,
      num: Option[Expression]): FrameBoundary = {
    boundaryType match {
      case FrameBoundaryType.UnboundedPreceding => UnboundedPreceding
      case FrameBoundaryType.ValuePreceding => ValuePreceding(boundaryInt(boundaryType, num))
      case FrameBoundaryType.CurrentRow => CurrentRow
      case FrameBoundaryType.UnboundedFollowing => UnboundedFollowing
      case FrameBoundaryType.ValueFollowing => ValueFollowing(boundaryInt(boundaryType, num))
    }
  }

  override def newSpecifiedWindowFrame(frameType: FrameType, frameStart: Any,
      frameEnd: Any): SpecifiedWindowFrame = {
    SpecifiedWindowFrame(frameType, frameStart.asInstanceOf[FrameBoundary],
      frameEnd.asInstanceOf[FrameBoundary])
  }

  override def newLogicalPlanWithHints(child: LogicalPlan,
      hints: Map[QueryHint.Type, HintName.Type]): LogicalPlanWithHints = {
    new PlanWithHints21(child, hints)
  }

  override def newTableSample(lowerBound: Double, upperBound: Double, withReplacement: Boolean,
      seed: Long, child: LogicalPlan): Sample = {
    Sample(lowerBound, upperBound, withReplacement, seed, child)(isTableSample = true)
  }

  override def isHintPlan(plan: LogicalPlan): Boolean = plan.isInstanceOf[BroadcastHint]

  override def getHints(plan: LogicalPlan): Map[QueryHint.Type, HintName.Type] = plan match {
    case p: PlanWithHints21 => p.allHints
    case _: BroadcastHint => Map(QueryHint.JoinType -> HintName.JoinType_Broadcast)
    case _ => Map.empty
  }

  override def isBroadcastable(plan: LogicalPlan): Boolean = plan.statistics.isBroadcastable

  override def newOneRowRelation(): LogicalPlan = OneRowRelation

  override def newGeneratePlan(generator: Generator, outer: Boolean, qualifier: Option[String],
      generatorOutput: Seq[Attribute], child: LogicalPlan): LogicalPlan = {
    Generate(generator, join = true, outer, qualifier, generatorOutput, child)
  }

  override def writeToDataSource(ds: DataSource, mode: SaveMode,
      data: Dataset[Row]): BaseRelation = {
    ds.write(mode, data)
    ds.copy(userSpecifiedSchema = Some(data.schema.asNullable)).resolveRelation()
  }

  override def newLogicalRelation(relation: BaseRelation,
      expectedOutputAttributes: Option[Seq[AttributeReference]],
      catalogTable: Option[CatalogTable], isStreaming: Boolean): LogicalRelation = {
    if (isStreaming) {
      throw new ParseException(s"Streaming relations not supported in Spark $version")
    }
    LogicalRelation(relation, expectedOutputAttributes, catalogTable)
  }

  override def internalCreateDataFrame(session: SparkSession, catalystRows: RDD[InternalRow],
      schema: StructType, isStreaming: Boolean): Dataset[Row] = {
    if (isStreaming) {
      throw new SparkException(s"Streaming datasets not supported in Spark $version")
    }
    session.internalCreateDataFrame(catalystRows, schema)
  }

  override def newRowDataSourceScanExec(fullOutput: Seq[Attribute], requiredColumnsIndex: Seq[Int],
      filters: Seq[Filter], handledFilters: Seq[Filter], rdd: RDD[InternalRow],
      metadata: Map[String, String], relation: BaseRelation,
      tableIdentifier: Option[TableIdentifier]): RowDataSourceScanExec = {
    RowDataSourceScanExec(requiredColumnsIndex.map(fullOutput), rdd, relation,
      UnknownPartitioning(0), metadata, tableIdentifier)
  }

  override def newCodegenSparkFallback(child: SparkPlan,
      session: SnappySession): CodegenSparkFallback = {
    new CodegenSparkFallback21(child, session)
  }

  override def newLogicalDStreamPlan(output: Seq[Attribute], stream: DStream[InternalRow],
      streamingSnappy: SnappyStreamingContext): LogicalDStreamPlan = {
    new LogicalDStreamPlan21(output, stream)(streamingSnappy)
  }

  override def newCatalogDatabase(name: String, description: String,
      locationUri: String, properties: Map[String, String]): CatalogDatabase = {
    CatalogDatabase(name, description, locationUri, properties)
  }

  override def catalogDatabaseLocationURI(database: CatalogDatabase): String = database.locationUri

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
    if (!schemaPreservesCase) {
      throw new SparkException(s"schemaPreservesCase should be always true in Spark $version")
    }
    if (ignoredProperties.nonEmpty) {
      throw new SparkException(s"ignoredProperties should be always empty in Spark $version")
    }
    val statistics = stats match {
      case None => None
      case Some(s) => Some(Statistics(s._1, s._2, s._3))
    }
    CatalogTable(identifier, tableType, storage, schema, provider, partitionColumnNames,
      bucketSpec, owner, createTime, lastAccessTime, properties, statistics, viewOriginalText,
      viewText, comment, unsupportedFeatures, tracksPartitionsInCatalog)
  }

  // scalastyle:on

  override def catalogTableViewOriginalText(catalogTable: CatalogTable): Option[String] =
    catalogTable.viewOriginalText

  override def catalogTableSchemaPreservesCase(catalogTable: CatalogTable): Boolean = true

  override def catalogTableIgnoredProperties(catalogTable: CatalogTable): Map[String, String] =
    Map.empty

  override def newCatalogTableWithViewOriginalText(catalogTable: CatalogTable,
      viewOriginalText: Option[String]): CatalogTable = {
    catalogTable.copy(viewOriginalText = viewOriginalText)
  }

  override def newCatalogStorageFormat(locationUri: Option[String], inputFormat: Option[String],
      outputFormat: Option[String], serde: Option[String], compressed: Boolean,
      properties: Map[String, String]): CatalogStorageFormat = {
    CatalogStorageFormat(locationUri, inputFormat, outputFormat, serde, compressed, properties)
  }

  override def catalogStorageFormatLocationUri(
      storageFormat: CatalogStorageFormat): Option[String] = storageFormat.locationUri

  override def catalogTablePartitionToRow(partition: CatalogTablePartition,
      partitionSchema: StructType, defaultTimeZoneId: String): InternalRow = {
    partition.toRow(partitionSchema)
  }

  override def loadDynamicPartitions(externalCatalog: ExternalCatalog, schema: String,
      table: String, loadPath: String, partition: TablePartitionSpec, replace: Boolean,
      numDP: Int, holdDDLTime: Boolean): Unit = {
    externalCatalog.loadDynamicPartitions(schema, table, loadPath, partition, replace,
      numDP, holdDDLTime)
  }

  override def alterTableStats(externalCatalog: ExternalCatalog, schema: String, table: String,
      stats: Option[(BigInt, Option[BigInt], Map[String, ColumnStat])]): Unit = {
    throw new ParseException(s"ALTER TABLE STATS not supported in Spark $version")
  }

  override def alterFunction(externalCatalog: ExternalCatalog, schema: String,
      function: CatalogFunction): Unit = {
    throw new ParseException(s"ALTER FUNCTION not supported in Spark $version")
  }

  override def columnStatToMap(stat: ColumnStat, colName: String,
      dataType: DataType): Map[String, String] = stat.toMap

  override def newEmbeddedHiveCatalog(conf: SparkConf, hadoopConf: Configuration,
      createTime: Long): SnappyHiveExternalCatalog = {
    new SnappyEmbeddedHiveCatalog210(conf, hadoopConf, createTime)
  }

  override def newSmartConnectorExternalCatalog(
      session: SparkSession): SmartConnectorExternalCatalog = {
    new SmartConnectorExternalCatalog210(session)
  }

  override def lookupDataSource(provider: String, conf: => SQLConf): Class[_] =
    DataSource.lookupDataSource(provider)

  override def newShuffleExchange(newPartitioning: Partitioning, child: SparkPlan): Exchange = {
    ShuffleExchange(newPartitioning, child)
  }

  override def isShuffleExchange(plan: SparkPlan): Boolean = plan.isInstanceOf[ShuffleExchange]

  override def classOfShuffleExchange(): Class[_] = classOf[ShuffleExchange]

  override def getStatistics(plan: LogicalPlan): Statistics = plan.statistics

  override def supportsPartial(aggregate: AggregateFunction): Boolean = aggregate.supportsPartial

  override def planAggregateWithoutPartial(groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression], resultExpressions: Seq[NamedExpression],
      planChild: () => SparkPlan): Seq[SparkPlan] = {
    aggregate.AggUtils.planAggregateWithoutPartial(
      groupingExpressions,
      aggregateExpressions,
      resultExpressions,
      planChild())
  }

  override def compile(code: CodeAndComment): GeneratedClass = CodeGenerator.compile(code)

  override def newJSONOptions(parameters: Map[String, String],
      session: Option[SparkSession]): JSONOptions = new JSONOptions(parameters)

  override def newSnappySessionState(snappySession: SnappySession): SnappySessionState = {
    new SnappySessionState21(snappySession)
  }

  override def newPreWriteCheck(sessionState: SnappySessionState): LogicalPlan => Unit = {
    // we pass wrapper catalog to make sure LogicalRelation
    // is passed in PreWriteCheck
    PreWriteCheck(sessionState.conf, sessionState.wrapperCatalog)
  }

  override def newCacheManager(): CacheManager = {
    // load by reflection since this class is not visible when compiling for 2.1.1 compatibility
    Utils.classForName("org.apache.spark.sql.internal.SnappyCacheManager210")
        .newInstance().asInstanceOf[CacheManager]
  }

  override def buildConf(key: String): ConfigBuilder = SQLConfigBuilder(key)
}

class SnappyEmbeddedHiveCatalog210(override val conf: SparkConf,
    override val hadoopConf: Configuration, override val createTime: Long)
    extends SnappyHiveCatalogBase(conf, hadoopConf) with SnappyHiveExternalCatalog {

  override def getTable(schema: String, table: String): CatalogTable =
    getTableImpl(schema, table)

  override def getTableOption(schema: String, table: String): Option[CatalogTable] =
    getTableIfExists(schema, table)

  override protected def baseCreateDatabase(schemaDefinition: CatalogDatabase,
      ignoreIfExists: Boolean): Unit = super.createDatabase(schemaDefinition, ignoreIfExists)

  override protected def baseDropDatabase(schema: String, ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = super.dropDatabase(schema, ignoreIfNotExists, cascade)

  override protected def baseCreateTable(tableDefinition: CatalogTable,
      ignoreIfExists: Boolean): Unit = super.createTable(tableDefinition, ignoreIfExists)

  override protected def baseDropTable(schema: String, table: String, ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = super.dropTable(schema, table, ignoreIfNotExists, purge)

  override protected def baseAlterTable(tableDefinition: CatalogTable): Unit =
    super.alterTable(tableDefinition)

  override protected def baseRenameTable(schema: String, oldName: String, newName: String): Unit =
    super.renameTable(schema, oldName, newName)

  override protected def baseLoadDynamicPartitions(schema: String, table: String, loadPath: String,
      partition: TablePartitionSpec, replace: Boolean, numDP: Int, holdDDLTime: Boolean): Unit = {
    super.loadDynamicPartitions(schema, table, loadPath, partition, replace, numDP, holdDDLTime)
  }

  override protected def baseCreateFunction(schema: String,
      funcDefinition: CatalogFunction): Unit = super.createFunction(schema, funcDefinition)

  override protected def baseDropFunction(schema: String, name: String): Unit =
    super.dropFunction(schema, name)

  override protected def baseRenameFunction(schema: String, oldName: String,
      newName: String): Unit = super.renameFunction(schema, oldName, newName)

  override def createDatabase(schemaDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit =
    createDatabaseImpl(schemaDefinition, ignoreIfExists)

  override def dropDatabase(schema: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit =
    dropDatabaseImpl(schema, ignoreIfNotExists, cascade)

  override def alterDatabase(schemaDefinition: CatalogDatabase): Unit =
    alterDatabaseImpl(schemaDefinition)

  override def createTable(table: CatalogTable, ignoreIfExists: Boolean): Unit =
    createTableImpl(table, ignoreIfExists)

  override def dropTable(schema: String, table: String, ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = {
    dropTableImpl(schema, table, ignoreIfNotExists, purge)
  }

  override def renameTable(schema: String, oldName: String, newName: String): Unit =
    renameTableImpl(schema, oldName, newName)

  override def alterTable(table: CatalogTable): Unit = alterTableImpl(table)

  override def loadDynamicPartitions(schema: String, table: String, loadPath: String,
      partition: TablePartitionSpec, replace: Boolean, numDP: Int, holdDDLTime: Boolean): Unit = {
    loadDynamicPartitionsImpl(schema, table, loadPath, partition, replace, numDP, holdDDLTime)
  }

  override def listPartitionsByFilter(schema: String, table: String,
      predicates: Seq[Expression]): Seq[CatalogTablePartition] = {
    withHiveExceptionHandling(super.listPartitionsByFilter(schema, table, predicates))
  }

  override def createFunction(schema: String, function: CatalogFunction): Unit =
    createFunctionImpl(schema, function)

  override def dropFunction(schema: String, funcName: String): Unit =
    dropFunctionImpl(schema, funcName)

  override def renameFunction(schema: String, oldName: String, newName: String): Unit =
    renameFunctionImpl(schema, oldName, newName)
}

class SmartConnectorExternalCatalog210(override val session: SparkSession)
    extends ExternalCatalog with SmartConnectorExternalCatalog {

  override def getTable(schema: String, table: String): CatalogTable =
    getTableImpl(schema, table)

  override def getTableOption(schema: String, table: String): Option[CatalogTable] =
    getTableIfExists(schema, table)

  override def createDatabase(schemaDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit =
    createDatabaseImpl(schemaDefinition, ignoreIfExists)

  override def dropDatabase(schema: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit =
    dropDatabaseImpl(schema, ignoreIfNotExists, cascade)

  override def alterDatabase(schemaDefinition: CatalogDatabase): Unit =
    throw new UnsupportedOperationException("Schema definitions cannot be altered")

  override def createTable(table: CatalogTable, ignoreIfExists: Boolean): Unit =
    createTableImpl(table, ignoreIfExists)

  override def dropTable(schema: String, table: String, ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = {
    dropTableImpl(schema, table, ignoreIfNotExists, purge)
  }

  override def renameTable(schema: String, oldName: String, newName: String): Unit =
    renameTableImpl(schema, oldName, newName)

  override def alterTable(table: CatalogTable): Unit = alterTableImpl(table)

  def alterTableSchema(db: String, table: String, schema: StructType): Unit = {}

  override def loadDynamicPartitions(schema: String, table: String, loadPath: String,
      partition: TablePartitionSpec, replace: Boolean, numDP: Int, holdDDLTime: Boolean): Unit = {
    loadDynamicPartitionsImpl(schema, table, loadPath, partition, replace, numDP, holdDDLTime)
  }

  override def listPartitionsByFilter(schema: String, table: String,
      predicates: Seq[Expression]): Seq[CatalogTablePartition] = {
    listPartitionsByFilterImpl(schema, table, predicates, defaultTimeZoneId = "")
  }

  override def createFunction(schema: String, function: CatalogFunction): Unit =
    createFunctionImpl(schema, function)

  override def dropFunction(schema: String, funcName: String): Unit =
    dropFunctionImpl(schema, funcName)

  override def renameFunction(schema: String, oldName: String, newName: String): Unit =
    renameFunctionImpl(schema, oldName, newName)
}

class SnappySessionCatalog21(override val snappySession: SnappySession,
    override val snappyExternalCatalog: SnappyExternalCatalog,
    override val globalTempViewManager: GlobalTempViewManager,
    override val functionResourceLoader: FunctionResourceLoader,
    override val functionRegistry: FunctionRegistry, override val parser: SnappySqlParser,
    override val sqlConf: SQLConf, hadoopConf: Configuration,
    override val wrappedCatalog: Option[SnappySessionCatalog])
    extends SessionCatalog(snappyExternalCatalog, globalTempViewManager, functionResourceLoader,
      functionRegistry, sqlConf, hadoopConf) with SnappySessionCatalog {

  override def getTableMetadataOption(name: TableIdentifier): Option[CatalogTable] = {
    super.getTableMetadataOption(name) match {
      case None => None
      case Some(table) => Some(convertCharTypes(table))
    }
  }

  override protected def newView(table: CatalogTable, child: LogicalPlan): LogicalPlan = child

  override protected def newCatalogRelation(schemaName: String, table: CatalogTable): LogicalPlan =
    SimpleCatalogRelation(schemaName, table)

  override def lookupRelation(name: TableIdentifier, alias: Option[String]): LogicalPlan =
    lookupRelationImpl(name, alias)

  override def makeFunctionBuilder(name: String, functionClassName: String): FunctionBuilder =
    makeFunctionBuilderImpl(name, functionClassName)
}

class SnappySessionState21(override val snappySession: SnappySession)
    extends SessionState(snappySession) with SnappySessionState {

  self =>

  override def catalogBuilder(wrapped: Option[SnappySessionCatalog]): SnappySessionCatalog = {
    new SnappySessionCatalog21(snappySession,
      snappySession.sharedState.getExternalCatalogInstance(snappySession),
      snappySession.sharedState.globalTempViewManager,
      functionResourceLoader, functionRegistry, sqlParser, conf, newHadoopConf(), wrapped)
  }

  override def analyzerBuilder(): Analyzer = new Analyzer(catalog, conf) with SnappyAnalyzer {

    self =>

    override def session: SnappySession = snappySession

    private def state: SnappySessionState = session.sessionState

    override val extendedResolutionRules: Seq[Rule[LogicalPlan]] = {
      val extensions1 = session.contextFunctions.getExtendedResolutionRules
      val extensions2 = session.contextFunctions.getPostHocResolutionRules
      val rules = new HiveConditionalRule(_.catalog.ParquetConversions, state) ::
          new HiveConditionalRule(_.catalog.OrcConversions, state) ::
          AnalyzeCreateTable(session) ::
          new PreprocessTable(state) ::
          ResolveAliasInGroupBy ::
          new FindDataSourceTable(session) ::
          DataSourceAnalysis(conf) ::
          AnalyzeMutableOperations(session, this) ::
          ResolveQueryHints(session) ::
          RowLevelSecurity ::
          ExternalRelationLimitFetch ::
          (if (conf.runSQLonFile) new ResolveDataSource(session) :: extensions2 else extensions2)
      if (extensions1.isEmpty) rules else extensions1 ++ rules
    }

    override val extendedCheckRules: Seq[LogicalPlan => Unit] = getExtendedCheckRules

    override lazy val baseAnalyzerInstance: Analyzer = new Analyzer(catalog, conf) {
      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] = self.extendedResolutionRules
      override val extendedCheckRules: Seq[LogicalPlan => Unit] = self.extendedCheckRules

      override def execute(plan: LogicalPlan): LogicalPlan = self.execute(plan)
    }
  }

  override def optimizerBuilder(): Optimizer = {
    new SparkOptimizer(catalog, conf, experimentalMethods) with DefaultOptimizer {
      override def state: SnappySessionState = self
    }
  }

  override lazy val conf: SQLConf = new SnappyConf(snappySession)

  override lazy val sqlParser: SnappySqlParser = snappySession.contextFunctions.newSQLParser()

  override lazy val streamingQueryManager: StreamingQueryManager = {
    // Disabling `SnappyAggregateStrategy` for streaming queries as it clashes with
    // `StatefulAggregationStrategy` which is applied by spark for streaming queries. This
    // implies that Snappydata aggregation optimisation will be turned off for any usage of
    // this session including non-streaming queries.

    HashAggregateSize.set(snappySession.sessionState.conf, "-1")
    new StreamingQueryManager(snappySession)
  }
}

class CodegenSparkFallback21(child: SparkPlan,
    session: SnappySession) extends CodegenSparkFallback(child, session) {

  override def generateTreeString(depth: Int, lastChildren: Seq[Boolean], builder: StringBuilder,
      verbose: Boolean, prefix: String): StringBuilder = {
    child.generateTreeString(depth, lastChildren, builder, verbose, prefix)
  }
}

class LogicalDStreamPlan21(output: Seq[Attribute],
    stream: DStream[InternalRow])(streamingSnappy: SnappyStreamingContext)
    extends LogicalDStreamPlan(output, stream)(streamingSnappy) {

  @transient override lazy val statistics: Statistics = Statistics(
    sizeInBytes = BigInt(streamingSnappy.snappySession.sessionState.conf.defaultSizeInBytes)
  )
}
