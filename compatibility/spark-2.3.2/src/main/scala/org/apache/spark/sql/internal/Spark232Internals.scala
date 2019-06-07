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
import java.net.URI
import java.nio.file.Paths

import scala.collection.mutable

import io.snappydata.sql.catalog.SnappyExternalCatalog
import io.snappydata.sql.catalog.impl.SmartConnectorExternalCatalog
import io.snappydata.{HintName, QueryHint}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.deploy.SparkSubmitUtils
import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, UnresolvedRelation, UnresolvedSubqueryColumnAliases, UnresolvedTableValuedFunction}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeGenerator, CodegenContext, GeneratedClass}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, CurrentRow, ExprId, Expression, ExpressionInfo, FrameType, Generator, NamedExpression, NullOrdering, SortDirection, SortOrder, SpecifiedWindowFrame, UnaryMinus, UnboundedFollowing, UnboundedPreceding}
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.catalyst.{AccessUtils, FunctionIdentifier, InternalRow, TableIdentifier}
import org.apache.spark.sql.execution.command.{ClearCacheCommand, CreateFunctionCommand, DescribeTableCommand, RunnableCommand}
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation, PreWriteCheck}
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.execution.ui.{SQLAppStatusListener, SQLAppStatusStore, SnappySQLAppListener}
import org.apache.spark.sql.execution.{CacheManager, CodegenSparkFallback, RowDataSourceScanExec, SparkOptimizer, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.hive.{HiveSessionResourceLoader, SnappyHiveCatalogBase, SnappyHiveExternalCatalog}
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.streaming.LogicalDStreamPlan
import org.apache.spark.sql.types.{DataType, Metadata, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.SnappyStreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext}

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

  override def getInlinedClassFields(ctx: CodegenContext): (Seq[(String, String)], Seq[String]) =
    AccessUtils.getInlinedMutableStates(ctx)

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
      s"unexpected copyPredicateSubquery call in Spark $version module")
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
    // no global SQLListener in Spark 2.3.x
  }

  override def createViewSQL(session: SparkSession, plan: LogicalPlan,
      originalText: Option[String]): String = originalText match {
    case Some(viewSQL) => viewSQL
    case None => throw new AnalysisException("Cannot create a persisted VIEW from the Dataset API")
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
      partitionSpec: Map[String, String], isExtended: Boolean,
      isFormatted: Boolean): RunnableCommand = {
    if (isFormatted) {
      throw new ParseException(s"DESCRIBE FORMATTED TABLE not supported in Spark $version")
    }
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

  override def withNewChild(insert: InsertIntoTable, newChild: LogicalPlan): InsertIntoTable = {
    insert.copy(query = newChild)
  }

  override def newInsertPlanWithCountOutput(table: LogicalPlan,
      partition: Map[String, Option[String]], child: LogicalPlan,
      overwrite: Boolean, ifNotExists: Boolean): InsertIntoTable = {
    new Insert23(table, partition, child, overwrite, ifNotExists)
  }

  override def getOverwriteOption(insert: InsertIntoTable): Boolean = insert.overwrite

  override def getIfNotExistsOption(insert: InsertIntoTable): Boolean = insert.ifPartitionNotExists

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

  override def newAlias(child: Expression, name: String,
      copyAlias: Option[NamedExpression]): Alias = {
    copyAlias match {
      case None => Alias(child, name)()
      case Some(a: Alias) => Alias(child, name)(a.exprId, a.qualifier, a.explicitMetadata)
      case Some(a) => Alias(child, name)(a.exprId, a.qualifier)
    }
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
    new ResolvedPlanWithHints23(child, hints)
  }

  override def isHintPlan(plan: LogicalPlan): Boolean = plan.isInstanceOf[ResolvedHint]

  override def getHints(plan: LogicalPlan): Map[QueryHint.Type, HintName.Type] = plan match {
    case p: ResolvedPlanWithHints23 => p.allHints
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

  override def internalCreateDataFrame(session: SparkSession, catalystRows: RDD[InternalRow],
      schema: StructType, isStreaming: Boolean): Dataset[Row] = {
    session.internalCreateDataFrame(catalystRows, schema, isStreaming)
  }

  override def newRowDataSourceScanExec(fullOutput: Seq[Attribute], requiredColumnsIndex: Seq[Int],
      filters: Seq[Filter], handledFilters: Seq[Filter], rdd: RDD[InternalRow],
      metadata: Map[String, String], relation: BaseRelation,
      tableIdentifier: Option[TableIdentifier]): RowDataSourceScanExec = {
    RowDataSourceScanExec(fullOutput, requiredColumnsIndex, filters.toSet, handledFilters.toSet,
      rdd, relation, tableIdentifier)
  }

  private def toURI(uri: String): URI = {
    if (uri.contains("://")) new URI(uri) else new URI("file://" + Paths.get(uri).toAbsolutePath)
  }

  override def newCodegenSparkFallback(child: SparkPlan,
      session: SnappySession): CodegenSparkFallback = {
    new CodegenSparkFallback23(child, session)
  }

  override def newLogicalDStreamPlan(output: Seq[Attribute], stream: DStream[InternalRow],
      streamingSnappy: SnappyStreamingContext): LogicalDStreamPlan = {
    new LogicalDStreamPlan23(output, stream)(streamingSnappy)
  }

  override def newCatalogDatabase(name: String, description: String,
      locationUri: String, properties: Map[String, String]): CatalogDatabase = {
    CatalogDatabase(name, description, toURI(locationUri), properties)
  }

  override def catalogDatabaseLocationURI(database: CatalogDatabase): String =
    database.locationUri.toString

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

  override def newCatalogStorageFormat(locationUri: Option[String], inputFormat: Option[String],
      outputFormat: Option[String], serde: Option[String], compressed: Boolean,
      properties: Map[String, String]): CatalogStorageFormat = {
    locationUri match {
      case None => CatalogStorageFormat(None, inputFormat, outputFormat,
        serde, compressed, properties)
      case Some(uri) => CatalogStorageFormat(Some(toURI(uri)), inputFormat, outputFormat,
        serde, compressed, properties)
    }
  }

  override def catalogStorageFormatLocationUri(
      storageFormat: CatalogStorageFormat): Option[String] = storageFormat.locationUri match {
    case None => None
    case Some(uri) => Some(uri.toString)
  }

  override def catalogTablePartitionToRow(partition: CatalogTablePartition,
      partitionSchema: StructType, defaultTimeZoneId: String): InternalRow = {
    partition.toRow(partitionSchema, defaultTimeZoneId)
  }

  override def loadDynamicPartitions(externalCatalog: ExternalCatalog, schema: String,
      table: String, loadPath: String, partition: TablePartitionSpec, replace: Boolean,
      numDP: Int, holdDDLTime: Boolean): Unit = {
    if (holdDDLTime) {
      throw new UnsupportedOperationException(
        s"unexpected loadDynamicPartitions with holdDDLTime=true in Spark $version module")
    }
    externalCatalog.loadDynamicPartitions(schema, table, loadPath, partition, replace, numDP)
  }

  override def alterTableStats(externalCatalog: ExternalCatalog, schema: String, table: String,
      stats: Option[(BigInt, Option[BigInt], Map[String, ColumnStat])]): Unit = {
    val catalogStats = stats match {
      case None => None
      case Some(s) => Some(CatalogStatistics(s._1, s._2, s._3))
    }
    externalCatalog.alterTableStats(schema, table, catalogStats)
  }

  override def alterFunction(externalCatalog: ExternalCatalog, schema: String,
      function: CatalogFunction): Unit = externalCatalog.alterFunction(schema, function)

  override def columnStatToMap(stat: ColumnStat, colName: String,
      dataType: DataType): Map[String, String] = stat.toMap(colName, dataType)

  override def newEmbeddedHiveCatalog(conf: SparkConf, hadoopConf: Configuration,
      createTime: Long): SnappyHiveExternalCatalog = {
    new SnappyEmbeddedHiveCatalog23(conf, hadoopConf, createTime)
  }

  override def newSmartConnectorExternalCatalog(
      session: SparkSession): SmartConnectorExternalCatalog = {
    new SmartConnectorExternalCatalog23(session)
  }

  override def newSnappySessionCatalog(sessionState: SnappySessionState,
      externalCatalog: SnappyExternalCatalog, globalTempViewManager: GlobalTempViewManager,
      functionRegistry: FunctionRegistry, conf: SQLConf,
      hadoopConf: Configuration): SnappySessionCatalog = {
    val session = sessionState.snappySession
    val functionResourceLoader = externalCatalog match {
      case c: SnappyHiveExternalCatalog => new HiveSessionResourceLoader(session, c.client())
      case _ => new SessionResourceLoader(session)
    }
    new SnappySessionCatalog23(session, externalCatalog, globalTempViewManager,
      functionResourceLoader, functionRegistry, sessionState.sqlParser, conf, hadoopConf)
  }

  override def lookupDataSource(provider: String, conf: => SQLConf): Class[_] =
    DataSource.lookupDataSource(provider, conf)

  override def newShuffleExchange(newPartitioning: Partitioning, child: SparkPlan): Exchange = {
    ShuffleExchangeExec(newPartitioning, child)
  }

  override def isShuffleExchange(plan: SparkPlan): Boolean = plan.isInstanceOf[ShuffleExchangeExec]

  override def classOfShuffleExchange(): Class[_] = classOf[ShuffleExchangeExec]

  override def getStatistics(plan: LogicalPlan): Statistics = plan.stats

  override def supportsPartial(aggregate: AggregateFunction): Boolean = true

  override def planAggregateWithoutPartial(groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression], resultExpressions: Seq[NamedExpression],
      planChild: () => SparkPlan): Seq[SparkPlan] = {
    throw new UnsupportedOperationException(
      s"unexpected planAggregateWithoutPartial call in Spark $version module")
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

  override def newCacheManager(): CacheManager = new SnappyCacheManager23

  override def buildConf(key: String): ConfigBuilder = SQLConf.buildConf(key)
}


/**
 * Simple extension to CacheManager to enable clearing cached plan on cache create/drop.
 */
final class SnappyCacheManager23 extends CacheManager {

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

final class SnappyEmbeddedHiveCatalog23(override val conf: SparkConf,
    override val hadoopConf: Configuration, override val createTime: Long)
    extends SnappyHiveCatalogBase(conf, hadoopConf) with SnappyHiveExternalCatalog {

  override protected def baseCreateDatabase(schemaDefinition: CatalogDatabase,
      ignoreIfExists: Boolean): Unit = super.doCreateDatabase(schemaDefinition, ignoreIfExists)

  override protected def baseDropDatabase(schema: String, ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = super.doDropDatabase(schema, ignoreIfNotExists, cascade)

  override protected def baseCreateTable(tableDefinition: CatalogTable,
      ignoreIfExists: Boolean): Unit = super.doCreateTable(tableDefinition, ignoreIfExists)

  override protected def baseDropTable(schema: String, table: String, ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = super.doDropTable(schema, table, ignoreIfNotExists, purge)

  override protected def baseAlterTable(tableDefinition: CatalogTable): Unit =
    super.doAlterTable(tableDefinition)

  override protected def baseRenameTable(schema: String, oldName: String, newName: String): Unit =
    super.doRenameTable(schema, oldName, newName)

  override protected def baseLoadDynamicPartitions(schema: String, table: String, loadPath: String,
      partition: TablePartitionSpec, replace: Boolean, numDP: Int, holdDDLTime: Boolean): Unit = {
    SparkSupport.internals().loadDynamicPartitions(this, schema, table, loadPath, partition,
      replace, numDP, holdDDLTime)
  }

  override protected def baseCreateFunction(schema: String,
      funcDefinition: CatalogFunction): Unit = super.doCreateFunction(schema, funcDefinition)

  override protected def baseDropFunction(schema: String, name: String): Unit =
    super.doDropFunction(schema, name)

  override protected def baseRenameFunction(schema: String, oldName: String,
      newName: String): Unit = super.doRenameFunction(schema, oldName, newName)

  override protected def doCreateDatabase(schemaDefinition: CatalogDatabase,
      ignoreIfExists: Boolean): Unit = createDatabaseImpl(schemaDefinition, ignoreIfExists)

  override protected def doDropDatabase(schema: String, ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = dropDatabaseImpl(schema, ignoreIfNotExists, cascade)

  override protected def doAlterDatabase(schemaDefinition: CatalogDatabase): Unit =
    alterDatabaseImpl(schemaDefinition)

  override protected def doCreateTable(table: CatalogTable, ignoreIfExists: Boolean): Unit =
    createTableImpl(table, ignoreIfExists)

  override protected def doDropTable(schema: String, table: String, ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = dropTableImpl(schema, table, ignoreIfNotExists, purge)

  override protected def doRenameTable(schema: String, oldName: String, newName: String): Unit =
    renameTableImpl(schema, oldName, newName)

  override protected def doAlterTable(table: CatalogTable): Unit = alterTableImpl(table)

  override protected def doAlterTableDataSchema(schemaName: String, table: String,
      newSchema: StructType): Unit = alterTableSchemaImpl(schemaName, table, newSchema)

  override protected def doAlterTableStats(schema: String, table: String,
      stats: Option[CatalogStatistics]): Unit = {
    withHiveExceptionHandling(super.doAlterTableStats(schema, table, stats))
  }

  override def loadDynamicPartitions(schema: String, table: String, loadPath: String,
      partition: TablePartitionSpec, replace: Boolean, numDP: Int): Unit = {
    loadDynamicPartitionsImpl(schema, table, loadPath, partition, replace, numDP,
      holdDDLTime = false)
  }

  override def listPartitionsByFilter(schema: String, table: String, predicates: Seq[Expression],
      defaultTimeZoneId: String): Seq[CatalogTablePartition] = {
    withHiveExceptionHandling(super.listPartitionsByFilter(schema, table,
      predicates, defaultTimeZoneId))
  }

  override protected def doCreateFunction(schema: String, function: CatalogFunction): Unit =
    createFunctionImpl(schema, function)

  override protected def doDropFunction(schema: String, funcName: String): Unit =
    dropFunctionImpl(schema, funcName)

  override protected def doAlterFunction(schema: String, function: CatalogFunction): Unit = {
    withHiveExceptionHandling(super.doAlterFunction(schema, function))
    SnappySession.clearAllCache()
  }

  override protected def doRenameFunction(schema: String, oldName: String, newName: String): Unit =
    renameFunctionImpl(schema, oldName, newName)
}

final class SmartConnectorExternalCatalog23(override val session: SparkSession)
    extends SmartConnectorExternalCatalog {

  override protected def doCreateDatabase(schemaDefinition: CatalogDatabase,
      ignoreIfExists: Boolean): Unit = createDatabaseImpl(schemaDefinition, ignoreIfExists)

  override protected def doDropDatabase(schema: String, ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = dropDatabaseImpl(schema, ignoreIfNotExists, cascade)

  override protected def doAlterDatabase(schemaDefinition: CatalogDatabase): Unit =
    alterDatabaseImpl(schemaDefinition)

  override protected def doCreateTable(table: CatalogTable, ignoreIfExists: Boolean): Unit =
    createTableImpl(table, ignoreIfExists)

  override protected def doDropTable(schema: String, table: String, ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = dropTableImpl(schema, table, ignoreIfNotExists, purge)

  override protected def doRenameTable(schema: String, oldName: String, newName: String): Unit =
    renameTableImpl(schema, oldName, newName)

  override protected def doAlterTable(table: CatalogTable): Unit = alterTableImpl(table)

  override protected def doAlterTableDataSchema(schemaName: String, table: String,
      newSchema: StructType): Unit = alterTableSchemaImpl(schemaName, table, newSchema)

  override protected def doAlterTableStats(schema: String, table: String,
      stats: Option[CatalogStatistics]): Unit = stats match {
    case None => alterTableStatsImpl(schema, table, None)
    case Some(s) => alterTableStatsImpl(schema, table,
      Some((s.sizeInBytes, s.rowCount, s.colStats)))
  }

  override def loadDynamicPartitions(schema: String, table: String, loadPath: String,
      partition: TablePartitionSpec, replace: Boolean, numDP: Int): Unit = {
    loadDynamicPartitionsImpl(schema, table, loadPath, partition, replace, numDP,
      holdDDLTime = false)
  }

  override def listPartitionsByFilter(schema: String, table: String, predicates: Seq[Expression],
      defaultTimeZoneId: String): Seq[CatalogTablePartition] = {
    listPartitionsByFilterImpl(schema, table, predicates, defaultTimeZoneId)
  }

  override protected def doCreateFunction(schema: String, function: CatalogFunction): Unit =
    createFunctionImpl(schema, function)

  override protected def doDropFunction(schema: String, funcName: String): Unit =
    dropFunctionImpl(schema, funcName)

  override protected def doAlterFunction(schema: String, function: CatalogFunction): Unit =
    alterFunctionImpl(schema, function)

  override protected def doRenameFunction(schema: String, oldName: String, newName: String): Unit =
    renameFunctionImpl(schema, oldName, newName)
}

final class SnappySessionCatalog23(override val snappySession: SnappySession,
    override val snappyExternalCatalog: SnappyExternalCatalog,
    override val globalTempViewManager: GlobalTempViewManager,
    override val functionResourceLoader: FunctionResourceLoader,
    override val functionRegistry: FunctionRegistry, override val parser: SnappySqlParser,
    override val sqlConf: SQLConf, override val hadoopConf: Configuration)
    extends SessionCatalog(snappyExternalCatalog, globalTempViewManager, functionRegistry,
      sqlConf, hadoopConf, parser, functionResourceLoader) with SnappySessionCatalog {

  override protected def newView(table: CatalogTable, child: LogicalPlan): LogicalPlan =
    View(desc = table, output = table.schema.toAttributes, child)

  override protected def newCatalogRelation(schemaName: String, table: CatalogTable): LogicalPlan =
    UnresolvedCatalogRelation(table)

  override def lookupRelation(name: TableIdentifier): LogicalPlan = lookupRelationImpl(name, None)

  override def registerFunction(funcDefinition: CatalogFunction,
      overrideIfExists: Boolean, functionBuilder: Option[FunctionBuilder]): Unit = {
    val func = funcDefinition.identifier
    if (functionRegistry.functionExists(func) && !overrideIfExists) {
      throw new AnalysisException(s"Function $func already exists")
    }
    val info = new ExpressionInfo(funcDefinition.className, func.database.orNull, func.funcName)
    val builder = functionBuilder.getOrElse {
      val className = funcDefinition.className
      if (!Utils.classIsLoadable(className)) {
        throw new AnalysisException(s"Can not load class '$className' when registering " +
            s"the function '$func', please make sure it is on the classpath")
      }
      makeFunctionBuilderImpl(func.unquotedString, className)
    }
    functionRegistry.registerFunction(func, info, builder)
  }
}

final class CodegenSparkFallback23(child: SparkPlan,
    session: SnappySession) extends CodegenSparkFallback(child, session) {

  override def generateTreeString(depth: Int, lastChildren: Seq[Boolean], builder: StringBuilder,
      verbose: Boolean, prefix: String, addSuffix: Boolean): StringBuilder = {
    child.generateTreeString(depth, lastChildren, builder, verbose, prefix, addSuffix)
  }
}

final class LogicalDStreamPlan23(output: Seq[Attribute],
    stream: DStream[InternalRow])(streamingSnappy: SnappyStreamingContext)
    extends LogicalDStreamPlan(output, stream)(streamingSnappy) {

  override def stats: Statistics = Statistics(
    sizeInBytes = BigInt(streamingSnappy.snappySession.sessionState.conf.defaultSizeInBytes)
  )
}
