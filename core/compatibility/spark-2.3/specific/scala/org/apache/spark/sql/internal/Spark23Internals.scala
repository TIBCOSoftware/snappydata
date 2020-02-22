/*
 * Copyright (c) 2017-2020 TIBCO Software Inc. All rights reserved.
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

import io.snappydata.sql.catalog.SnappyExternalCatalog
import io.snappydata.sql.catalog.impl.SmartConnectorExternalCatalog
import org.apache.hadoop.conf.Configuration

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.analysis.TypeCoercion.PromoteStrings
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, ExprId, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Except, Intersect, LogicalPlan, Pivot}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.bootstrap.{ApproxColumnExtractor, Tag, TaggedAlias, TaggedAttribute, TransformableTag}
import org.apache.spark.sql.execution.closedform.{ClosedFormColumnExtractor, ErrorAggregate, ErrorEstimateAttribute}
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.common.HAC
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.{CacheManager, SparkOptimizer, SparkPlan, python}
import org.apache.spark.sql.hive.{HiveSessionResourceLoader, SnappyAnalyzer, SnappyHiveExternalCatalog, SnappySessionState}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{DataType, Metadata, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Base implementation of [[SparkInternals]] for Spark 2.3.x releases.
 */
class Spark23Internals(override val version: String) extends Spark23_4_Internals {

  override def uncacheQuery(spark: SparkSession, plan: LogicalPlan,
      cascade: Boolean, blocking: Boolean): Unit = {
    spark.sharedState.cacheManager.uncacheQuery(spark, plan, blocking)
  }

  override def newSharedState(sparkContext: SparkContext): SnappySharedState = {
    // remove any existing SQLTab since a new one will be created by SharedState constructor
    removeSQLTabs(sparkContext, except = null)
    val state = new SnappySharedState23(sparkContext)
    createAndAttachSQLListener(state, sparkContext)
    state
  }

  override def newAttributeReference(name: String, dataType: DataType, nullable: Boolean,
      metadata: Metadata, exprId: ExprId, qualifier: Seq[String],
      isGenerated: Boolean): AttributeReference = {
    AttributeReference(name, dataType, nullable, metadata)(exprId, qualifier.headOption)
  }

  override def newErrorEstimateAttribute(name: String, dataType: DataType,
      nullable: Boolean, metadata: Metadata, realExprId: ExprId, exprId: ExprId,
      qualifier: Seq[String]): ErrorEstimateAttribute = {
    ErrorEstimateAttribute23(name, dataType, nullable, metadata, realExprId)(
      exprId, qualifier.headOption)
  }

  override def newApproxColumnExtractor(child: Expression, name: String, ordinal: Int,
      dataType: DataType, nullable: Boolean, exprId: ExprId,
      qualifier: Seq[String]): ApproxColumnExtractor = {
    ApproxColumnExtractor23(child, name, ordinal, dataType, nullable)(exprId, qualifier.headOption)
  }

  override def newTaggedAttribute(tag: Tag, name: String, dataType: DataType, nullable: Boolean,
      metadata: Metadata, exprId: ExprId, qualifier: Seq[String]): TaggedAttribute = {
    TaggedAttribute23(tag, name, dataType, nullable, metadata)(exprId, qualifier.headOption)
  }

  override def newTaggedAlias(tag: TransformableTag, child: Expression, name: String,
      exprId: ExprId, qualifier: Seq[String]): TaggedAlias = {
    TaggedAlias23(tag, child, name)(exprId, qualifier.headOption)
  }

  // scalastyle:off

  override def newClosedFormColumnExtractor(child: Expression, name: String, confidence: Double,
      confFactor: Double, aggType: ErrorAggregate.Type, error: Double, dataType: DataType,
      behavior: HAC.Type, nullable: Boolean, exprId: ExprId,
      qualifier: Seq[String]): ClosedFormColumnExtractor = {
    ClosedFormColumnExtractor23(child, name, confidence, confFactor, aggType, error,
      dataType, behavior, nullable)(exprId, qualifier.headOption)
  }

  // scalastyle:on

  override def toAttributeReference(attr: Attribute)(name: String,
      dataType: DataType, nullable: Boolean, metadata: Metadata,
      exprId: ExprId): AttributeReference = {
    AttributeReference(name = name, dataType = dataType, nullable = nullable, metadata = metadata)(
      exprId, qualifier = attr.qualifier)
  }

  override def newAlias(child: Expression, name: String, copyAlias: Option[NamedExpression],
      exprId: ExprId, qualifier: Seq[String]): Alias = {
    copyAlias match {
      case None => Alias(child, name)(exprId, qualifier.headOption)
      case Some(a: Alias) => Alias(child, name)(a.exprId, a.qualifier, a.explicitMetadata)
      case Some(a) => Alias(child, name)(a.exprId, a.qualifier)
    }
  }

  override def writeToDataSource(ds: DataSource, mode: SaveMode,
      data: Dataset[Row]): BaseRelation = {
    ds.writeAndRead(mode, data.planWithBarrier, data.planWithBarrier.output.map(_.name),
      data.queryExecution.executedPlan)
  }

  override def columnStatToMap(stat: Any, colName: String,
      dataType: DataType): Map[String, String] = {
    stat.asInstanceOf[ColumnStat].toMap(colName, dataType)
  }

  override def columnStatFromMap(table: String, field: StructField,
      map: Map[String, String]): Option[AnyRef] = {
    ColumnStat.fromMap(table, field, map)
  }

  override def toCatalogStatistics(sizeInBytes: BigInt, rowCount: Option[BigInt],
      colStats: Map[String, AnyRef]): AnyRef = {
    CatalogStatistics(sizeInBytes, rowCount, colStats.asInstanceOf[Map[String, ColumnStat]])
  }

  override def newEmbeddedHiveCatalog(conf: SparkConf, hadoopConf: Configuration,
      createTime: Long): SnappyHiveExternalCatalog = {
    new SnappyEmbeddedHiveCatalog23(conf, hadoopConf, createTime)
  }

  override def newSmartConnectorExternalCatalog(session: SparkSession): SnappyExternalCatalog = {
    new SmartConnectorExternalCatalog23(session)
  }

  override def newSnappySessionState(snappySession: SnappySession): SnappySessionState = {
    new SnappySessionStateBuilder23(snappySession).build()
  }

  override def newCacheManager(): CacheManager = new SnappyCacheManager23

  override def newExprCode(code: String, isNull: String, value: String, dt: DataType): ExprCode = {
    ExprCode(code, isNull, value)
  }

  override def copyExprCode(ev: ExprCode, code: String, isNull: String,
      value: String, dt: DataType): ExprCode = {
    ev.copy(code = if (code ne null) code else ev.code,
      isNull = if (isNull ne null) isNull else ev.isNull,
      value = if (value ne null) value else ev.value)
  }

  override def resetCode(ev: ExprCode): Unit = {
    ev.code = ""
  }

  override def exprCodeIsNull(ev: ExprCode): String = ev.isNull

  override def setExprCodeIsNull(ev: ExprCode, isNull: String): Unit = {
    ev.isNull = isNull
  }

  override def exprCodeValue(ev: ExprCode): String = ev.value

  override def javaType(dt: DataType, ctx: CodegenContext): String = ctx.javaType(dt)

  override def boxedType(javaType: String, ctx: CodegenContext): String = ctx.boxedType(javaType)

  override def defaultValue(dt: DataType, ctx: CodegenContext): String = ctx.defaultValue(dt)

  override def isPrimitiveType(javaType: String, ctx: CodegenContext): Boolean = {
    ctx.isPrimitiveType(javaType)
  }

  override def primitiveTypeName(javaType: String, ctx: CodegenContext): String = {
    ctx.primitiveTypeName(javaType)
  }

  override def getValue(input: String, dataType: DataType, ordinal: String,
      ctx: CodegenContext): String = {
    ctx.getValue(input, dataType, ordinal)
  }

  override def optionalQueryPreparations(session: SparkSession): Seq[Rule[SparkPlan]] = {
    python.ExtractPythonUDFs :: Nil
  }

  override def newPivot(groupByExprs: Seq[NamedExpression], pivotColumn: Expression,
      pivotValues: Seq[Expression], aggregates: Seq[Expression], child: LogicalPlan): Pivot = {
    if (!pivotValues.forall(_.isInstanceOf[Literal])) {
      throw new AnalysisException(
        s"Literal expressions required for pivot values, found: ${pivotValues.mkString("; ")}")
    }
    Pivot(groupByExprs, pivotColumn, pivotValues.map(_.asInstanceOf[Literal]), aggregates, child)
  }

  override def copyPivot(pivot: Pivot, groupByExprs: Seq[NamedExpression]): Pivot = {
    pivot.copy(groupByExprs = groupByExprs)
  }

  override def newIntersect(left: LogicalPlan, right: LogicalPlan, isAll: Boolean): Intersect = {
    if (isAll) {
      throw new ParseException(s"INTERSECT ALL not supported in spark $version")
    }
    Intersect(left, right)
  }

  override def newExcept(left: LogicalPlan, right: LogicalPlan, isAll: Boolean): Except = {
    if (isAll) {
      throw new ParseException(s"EXCEPT ALL not supported in spark $version")
    }
    Except(left, right)
  }

  override def cachedColumnBuffers(relation: InMemoryRelation): RDD[_] = {
    relation.cachedColumnBuffers
  }

  override def addStringPromotionRules(rules: Seq[Rule[LogicalPlan]],
      analyzer: SnappyAnalyzer, conf: SQLConf): Seq[Rule[LogicalPlan]] = {
    rules.flatMap {
      case PromoteStrings =>
        (analyzer.StringPromotionCheckForUpdate :: analyzer.SnappyPromoteStrings ::
            PromoteStrings :: Nil).asInstanceOf[Seq[Rule[LogicalPlan]]]
      case r => r :: Nil
    }
  }
}

/**
 * Extension of SnappyCacheManager23_4 to enable clearing cached plans on cache create/drop.
 */
class SnappyCacheManager23 extends SnappyCacheManager23_4 {

  override def uncacheQuery(session: SparkSession, plan: LogicalPlan, blocking: Boolean): Unit = {
    super.uncacheQuery(session, plan, blocking)
    session.asInstanceOf[SnappySession].clearPlanCache()
  }
}

class SnappyEmbeddedHiveCatalog23(_conf: SparkConf, _hadoopConf: Configuration,
    _createTime: Long) extends SnappyHiveExternalCatalog(_conf, _hadoopConf, _createTime) {

  override def getTable(schema: String, table: String): CatalogTable =
    getTableImpl(schema, table)

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
    super.loadDynamicPartitions(schema, table, loadPath, partition, replace, numDP)
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

  override def doAlterDatabase(schemaDefinition: CatalogDatabase): Unit =
    alterDatabaseImpl(schemaDefinition)

  override protected def doCreateTable(table: CatalogTable, ignoreIfExists: Boolean): Unit =
    createTableImpl(table, ignoreIfExists)

  override protected def doDropTable(schema: String, table: String, ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = dropTableImpl(schema, table, ignoreIfNotExists, purge)

  override protected def doRenameTable(schema: String, oldName: String, newName: String): Unit =
    renameTableImpl(schema, oldName, newName)

  override def doAlterTable(table: CatalogTable): Unit = alterTableImpl(table)

  override def doAlterTableStats(schema: String, table: String,
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

class SmartConnectorExternalCatalog23(override val session: SparkSession)
    extends SmartConnectorExternalCatalog {

  override def getTable(schema: String, table: String): CatalogTable =
    getTableImpl(schema, table)

  override protected def doCreateDatabase(schemaDefinition: CatalogDatabase,
      ignoreIfExists: Boolean): Unit = createDatabaseImpl(schemaDefinition, ignoreIfExists)

  override protected def doDropDatabase(schema: String, ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = dropDatabaseImpl(schema, ignoreIfNotExists, cascade)

  override protected def doAlterDatabase(schemaDefinition: CatalogDatabase): Unit =
    throw new UnsupportedOperationException("Schema definitions cannot be altered")

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

class SnappySessionCatalog23(override val snappySession: SnappySession,
    override val snappyExternalCatalog: SnappyExternalCatalog,
    override val globalTempManager: GlobalTempViewManager,
    override val functionResourceLoader: FunctionResourceLoader,
    override val functionRegistry: FunctionRegistry, override val parser: SnappySqlParser,
    override val sqlConf: SQLConf, hadoopConf: Configuration,
    override val wrappedCatalog: Option[SnappySessionCatalog])
    extends SessionCatalog(snappyExternalCatalog, globalTempManager, functionRegistry,
      sqlConf, hadoopConf, parser, functionResourceLoader) with SnappySessionCatalog23_4 {

  override protected def baseCreateTable(table: CatalogTable, ignoreIfExists: Boolean,
      validateTableLocation: Boolean): Unit = super.createTable(table, ignoreIfExists)

  override def createTable(table: CatalogTable, ignoreIfExists: Boolean): Unit = {
    createTableImpl(table, ignoreIfExists, validateTableLocation = true)
  }
}

class SnappySessionStateBuilder23(session: SnappySession, parentState: Option[SessionState] = None)
    extends SnappySessionStateBuilder23_4(session, parentState) {

  override protected lazy val resourceLoader: SessionResourceLoader = externalCatalog match {
    case c: SnappyHiveExternalCatalog => new HiveSessionResourceLoader(session, c.client())
    case _ => new SessionResourceLoader(session)
  }

  override protected def newSessionCatalog(
      wrapped: Option[SnappySessionCatalog]): SnappySessionCatalog = {
    new SnappySessionCatalog23(
      session,
      externalCatalog,
      session.sharedState.globalTempViewManager,
      resourceLoader,
      functionRegistry,
      sqlParser,
      conf,
      SessionState.newHadoopConf(session.sparkContext.hadoopConfiguration, conf),
      wrapped)
  }

  override protected def optimizer: Optimizer = {
    new SparkOptimizer(catalog, experimentalMethods) with DefaultOptimizer {

      override def state: SnappySessionState = session.snappySessionState

      override def batches: Seq[Batch] = batchesImpl

      override def extendedOperatorOptimizationRules: Seq[Rule[LogicalPlan]] =
        super.extendedOperatorOptimizationRules ++ customOperatorOptimizationRules
    }
  }

  override protected def newBuilder: NewBuilder = (session, optState) =>
    new SnappySessionStateBuilder23(session.asInstanceOf[SnappySession], optState)
}
