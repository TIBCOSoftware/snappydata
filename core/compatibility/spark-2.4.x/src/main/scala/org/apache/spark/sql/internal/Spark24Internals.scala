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
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TypeCoercion}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, ExprId, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.{Except, Intersect, LogicalPlan, Pivot}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.bootstrap.{ApproxColumnExtractor, Tag, TaggedAlias, TaggedAttribute, TransformableTag}
import org.apache.spark.sql.execution.closedform.{ClosedFormColumnExtractor, ErrorAggregate, ErrorEstimateAttribute}
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.common.HAC
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.{CacheManager, SparkOptimizer, SparkPlan}
import org.apache.spark.sql.hive.{HiveSessionResourceLoader, SnappyAnalyzer, SnappyHiveExternalCatalog, SnappySessionState}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{DataType, Metadata, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Base implementation of [[SparkInternals]] for Spark 2.4.x releases.
 */
abstract class Spark24Internals extends Spark23_4_Internals {

  override def uncacheQuery(spark: SparkSession, plan: LogicalPlan,
      cascade: Boolean, blocking: Boolean): Unit = {
    spark.sharedState.cacheManager.uncacheQuery(spark, plan, cascade, blocking)
  }

  override def toAttributeReference(attr: Attribute)(name: String,
      dataType: DataType, nullable: Boolean, metadata: Metadata,
      exprId: ExprId): AttributeReference = {
    AttributeReference(name = name, dataType = dataType, nullable = nullable, metadata = metadata)(
      exprId, qualifier = attr.qualifier)
  }

  override def newAttributeReference(name: String, dataType: DataType, nullable: Boolean,
      metadata: Metadata, exprId: ExprId, qualifier: Seq[String],
      isGenerated: Boolean): AttributeReference = {
    AttributeReference(name, dataType, nullable, metadata)(exprId, qualifier)
  }

  override def newErrorEstimateAttribute(name: String, dataType: DataType,
      nullable: Boolean, metadata: Metadata, realExprId: ExprId, exprId: ExprId,
      qualifier: Seq[String]): ErrorEstimateAttribute = {
    ErrorEstimateAttribute24(name, dataType, nullable, metadata, realExprId)(exprId, qualifier)
  }

  override def newApproxColumnExtractor(child: Expression, name: String, ordinal: Int,
      dataType: DataType, nullable: Boolean, exprId: ExprId,
      qualifier: Seq[String]): ApproxColumnExtractor = {
    ApproxColumnExtractor24(child, name, ordinal, dataType, nullable)(exprId, qualifier)
  }

  override def newTaggedAttribute(tag: Tag, name: String, dataType: DataType, nullable: Boolean,
      metadata: Metadata, exprId: ExprId, qualifier: Seq[String]): TaggedAttribute = {
    TaggedAttribute24(tag, name, dataType, nullable, metadata)(exprId, qualifier)
  }

  override def newTaggedAlias(tag: TransformableTag, child: Expression, name: String,
      exprId: ExprId, qualifier: Seq[String]): TaggedAlias = {
    TaggedAlias24(tag, child, name)(exprId, qualifier)
  }

  // scalastyle:off

  override def newClosedFormColumnExtractor(child: Expression, name: String, confidence: Double,
      confFactor: Double, aggType: ErrorAggregate.Type, error: Double, dataType: DataType,
      behavior: HAC.Type, nullable: Boolean, exprId: ExprId,
      qualifier: Seq[String]): ClosedFormColumnExtractor = {
    ClosedFormColumnExtractor24(child, name, confidence, confFactor, aggType, error,
      dataType, behavior, nullable)(exprId, qualifier)
  }

  // scalastyle:on

  override def newAlias(child: Expression, name: String, copyAlias: Option[NamedExpression],
      exprId: ExprId, qualifier: Seq[String]): Alias = {
    copyAlias match {
      case None => Alias(child, name)(exprId, qualifier)
      case Some(a: Alias) => Alias(child, name)(a.exprId, a.qualifier, a.explicitMetadata)
      case Some(a) => Alias(child, name)(a.exprId, a.qualifier)
    }
  }

  override def writeToDataSource(ds: DataSource, mode: SaveMode,
      data: Dataset[Row]): BaseRelation = {
    ds.writeAndRead(mode, data.logicalPlan, data.logicalPlan.output.map(_.name),
      data.queryExecution.executedPlan)
  }

  override def columnStatToMap(stat: Any, colName: String,
      dataType: DataType): Map[String, String] = {
    stat.asInstanceOf[CatalogColumnStat].toMap(colName)
  }

  override def columnStatFromMap(table: String, field: StructField,
      map: Map[String, String]): Option[AnyRef] = {
    CatalogColumnStat.fromMap(table, field.name, map)
  }

  override def toCatalogStatistics(sizeInBytes: BigInt, rowCount: Option[BigInt],
      colStats: Map[String, AnyRef]): AnyRef = {
    CatalogStatistics(sizeInBytes, rowCount, colStats.asInstanceOf[Map[String, CatalogColumnStat]])
  }

  override def newEmbeddedHiveCatalog(conf: SparkConf, hadoopConf: Configuration,
      createTime: Long): SnappyHiveExternalCatalog = {
    new SnappyEmbeddedHiveCatalog24(conf, hadoopConf, createTime)
  }

  override def newSmartConnectorExternalCatalog(session: SparkSession): SnappyExternalCatalog = {
    new SmartConnectorExternalCatalog24(session)
  }

  override def newSharedState(sparkContext: SparkContext): SnappySharedState = {
    val state = new SnappySharedState24(sparkContext)
    createAndAttachSQLListener(state, sparkContext)
    state
  }

  override def newSnappySessionState(snappySession: SnappySession): SnappySessionState = {
    new SnappySessionStateBuilder24(snappySession).build()
  }

  override def newCacheManager(): CacheManager = new SnappyCacheManager24

  private def exprValue(v: String, javaClass: Class[_]): ExprValue = v match {
    case "false" => FalseLiteral
    case "true" => TrueLiteral
    case _ => VariableValue(v, javaClass)
  }

  override def newExprCode(code: String, isNull: String,
      value: String, javaClass: Class[_]): ExprCode = {
    ExprCode(CodeBlock(code :: Nil, EmptyBlock :: Nil),
      isNull = exprValue(isNull, java.lang.Boolean.TYPE),
      value = exprValue(value, javaClass))
  }

  override def copyExprCode(ev: ExprCode, code: String, isNull: String,
      value: String, javaClass: Class[_]): ExprCode = {
    val codeBlock =
      if (code eq null) ev.code
      else if (code.isEmpty) EmptyBlock
      else CodeBlock(code :: Nil, EmptyBlock :: Nil)
    ev.copy(codeBlock,
      if (isNull ne null) VariableValue(isNull, java.lang.Boolean.TYPE) else ev.isNull,
      if (value ne null) VariableValue(value, javaClass) else ev.value)
  }

  override def resetCode(ev: ExprCode): Unit = {
    ev.code = EmptyBlock
  }

  override def exprCodeIsNull(ev: ExprCode): String = ev.isNull.code

  override def setExprCodeIsNull(ev: ExprCode, isNull: String): Unit = {
    ev.isNull = exprValue(isNull, classOf[Boolean])
  }

  override def exprCodeValue(ev: ExprCode): String = ev.value.code

  override def javaType(dt: DataType, ctx: CodegenContext): String = CodeGenerator.javaType(dt)

  override def boxedType(javaType: String, ctx: CodegenContext): String = {
    CodeGenerator.boxedType(javaType)
  }

  override def defaultValue(dt: DataType, ctx: CodegenContext): String = {
    CodeGenerator.defaultValue(dt)
  }

  override def isPrimitiveType(javaType: String, ctx: CodegenContext): Boolean = {
    CodeGenerator.isPrimitiveType(javaType)
  }

  override def primitiveTypeName(javaType: String, ctx: CodegenContext): String = {
    CodeGenerator.primitiveTypeName(javaType)
  }

  override def getValue(input: String, dataType: DataType, ordinal: String,
      ctx: CodegenContext): String = {
    CodeGenerator.getValue(input, dataType, ordinal)
  }

  override def optionalQueryPreparations(session: SparkSession): Seq[Rule[SparkPlan]] = Nil

  override def newPivot(groupByExprs: Seq[NamedExpression], pivotColumn: Expression,
      pivotValues: Seq[Expression], aggregates: Seq[Expression], child: LogicalPlan): Pivot = {
    Pivot(if (groupByExprs.isEmpty) None else Some(groupByExprs), pivotColumn, pivotValues,
      aggregates, child)
  }

  override def copyPivot(pivot: Pivot, groupByExprs: Seq[NamedExpression]): Pivot = {
    pivot.copy(groupByExprsOpt = if (groupByExprs.isEmpty) None else Some(groupByExprs))
  }

  override def newIntersect(left: LogicalPlan, right: LogicalPlan, isAll: Boolean): Intersect = {
    Intersect(left, right, isAll)
  }

  override def newExcept(left: LogicalPlan, right: LogicalPlan, isAll: Boolean): Except = {
    Except(left, right, isAll)
  }

  override def cachedColumnBuffers(relation: InMemoryRelation): RDD[_] = {
    relation.cacheBuilder.cachedColumnBuffers
  }

  override def addStringPromotionRules(rules: Seq[Rule[LogicalPlan]],
      analyzer: SnappyAnalyzer, conf: SQLConf): Seq[Rule[LogicalPlan]] = {
    rules.flatMap {
      case _: TypeCoercion.PromoteStrings =>
        (analyzer.StringPromotionCheckForUpdate :: analyzer.SnappyPromoteStrings ::
            TypeCoercion.PromoteStrings(conf) :: Nil).asInstanceOf[Seq[Rule[LogicalPlan]]]
      case r => r :: Nil
    }
  }

  override def createTable(catalog: SessionCatalog, tableDefinition: CatalogTable,
      ignoreIfExists: Boolean, validateLocation: Boolean): Unit = {
    catalog.createTable(tableDefinition, ignoreIfExists, validateLocation)
  }
}

final class SnappyEmbeddedHiveCatalog24(_conf: SparkConf, _hadoopConf: Configuration,
    _createTime: Long) extends SnappyHiveExternalCatalog(_conf, _hadoopConf, _createTime) {

  override def getTable(schema: String, table: String): CatalogTable =
    getTableImpl(schema, table)

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
    super.loadDynamicPartitions(schema, table, loadPath, partition, replace, numDP)
  }

  override protected def baseCreateFunction(schema: String,
      funcDefinition: CatalogFunction): Unit = super.createFunction(schema, funcDefinition)

  override protected def baseDropFunction(schema: String, name: String): Unit =
    super.dropFunction(schema, name)

  override protected def baseRenameFunction(schema: String, oldName: String,
      newName: String): Unit = super.renameFunction(schema, oldName, newName)

  override def createDatabase(schemaDefinition: CatalogDatabase,
      ignoreIfExists: Boolean): Unit = createDatabaseImpl(schemaDefinition, ignoreIfExists)

  override def dropDatabase(schema: String, ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = dropDatabaseImpl(schema, ignoreIfNotExists, cascade)

  override def alterDatabase(schemaDefinition: CatalogDatabase): Unit =
    alterDatabaseImpl(schemaDefinition)

  override def createTable(table: CatalogTable, ignoreIfExists: Boolean): Unit =
    createTableImpl(table, ignoreIfExists)

  override def dropTable(schema: String, table: String, ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = dropTableImpl(schema, table, ignoreIfNotExists, purge)

  override def renameTable(schema: String, oldName: String, newName: String): Unit =
    renameTableImpl(schema, oldName, newName)

  override def alterTable(table: CatalogTable): Unit = alterTableImpl(table)

  override def alterTableStats(schema: String, table: String,
      stats: Option[CatalogStatistics]): Unit = {
    withHiveExceptionHandling(super.alterTableStats(schema, table, stats))
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

  override def createFunction(schema: String, function: CatalogFunction): Unit =
    createFunctionImpl(schema, function)

  override def dropFunction(schema: String, funcName: String): Unit =
    dropFunctionImpl(schema, funcName)

  override def alterFunction(schema: String, function: CatalogFunction): Unit = {
    withHiveExceptionHandling(super.alterFunction(schema, function))
    SnappySession.clearAllCache()
  }

  override def renameFunction(schema: String, oldName: String, newName: String): Unit =
    renameFunctionImpl(schema, oldName, newName)
}

final class SmartConnectorExternalCatalog24(override val session: SparkSession)
    extends SmartConnectorExternalCatalog {

  override def getTable(schema: String, table: String): CatalogTable =
    getTableImpl(schema, table)

  override def createDatabase(schemaDefinition: CatalogDatabase,
      ignoreIfExists: Boolean): Unit = createDatabaseImpl(schemaDefinition, ignoreIfExists)

  override def dropDatabase(schema: String, ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = dropDatabaseImpl(schema, ignoreIfNotExists, cascade)

  override def alterDatabase(schemaDefinition: CatalogDatabase): Unit =
    throw new UnsupportedOperationException("Schema definitions cannot be altered")

  override def createTable(table: CatalogTable, ignoreIfExists: Boolean): Unit =
    createTableImpl(table, ignoreIfExists)

  override def dropTable(schema: String, table: String, ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = dropTableImpl(schema, table, ignoreIfNotExists, purge)

  override def renameTable(schema: String, oldName: String, newName: String): Unit =
    renameTableImpl(schema, oldName, newName)

  override def alterTable(table: CatalogTable): Unit = alterTableImpl(table)

  override def alterTableDataSchema(schemaName: String, table: String,
      newSchema: StructType): Unit = alterTableSchemaImpl(schemaName, table, newSchema)

  override def alterTableStats(schema: String, table: String,
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

  override def createFunction(schema: String, function: CatalogFunction): Unit =
    createFunctionImpl(schema, function)

  override def dropFunction(schema: String, funcName: String): Unit =
    dropFunctionImpl(schema, funcName)

  override def alterFunction(schema: String, function: CatalogFunction): Unit =
    alterFunctionImpl(schema, function)

  override def renameFunction(schema: String, oldName: String, newName: String): Unit =
    renameFunctionImpl(schema, oldName, newName)
}

class SnappySessionCatalog24(override val snappySession: SnappySession,
    override val snappyExternalCatalog: SnappyExternalCatalog,
    override val globalTempManager: GlobalTempViewManager,
    override val functionResourceLoader: FunctionResourceLoader,
    override val functionRegistry: FunctionRegistry, override val parser: SnappySqlParser,
    override val sqlConf: SQLConf, hadoopConf: Configuration,
    override val wrappedCatalog: Option[SnappySessionCatalog])
    extends SessionCatalog(() => snappyExternalCatalog, () => globalTempManager, functionRegistry,
      sqlConf, hadoopConf, parser, functionResourceLoader) with SnappySessionCatalog23_4 {

  override protected def baseCreateTable(table: CatalogTable, ignoreIfExists: Boolean,
      validateTableLocation: Boolean): Unit = {
    super.createTable(table, ignoreIfExists, validateTableLocation)
  }

  override def createTable(table: CatalogTable, ignoreIfExists: Boolean,
      validateTableLocation: Boolean): Unit = {
    createTableImpl(table, ignoreIfExists, validateTableLocation)
  }
}

class SnappySessionStateBuilder24(session: SnappySession, parentState: Option[SessionState] = None)
    extends SnappySessionStateBuilder23_4(session, parentState) {

  override protected lazy val resourceLoader: SessionResourceLoader = externalCatalog match {
    case c: SnappyHiveExternalCatalog => new HiveSessionResourceLoader(session, c.client)
    case _ => new SessionResourceLoader(session)
  }

  override protected def newSessionCatalog(
      wrapped: Option[SnappySessionCatalog]): SnappySessionCatalog = {
    new SnappySessionCatalog24(
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

      override def state: SnappySessionState = session.sessionState

      override def defaultBatches: Seq[Batch] = batchesImpl

      override def extendedOperatorOptimizationRules: Seq[Rule[LogicalPlan]] =
        super.extendedOperatorOptimizationRules ++ customOperatorOptimizationRules
    }
  }

  override protected def newBuilder: NewBuilder = (session, optState) =>
    new SnappySessionStateBuilder24(session.asInstanceOf[SnappySession], optState)
}

/**
 * Simple extension to CacheManager to enable clearing cached plan on cache create/drop.
 */
final class SnappyCacheManager24 extends SnappyCacheManager23_4 {

  override def uncacheQuery(session: SparkSession, plan: LogicalPlan,
      cascade: Boolean, blocking: Boolean): Unit = {
    super.uncacheQuery(session, plan, cascade, blocking)
    session.asInstanceOf[SnappySession].clearPlanCache()
  }
}
