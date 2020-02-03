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
import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TypeCoercion}
import org.apache.spark.sql.catalyst.catalog.{CatalogColumnStat, CatalogTable, FunctionResourceLoader, GlobalTempViewManager}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, ExprId, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Except, Intersect, LogicalPlan, Pivot}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SparkOptimizer, SparkPlan}
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.common.ErrorEstimateAttribute
import org.apache.spark.sql.hive.{SnappyAnalyzer, SnappySessionState}
import org.apache.spark.sql.types.{DataType, Metadata, StructField}
import org.apache.spark.sql.{SnappySession, SnappySqlParser, SparkInternals, SparkSession}

/**
 * Implementation of [[SparkInternals]] for Spark 2.4.4.
 */
class Spark244Internals extends Spark232Internals {

  override def uncacheQuery(spark: SparkSession, plan: LogicalPlan,
      cascade: Boolean, blocking: Boolean): Unit = {
    spark.sharedState.cacheManager.uncacheQuery(spark, plan, cascade, blocking)
  }

  private def toQualifier(q: Option[String]): Seq[String] = if (q.isEmpty) Nil else q.get :: Nil

  override def toAttributeReference(attr: Attribute)(name: String,
      dataType: DataType, nullable: Boolean, metadata: Metadata,
      exprId: ExprId): AttributeReference = {
    AttributeReference(name = name, dataType = dataType, nullable = nullable, metadata = metadata)(
      exprId, qualifier = toQualifier(attr.qualifier))
  }

  override def newAttributeReference(name: String, dataType: DataType, nullable: Boolean,
      metadata: Metadata, exprId: ExprId, qualifier: Option[String],
      isGenerated: Boolean): AttributeReference = {
    AttributeReference(name, dataType, nullable, metadata)(exprId, toQualifier(qualifier))
  }

  override def newErrorEstimateAttribute(name: String, dataType: DataType,
      nullable: Boolean, metadata: Metadata, exprId: ExprId, realExprId: ExprId,
      qualifier: Seq[String]): ErrorEstimateAttribute = {
    ErrorEstimateAttribute24(name, dataType, nullable, metadata, exprId, realExprId)(qualifier)
  }

  override def newAlias(child: Expression, name: String, copyAlias: Option[NamedExpression],
      exprId: ExprId, qualifier: Option[String]): Alias = {
    copyAlias match {
      case None => Alias(child, name)(exprId, toQualifier(qualifier))
      case Some(a: Alias) => Alias(child, name)(a.exprId, a.qualifier, a.explicitMetadata)
      case Some(a) => Alias(child, name)(a.exprId, a.qualifier)
    }
  }

  override def columnStatToMap(stat: Any, colName: String,
      dataType: DataType): Map[String, String] = {
    stat.asInstanceOf[CatalogColumnStat].toMap(colName)
  }

  override def columnStatFromMap(table: String, field: StructField,
      map: Map[String, String]): Option[ColumnStat] = {
    CatalogColumnStat.fromMap(table, field.name, map).map(_.toPlanStat(field.name, field.dataType))
  }

  override def newSharedState(sparkContext: SparkContext): SnappySharedState = {
    val state = new SnappySharedState24(sparkContext)
    createAndAttachSQLListener(state, sparkContext)
    state
  }

  override def newSnappySessionState(snappySession: SnappySession): SnappySessionState = {
    new SnappySessionStateBuilder24(snappySession).build()
  }

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
}

class SnappySessionStateBuilder24(session: SnappySession, parentState: Option[SessionState] = None)
    extends SnappySessionStateBuilder23(session, parentState) {

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

class SnappySessionCatalog24(override val snappySession: SnappySession,
    override val snappyExternalCatalog: SnappyExternalCatalog,
    override val globalTempViewManager: GlobalTempViewManager,
    override val functionResourceLoader: FunctionResourceLoader,
    override val functionRegistry: FunctionRegistry, override val parser: SnappySqlParser,
    override val sqlConf: SQLConf, hadoopConf: Configuration,
    override val wrappedCatalog: Option[SnappySessionCatalog])
    extends SnappySessionCatalog23(snappySession, snappyExternalCatalog, globalTempViewManager,
      functionResourceLoader, functionRegistry, parser, sqlConf, hadoopConf, wrappedCatalog) {

  override protected def baseCreateTable(table: CatalogTable, ignoreIfExists: Boolean,
      validateTableLocation: Boolean): Unit = {
    super.createTable(table, ignoreIfExists, validateTableLocation)
  }

  override def createTable(table: CatalogTable, ignoreIfExists: Boolean,
      validateTableLocation: Boolean): Unit = {
    createTableImpl(table, ignoreIfExists, validateTableLocation)
  }
}

case class ErrorEstimateAttribute24(name: String, dataType: DataType, nullable: Boolean,
    override val metadata: Metadata, exprId: ExprId, realExprId: ExprId)(
    val qualifier: Seq[String]) extends ErrorEstimateAttribute {

  override def singleQualifier: Option[String] = qualifier.headOption

  override def withQualifier(newQualifier: Seq[String]): Attribute = {
    if (newQualifier == qualifier) {
      this
    } else {
      ErrorEstimateAttribute24(name, dataType, nullable, metadata, exprId,
        realExprId)(newQualifier)
    }
  }
}
