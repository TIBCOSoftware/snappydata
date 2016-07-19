/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql.aqp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{InternalRow, ParserDialect}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.{DDLParser, ResolveDataSource, StoreDataSourceStrategy}
import org.apache.spark.sql.hive.{ExternalTableType, QualifiedTableName, SnappyStoreHiveCatalog}
import org.apache.spark.sql.sources.{BaseRelation, StoreStrategy}
import org.apache.spark.sql.streaming.StreamBaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{execution => sparkexecution, _}
import org.apache.spark.util.Utils

object SnappyContextDefaultFunctions extends SnappyContextFunctions {


  def clear(): Unit = {}
  def postRelationCreation(relation: BaseRelation, snc: SnappyContext): Unit = {}
  def getAQPRuleExecutor(sqlContext: SQLContext): RuleExecutor[SparkPlan] =
    new RuleExecutor[SparkPlan] {
      val batches = Seq(
        Batch("Add exchange", Once, EnsureRequirements(sqlContext)),
        Batch("Add row converters", Once, EnsureRowFormats)
      )
    }

  override def registerAQPErrorFunctions(context: SnappyContext){}


  protected[sql] def executePlan(context: SnappyContext,
      plan: LogicalPlan): QueryExecution =
    new sparkexecution.QueryExecution(context, plan)

  override def createTopK(context: SnappyContext, tableName: String,
      keyColumnName: String, schema: StructType,
      topkOptions: Map[String, String], ifExists: Boolean): Unit =
    throw new UnsupportedOperationException("missing aqp jar")

  override def dropTopK(context: SnappyContext, topKName: String): Unit =
    throw new UnsupportedOperationException("missing aqp jar")

  def insertIntoTopK(context: SnappyContext, rows: RDD[Row],
      topKName: QualifiedTableName, time: Long): Unit =
    throw new UnsupportedOperationException("missing aqp jar")

  override def queryTopK(context: SnappyContext, topKName: String,
      startTime: String, endTime: String, k: Int): DataFrame =
    throw new UnsupportedOperationException("missing aqp jar")

  override def queryTopK(context: SnappyContext, topK: String,
      startTime: Long, endTime: Long, k: Int): DataFrame =
    throw new UnsupportedOperationException("missing aqp jar")

  override def queryTopKRDD(context: SnappyContext, topK: String,
      startTime: String, endTime: String, schema: StructType): RDD[InternalRow] =
    throw new UnsupportedOperationException("missing aqp jar")

  protected[sql] def collectSamples(context: SnappyContext, rows: RDD[Row],
      aqpTables: Seq[String], time: Long): Unit =
    throw new UnsupportedOperationException("missing aqp jar")

  def createSampleDataFrameContract(context: SnappyContext, df: DataFrame,
      logicalPlan: LogicalPlan): SampleDataFrameContract =
    throw new UnsupportedOperationException("missing aqp jar")

  def convertToStratifiedSample(options: Map[String, Any], snc: SnappyContext,
      logicalPlan: LogicalPlan): LogicalPlan =
    throw new UnsupportedOperationException("missing aqp jar")

  def isStratifiedSample(logicalPlan: LogicalPlan): Boolean =
    throw new UnsupportedOperationException("missing aqp jar")

  def withErrorDataFrame(df: DataFrame, error: Double,
      confidence: Double, behavior: String): DataFrame =
    throw new UnsupportedOperationException("missing aqp jar")

  def getPlanner(context: SnappyContext): SparkPlanner =
    new DefaultPlanner(context)

  def getSQLDialect(context: SnappyContext): ParserDialect = {
    try {
      val clazz = Utils.classForName(
        "org.apache.spark.sql.SnappyExtendedParserDialect")
      clazz.getConstructor(classOf[SnappyContext]).newInstance(context)
          .asInstanceOf[ParserDialect]
    } catch {
      case _: Exception =>
        new SnappyParserDialect(context)
    }
  }

  def aqpTablePopulator(context: SnappyContext): Unit = {
    // register blank tasks for the stream tables so that the streams start
    val catalog = context.catalog
    catalog.getDataSourceRelations[StreamBaseRelation](Seq(ExternalTableType
        .Stream), None).foreach(_.rowStream.foreachRDD(rdd => Unit))
  }

  def getSnappyCatalog(context: SnappyContext): SnappyStoreHiveCatalog = {
    SnappyStoreHiveCatalog.closeCurrent()
    new SnappyStoreHiveCatalog(context)
  }
  def getSnappyDDLParser(context: SnappyContext,
      planGenerator: String => LogicalPlan): DDLParser =
    new SnappyDDLParser(context.conf.caseSensitiveAnalysis, planGenerator)

  def createAnalyzer(context: SnappyContext): Analyzer =
    new Analyzer(context.catalog, context.functionRegistry, context.conf) {
      override val extendedResolutionRules =
          ExtractPythonUDFs ::
          PreInsertCheckCastAndRename ::
          (if (context.conf.runSQLOnFile) new ResolveDataSource(context) :: Nil
          else Nil)

      override val extendedCheckRules = Seq(
        sparkexecution.datasources.PreWriteCheck(context.catalog), PrePutCheck)
    }

  def handleErrorLimitExceeded[T](fn: => (RDD[InternalRow], DataFrame) => T,
      rowRDD: RDD[InternalRow], df: DataFrame, lp: LogicalPlan): T = fn(rowRDD, df)
}

class DefaultPlanner(snappyContext: SnappyContext)
    extends SparkPlanner(snappyContext) with SnappyStrategies {

  val sampleSnappyCase: PartialFunction[LogicalPlan, Seq[SparkPlan]] = {
    case _ => Nil
  }
  val sampleStreamCase: PartialFunction[LogicalPlan, Seq[SparkPlan]] = {
    case _ => Nil
  }

  // TODO temporary flag till we determine every thing works fine with the optimizations
  val storeOptimization = snappyContext.sparkContext.getConf.get(
    "snappy.store.optimization", "true").toBoolean

  val storeOptimizedRules: Seq[Strategy] = if (storeOptimization) {
    Seq(StoreDataSourceStrategy, LocalJoinStrategies)
  } else Nil

  override def strategies: Seq[Strategy] =
    Seq(SnappyStrategies,
      StreamDDLStrategy(sampleStreamCase),
      StoreStrategy, StreamQueryStrategy) ++
        storeOptimizedRules ++
        super.strategies
}
