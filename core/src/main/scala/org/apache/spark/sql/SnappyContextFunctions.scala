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
package org.apache.spark.sql

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable

import io.snappydata.SnappyDataFunctions
import io.snappydata.sql.catalog.CatalogObjectType

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReuseExchange}
import org.apache.spark.sql.execution.{CollapseCodegenStages, PlanLater, QueryExecution, SparkPlan, TopK, python}
import org.apache.spark.sql.hive.OptimizeSortAndFilePlans
import org.apache.spark.sql.internal.{BypassRowLevelSecurity, MarkerForCreateTableAsSelect}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.streaming.StreamBaseRelation
import org.apache.spark.sql.types.StructType

class SnappyContextFunctions(val session: SnappySession) extends SparkSupport {

  /**
   * Temporary sample dataFrames registered using stratifiedSample API that do not go
   * in external catalog.
   */
  protected[sql] val mainDFToSamples =
    new ConcurrentHashMap[LogicalPlan, mutable.ArrayBuffer[(LogicalPlan, String)]]()

  protected final lazy val queryPreparationsTopLevel: Seq[Rule[SparkPlan]] =
    createQueryPreparations(topLevel = true)

  protected final lazy val queryPreparationsNode: Seq[Rule[SparkPlan]] =
    createQueryPreparations(topLevel = false)

  def clear(): Unit = {}

  def clearStatic(): () => Unit = () => {}

  def postRelationCreation(relation: Option[BaseRelation]): Unit = {}

  def registerSnappyFunctions(): Unit = {
    val registry = session.sessionState.functionRegistry
    SnappyDataFunctions.builtin.foreach(fn => registry.registerFunction(fn._1, fn._2, fn._3))
  }

  private def missingAQPException(): AnalysisException =
    new AnalysisException("requires AQP support")

  def setQueryExecutor(qe: Option[QueryExecution]): Unit = throw missingAQPException()

  def getQueryExecution: Option[QueryExecution] = throw missingAQPException()

  def addSampleDataFrame(base: LogicalPlan, sample: LogicalPlan, name: String): Unit =
    throw missingAQPException()

  /**
   * Return the set of temporary samples for a given table that are not tracked in catalog.
   */
  def getSamples(base: LogicalPlan): Seq[LogicalPlan] = throw missingAQPException()

  /**
   * Return the set of samples for a given table that are tracked in catalog and are not temporary.
   */
  def getSampleRelations(baseTable: TableIdentifier): Seq[(LogicalPlan, String)] =
    throw missingAQPException()

  def postCreateTable(table: CatalogTable): Unit = {}

  def dropTemporaryTable(tableIdent: TableIdentifier): Unit = {}

  def dropFromTemporaryBaseTable(table: CatalogTable): Unit = {}

  def createTopK(tableName: String, keyColumnName: String, schema: StructType,
      topkOptions: Map[String, String], ifExists: Boolean): Boolean = throw missingAQPException()

  def dropTopK(topKName: String): Unit = throw missingAQPException()

  def insertIntoTopK(rows: RDD[Row], topKName: String, time: Long): Unit =
    throw missingAQPException()

  def queryTopK(topKName: String, startTime: String, endTime: String, k: Int): DataFrame =
    throw missingAQPException()

  def queryTopK(topK: String, startTime: Long, endTime: Long, k: Int): DataFrame =
    throw missingAQPException()

  def queryTopKRDD(topK: String, startTime: String, endTime: String,
      schema: StructType): RDD[InternalRow] = throw missingAQPException()

  def lookupTopK(topKName: String): Option[(AnyRef, RDD[(Int, TopK)])] =
    throw missingAQPException()

  def registerTopK(topK: AnyRef, rdd: RDD[(Int, TopK)], ifExists: Boolean,
      overwrite: Boolean): Boolean = throw missingAQPException()

  def unregisterTopK(topKName: String): Unit = throw missingAQPException()

  protected[sql] def collectSamples(rows: RDD[Row], aqpTables: Seq[String],
      time: Long): Unit = throw missingAQPException()

  def createSampleDataFrameContract(df: DataFrame,
      logicalPlan: LogicalPlan): SampleDataFrameContract = throw missingAQPException()

  def convertToStratifiedSample(options: Map[String, Any],
      logicalPlan: LogicalPlan): LogicalPlan = throw missingAQPException()

  def isStratifiedSample(logicalPlan: LogicalPlan): Boolean = throw missingAQPException()

  def withErrorDataFrame(df: DataFrame, error: Double,
      confidence: Double, behavior: String): DataFrame = throw missingAQPException()

  def newSQLParser(): SnappySqlParser = new SnappySqlParser(session)

  def aqpTablePopulator(): Unit = {
    // register blank tasks for the stream tables so that the streams start
    session.sessionState.catalog.getDataSourceRelations[StreamBaseRelation](
      CatalogObjectType.Stream).foreach(_.rowStream.foreachRDD(_ => Unit))
  }

  def createSampleSnappyCase(): PartialFunction[LogicalPlan, Seq[SparkPlan]] = {
    case MarkerForCreateTableAsSelect(child) => PlanLater(child) :: Nil
    case BypassRowLevelSecurity(child) => PlanLater(child) :: Nil
    case _ => Nil
  }

  def getExtendedResolutionRules: List[Rule[LogicalPlan]] = Nil

  protected def createQueryPreparations(
      topLevel: Boolean): Seq[Rule[SparkPlan]] = Seq[Rule[SparkPlan]](
    python.ExtractPythonUDFs,
    TokenizeSubqueries(session),
    EnsureRequirements(session.sessionState.conf),
    OptimizeSortAndFilePlans(session.sessionState.snappyConf),
    CollapseCollocatedPlans(session),
    CollapseCodegenStages(session.sessionState.conf),
    InsertCachedPlanFallback(session, topLevel),
    ReuseExchange(session.sessionState.conf))

  def queryPreparations(topLevel: Boolean): Seq[Rule[SparkPlan]] =
    if (topLevel) queryPreparationsTopLevel else queryPreparationsNode

  def executePlan(analyzer: Analyzer, plan: LogicalPlan): LogicalPlan = analyzer.execute(plan)

  def sql[T](fn: => T): T = fn
}
