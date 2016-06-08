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

package org.apache.spark.sql.internal


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.aqp.SnappyContextFunctions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{Analyzer, EliminateSubqueryAliases}
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, PreInsertCastAndRename, ResolveDataSource, StoreDataSourceStrategy}
import org.apache.spark.sql.execution.exchange.EnsureRequirements
import org.apache.spark.sql.execution.python.ExtractPythonUDFs
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan, SparkPlanner}
import org.apache.spark.sql.hive.{ExternalTableType, QualifiedTableName, SnappyStoreHiveCatalog}
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, PutIntoTable, RowInsertableRelation, RowPutRelation, SchemaInsertableRelation, StoreStrategy}
import org.apache.spark.sql.streaming.StreamBaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{execution => sparkexecution, _}
//import org.apache.spark.util.Utils


class SnappySessionState(snappySession: SnappySession)
    extends SessionState(snappySession) with SnappyContextFunctions {

  self =>


  private lazy val sharedState: SnappySharedState =
    snappySession.sharedState.asInstanceOf[SnappySharedState]

  override lazy val sqlParser: ParserInterface =
    new SnappySqlParser(this.snappySession, getSQLDialect(snappySession))

  protected[sql] override lazy val conf: SQLConf = new SQLConf {
    override def caseSensitiveAnalysis: Boolean =
      getConf(SQLConf.CASE_SENSITIVE, false)
  }

  /**
   * Internal catalog for managing table and database states.
   */
  override lazy val catalog =
    new SnappyStoreHiveCatalog(
      sharedState.externalCatalog,
      snappySession,
      functionResourceLoader,
      functionRegistry,
      conf,
      newHadoopConf())

  override def planner: SparkPlanner = new DefaultPlanner(snappySession, conf,
    experimentalMethods.extraStrategies)


  override def executePlan(plan: LogicalPlan): QueryExecution =
    new sparkexecution.QueryExecution(snappySession, plan)

  @transient
  override protected[sql] lazy val analyzer: Analyzer = new Analyzer(catalog, conf) {
    override val extendedResolutionRules =
      ExtractPythonUDFs ::
          PreInsertCheckCastAndRename ::
          new ResolveDataSource(catalog.snappySession) :: Nil

    override val extendedCheckRules = Seq(
      sparkexecution.datasources.PreWriteCheck(conf, catalog), PrePutCheck)
  }

  override def clear(): Unit = {}

  override def postRelationCreation(relation: BaseRelation, session: SnappySession): Unit = {}

  override def getAQPRuleExecutor(session: SnappySession): RuleExecutor[SparkPlan] =
    new RuleExecutor[SparkPlan] {
      val batches = Seq(
        Batch("Add exchange", Once, EnsureRequirements(conf))
        //Batch("Add row converters", Once, EnsureRowFormats) //@TODO check why not needed, may
        // be because everything is expected in terms of unsafe row now
      )
    }

  override def registerAQPErrorFunctions(session: SnappySession) {}

  override def createTopK(session: SnappySession, tableName: String,
      keyColumnName: String, schema: StructType,
      topkOptions: Map[String, String], ifExists: Boolean): Unit =
    throw new UnsupportedOperationException("missing aqp jar")

  override def dropTopK(session: SnappySession, topKName: String): Unit =
    throw new UnsupportedOperationException("missing aqp jar")

  def insertIntoTopK(session: SnappySession, rows: RDD[Row],
      topKName: QualifiedTableName, time: Long): Unit =
    throw new UnsupportedOperationException("missing aqp jar")

  override def queryTopK(session: SnappySession, topKName: String,
      startTime: String, endTime: String, k: Int): DataFrame =
    throw new UnsupportedOperationException("missing aqp jar")

  override def queryTopK(session: SnappySession, topK: String,
      startTime: Long, endTime: Long, k: Int): DataFrame =
    throw new UnsupportedOperationException("missing aqp jar")

  override def queryTopKRDD(session: SnappySession, topK: String,
      startTime: String, endTime: String, schema: StructType): RDD[InternalRow] =
    throw new UnsupportedOperationException("missing aqp jar")

  protected[sql] def collectSamples(session: SnappySession, rows: RDD[Row],
      aqpTables: Seq[String], time: Long): Unit =
    throw new UnsupportedOperationException("missing aqp jar")

  def createSampleDataFrameContract(session: SnappySession, df: DataFrame,
      logicalPlan: LogicalPlan): SampleDataFrameContract =
    throw new UnsupportedOperationException("missing aqp jar")

  def convertToStratifiedSample(options: Map[String, Any], session: SnappySession,
      logicalPlan: LogicalPlan): LogicalPlan =
    throw new UnsupportedOperationException("missing aqp jar")

  def isStratifiedSample(logicalPlan: LogicalPlan): Boolean =
    throw new UnsupportedOperationException("missing aqp jar")

  def withErrorDataFrame(df: DataFrame, error: Double,
      confidence: Double): DataFrame =
    throw new UnsupportedOperationException("missing aqp jar")

  override def getSQLDialect(session: SnappySession): ParserDialect = {
    try {
      val clazz = Utils.classForName(
        "org.apache.spark.sql.SnappyExtendedParserDialect")
      clazz.getConstructor(classOf[SnappySession]).newInstance(session)
          .asInstanceOf[ParserDialect]
    } catch {
      case _: Exception =>
        new SnappyParserDialect(session)
    }
  }

  def aqpTablePopulator(session: SnappySession): Unit = {
    // register blank tasks for the stream tables so that the streams start

    catalog.getDataSourceRelations[StreamBaseRelation](Seq(ExternalTableType
        .Stream), None).foreach(_.rowStream.foreachRDD(rdd => Unit))
  }
}


class DefaultPlanner(snappySession: SnappySession, conf: SQLConf, extraStrategies: Seq[Strategy])
    extends SparkPlanner(snappySession.sparkContext, conf, extraStrategies)
    with SnappyStrategies {

  val sampleSnappyCase: PartialFunction[LogicalPlan, Seq[SparkPlan]] = {
    case _ => Nil
  }
  val sampleStreamCase: PartialFunction[LogicalPlan, Seq[SparkPlan]] = {
    case _ => Nil
  }

  // TODO temporary flag till we determine every thing works fine with the optimizations
  val storeOptimization = conf.getConfString(
    "snappy.store.optimization", "true").toBoolean

  val storeOptimizedRules: Seq[Strategy] = if (storeOptimization)
    Seq(StoreDataSourceStrategy, LocalJoinStrategies)
  else Nil

  override def strategies: Seq[Strategy] =
    Seq(SnappyStrategies,
      StreamDDLStrategy(sampleStreamCase),
      StoreStrategy, StreamQueryStrategy) ++
        storeOptimizedRules ++
        super.strategies
}

private[sql] object PreInsertCheckCastAndRename extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // Wait until children are resolved.
    case p: LogicalPlan if !p.childrenResolved => p

    // Check for SchemaInsertableRelation first
    case i@InsertIntoTable(l@LogicalRelation(r:
        SchemaInsertableRelation, _, _), _, query, _, _) =>
      r.schemaForInsert(query.output) match {
        case Some(output) =>
          PreInsertCastAndRename.castAndRenameChildOutput(i, output, query)
        case None =>
          throw new AnalysisException(s"$l requires that the query in the " +
              "SELECT clause of the INSERT INTO/OVERWRITE statement " +
              "generates the same number of columns as its schema.")
      }

    // Check for PUT
    // Need to eliminate subqueries here. Unlike InsertIntoTable whose
    // subqueries have already been eliminated by special check in
    // ResolveRelations, no such special rule has been added for PUT
    case p@PutIntoTable(table, query) =>
      EliminateSubqueryAliases(table) match {
        case l@LogicalRelation(_: RowInsertableRelation, _, _) =>
          // First, make sure the data to be inserted have the same number of
          // fields with the schema of the relation.
          if (l.output.size != query.output.size) {
            throw new AnalysisException(s"$l requires that the query in the " +
                "SELECT clause of the PUT INTO statement " +
                "generates the same number of columns as its schema.")
          }
          castAndRenameChildOutput(p, l, query)

        case _ => p
      }

    // We are inserting into an InsertableRelation or HadoopFsRelation.
    case i@InsertIntoTable(l@LogicalRelation(_: InsertableRelation |
                                             _: HadoopFsRelation, _, _), _, child, _, _) =>
      // First, make sure the data to be inserted have the same number of
      // fields with the schema of the relation.
      if (l.output.size != child.output.size) {
        throw new AnalysisException(s"$l requires that the query in the " +
            "SELECT clause of the INSERT/OVERWRITE statement " +
            "generates the same number of columns as its schema.")
      }
      PreInsertCastAndRename.castAndRenameChildOutput(i, l.output, child)
  }

  /**
   * If necessary, cast data types and rename fields to the expected
   * types and names.
   */
  def castAndRenameChildOutput(
      putInto: PutIntoTable,
      relation: LogicalRelation,
      child: LogicalPlan): PutIntoTable = {
    val newChildOutput = relation.output.zip(child.output).map {
      case (expected, actual) =>
        val needCast = !expected.dataType.sameType(actual.dataType)
        // We want to make sure the filed names in the data to be inserted exactly match
        // names in the schema.
        val needRename = expected.name != actual.name
        (needCast, needRename) match {
          case (true, _) => Alias(Cast(actual, expected.dataType), expected.name)()
          case (false, true) => Alias(actual, expected.name)()
          case (_, _) => actual
        }
    }

    if (newChildOutput == child.output) {
      putInto.copy(table = relation)
    } else {
      putInto.copy(table = relation, child = Project(newChildOutput, child))
    }
  }
}

private[sql] case object PrePutCheck extends (LogicalPlan => Unit) {

  def apply(plan: LogicalPlan): Unit = {
    plan.foreach {
      case PutIntoTable(LogicalRelation(t: RowPutRelation, _, _), query) =>
        // Get all input data source relations of the query.
        val srcRelations = query.collect {
          case LogicalRelation(src: BaseRelation, _, _) => src
        }
        if (srcRelations.contains(t)) {
          throw Utils.analysisException(
            "Cannot put into table that is also being read from.")
        } else {
          // OK
        }

      case PutIntoTable(table, query) =>
        throw Utils.analysisException(s"$table does not allow puts.")

      case _ => // OK
    }
  }
}
