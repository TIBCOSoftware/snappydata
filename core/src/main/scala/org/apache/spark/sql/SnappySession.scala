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
  package org.apache.spark.sql

import java.sql.SQLException
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.runtime.{universe => u}
import scala.util.control.NonFatal

import com.gemstone.gemfire.cache.EntryExistsException
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor.Profile
import com.gemstone.gemfire.distributed.internal.ProfileListener
import com.gemstone.gemfire.internal.cache.PartitionedRegion
import com.gemstone.gemfire.internal.shared.{ClientResolverUtils, FinalizeHolder, FinalizeObject}
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.common.util.concurrent.UncheckedExecutionException
import com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds
import io.snappydata.{Constant, SnappyTableStatsProviderService}

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.backwardcomp.ExecutedCommand
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, UnresolvedRelation}
import org.apache.spark.sql.catalyst.encoders.{RowEncoder, _}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, AttributeReference, Descending, Exists, ExprId, Expression, GenericRow, ListQuery, LiteralValue, ParamLiteral, PredicateSubquery, ScalarSubquery, SortDirection}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Union}
import org.apache.spark.sql.catalyst.{DefinedByConstructorParams, InternalRow, TableIdentifier}
import org.apache.spark.sql.collection.{Utils, WrappedInternalRow}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.CollectAggregateExec
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation
import org.apache.spark.sql.execution.columnar.{ExternalStoreUtils, InMemoryTableScanExec}
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.hive.{ConnectorCatalog, QualifiedTableName, SnappyStoreHiveCatalog}
import org.apache.spark.sql.internal.{PreprocessTableInsertOrPut, SnappySessionState, SnappySharedState}
import org.apache.spark.sql.row.GemFireXDDialect
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.{CodeGeneration, StoreUtils}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{Logging, ShuffleDependency, SparkContext}


class SnappySession(@transient private val sc: SparkContext,
    @transient private val existingSharedState: Option[SnappySharedState])
    extends SparkSession(sc) {

  self =>

  def this(sc: SparkContext) {
    this(sc, None)
  }


  // initialize GemFireXDDialect so that it gets registered

  GemFireXDDialect.init()

  /* ----------------------- *
   |  Session-related state  |
   * ----------------------- */

  private[spark] val id = SnappySession.newId()

  new FinalizeSession(this)

  /**
   * State shared across sessions, including the [[SparkContext]], cached data, listener,
   * and a catalog that interacts with external systems.
   */
  @transient
  private[spark] lazy override val sharedState: SnappySharedState = {
    existingSharedState.getOrElse(new SnappySharedState(sc, id))
  }

  private[spark] lazy val cacheManager = sharedState.cacheManager

  /**
   * State isolated across sessions, including SQL configurations, temporary tables, registered
   * functions, and everything else that accepts a [[org.apache.spark.sql.internal.SQLConf]].
   */
  @transient
  private[spark] lazy override val sessionState: SnappySessionState = {
    try {
      val clazz = org.apache.spark.util.Utils.classForName(
        "org.apache.spark.sql.internal.SnappyAQPSessionState")
      clazz.getConstructor(classOf[SnappySession]).
          newInstance(self).asInstanceOf[SnappySessionState]
    } catch {
      case NonFatal(_) => new SnappySessionState(this)
    }
  }

  @transient
  lazy val sessionCatalog: SnappyStoreHiveCatalog = {
    SnappyContext.getClusterMode(sc) match {
      case ThinClientConnectorMode(_, _) => sessionState.catalog.asInstanceOf[ConnectorCatalog]
      case _ => sessionState.catalog.asInstanceOf[SnappyStoreHiveCatalog]
    }
  }

  @transient
  private[spark] val snappyContextFunctions = sessionState.contextFunctions

  SnappyContext.initGlobalSnappyContext(sparkContext, this)
  snappyContextFunctions.registerAQPErrorFunctions(this)

  /**
   * A wrapped version of this session in the form of a [[SQLContext]],
   * for backward compatibility.
   */
  @transient
  private[spark] val snappyContext: SnappyContext = new SnappyContext(this)

  /**
   * A wrapped version of this session in the form of a [[SQLContext]],
   * for backward compatibility.
   *
   * @since 2.0.0
   */
  @transient
  override val sqlContext: SnappyContext = snappyContext

  /**
   * Start a new session with isolated SQL configurations, temporary tables, registered
   * functions are isolated, but sharing the underlying [[SparkContext]] and cached data.
   *
   * Note: Other than the [[SparkContext]], all shared state is initialized lazily.
   * This method will force the initialization of the shared state to ensure that parent
   * and child sessions are set up with the same shared state. If the underlying catalog
   * implementation is Hive, this will initialize the metastore, which may take some time.
   *
   * @group basic
   * @since 2.0.0
   */
  override def newSession(): SnappySession = {
    new SnappySession(sparkContext, Some(sharedState))
  }

  override def sql(sqlText: String): CachedDataFrame =
    snappyContextFunctions.sql(SnappySession.getPlan(this, sqlText))

  def sqlUncached(sqlText: String): DataFrame =
    snappyContextFunctions.sql(super.sql(sqlText))

  private[sql] final def prepareSQL(sqlText: String): LogicalPlan = {
    val logical = sessionState.sqlParser.parsePlan(sqlText)
    SparkSession.setActiveSession(this)
    sessionState.analyzer.execute(logical)
  }

  private[sql] final def executeSQL(sqlText: String): DataFrame = {
    try {
      super.sql(sqlText)
    } catch {
      case e: AnalysisException =>
        // in case of connector mode, exception can be thrown if
        // table form is changed (altered) and we have old table
        // object in SnappyStoreHiveCatalog.cachedDataSourceTables
        SnappyContext.getClusterMode(sparkContext) match {
          case ThinClientConnectorMode(_, _) =>
            val plan = sessionState.sqlParser.parsePlan(sqlText)
            var tables: Seq[TableIdentifier] = Seq()
            plan.foreach {
              case UnresolvedRelation(table, _) => tables = tables.+:(table)
              case _ =>
            }
            tables.foreach(sessionCatalog.refreshTable)
            Dataset.ofRows(snappyContext.sparkSession, plan)
          case _ =>
            throw e
        }
    }
  }

  def onlyParseSQL(sqLText: String): LogicalPlan = {
    sessionState.sqlParser.parsePlan(sqLText)
  }

  @transient
  private[sql] val queryHints: mutable.Map[String, String] = mutable.Map.empty

  def getPreviousQueryHints: Map[String, String] = Utils.immutableMap(queryHints)

  private val contextObjects = new mutable.HashMap[Any, Any]

  @transient
  private[sql] var currentKey: SnappySession.CachedKey = _

  /**
   * Get a previously registered context object using [[addContextObject]].
   */
  private[sql] def getContextObject[T](key: Any): Option[T] = {
    contextObjects.get(key).asInstanceOf[Option[T]]
  }

  /**
   * Get a previously registered CodegenSupport context object
   * by [[addContextObject]].
   */
  private[sql] def getContextObject[T](ctx: CodegenContext, objectType: String,
      key: Any): Option[T] = {
    getContextObject[T](ctx -> (objectType -> key))
  }

  /**
   * Register a new context object for this query.
   */
  private[sql] def addContextObject[T](key: Any, value: T): Unit = {
    contextObjects.put(key, value)
  }

  /**
   * Register a new context object for <code>CodegenSupport</code>.
   */
  private[sql] def addContextObject[T](ctx: CodegenContext, objectType: String,
      key: Any, value: T): Unit = {
    addContextObject(ctx -> (objectType -> key), value)
  }

  /**
   * Remove a context object registered using [[addContextObject]].
   */
  private[sql] def removeContextObject(key: Any): Unit = {
    contextObjects.remove(key)
  }

  /**
   * Remove a CodegenSupport context object registered by [[addContextObject]].
   */
  private[sql] def removeContextObject(ctx: CodegenContext, objectType: String,
      key: Any): Unit = {
    removeContextObject(ctx -> (objectType -> key))
  }

  private[sql] def linkPartitionsToBuckets(flag: Boolean): Unit = {
    addContextObject(StoreUtils.PROPERTY_PARTITION_BUCKET_LINKED, flag)
  }

  private[sql] def hasLinkPartitionsToBuckets: Boolean = {
    getContextObject[Boolean](StoreUtils.PROPERTY_PARTITION_BUCKET_LINKED)
        .getOrElse(false)
  }

  private[sql] def addFinallyCode(ctx: CodegenContext, code: String): Int = {
    val depth = getContextObject[Int](ctx, "D", "depth").getOrElse(0) + 1
    addContextObject(ctx, "D", "depth", depth)
    addContextObject(ctx, "F", "finally" -> depth, code)
    depth
  }

  private[sql] def evaluateFinallyCode(ctx: CodegenContext,
      body: String = "", depth: Int = -1): String = {
    // if no depth given then use the most recent one
    val d = if (depth == -1) {
      getContextObject[Int](ctx, "D", "depth").getOrElse(0)
    } else depth
    if (d <= 1) removeContextObject(ctx, "D", "depth")
    else addContextObject(ctx, "D", "depth", d - 1)

    val key = "finally" -> d
    getContextObject[String](ctx, "F", key) match {
      case Some(finallyCode) => removeContextObject(ctx, "F", key)
        if (body.isEmpty) finallyCode
        else {
          s"""
             |try {
             |  $body
             |} finally {
             |   $finallyCode
             |}
          """.stripMargin
        }
      case None => body
    }
  }

  /**
   * Get name of a previously registered class using [[addClass]].
   */
  def getClass(ctx: CodegenContext, baseTypes: Seq[(DataType, Boolean)],
      keyTypes: Seq[(DataType, Boolean)],
      types: Seq[(DataType, Boolean)], multimap: Boolean): Option[(String, String)] = {
    getContextObject[(String, String)](ctx, "C", (baseTypes, keyTypes, types, multimap))
  }
  /**
   * Register code generated for a new class (for <code>CodegenSupport</code>).
   */
  private[sql] def addClass(ctx: CodegenContext,
      baseTypes: Seq[(DataType, Boolean)], keyTypes: Seq[(DataType, Boolean)],
      types: Seq[(DataType, Boolean)], baseClassName: String,
      className: String, multiMap: Boolean): Unit = {
    addContextObject(ctx, "C", (baseTypes, keyTypes, types, multiMap),
      baseClassName -> className)
  }

  /**
   * Register additional [[DictionaryCode]] for a variable in ExprCode.
   */
  private[sql] def addDictionaryCode(ctx: CodegenContext, keyVar: String,
      dictCode: DictionaryCode): Unit =
    addContextObject(ctx, "D", keyVar, dictCode)

  /**
   * Get [[DictionaryCode]] for a previously registered variable in ExprCode
   * using [[addDictionaryCode]].
   */
  def getDictionaryCode(ctx: CodegenContext,
      keyVar: String): Option[DictionaryCode] =
    getContextObject[DictionaryCode](ctx, "D", keyVar)

  /**
   * Register hash variable holding the evaluated hashCode for some variables.
   */
  private[sql] def addHashVar(ctx: CodegenContext, keyVars: Seq[String],
      hashVar: String): Unit = addContextObject(ctx, "H", keyVars, hashVar)

  /**
   * Get hash variable for previously registered variables using [[addHashVar]].
   */
  private[sql] def getHashVar(ctx: CodegenContext,
      keyVars: Seq[String]): Option[String] = getContextObject(ctx, "H", keyVars)

  private[sql] def getAllLiterals(key: SnappySession.CachedKey): Array[LiteralValue] = {
    var allLiterals: Array[LiteralValue] = Array.empty
    if (key != null && key.valid) {
      allLiterals = CachedPlanHelperExec.allLiterals(
        getContextObject[ArrayBuffer[ArrayBuffer[Any]]](
          CachedPlanHelperExec.REFERENCES_KEY).getOrElse(Seq.empty)
      ).filter(!_.collectedForPlanCaching)

      allLiterals.foreach(_.collectedForPlanCaching = true)
    }
    allLiterals
  }

  private[sql] def clearContext(): Unit = synchronized {
    // println(s"clearing context")
    // new Throwable().printStackTrace()
    contextObjects.clear()
  }

  private[sql] def clearQueryData(): Unit = synchronized {
    queryHints.clear()
    clearContext()
  }

  def clearPlanCache(): Unit = synchronized {
    SnappySession.clearSessionCache(id)
  }


  def clear(): Unit = synchronized {
    clearContext()
    clearQueryData()
    clearPlanCache()
    snappyContextFunctions.clear()
  }

  /**
   * :: DeveloperApi ::
   *
   * @todo do we need this anymore? If useful functionality, make this
   *       private to sql package ... SchemaDStream should use the data source
   *       API?
   *       Tagging as developer API, for now
   * @param stream
   * @param aqpTables
   * @param transformer
   * @param v
   * @tparam T
   * @return
   */
  @DeveloperApi
  def saveStream[T](stream: DStream[T],
      aqpTables: Seq[String],
      transformer: Option[(RDD[T]) => RDD[Row]])(implicit v: u.TypeTag[T]) {
    val transform = transformer match {
      case Some(x) => x
      case None => if (!(v.tpe =:= u.typeOf[Row])) {
        // check if the stream type is already a Row
        throw new IllegalStateException(" Transformation to Row type needs to be supplied")
      } else {
        null
      }
    }
    stream.foreachRDD((rdd: RDD[T], time: Time) => {

      val rddRows = if (transform != null) {
        transform(rdd)
      } else {
        rdd.asInstanceOf[RDD[Row]]
      }
      snappyContextFunctions.collectSamples(this, rddRows, aqpTables,
        time.milliseconds)
    })
  }

  /**
   * Append dataframe to cache table in Spark.
   *
   * @param df
   * @param table
   * @param storageLevel default storage level is MEMORY_AND_DISK
   * @return @todo -> return type?
   */
  @DeveloperApi
  def appendToTempTableCache(df: DataFrame, table: String,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK): Unit = {
    val tableIdent = sessionCatalog.newQualifiedTableName(table)
    val plan = sessionCatalog.lookupRelation(tableIdent, None)
    // cache the new DataFrame
    df.persist(storageLevel)
    // trigger an Action to materialize 'cached' batch
    if (df.count() > 0) {
      // create a union of the two plans and store that in catalog
      val union = Union(plan, df.logicalPlan)
      sessionCatalog.unregisterTable(tableIdent)
      sessionCatalog.registerTable(tableIdent, union)
    }
  }

  /**
   * Empties the contents of the table without deleting the catalog entry.
   *
   * @param tableName full table name to be truncated
   * @param ifExists  attempt truncate only if the table exists
   */
  def truncateTable(tableName: String, ifExists: Boolean = false): Unit = {
    truncateTable(sessionCatalog.newQualifiedTableName(tableName), ifExists,
      ignoreIfUnsupported = false)
  }

  /**
   * Empties the contents of the table without deleting the catalog entry.
   *
   * @param tableIdent qualified name of table to be truncated
   * @param ifExists   attempt truncate only if the table exists
   */
  private[sql] def truncateTable(tableIdent: QualifiedTableName,
      ifExists: Boolean, ignoreIfUnsupported: Boolean): Unit = {
    val plan = try {
      sessionCatalog.lookupRelation(tableIdent)
    } catch {
      case tnfe: TableNotFoundException =>
        if (ifExists) return else throw tnfe
    }
    cacheManager.uncacheQuery(Dataset.ofRows(this, plan))
    plan match {
      case LogicalRelation(br, _, _) =>
        br match {
          case d: DestroyRelation => d.truncate()
          case _ => if (!ignoreIfUnsupported) {
            throw new AnalysisException(s"Table $tableIdent cannot be truncated")
          }
        }
      case _ => if (!ignoreIfUnsupported) {
        throw new AnalysisException(s"Table $tableIdent cannot be truncated")
      }
    }
  }

  override def createDataset[T: Encoder](data: RDD[T]): Dataset[T] = {
    val encoder = encoderFor[T]
    val output = normalizeSchema(encoder.schema).toAttributes
    val c = encoder.clsTag.runtimeClass
    val isFlat = !(classOf[Product].isAssignableFrom(c) ||
        classOf[DefinedByConstructorParams].isAssignableFrom(c))
    val plan = new EncoderPlan[T](data, encoder, isFlat, output, self)
    Dataset[T](self, plan)
  }

  /**
   * Creates a [[DataFrame]] from an RDD[Row]. User can specify whether
   * the input rows should be converted to Catalyst rows.
   */
  override private[sql] def createDataFrame(
      rowRDD: RDD[Row],
      schema: StructType,
      needsConversion: Boolean) = {
    // TODO: use MutableProjection when rowRDD is another DataFrame and the applied
    // schema differs from the existing schema on any field data type.
    val catalystRows = if (needsConversion) {
      val encoder = RowEncoder(schema)
      rowRDD.map {
        case r: WrappedInternalRow => r.internalRow
        case r => encoder.toRow(r)
      }
    } else {
      rowRDD.map(r => InternalRow.fromSeq(r.toSeq))
    }
    val logicalPlan = LogicalRDD(schema.toAttributes, catalystRows)(self)
    Dataset.ofRows(self, logicalPlan)
  }


  /**
   * Create a stratified sample table.
   *
   * @todo provide lot more details and examples to explain creating and
   *       using sample tables with time series and otherwise
   * @param tableName       the qualified name of the table
   * @param baseTable       the base table of the sample table, if any
   * @param samplingOptions sampling options like QCS, reservoir size etc.
   * @param allowExisting   When set to true it will ignore if a table with the same
   *                        name is present, else it will throw table exist exception
   */
  def createSampleTable(tableName: String,
      baseTable: Option[String],
      samplingOptions: Map[String, String],
      allowExisting: Boolean): DataFrame = {
    val plan = createTable(sessionCatalog.newQualifiedTableName(tableName),
      SnappyContext.SAMPLE_SOURCE, userSpecifiedSchema = None, schemaDDL = None,
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      addBaseTableOption(baseTable, samplingOptions), isBuiltIn = true)
    Dataset.ofRows(this, plan)
  }

  /**
   * Create a stratified sample table. Java friendly version.
   *
   * @todo provide lot more details and examples to explain creating and
   *       using sample tables with time series and otherwise
   * @param tableName       the qualified name of the table
   * @param baseTable       the base table of the sample table, if any, or null
   * @param samplingOptions sampling options like QCS, reservoir size etc.
   * @param allowExisting   When set to true it will ignore if a table with the same
   *                        name is present, else it will throw table exist exception
   */
  def createSampleTable(tableName: String,
      baseTable: String, samplingOptions: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createSampleTable(tableName, Option(baseTable),
      samplingOptions.asScala.toMap, allowExisting)
  }


  /**
   * Create a stratified sample table.
   *
   * @todo provide lot more details and examples to explain creating and
   *       using sample tables with time series and otherwise
   * @param tableName       the qualified name of the table
   * @param baseTable       the base table of the sample table, if any
   * @param schema          schema of the table
   * @param samplingOptions sampling options like QCS, reservoir size etc.
   * @param allowExisting   When set to true it will ignore if a table with the same
   *                        name is present, else it will throw table exist exception
   */
  def createSampleTable(tableName: String,
      baseTable: Option[String],
      schema: StructType,
      samplingOptions: Map[String, String],
      allowExisting: Boolean = false): DataFrame = {
    val plan = createTable(sessionCatalog.newQualifiedTableName(tableName),
      SnappyContext.SAMPLE_SOURCE, Some(normalizeSchema(schema)), schemaDDL = None,
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      addBaseTableOption(baseTable, samplingOptions), isBuiltIn = true)
    Dataset.ofRows(this, plan)
  }

  /**
   * Create a stratified sample table. Java friendly version.
   *
   * @todo provide lot more details and examples to explain creating and
   *       using sample tables with time series and otherwise
   * @param tableName       the qualified name of the table
   * @param baseTable       the base table of the sample table, if any, or null
   * @param schema          schema of the table
   * @param samplingOptions sampling options like QCS, reservoir size etc.
   * @param allowExisting   When set to true it will ignore if a table with the same
   *                        name is present, else it will throw table exist exception
   */
  def createSampleTable(tableName: String,
      baseTable: String, schema: StructType,
      samplingOptions: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createSampleTable(tableName, Option(baseTable), schema,
      samplingOptions.asScala.toMap, allowExisting)
  }


  /**
   * Create approximate structure to query top-K with time series support.
   *
   * @todo provide lot more details and examples to explain creating and
   *       using TopK with time series
   * @param topKName      the qualified name of the top-K structure
   * @param baseTable     the base table of the top-K structure, if any
   * @param keyColumnName
   * @param inputDataSchema
   * @param topkOptions
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   */
  def createApproxTSTopK(topKName: String, baseTable: Option[String],
      keyColumnName: String, inputDataSchema: StructType,
      topkOptions: Map[String, String],
      allowExisting: Boolean = false): DataFrame = {
    val plan = createTable(sessionCatalog.newQualifiedTableName(topKName),
      SnappyContext.TOPK_SOURCE, Some(normalizeSchema(inputDataSchema)), schemaDDL = None,
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      addBaseTableOption(baseTable, topkOptions) + ("key" -> keyColumnName),
      isBuiltIn = true)
    Dataset.ofRows(this, plan)
  }

  /**
   * Create approximate structure to query top-K with time series support.
   * Java friendly api.
   *
   * @todo provide lot more details and examples to explain creating and
   *       using TopK with time series
   * @param topKName      the qualified name of the top-K structure
   * @param baseTable     the base table of the top-K structure, if any, or null
   * @param keyColumnName
   * @param inputDataSchema
   * @param topkOptions
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   */
  def createApproxTSTopK(topKName: String, baseTable: String,
      keyColumnName: String, inputDataSchema: StructType,
      topkOptions: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createApproxTSTopK(topKName, Option(baseTable), keyColumnName,
      inputDataSchema, topkOptions.asScala.toMap, allowExisting)
  }

  /**
   * Create approximate structure to query top-K with time series support.
   *
   * @todo provide lot more details and examples to explain creating and
   *       using TopK with time series
   * @param topKName      the qualified name of the top-K structure
   * @param baseTable     the base table of the top-K structure, if any
   * @param keyColumnName
   * @param topkOptions
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   */
  def createApproxTSTopK(topKName: String, baseTable: Option[String],
      keyColumnName: String, topkOptions: Map[String, String],
      allowExisting: Boolean): DataFrame = {
    val plan = createTable(sessionCatalog.newQualifiedTableName(topKName),
      SnappyContext.TOPK_SOURCE, userSpecifiedSchema = None, schemaDDL = None,
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      addBaseTableOption(baseTable, topkOptions) + ("key" -> keyColumnName),
      isBuiltIn = true)
    Dataset.ofRows(this, plan)
  }

  /**
   * Create approximate structure to query top-K with time series support. Java
   * friendly api.
   *
   * @todo provide lot more details and examples to explain creating and
   *       using TopK with time series
   * @param topKName      the qualified name of the top-K structure
   * @param baseTable     the base table of the top-K structure, if any, or null
   * @param keyColumnName
   * @param topkOptions
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   */
  def createApproxTSTopK(topKName: String, baseTable: String,
      keyColumnName: String, topkOptions: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createApproxTSTopK(topKName, Option(baseTable), keyColumnName,
      topkOptions.asScala.toMap, allowExisting)
  }

  /**
   * Creates a SnappyData managed table. Any relation providers
   * (e.g. row, column etc) supported by SnappyData can be created here.
   *
   * {{{
   *
   * val airlineDF = snappyContext.createTable(stagingAirline,
   *   "column", Map("buckets" -> "29"))
   *
   * }}}
   *
   * <p>
   * For other external relation providers, use createExternalTable.
   * <p>
   *
   * @param tableName     Name of the table
   * @param provider      Provider name such as 'COLUMN', 'ROW', 'JDBC' etc.
   * @param options       Properties for table creation
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   * @return DataFrame for the table
   */
  def createTable(
      tableName: String,
      provider: String,
      options: Map[String, String],
      allowExisting: Boolean): DataFrame = {
    val plan = createTable(sessionCatalog.newQualifiedTableName(tableName),
      provider, userSpecifiedSchema = None, schemaDDL = None,
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      options, isBuiltIn = true)
    Dataset.ofRows(this, plan)
  }

  /**
   * Creates a SnappyData managed table. Any relation providers
   * (e.g. row, column etc) supported by SnappyData can be created here.
   *
   * {{{
   *
   * val airlineDF = snappyContext.createTable(stagingAirline,
   *   "column", Map("buckets" -> "29"))
   *
   * }}}
   *
   * <p>
   * For other external relation providers, use createExternalTable.
   * <p>
   *
   * @param tableName     Name of the table
   * @param provider      Provider name such as 'COLUMN', 'ROW', 'JDBC' etc.
   * @param options       Properties for table creation
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   * @return DataFrame for the table
   */
  @Experimental
  def createTable(
      tableName: String,
      provider: String,
      options: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createTable(tableName, provider, options.asScala.toMap, allowExisting)
  }

  /**
   * Creates a SnappyData managed table. Any relation providers
   * (e.g. row, column etc) supported by SnappyData can be created here.
   *
   * {{{
   *
   * case class Data(col1: Int, col2: Int, col3: Int)
   * val props = Map.empty[String, String]
   * val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
   * val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
   * val dataDF = snc.createDataFrame(rdd)
   * snappyContext.createTable(tableName, "column", dataDF.schema, props)
   *
   * }}}
   *
   * <p>
   * For other external relation providers, use createExternalTable.
   * <p>
   *
   * @param tableName     Name of the table
   * @param provider      Provider name such as 'COLUMN', 'ROW', 'JDBC' etc.
   * @param schema        Table schema
   * @param options       Properties for table creation. See options list for different tables.
   *                      https://github
   *                      .com/SnappyDataInc/snappydata/blob/master/docs/rowAndColumnTables.md
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   * @return DataFrame for the table
   */
  def createTable(
      tableName: String,
      provider: String,
      schema: StructType,
      options: Map[String, String],
      allowExisting: Boolean = false): DataFrame = {
    val plan = createTable(sessionCatalog.newQualifiedTableName(tableName),
      provider, Some(normalizeSchema(schema)), schemaDDL = None,
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      options, isBuiltIn = true)
    Dataset.ofRows(this, plan)
  }

  /**
   * Creates a SnappyData managed table. Any relation providers
   * (e.g. row, column etc) supported by SnappyData can be created here.
   *
   * {{{
   *
   *    case class Data(col1: Int, col2: Int, col3: Int)
   *    val props = Map.empty[String, String]
   *    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
   *    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
   *    val dataDF = snc.createDataFrame(rdd)
   *    snappyContext.createTable(tableName, "column", dataDF.schema, props)
   *
   * }}}
   *
   * <p>
   * For other external relation providers, use createExternalTable.
   * <p>
   *
   * @param tableName     Name of the table
   * @param provider      Provider name such as 'COLUMN', 'ROW', 'JDBC' etc.
   * @param schema        Table schema
   * @param options       Properties for table creation. See options list for different tables.
   *                      https://github
   *                      .com/SnappyDataInc/snappydata/blob/master/docs/rowAndColumnTables.md
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   * @return DataFrame for the table
   */
  @Experimental
  def createTable(
      tableName: String,
      provider: String,
      schema: StructType,
      options: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createTable(tableName, provider, schema, options.asScala.toMap, allowExisting)
  }

  /**
   * Creates a SnappyData managed JDBC table which takes a free format ddl
   * string. The ddl string should adhere to syntax of underlying JDBC store.
   * SnappyData ships with inbuilt JDBC store, which can be accessed by
   * Row format data store. The option parameter can take connection details.
   *
   * {{{
   *    val props = Map(
   *      "url" -> s"jdbc:derby:$path",
   *      "driver" -> "org.apache.derby.jdbc.EmbeddedDriver",
   *      "poolImpl" -> "tomcat",
   *      "user" -> "app",
   *      "password" -> "app"
   *    )
   *
   * val schemaDDL = "(OrderId INT NOT NULL PRIMARY KEY,ItemId INT, ITEMREF INT)"
   * snappyContext.createTable("jdbcTable", "jdbc", schemaDDL, props)
   *
   * }}}
   *
   * Any DataFrame of the same schema can be inserted into the JDBC table using
   * DataFrameWriter API.
   *
   * e.g.
   *
   * {{{
   *
   * case class Data(col1: Int, col2: Int, col3: Int)
   *
   * val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
   * val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
   * val dataDF = snc.createDataFrame(rdd)
   * dataDF.write.insertInto("jdbcTable")
   *
   * }}}
   *
   * @param tableName     Name of the table
   * @param provider      Provider name 'ROW' or 'JDBC'.
   * @param schemaDDL     Table schema as a string interpreted by provider
   * @param options       Properties for table creation. See options list for different tables.
   *                      https://github
   *                      .com/SnappyDataInc/snappydata/blob/master/docs/rowAndColumnTables.md
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   * @return DataFrame for the table
   */
  def createTable(
      tableName: String,
      provider: String,
      schemaDDL: String,
      options: Map[String, String],
      allowExisting: Boolean): DataFrame = {
    var schemaStr = schemaDDL.trim
    if (schemaStr.charAt(0) != '(') {
      schemaStr = "(" + schemaStr + ")"
    }
    val plan = createTable(sessionCatalog.newQualifiedTableName(tableName),
      provider, userSpecifiedSchema = None, Some(schemaStr),
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      options, isBuiltIn = true)
    Dataset.ofRows(this, plan)
  }

  /**
   * Creates a SnappyData managed JDBC table which takes a free format ddl
   * string. The ddl string should adhere to syntax of underlying JDBC store.
   * SnappyData ships with inbuilt JDBC store, which can be accessed by
   * Row format data store. The option parameter can take connection details.
   *
   * {{{
   *    val props = Map(
   *      "url" -> s"jdbc:derby:$path",
   *      "driver" -> "org.apache.derby.jdbc.EmbeddedDriver",
   *      "poolImpl" -> "tomcat",
   *      "user" -> "app",
   *      "password" -> "app"
   *    )
   *
   * val schemaDDL = "(OrderId INT NOT NULL PRIMARY KEY,ItemId INT, ITEMREF INT)"
   * snappyContext.createTable("jdbcTable", "jdbc", schemaDDL, props)
   *
   * }}}
   *
   * Any DataFrame of the same schema can be inserted into the JDBC table using
   * DataFrameWriter API.
   *
   * e.g.
   *
   * {{{
   *
   * case class Data(col1: Int, col2: Int, col3: Int)
   *
   * val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
   * val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
   * val dataDF = snc.createDataFrame(rdd)
   * dataDF.write.insertInto("jdbcTable")
   *
   * }}}
   *
   * @param tableName     Name of the table
   * @param provider      Provider name 'ROW' or 'JDBC'.
   * @param schemaDDL     Table schema as a string interpreted by provider
   * @param options       Properties for table creation. See options list for different tables.
   *                      https://github
   *                      .com/SnappyDataInc/snappydata/blob/master/docs/rowAndColumnTables.md
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   * @return DataFrame for the table
   */
  @Experimental
  def createTable(
      tableName: String,
      provider: String,
      schemaDDL: String,
      options: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createTable(tableName, provider, schemaDDL, options.asScala.toMap,
      allowExisting)
  }

  private[sql] def normalizeSchema(schema: StructType): StructType =
    sessionCatalog.normalizeSchema(schema)

  /**
   * Create a table with given options.
   */
  private[sql] def createTable(
      tableIdent: QualifiedTableName,
      provider: String,
      userSpecifiedSchema: Option[StructType],
      schemaDDL: Option[String],
      mode: SaveMode,
      options: Map[String, String],
      isBuiltIn: Boolean): LogicalPlan = {

    SnappyContext.getClusterMode(sc) match {
      case ThinClientConnectorMode(_, _) =>
        return sessionCatalog.asInstanceOf[ConnectorCatalog].connectorHelper.createTable(tableIdent,
          provider, userSpecifiedSchema, schemaDDL, mode, options, isBuiltIn)
      case _ =>
    }

    if (sessionCatalog.tableExists(tableIdent)) {
      mode match {
        case SaveMode.ErrorIfExists =>
          throw new AnalysisException(
            s"createTable: Table $tableIdent already exists.")
        case _ =>
          return sessionCatalog.lookupRelation(tableIdent, None)
      }
    }

    // add tableName in properties if not already present
    val dbtableProp = JdbcExtendedUtils.DBTABLE_PROPERTY
    val params = if (options.keysIterator.exists(_.equalsIgnoreCase(
      dbtableProp))) {
      options
    }
    else {
      options + (dbtableProp -> tableIdent.toString)
    }

    val source = if (isBuiltIn) SnappyContext.getProvider(provider,
      onlyBuiltIn = true) else provider

    val relation = schemaDDL match {
      case Some(cols) => JdbcExtendedUtils.externalResolvedDataSource(self,
        cols, source, mode, params)

      case None =>
        // add allowExisting in properties used by some implementations
        val r = DataSource(
          self,
          userSpecifiedSchema = userSpecifiedSchema,
          className = source,
          options = params + (JdbcExtendedUtils.ALLOW_EXISTING_PROPERTY ->
              (mode != SaveMode.ErrorIfExists).toString)).resolveRelation(true)
        r
    }

    val plan = LogicalRelation(relation, metastoreTableIdentifier = Some(tableIdent))
    if (!SnappyContext.internalTableSources.exists(_.equals(source))) {
      sessionCatalog.registerDataSourceTable(tableIdent, userSpecifiedSchema,
        Array.empty[String], source, params, relation)
    }
    snappyContextFunctions.postRelationCreation(relation, this)
    plan
  }

  private[sql] def createTable(
      tableIdent: QualifiedTableName,
      provider: String,
      userSpecifiedSchema: Option[StructType],
      schemaDDL: Option[String],
      partitionColumns: Array[String],
      mode: SaveMode,
      options: Map[String, String],
      query: LogicalPlan,
      isBuiltIn: Boolean): LogicalPlan = {

    if (sessionCatalog.tableExists(tableIdent)) {
      mode match {
        case SaveMode.ErrorIfExists =>
          throw new AnalysisException(s"Table $tableIdent already exists. " +
              "If using SQL CREATE TABLE, you need to use the " +
              s"APPEND or OVERWRITE mode, or drop $tableIdent first.")
        case SaveMode.Ignore =>
          return sessionCatalog.lookupRelation(tableIdent, None)
        case _ =>
      }
    }

    val clusterMode = SnappyContext.getClusterMode(sc)
    val plan = clusterMode match {
      // for smart connector mode create the table here and allow
      // further processing to load the data
      case ThinClientConnectorMode(_, _) =>
        val userSchema = userSpecifiedSchema.getOrElse(
          normalizeSchema(Dataset.ofRows(sqlContext.sparkSession, query).schema))
        sessionCatalog.asInstanceOf[ConnectorCatalog].connectorHelper.createTable(tableIdent,
          provider, Option(userSchema), schemaDDL, mode, options, isBuiltIn)
        createTableAsSelect(tableIdent, provider, Option(userSchema), schemaDDL,
          partitionColumns, SaveMode.Append, options, query, isBuiltIn)
      case _ =>
        createTableAsSelect(tableIdent, provider, userSpecifiedSchema, schemaDDL,
          partitionColumns, mode, options, query, isBuiltIn)
    }

    plan
  }

  /**
   * Create an external table with given options.
   */
  private[sql] def createTableAsSelect(
      tableIdent: QualifiedTableName,
      provider: String,
      userSpecifiedSchema: Option[StructType],
      schemaDDL: Option[String],
      partitionColumns: Array[String],
      mode: SaveMode,
      options: Map[String, String],
      query: LogicalPlan,
      isBuiltIn: Boolean): LogicalPlan = {

    // add tableName in properties if not already present
    // add allowExisting in properties used by some implementations
    val dbtableProp = JdbcExtendedUtils.DBTABLE_PROPERTY
    val params = if (options.keysIterator.exists(_.equalsIgnoreCase(
      dbtableProp))) {
      options
    }
    else {
      options + (dbtableProp -> tableIdent.toString)
    } + (JdbcExtendedUtils.ALLOW_EXISTING_PROPERTY ->
        (mode != SaveMode.ErrorIfExists).toString)

    val source = if (isBuiltIn) SnappyContext.getProvider(provider,
      onlyBuiltIn = true)
    else provider
    val overwrite = mode == SaveMode.Overwrite

    val insertRelation = if (sessionCatalog.tableExists(tableIdent)) {
      mode match {
        case SaveMode.ErrorIfExists =>
          throw new AnalysisException(s"Table $tableIdent already exists. " +
              "If using SQL CREATE TABLE, you need to use the " +
              s"APPEND or OVERWRITE mode, or drop $tableIdent first.")
        case SaveMode.Ignore =>
          return sessionCatalog.lookupRelation(tableIdent, None)
        case _ =>
          // Check if the specified data source match the data source
          // of the existing table.
          val plan = new PreprocessTableInsertOrPut(sessionState.conf).apply(
            sessionState.catalog.lookupRelation(tableIdent))
          EliminateSubqueryAliases(plan) match {
            case LogicalRelation(ir: InsertableRelation, _, _) => Some(ir)
            case o => throw new AnalysisException(
              s"Saving data in ${o.toString} is not supported.")
          }
      }
    } else None

    val relation = schemaDDL match {
      case Some(cols) => JdbcExtendedUtils.externalResolvedDataSource(self,
        cols, source, mode, params, Some(query))

      case None =>
        val data = Dataset.ofRows(this, query)
        val df = userSpecifiedSchema match {
          // If we are inserting into an existing table, just use the existing schema.
          case Some(s) =>
            if (s.size != data.schema.size) {
              throw new AnalysisException(s"The column number " +
                  s"of the specified schema[$s] "
                  + s"doesn't match the data schema[${data.schema}]'s")
            }
            s.zip(data.schema).
                find(x => x._1.dataType != x._2.dataType) match {
              case Some(_) => throw new AnalysisException(s"The column types " +
                  s"of the specified schema[$s] " +
                  s"doesn't match the data schema[${data.schema}]'s")
              case None => // do nothing
            }
            data.toDF(s.fieldNames: _*)
          case None => data
        }

        insertRelation match {
          case Some(ir) =>
            var success = false
            try {
              ir.insert(data, overwrite)
              success = true
              ir
            } finally {
              if (!success) ir match {
                case dr: DestroyRelation =>
                  if (!dr.tableExists) dr.destroy(ifExists = false)
                case _ =>
              }
            }
          case None =>
            DataSource(self,
              className = source,
              userSpecifiedSchema = userSpecifiedSchema,
              partitionColumns = partitionColumns,
              options = params).write(mode, df)
        }
    }

    // need to register if not existing in catalog
    if (insertRelation.isEmpty || overwrite) {
      if (!SnappyContext.internalTableSources.exists(_.equals(source))) {
        sessionCatalog.registerDataSourceTable(tableIdent, userSpecifiedSchema,
          partitionColumns, source, params, relation)
      }
      snappyContextFunctions.postRelationCreation(relation, this)
    }
    LogicalRelation(relation, metastoreTableIdentifier = Some(tableIdent))

  }

  private[sql] def addBaseTableOption(baseTable: Option[_],
      options: Map[String, String]): Map[String, String] = baseTable match {
    // TODO: SW: proper schema handling here and everywhere else in our query
    // processing rules as well as of Catalyst
    case Some(t: TableIdentifier) => options + (JdbcExtendedUtils
        .BASETABLE_PROPERTY -> sessionCatalog.formatTableName(t.table))
    case Some(s: String) => options + (JdbcExtendedUtils
        .BASETABLE_PROPERTY -> sessionCatalog.formatTableName(s).toString)
    case _ => options
  }

  /**
   * Drop a SnappyData table created by a call to SnappyContext.createTable,
   * createExternalTable or registerTempTable.
   *
   * @param tableName table to be dropped
   * @param ifExists  attempt drop only if the table exists
   */
  def dropTable(tableName: String, ifExists: Boolean = false): Unit =
    dropTable(sessionCatalog.newQualifiedTableName(tableName), ifExists)

  /**
   * Drop a SnappyData table created by a call to SnappyContext.createTable,
   * createExternalTable or registerTempTable.
   *
   * @param tableIdent table to be dropped
   * @param ifExists   attempt drop only if the table exists
   */
  private[sql] def dropTable(tableIdent: QualifiedTableName,
      ifExists: Boolean): Unit = {

    SnappyContext.getClusterMode(sc) match {
      case ThinClientConnectorMode(_, _) =>
        val isTempTable = sessionCatalog.isTemporaryTable(tableIdent)
        if (!isTempTable) {
          sessionCatalog.asInstanceOf[ConnectorCatalog].connectorHelper
              .dropTable(tableIdent, ifExists)
          return
        }
      case _ =>
    }

    val plan = try {
      sessionCatalog.lookupRelation(tableIdent)
    } catch {
      case tnfe: TableNotFoundException =>
        if (ifExists) return else throw tnfe
    }
    // additional cleanup for external and temp tables, if required
    plan match {
      case LogicalRelation(br, _, _) =>
        br match {
          case p: ParentRelation =>
            // fail if any existing dependents
            val dependents = p.getDependents(sessionCatalog)
            if (dependents.nonEmpty) {
              throw new AnalysisException(s"Object $tableIdent cannot be " +
                  "dropped because of dependent objects: " +
                  s"${dependents.mkString(",")}")
            }
          case _ => // ignore
        }
        val isTempTable = sessionCatalog.isTemporaryTable(tableIdent)
        cacheManager.uncacheQuery(Dataset.ofRows(this, plan))
        if (isTempTable) {
          // This is due to temp table
          // can be made from a backing relation like Parquet or Hadoop
          sessionCatalog.unregisterTable(tableIdent)
        }
        br match {
          case d: DestroyRelation => d.destroy(ifExists)
            sessionCatalog.unregisterDataSourceTable(tableIdent, Some(br))
          case _ => if (!isTempTable) {
            sessionCatalog.unregisterDataSourceTable(tableIdent, Some(br))
          }
        }
      case _ => // This is a temp table with no relation as source
        cacheManager.uncacheQuery(Dataset.ofRows(this, plan))
        sessionCatalog.unregisterTable(tableIdent)
    }
  }

  /**
   * Set current database/schema.
   *
   * @param schemaName schema name which goes in the catalog
   */
  def setSchema(schemaName: String): Unit = {
    sessionCatalog.setSchema(schemaName)
  }

  /**
   * Create an index on a table.
   *
   * @param indexName    Index name which goes in the catalog
   * @param baseTable    Fully qualified name of table on which the index is created.
   * @param indexColumns Columns on which the index has to be created along with the
   *                     sorting direction.The direction of index will be ascending
   *                     if value is true and descending when value is false.
   *                     Direction can be specified as null
   * @param options      Options for indexes. For e.g.
   *                     column table index - ("COLOCATE_WITH"->"CUSTOMER").
   *                     row table index - ("INDEX_TYPE"->"GLOBAL HASH") or ("INDEX_TYPE"->"UNIQUE")
   */
  def createIndex(indexName: String,
      baseTable: String,
      indexColumns: java.util.Map[String, java.lang.Boolean],
      options: java.util.Map[String, String]): Unit = {


    val indexCol = indexColumns.asScala.mapValues {
      case null => None
      case java.lang.Boolean.TRUE => Some(Ascending)
      case java.lang.Boolean.FALSE => Some(Descending)
    }

    createIndex(indexName, baseTable, indexCol.toMap, options.asScala.toMap)
  }

  /**
   * Create an index on a table.
   *
   * @param indexName    Index name which goes in the catalog
   * @param baseTable    Fully qualified name of table on which the index is created.
   * @param indexColumns Columns on which the index has to be created with the
   *                     direction of sorting. Direction can be specified as None.
   * @param options      Options for indexes. For e.g.
   *                     column table index - ("COLOCATE_WITH"->"CUSTOMER").
   *                     row table index - ("INDEX_TYPE"->"GLOBAL HASH") or ("INDEX_TYPE"->"UNIQUE")
   */
  def createIndex(indexName: String,
      baseTable: String,
      indexColumns: Map[String, Option[SortDirection]],
      options: Map[String, String]): Unit = {

    val tableIdent = sessionCatalog.newQualifiedTableName(baseTable)
    val indexIdent = sessionCatalog.newQualifiedTableName(indexName)
    createIndex(indexIdent, tableIdent, indexColumns, options)
  }

  /**
   * Create an index on a table.
   */
  private[sql] def createIndex(indexIdent: QualifiedTableName,
      tableIdent: QualifiedTableName,
      indexColumns: Map[String, Option[SortDirection]],
      options: Map[String, String]): Unit = {

    SnappyContext.getClusterMode(sc) match {
      case ThinClientConnectorMode(_, _) =>
        sessionCatalog.asInstanceOf[ConnectorCatalog].connectorHelper.
            createIndex(indexIdent, tableIdent, indexColumns, options)
        return
      case _ =>
    }

    if (indexIdent.database != tableIdent.database) {
      throw new AnalysisException(
        s"Index and table have different databases " +
            s"specified ${indexIdent.database} and ${tableIdent.database}")
    }
    if (!sessionCatalog.tableExists(tableIdent)) {
      throw new AnalysisException(
        s"Could not find $tableIdent in catalog")
    }
    sessionCatalog.lookupRelation(tableIdent) match {
      case LogicalRelation(ir: IndexableRelation, _, _) =>
        ir.createIndex(indexIdent,
          tableIdent,
          indexColumns,
          options)

      case _ => throw new AnalysisException(
        s"$tableIdent is not an indexable table")
    }
    SnappySession.clearAllCache()
  }

  private[sql] def getIndexTable(
      indexIdent: QualifiedTableName): QualifiedTableName = {
    // TODO: SW: proper schema handling required here
    new QualifiedTableName(Constant.SHADOW_SCHEMA_NAME, indexIdent.table)
  }

  private def constructDropSQL(indexName: String,
      ifExists: Boolean): String = {

    val ifExistsClause = if (ifExists) "IF EXISTS" else ""

    s"DROP INDEX $ifExistsClause $indexName"
  }

  /**
   * Drops an index on a table
   *
   * @param indexName Index name which goes in catalog
   * @param ifExists  Drop if exists, else exit gracefully
   */
  def dropIndex(indexName: String, ifExists: Boolean): Unit = {
    dropIndex(sessionCatalog.newQualifiedTableName(indexName), ifExists)
  }

  /**
   * Drops an index on a table
   */
  def dropIndex(indexName: QualifiedTableName, ifExists: Boolean): Unit = {

    SnappyContext.getClusterMode(sc) match {
      case ThinClientConnectorMode(_, _) =>
        sessionCatalog.asInstanceOf[ConnectorCatalog].connectorHelper
            .dropIndex(indexName, ifExists)
        return
      case _ =>
    }

    val indexIdent = getIndexTable(indexName)

    // Since the index does not exist in catalog, it may be a row table index.
    if (!sessionCatalog.tableExists(indexIdent)) {
      dropRowStoreIndex(indexName.toString(), ifExists)
    } else {
      sessionCatalog.lookupRelation(indexIdent) match {
        case LogicalRelation(dr: DependentRelation, _, _) =>
          // Remove the index from the bse table props
          val baseTableIdent = sessionCatalog.newQualifiedTableName(dr.baseTable.get)
          sessionCatalog.lookupRelation(baseTableIdent) match {
            case LogicalRelation(cr: ColumnFormatRelation, _, _) =>
              cr.removeDependent(dr, sessionCatalog)
              cr.dropIndex(indexIdent, baseTableIdent, ifExists)
          }

        case _ => if (!ifExists) throw new AnalysisException(
          s"No index found for $indexName")
      }
    }
    SnappySession.clearAllCache()
  }

  private def dropRowStoreIndex(indexName: String, ifExists: Boolean): Unit = {
    val connProperties = ExternalStoreUtils.validateAndGetAllProps(
      Some(this), new mutable.HashMap[String, String])
    val conn = JdbcUtils.createConnectionFactory(connProperties.url,
      connProperties.connProps)()
    try {
      val sql = constructDropSQL(indexName, ifExists)
      JdbcExtendedUtils.executeUpdate(sql, conn)
    } catch {
      case sqle: SQLException =>
        if (sqle.getMessage.contains("No suitable driver found")) {
          throw new AnalysisException(s"${sqle.getMessage}\n" +
              "Ensure that the 'driver' option is set appropriately and " +
              "the driver jars available (--jars option in spark-submit).")
        } else {
          throw sqle
        }
    } finally {
      conn.commit()
      conn.close()
    }
  }

  /**
   * Insert one or more [[org.apache.spark.sql.Row]] into an existing table
   * A user can insert a DataFrame using foreachPartition...
   * {{{
   *         someDataFrame.foreachPartition (x => snappyContext.insert
   *            ("MyTable", x.toSeq)
   *         )
   * }}}
   * If insert is on a column table then a row insert can trigger an overflow
   * to column store form row buffer. If the overflow fails due to some condition like
   * low memory , then the overflow fails and exception is thrown,
   * but row buffer values are kept as it is. Any user level counter of number of rows inserted
   * might be invalid in such a case.
   * @param tableName table name for the insert operation
   * @param rows      list of rows to be inserted into the table
   * @return number of rows inserted
   */
  @DeveloperApi
  def insert(tableName: String, rows: Row*): Int = {
    sessionCatalog.lookupRelation(sessionCatalog.newQualifiedTableName(tableName)) match {
      case LogicalRelation(r: RowInsertableRelation, _, _) => r.insert(rows)
      case _ => throw new AnalysisException(
        s"$tableName is not a row insertable table")
    }
  }

  /**
   * Insert one or more [[org.apache.spark.sql.Row]] into an existing table
   * A user can insert a DataFrame using foreachPartition...
   * {{{
   *         someDataFrame.foreachPartition (x => snappyContext.insert
   *            ("MyTable", x.toSeq)
   *         )
   * }}}
   *
   * @param tableName table name for the insert operation
   * @param rows      list of rows to be inserted into the table
   * @return number of rows successfully put
   * @return number of rows inserted
   */
  @Experimental
  def insert(tableName: String, rows: java.util.ArrayList[java.util.ArrayList[_]]): Int = {
    val convertedRowSeq: Seq[Row] = rows.asScala.map(row => convertListToRow(row))
    sessionCatalog.lookupRelation(sessionCatalog.newQualifiedTableName(tableName)) match {
      case LogicalRelation(r: RowInsertableRelation, _, _) => r.insert(convertedRowSeq)
      case _ => throw new AnalysisException(
        s"$tableName is not a row insertable table")
    }
  }

  /**
   * Upsert one or more [[org.apache.spark.sql.Row]] into an existing table
   * upsert a DataFrame using foreachPartition...
   * {{{
   *         someDataFrame.foreachPartition (x => snappyContext.put
   *            ("MyTable", x.toSeq)
   *         )
   * }}}
   *
   * @param tableName table name for the put operation
   * @param rows      list of rows to be put on the table
   * @return number of rows successfully put
   */
  @DeveloperApi
  def put(tableName: String, rows: Row*): Int = {
    sessionCatalog.lookupRelation(sessionCatalog.newQualifiedTableName(tableName)) match {
      case LogicalRelation(r: RowPutRelation, _, _) => r.put(rows)
      case _ => throw new AnalysisException(
        s"$tableName is not a row upsertable table")
    }
  }

  /**
   * Update all rows in table that match passed filter expression
   * {{{
   *   snappyContext.update("jdbcTable", "ITEMREF = 3" , Row(99) , "ITEMREF" )
   * }}}
   *
   * @param tableName        table name which needs to be updated
   * @param filterExpr       SQL WHERE criteria to select rows that will be updated
   * @param newColumnValues  A single Row containing all updated column
   *                         values. They MUST match the updateColumn list
   *                         passed
   * @param updateColumns    List of all column names being updated
   * @return
   */
  @DeveloperApi
  def update(tableName: String, filterExpr: String, newColumnValues: Row,
      updateColumns: String*): Int = {
    sessionCatalog.lookupRelation(sessionCatalog.newQualifiedTableName(tableName)) match {
      case LogicalRelation(u: UpdatableRelation, _, _) =>
        u.update(filterExpr, newColumnValues, updateColumns)
      case _ => throw new AnalysisException(
        s"$tableName is not an updatable table")
    }
  }

  /**
   * Update all rows in table that match passed filter expression
   * {{{
   *   snappyContext.update("jdbcTable", "ITEMREF = 3" , Row(99) , "ITEMREF" )
   * }}}
   *
   * @param tableName       table name which needs to be updated
   * @param filterExpr      SQL WHERE criteria to select rows that will be updated
   * @param newColumnValues A list containing all the updated column
   *                        values. They MUST match the updateColumn list
   *                        passed
   * @param updateColumns   List of all column names being updated
   * @return
   */
  @Experimental
  def update(tableName: String, filterExpr: String, newColumnValues: java.util.ArrayList[_],
      updateColumns: java.util.ArrayList[String]): Int = {
    sessionCatalog.lookupRelation(sessionCatalog.newQualifiedTableName(tableName)) match {
      case LogicalRelation(u: UpdatableRelation, _, _) =>
        u.update(filterExpr, convertListToRow(newColumnValues), updateColumns.asScala)
      case _ => throw new AnalysisException(
        s"$tableName is not an updatable table")
    }
  }

  /**
   * Upsert one or more [[org.apache.spark.sql.Row]] into an existing table
   * upsert a DataFrame using foreachPartition...
   * {{{
   *         someDataFrame.foreachPartition (x => snappyContext.put
   *            ("MyTable", x.toSeq)
   *         )
   * }}}
   *
   * @param tableName table name for the put operation
   * @param rows      list of rows to be put on the table
   * @return number of rows successfully put
   */
  @Experimental
  def put(tableName: String, rows: java.util.ArrayList[java.util.ArrayList[_]]): Int = {
    sessionCatalog.lookupRelation(sessionCatalog.newQualifiedTableName(tableName)) match {
      case LogicalRelation(r: RowPutRelation, _, _) =>
        r.put(rows.asScala.map(row => convertListToRow(row)))
      case _ => throw new AnalysisException(
        s"$tableName is not a row upsertable table")
    }
  }


  /**
   * Delete all rows in table that match passed filter expression
   *
   * @param tableName  table name
   * @param filterExpr SQL WHERE criteria to select rows that will be updated
   * @return number of rows deleted
   */
  @DeveloperApi
  def delete(tableName: String, filterExpr: String): Int = {
    sessionCatalog.lookupRelation(sessionCatalog.newQualifiedTableName(tableName)) match {
      case LogicalRelation(d: DeletableRelation, _, _) => d.delete(filterExpr)
      case _ => throw new AnalysisException(
        s"$tableName is not a deletable table")
    }
  }

  private def convertListToRow(row: java.util.ArrayList[_]): Row = {
    val rowAsArray: Array[Any] = row.asScala.toArray
    new GenericRow(rowAsArray)
  }


  /**
   * Fetch the topK entries in the Approx TopK synopsis for the specified
   * time interval. See _createTopK_ for how to create this data structure
   * and associate this to a base table (i.e. the full data set). The time
   * interval specified here should not be less than the minimum time interval
   * used when creating the TopK synopsis.
   *
   * @todo provide an example and explain the returned DataFrame. Key is the
   *       attribute stored but the value is a struct containing
   *       count_estimate, and lower, upper bounds? How many elements are
   *       returned if K is not specified?
   * @param topKName  - The topK structure that is to be queried.
   * @param startTime start time as string of the format "yyyy-mm-dd hh:mm:ss".
   *                  If passed as null, oldest interval is considered as the start interval.
   * @param endTime   end time as string of the format "yyyy-mm-dd hh:mm:ss".
   *                  If passed as null, newest interval is considered as the last interval.
   * @param k         Optional. Number of elements to be queried.
   *                  This is to be passed only for stream summary
   * @return returns the top K elements with their respective frequencies between two time
   */
  def queryApproxTSTopK(topKName: String,
      startTime: String = null, endTime: String = null,
      k: Int = -1): DataFrame =
    snappyContextFunctions.queryTopK(this, topKName,
      startTime, endTime, k)

  /**
   * @todo why do we need this method? K is optional in the above method
   */
  def queryApproxTSTopK(topKName: String,
      startTime: Long, endTime: Long): DataFrame =
    queryApproxTSTopK(topKName, startTime, endTime, -1)

  def queryApproxTSTopK(topK: String,
      startTime: Long, endTime: Long, k: Int): DataFrame =
    snappyContextFunctions.queryTopK(this, topK, startTime, endTime, k)

  def setPreparedQuery(preparePhase: Boolean, paramSet: Option[ParameterValueSet]): Unit =
    sessionState.setPreparedQuery(preparePhase, paramSet)
}

private class FinalizeSession(session: SnappySession)
    extends FinalizeObject(session, true) {

  private var sessionId = session.id

  override def getHolder: FinalizeHolder = FinalizeObject.getServerHolder

  override protected def doFinalize(): Boolean = {
    if (sessionId != SnappySession.INVALID_ID) {
      SnappySession.clearSessionCache(sessionId)
      sessionId = SnappySession.INVALID_ID
    }
    true
  }

  override protected def clearThis(): Unit = {
    sessionId = SnappySession.INVALID_ID
  }
}

object SnappySession extends Logging {

  private[spark] val INVALID_ID = -1
  private[this] val ID = new AtomicInteger(0)
  private[sql] val ExecutionKey = "EXECUTION"

  private[this] val bucketProfileListener = new ProfileListener {

    override def profileCreated(profile: Profile): Unit = {
      // clear all plans pessimistically for now
      clearAllCache(onlyQueryPlanCache = true)
    }

    override def profileUpdated(profile: Profile): Unit = {}

    override def profileRemoved(profile: Profile, destroyed: Boolean): Unit = {
      // clear all plans pessimistically for now
      clearAllCache(onlyQueryPlanCache = true)
    }
  }

  private def findShuffleDependencies(rdd: RDD[_]): Seq[Int] = {
    rdd.dependencies.flatMap {
      case s: ShuffleDependency[_, _, _] => if (s.rdd ne rdd) {
        s.shuffleId +: findShuffleDependencies(s.rdd)
      } else s.shuffleId :: Nil

      case d => if (d.rdd ne rdd) findShuffleDependencies(d.rdd) else Nil
    }
  }

  private def getAllParamLiterals(queryplan: QueryPlan[_]): Array[ParamLiteral] = {
    val res = new ArrayBuffer[ParamLiteral]()
    queryplan transformAllExpressions {
      case p: ParamLiteral =>
        res += p
        p
    }
    res.toSet[ParamLiteral].toArray.sortBy(_.pos)
  }

  private def evaluatePlan(df: DataFrame,
      session: SnappySession, sqlText: String,
      key: CachedKey = null): (CachedDataFrame, Map[String, String]) = {
    var withFallback: CodegenSparkFallback = null
    val executedPlan = df.queryExecution.executedPlan match {
      case CodegenSparkFallback(WholeStageCodegenExec(CachedPlanHelperExec(plan))) =>
        withFallback = CodegenSparkFallback(plan); plan
      case cg@CodegenSparkFallback(plan) => withFallback = cg; plan
      case WholeStageCodegenExec(CachedPlanHelperExec(plan)) => plan
      case plan => plan
    }

    if (key ne null) {
      val noCaching = session.getContextObject[Boolean](
        CachedPlanHelperExec.NOCACHING_KEY).getOrElse(false)
      if (noCaching) {
        key.invalidatePlan()
      }
      else {
        val params1 = getAllParamLiterals(executedPlan)
        if (!params1.sameElements(key.pls)) {
          key.invalidatePlan()
        }
      }
    }
    // keep the broadcast hash join plans and their references as well
    val allBroadcastPlans = session.getContextObject[mutable.Map[BroadcastHashJoinExec,
        ArrayBuffer[Any]]](CachedPlanHelperExec.BROADCASTS_KEY).getOrElse(
      mutable.Map.empty[BroadcastHashJoinExec, ArrayBuffer[Any]])

    val (cachedRDD, shuffleDeps, rddId, localCollect) = executedPlan match {
      case _: ExecutedCommandExec | _: ExecutedCommand | _: ExecutePlan =>
        throw new EntryExistsException("uncached plan", df) // don't cache
      case plan: CollectAggregateExec =>
        val childRDD = if (withFallback ne null) withFallback.execute(plan.child)
        else plan.childRDD
        (null, findShuffleDependencies(childRDD).toArray, childRDD.id, true)
      case _: LocalTableScanExec =>
        (null, Array.empty[Int], -1, false) // cache plan but no cached RDD
      // case _ if allbroadcastplans.nonEmpty =>
        // (null, Array.empty[Int], -1, false) // cache plan but no cached RDD
      case _ =>
        val rdd = executedPlan match {
          case plan: CollectLimitExec =>
            if (withFallback ne null) withFallback.execute(plan.child)
            else plan.child.execute()
          case _ => df.queryExecution.executedPlan.execute()
        }

        // TODO: Skipping the adding of profile listener in case of
        // ThinClientConnectorMode, confirm with Sumedh whether something
        // more needs to be done
        SnappyContext.getClusterMode(session.sparkContext) match {
          case ThinClientConnectorMode(_, _) =>
          case _ =>
            // add profile listener for all regions that are using cached
            // partitions of their "leader" region
            if (rdd.getNumPartitions > 0) {
              session.sessionState.leaderPartitions.keysIterator.foreach(
                addBucketProfileListener)
            }
        }
        (rdd, findShuffleDependencies(rdd).toArray, rdd.id, false)
    }

    /*
    val allAllBroadcastPlans = session.getContextObject[mutable.Map[BroadcastHashJoinExec,
        ArrayBuffer[Any]]](CachedPlanHelperExec.BROADCASTS_KEY).getOrElse(
      mutable.Map.empty[BroadcastHashJoinExec, ArrayBuffer[Any]])
    */

    // keep references as well
    // filter unvisited literals. If the query is on a view for example the
    // modified tpch query no 15, It even picks those literal which we don't want.
    val allLiterals = session.getAllLiterals(key)

    logDebug(s"qe.executedPlan = ${df.queryExecution.executedPlan}")

    // This part is the defensive coding for all those cases where Tokenization
    // support is not smart enough to deal with cases where the execution plan
    // is modified in such a way that we cannot track those constants which
    // need to be replaced in the subsequent execution
    if (key != null) {
      val nocaching = session.getContextObject[Boolean](
        CachedPlanHelperExec.NOCACHING_KEY).getOrElse(false)
      if (nocaching /* || allallbroadcastplans.nonEmpty */) {
        logDebug(s"Invalidating the key because explicit nocaching")
        key.invalidatePlan()
      }
      else {
        val params1 = getAllParamLiterals(executedPlan)
        if (allLiterals.length != params1.length || !params1.sameElements(key.pls)) {
          logDebug(s"Invalidating the key because nocaching " +
              s"allLiterals.length = ${allLiterals.length}," +
              s" params1.length = ${params1.length} and key.pls = ${key.pls.length}")
          key.invalidatePlan()
        }
        else if (params1.length != 0 ) {
          params1.foreach(p => {
            if (!allLiterals.exists(_.position == p.pos)) {
              logDebug(s"No plan caching for sql ${key.sqlText} as " +
                  s"literals and expected parameters are not having the same positions")
              key.invalidatePlan()
            }
          })
        }
      }
    }

    if (key != null && key.valid) {
      logDebug(s"Plan caching will be used for sql ${key.sqlText}")
    }
    val cdf = new CachedDataFrame(df, sqlText, cachedRDD, shuffleDeps, rddId,
      localCollect, allLiterals, allBroadcastPlans)

    // Now check if optimization plans have been applied such that
    val queryHints = session.synchronized {
      val hints = session.queryHints.toMap
      session.clearQueryData()
      hints
    }
    (cdf, queryHints)
  }

  private[this] val planCache = {
    val loader = new CacheLoader[CachedKey,
        (CachedDataFrame, Map[String, String])] {
      override def load(key: CachedKey): (CachedDataFrame,
          Map[String, String]) = {
        val session = key.session
        session.currentKey = key
        var df: DataFrame = null
        try {
          df = session.executeSQL(key.sqlText)
        } finally {
          session.currentKey = null
        }
        val plan = df.queryExecution.executedPlan
        // if this has in-memory caching then don't cache the first time
        // since plan can change once caching is done (due to size stats)
        if (plan.find(_.isInstanceOf[InMemoryTableScanExec]).isDefined) {
          (null, null)
        } else {
          evaluatePlan(df, session, key.sqlText, key)
        }
      }
    }
    CacheBuilder.newBuilder().maximumSize(300).build(loader)
  }

  def getPlanCache: LoadingCache[CachedKey, (CachedDataFrame,
      Map[String, String])] = planCache

  private[spark] def addBucketProfileListener(pr: PartitionedRegion): Unit = {
    val advisers = pr.getRegionAdvisor.getAllBucketAdvisorsHostedAndProxies
        .values().iterator()
    while (advisers.hasNext) {
      advisers.next().addProfileChangeListener(bucketProfileListener)
    }
  }

  class CachedKey(
      val session: SnappySession, val lp: LogicalPlan,
      val sqlText: String, val hintHashcode: Int, val pls: Array[ParamLiteral]) {

    override def hashCode(): Int = {
      (session, lp, hintHashcode).hashCode()
    }

    override def equals(obj: Any): Boolean = {
      obj match {
        case x: CachedKey =>
          (x.session, x.lp, x.hintHashcode).equals(session, lp, hintHashcode)
        case _ => false
      }
    }

    private[sql] var valid = true
    def invalidatePlan(): Unit = valid = false
  }

  object CachedKey {
    def apply(session: SnappySession, lp: LogicalPlan, sqlText: String, pls:
    ArrayBuffer[ParamLiteral]): CachedKey = {

      var invalidate = false

      def normalizeExprIds: PartialFunction[Expression, Expression] = {
        /*
         Fix for SNAP-1642. Not changing the exprId should have been enough
         to not let it tokenize. But invalidating it explicitly so by chance
         also we do not cache it. Will revisit this soon after 0.9
         */
        case s: ScalarSubquery =>
          invalidate = true
          // s.copy(exprId = ExprId(0))
          s
        case e: Exists =>
          e.copy(exprId = ExprId(0))
        case p: PredicateSubquery =>
          p.copy(exprId = ExprId(0))
        case a: AttributeReference =>
          AttributeReference(a.name, a.dataType, a.nullable)(exprId = ExprId(0))
        case a: Alias =>
          Alias(a.child, a.name)(exprId = ExprId(0))
        case l@ListQuery(query, _) =>
          l.copy(query = query.transformAllExpressions(normalizeExprIds),
            exprId = ExprId(0))
        case ae: AggregateExpression =>
          ae.copy(resultId = ExprId(0))
      }

      def transformExprID: PartialFunction[LogicalPlan, LogicalPlan] = {
        case f@Filter(condition, child) => f.copy(
          condition = condition.transform(normalizeExprIds),
          child = child.transformAllExpressions(normalizeExprIds))
        case q: LogicalPlan => q.transformAllExpressions(normalizeExprIds)
      }

      // normalize lp so that two queries can be determined to be equal
      val tlp = lp.transform(transformExprID)
      val key = new CachedKey(session, tlp,
        sqlText, session.queryHints.hashCode(), pls.sortBy(_.pos).toArray)
      if (invalidate) {
        key.invalidatePlan()
      }
      key
    }
  }

  def getPlan(session: SnappySession, sqlText: String): CachedDataFrame = {
    try {
      val lp = session.onlyParseSQL(sqlText)
      val currentWrappedConstants = session.getContextObject[mutable.ArrayBuffer[ParamLiteral]](
        CachedPlanHelperExec.WRAPPED_CONSTANTS) match {
        case Some(list) => list
        case None => mutable.ArrayBuffer.empty[ParamLiteral]
      }
      val key = CachedKey(session, lp, sqlText, currentWrappedConstants)
      val evaluation = planCache.getUnchecked(key)
      if (!key.valid) {
        logDebug(s"Invalidating cached plan for sql: ${key.sqlText}")
        planCache.invalidate(key)
      }
      var cachedDF = evaluation._1
      var queryHints = evaluation._2

      // if null has been returned, then evaluate
      if (cachedDF eq null) {
        val df = session.executeSQL(sqlText)
        val evaluation = evaluatePlan(df, session, sqlText)
        // default is enable caching
        if (!java.lang.Boolean.getBoolean("DISABLE_PLAN_CACHING")) {
          if (queryHints eq null) {
            // put token to cache from next call
            planCache.put(key, (null, Map.empty))
          } else {
            planCache.put(key, evaluation)
          }
        }
        cachedDF = evaluation._1
        queryHints = evaluation._2
      } else {
        cachedDF.waitForLastShuffleCleanup()
        cachedDF.reset()
      }
      cachedDF.queryString = sqlText
      if (key.valid) {
        // logDebug(s"calling reprepare broadcast with new constants ${currentWrappedConstants}")
        // cachedDF.reprepareBroadcast(lp, currentWrappedConstants)
        logDebug(s"calling replace constants with new constants $currentWrappedConstants" +
            s" in Literal values = ${cachedDF.allLiterals.toSet}")
        CachedPlanHelperExec.replaceConstants(cachedDF.allLiterals, lp, currentWrappedConstants)
      }
      // set the query hints as would be set at the end of un-cached sql()
      session.synchronized {
        session.queryHints.clear()
        session.queryHints ++= queryHints
      }
      cachedDF
    } catch {
      case e: UncheckedExecutionException => e.getCause match {
        case ee: EntryExistsException => new CachedDataFrame(
          ee.getOldValue.asInstanceOf[DataFrame], sqlText, null,
          Array.empty, -1, false)
        case t => throw t
      }
      case ee: EntryExistsException => new CachedDataFrame(
        ee.getOldValue.asInstanceOf[DataFrame], sqlText, null,
        Array.empty, -1, false)
    }
  }

  private def newId(): Int = {
    val id = ID.incrementAndGet()
    if (id != INVALID_ID) id
    else ID.incrementAndGet()
  }

  private[spark] def clearSessionCache(sessionId: Long): Unit = {
    val iter = planCache.asMap().keySet().iterator()
    while (iter.hasNext) {
      val item = iter.next()
      val session = item.asInstanceOf[CachedKey].session
      if (session.id == sessionId) {
        iter.remove()
      }
    }
  }

  def clearAllCache(onlyQueryPlanCache: Boolean = false): Unit = {
    if (!SnappyTableStatsProviderService.suspendCacheInvalidation) {
      planCache.invalidateAll()
      if (!onlyQueryPlanCache) {
        CodeGeneration.clearAllCache()
        val sc = SnappyContext.globalSparkContext
        if (sc ne null) {
          Utils.mapExecutors(sc, (_, _) => {
            CodeGeneration.clearAllCache()
            Iterator.empty
          })
        }
      }
    }
  }

  // TODO: hemant : Its clumsy design to use where SnappySession.Builder
  // is using the SparkSession object's functions. This needs to be revisited
  // once we decide on how our Builder API should be.
  def getOrCreate(sc: SparkContext): SnappySession = {
    // TODO: hemant - Is this needed for Snappy. Snappy should always use newSession.
    // Get the session from current thread's active session.
    var session = SparkSession.getActiveSession.orNull
    if ((session ne null) && !session.sparkContext.isStopped) {
      return session.asInstanceOf[SnappySession]
    }

    // Global synchronization so we will only set the default session once.
    SparkSession.synchronized {
      // If the current thread does not have an active session, get it from the global session.
      session = SparkSession.getDefaultSession.orNull
      if ((session ne null) && !session.sparkContext.isStopped) {
        return session.asInstanceOf[SnappySession]
      }

      session = new SnappySession(sc, None)
      SparkSession.setDefaultSession(session)

      // Register a successfully instantiated context to the singleton.
      // This should be at the end of the class definition so that the
      // singleton is updated only if there is no exception in the
      // construction of the instance.
      sc.addSparkListener(new SparkListener {
        override def onApplicationEnd(
            applicationEnd: SparkListenerApplicationEnd): Unit = {
          SparkSession.setDefaultSession(null)
          SparkSession.sqlListener.set(null)
        }
      })
    }

    session.asInstanceOf[SnappySession]
  }

  // One-to-One Mapping with SparkSQLPrepareImpl.getSQLType
  def getDataType(storeType: Int, precision: Int, scale: Int): DataType = storeType match {
    case StoredFormatIds.SQL_INTEGER_ID => IntegerType
    case StoredFormatIds.SQL_CLOB_ID => StringType
    case StoredFormatIds.SQL_LONGINT_ID => LongType
    case StoredFormatIds.SQL_TIMESTAMP_ID => TimestampType
    case StoredFormatIds.SQL_DATE_ID => DateType
    case StoredFormatIds.SQL_DOUBLE_ID => DoubleType
    case StoredFormatIds.SQL_DECIMAL_ID => DecimalType(precision, scale)
    case StoredFormatIds.SQL_REAL_ID => FloatType
    case StoredFormatIds.SQL_BOOLEAN_ID => BooleanType
    case StoredFormatIds.SQL_SMALLINT_ID => ShortType
    case StoredFormatIds.SQL_TINYINT_ID => ByteType
    case StoredFormatIds.SQL_BLOB_ID => BinaryType
    case _ => StringType
  }
}

private final class Expr(val name: String, val e: Expression) {
  override def equals(o: Any): Boolean = o match {
    case other: Expr => name == other.name && e.semanticEquals(other.e)
    case _ => false
  }

  override def hashCode: Int = ClientResolverUtils.fastHashLong(
    name.hashCode.toLong << 32L | (e.semanticHash() & 0xffffffffL))
}
