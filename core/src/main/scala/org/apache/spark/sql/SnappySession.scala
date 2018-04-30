/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
import java.util.Calendar
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Future
import scala.language.implicitConversions
import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.util.control.NonFatal

import com.gemstone.gemfire.internal.GemFireVersion
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.gemstone.gemfire.internal.shared.{ClientResolverUtils, FinalizeHolder, FinalizeObject}
import com.google.common.cache.{Cache, CacheBuilder}
import com.pivotal.gemfirexd.internal.GemFireXDVersion
import com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet
import com.pivotal.gemfirexd.internal.iapi.{types => stypes}
import com.pivotal.gemfirexd.internal.shared.common.{SharedUtils, StoredFormatIds}
import io.snappydata.collection.ObjectObjectHashMap
import io.snappydata.{Constant, Property, SnappyDataFunctions, SnappyTableStatsProviderService}

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, NoSuchTableException}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, AttributeReference, Descending, Exists, ExprId, Expression, GenericRow, ListQuery, ParamLiteral, PredicateSubquery, ScalarSubquery, SortDirection, TokenLiteral}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Union}
import org.apache.spark.sql.catalyst.{DefinedByConstructorParams, InternalRow, ScalaReflection, TableIdentifier}
import org.apache.spark.sql.collection.{Utils, WrappedInternalRow}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.CollectAggregateExec
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation
import org.apache.spark.sql.execution.columnar.{ExternalStoreUtils, InMemoryTableScanExec}
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}
import org.apache.spark.sql.execution.ui.SparkListenerSQLPlanExecutionStart
import org.apache.spark.sql.hive.{ConnectorCatalog, ExternalTableType, HiveClientUtil, QualifiedTableName, SnappySharedState, SnappyStoreHiveCatalog}
import org.apache.spark.sql.internal.{PreprocessTableInsertOrPut, SnappySessionState}
import org.apache.spark.sql.row.GemFireXDDialect
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.{CodeGeneration, StoreUtils}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{Logging, ShuffleDependency, SparkContext, SparkEnv}


class SnappySession(_sc: SparkContext) extends SparkSession(_sc) {

  self =>

  // initialize GemFireXDDialect so that it gets registered

  GemFireXDDialect.init()

  /* ----------------------- *
   |  Session-related state  |
   * ----------------------- */

  private[spark] val id = SnappySession.newId()

  new FinalizeSession(this)

  private def sc: SparkContext = sparkContext

  /**
   * State shared across sessions, including the [[SparkContext]], cached data, listener,
   * and a catalog that interacts with external systems.
   */
  @transient
  override private[sql] lazy val sharedState: SnappySharedState = {
    SnappyContext.sharedState(sparkContext)
  }

  /**
   * State isolated across sessions, including SQL configurations, temporary tables, registered
   * functions, and everything else that accepts a [[org.apache.spark.sql.internal.SQLConf]].
   */
  @transient
  lazy override val sessionState: SnappySessionState = {
    SnappySession.aqpSessionStateClass match {
      case Some(aqpClass) => aqpClass.getConstructor(classOf[SnappySession]).
          newInstance(self).asInstanceOf[SnappySessionState]
      case None => new SnappySessionState(self)
    }
  }

  @transient
  lazy val sessionCatalog: SnappyStoreHiveCatalog = {
    SnappyContext.getClusterMode(sc) match {
      case ThinClientConnectorMode(_, _) => sessionState.catalog.asInstanceOf[ConnectorCatalog]
      case _ => sessionState.catalog.asInstanceOf[SnappyStoreHiveCatalog]
    }
  }

  def snappyParser: SnappyParser = sessionState.sqlParser.sqlParser

  private[spark] def snappyContextFunctions = sessionState.contextFunctions

  SnappyContext.initGlobalSnappyContext(sparkContext, this)
  SnappyDataFunctions.registerSnappyFunctions(sessionState.functionRegistry)
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
    new SnappySession(sparkContext)
  }

 /**
  * :: Experimental ::
  * Creates a [[DataFrame]] from an RDD of Product (e.g. case classes, tuples).
  * This method handles generic array datatype like Array[Decimal]
  */
  def createDataFrameUsingRDD[A <: Product : TypeTag](rdd: RDD[A]): DataFrame = {
    SparkSession.setActiveSession(this)
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    val attributeSeq = schema.toAttributes
    val rowRDD = RDDConversions.productToRowRdd(rdd, schema.map(_.dataType))
    Dataset.ofRows(self, LogicalRDD(attributeSeq, rowRDD)(self))
  }

  override def sql(sqlText: String): CachedDataFrame =
    snappyContextFunctions.sql(SnappySession.sqlPlan(this, sqlText))

  @DeveloperApi
  def sqlUncached(sqlText: String): DataFrame = {
    if (planCaching) {
      planCaching = false
      try {
        snappyContextFunctions.sql(super.sql(sqlText))
      } finally {
        planCaching = Property.PlanCaching.get(sessionState.conf)
      }
    } else {
      snappyContextFunctions.sql(super.sql(sqlText))
    }
  }

  final def prepareSQL(sqlText: String): LogicalPlan = {
    val logical = sessionState.sqlParser.parsePlan(sqlText)
    SparkSession.setActiveSession(this)
    sessionState.analyzerPrepare.execute(logical)
  }

  private[sql] final def executePlan(plan: LogicalPlan): QueryExecution = {
    try {
      val execution = sessionState.executePlan(plan)
      execution.assertAnalyzed()
      execution
    } catch {
      case e: AnalysisException =>
        // in case of connector mode, exception can be thrown if
        // table form is changed (altered) and we have old table
        // object in SnappyStoreHiveCatalog.cachedDataSourceTables
        SnappyContext.getClusterMode(sparkContext) match {
          case ThinClientConnectorMode(_, _) =>
            sessionCatalog.invalidateAll()
            throw e
          case _ =>
            throw e
        }
    }
  }

  @transient
  private[sql] val queryHints = new ConcurrentHashMap[String, String](4, 0.7f, 1)

  def getPreviousQueryHints: java.util.Map[String, String] =
    java.util.Collections.unmodifiableMap(queryHints)

  @transient
  private val contextObjects = new ConcurrentHashMap[Any, Any](16, 0.7f, 1)

  @transient
  private[sql] var currentKey: CachedKey = _

  @transient
  private[sql] var planCaching: Boolean = Property.PlanCaching.get(sessionState.conf)

  @transient
  private[sql] var partitionPruning: Boolean = Property.PartitionPruning.get(sessionState.conf)


  /**
   * Get a previously registered context object using [[addContextObject]].
   */
  private[sql] def getContextObject[T](key: Any): Option[T] = {
    Option(contextObjects.get(key).asInstanceOf[T])
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
    getContextObject[Boolean](StoreUtils.PROPERTY_PARTITION_BUCKET_LINKED) match {
      case Some(b) => b
      case None => false
    }
  }

  private[sql] def preferPrimaries: Boolean =
    Property.PreferPrimariesInQuery.get(sessionState.conf)

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

  private[sql] def clearContext(): Unit = synchronized {
    getContextObject[LogicalPlan](SnappySession.CACHED_PUTINTO_UPDATE_PLAN).
        foreach { cachedPlan =>
          sharedState.cacheManager.uncacheQuery(this, cachedPlan, blocking = true)
        }
    contextObjects.clear()
    planCaching = Property.PlanCaching.get(sessionState.conf)
  }

  private[sql] def clearQueryData(): Unit = synchronized {
    queryHints.clear()
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

  /** Close the session which will be unusable after this call. */
  override def close(): Unit = synchronized {
    clear()
    sessionCatalog.close()
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
      transformer: Option[(RDD[T]) => RDD[Row]])(implicit v: TypeTag[T]) {
    val transform = transformer match {
      case Some(x) => x
      case None => if (!(v.tpe =:= typeOf[Row])) {
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
    Dataset.ofRows(this, plan).unpersist(blocking = true)
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
    createTable(tableIdent, provider, userSpecifiedSchema, schemaDDL, mode, options,
      isBuiltIn, resolveRelation = true)
  }

  private[sql] def createTable(
      tableIdent: QualifiedTableName,
      provider: String,
      userSpecifiedSchema: Option[StructType],
      schemaDDL: Option[String],
      mode: SaveMode,
      options: Map[String, String],
      isBuiltIn: Boolean,
      resolveRelation: Boolean): LogicalPlan = {

    if (sessionCatalog.tableExists(tableIdent)) {
      mode match {
        case SaveMode.ErrorIfExists =>
          throw new AnalysisException(
            s"createTable: Table $tableIdent already exists.")
        case _ =>
          return sessionCatalog.lookupRelation(tableIdent, None)
      }
    }

    SnappyContext.getClusterMode(sc) match {
      case ThinClientConnectorMode(_, _) =>
        // resolve schema at source for external tables since the required
        // classes to resolve may not be available in embedded cluster
        val userSchema = userSpecifiedSchema match {
          case None if !isBuiltIn && resolveRelation && schemaDDL.isEmpty =>
            Some(DataSource(self,
              userSpecifiedSchema = userSpecifiedSchema,
              className = provider, options = options).resolveRelation().schema)
          case s => s
        }
        return sessionCatalog.asInstanceOf[ConnectorCatalog].connectorHelper.createTable(
          tableIdent, provider, userSchema, schemaDDL, mode, options, isBuiltIn)
      case _ =>
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
      case Some(cols) => Some(JdbcExtendedUtils.externalResolvedDataSource(self,
        cols, source, mode, params))

      case None if resolveRelation =>
        // add allowExisting in properties used by some implementations
        Some(DataSource(
          self,
          userSpecifiedSchema = userSpecifiedSchema,
          className = source,
          options = params + (JdbcExtendedUtils.ALLOW_EXISTING_PROPERTY ->
              (mode != SaveMode.ErrorIfExists).toString)).resolveRelation())

      case _ => None
    }

    val plan = relation.map(LogicalRelation(_))

    if (!SnappyContext.internalTableSources.exists(_.equals(source))) {
      sessionCatalog.registerDataSourceTable(tableIdent, userSpecifiedSchema,
        Array.empty[String], source, params, relation)
    }
    snappyContextFunctions.postRelationCreation(relation, this)
    plan.orNull
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
    val schema = userSpecifiedSchema.getOrElse(
      normalizeSchema(sessionState.executePlan(query).analyzed.schema))
    val userSchema = Some(schema)
    val plan = clusterMode match {
      // for smart connector mode create the table here and allow
      // further processing to load the data
      case ThinClientConnectorMode(_, _) =>
        sessionCatalog.asInstanceOf[ConnectorCatalog].connectorHelper.createTable(tableIdent,
          provider, userSchema, schemaDDL, mode, options, isBuiltIn)
        createTableAsSelect(tableIdent, provider, userSchema, schemaDDL,
          partitionColumns, SaveMode.Append, options, query, isBuiltIn)
      case _ =>
        createTableAsSelect(tableIdent, provider, userSchema, schemaDDL,
          partitionColumns, mode, options, query, isBuiltIn)
    }

    plan
  }

  /**
   * Create an external/builtin table with given options.
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
              find(x => !compareDataTypeIgnoreNameAndNullability(x._1.dataType, x._2.dataType))
            match {
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
            val ds = DataSource(self,
              className = source,
              userSpecifiedSchema = userSpecifiedSchema,
              partitionColumns = partitionColumns,
              options = params)
            ds.write(mode, df)
            ds.copy(userSpecifiedSchema = Some(df.schema.asNullable)).resolveRelation()
        }
    }

    // need to register if not existing in catalog
    if (insertRelation.isEmpty || overwrite) {
      val relationOpt = Some(relation)
      if (!SnappyContext.internalTableSources.exists(_.equals(source))) {
        sessionCatalog.registerDataSourceTable(tableIdent, userSpecifiedSchema,
          partitionColumns, source, params, relationOpt)
      }
      snappyContextFunctions.postRelationCreation(relationOpt, this)
    }
    LogicalRelation(relation, catalogTable = Some(tableIdent.getTable(this.sessionCatalog)))
  }

  /**
   * Compares two types, ignoring nullability of ArrayType, MapType, StructType, and ignoring
   * field names
   */
  private[sql] def compareDataTypeIgnoreNameAndNullability(from: DataType, to: DataType):
  Boolean = {
    (from, to) match {
      case (ArrayType(fromElement, _), ArrayType(toElement, _)) =>
        compareDataTypeIgnoreNameAndNullability(fromElement, toElement)

      case (MapType(fromKey, fromValue, _), MapType(toKey, toValue, _)) =>
        compareDataTypeIgnoreNameAndNullability(fromKey, toKey) &&
          compareDataTypeIgnoreNameAndNullability(fromValue, toValue)

      case (StructType(fromFields), StructType(toFields)) =>
        fromFields.length == toFields.length &&
          fromFields.zip(toFields).forall { case (l, r) =>
              compareDataTypeIgnoreNameAndNullability(l.dataType, r.dataType)
          }

      case (fromDataType, toDataType) => fromDataType == toDataType
    }
  }

  private[sql] def addBaseTableOption(baseTable: Option[_],
      options: Map[String, String]): Map[String, String] = baseTable match {
    case Some(t: TableIdentifier) => options + (JdbcExtendedUtils
        .BASETABLE_PROPERTY -> sessionCatalog.formatTableName(t.table))
    case Some(s: String) => options + (JdbcExtendedUtils
        .BASETABLE_PROPERTY -> sessionCatalog.formatTableName(s).toString)
    case _ => options
  }

  /**
   * Drop a SnappyData table created by a call to SnappySession.createTable,
   * Catalog.createExternalTable or Dataset.createOrReplaceTempView.
   *
   * @param tableName table to be dropped
   * @param ifExists  attempt drop only if the table exists
   */
  def dropTable(tableName: String, ifExists: Boolean = false): Unit =
    dropTable(sessionCatalog.newQualifiedTableName(tableName), ifExists)

  /**
   * Drop a SnappyData table created by a call to SnappySession.createTable,
   * Catalog.createExternalTable or Dataset.createOrReplaceTempView.
   *
   * @param tableIdent table to be dropped
   * @param ifExists   attempt drop only if the table exists
   */
  private[sql] def dropTable(tableIdent: QualifiedTableName,
      ifExists: Boolean): Unit = {
    dropTable(tableIdent, ifExists, resolveRelation = true)
  }

  /**
   * Drop a SnappyData table created by a call to SnappySession.createTable,
   * Catalog.createExternalTable or Dataset.createOrReplaceTempView.
   *
   * @param tableIdent      table to be dropped
   * @param ifExists        attempt drop only if the table exists
   * @param resolveRelation if true then attempt to create BaseRelation for any
   *                        additional actions (like DestroyRelation.destroy)
   *                        else skip resolution of relation and
   *                        only drop from catalog
   */
  private[sql] def dropTable(tableIdent: QualifiedTableName,
      ifExists: Boolean, resolveRelation: Boolean): Unit = {

    val planOpt = try {
      Some(sessionCatalog.lookupRelation(tableIdent))
    } catch {
      case e@(_: TableNotFoundException | _: SQLException | _: NoSuchTableException) =>
        if (ifExists) return else throw e
      case NonFatal(_) if !resolveRelation => None
    }
    val isTempTable = sessionCatalog.isTemporaryTable(tableIdent)

    SnappyContext.getClusterMode(sc) match {
      case ThinClientConnectorMode(_, _) =>
        if (!isTempTable) {
          // resolve whether table is external or not at source since the required
          // classes to resolve may not be available in embedded cluster
          val isExternal = planOpt match {
            case Some(LogicalRelation(br, _, _)) =>
              sessionCatalog.getTableType(br) == ExternalTableType.External
            case _ => false
          }
          sessionCatalog.asInstanceOf[ConnectorCatalog].connectorHelper
              .dropTable(tableIdent, ifExists, isExternal)
          return
        }
      case _ =>
    }

    // additional cleanup for external and temp tables, if required
    planOpt match {
      case Some(plan@LogicalRelation(br, _, _)) =>
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
        Dataset.ofRows(this, plan).unpersist(blocking = true)
        if (isTempTable) {
          // This is due to temp table
          // can be made from a backing relation like Parquet or Hadoop
          sessionCatalog.unregisterTable(tableIdent)
        }
        br match {
          case d: DestroyRelation => d.destroy(ifExists)
            sessionCatalog.unregisterDataSourceTable(tableIdent, Some(br))
          case _ => if (!isTempTable && !sessionCatalog.unregisterGlobalView(tableIdent)) {
            sessionCatalog.unregisterDataSourceTable(tableIdent, Some(br))
          }
        }
      case _ if isTempTable => // This is a temp table with no relation as source
        planOpt.foreach(Dataset.ofRows(this, _).unpersist(blocking = true))
        sessionCatalog.unregisterTable(tableIdent)
      case _ =>
        // this is a table in smart connector remote call or a view
        if (!sessionCatalog.unregisterGlobalView(tableIdent)) {
          sessionCatalog.unregisterDataSourceTable(tableIdent, None)
        }
    }
  }

  private[sql] def alterTable(tableName: String, isAddColumn: Boolean,
      column: StructField): Unit = {
    val qualifiedTable = sessionCatalog.newQualifiedTableName(tableName)
    if (sessionCatalog.caseSensitiveAnalysis) {
      alterTable(qualifiedTable, isAddColumn, column)
    } else {
      val colName = Utils.fieldName(column)
      alterTable(qualifiedTable, isAddColumn,
        if (Utils.hasLowerCase(colName)) sessionCatalog.normalizeField(column, colName) else column)
    }
  }

  private[sql] def alterTable(tableIdent: QualifiedTableName, isAddColumn: Boolean,
      column: StructField): Unit = {
    val plan = try {
      sessionCatalog.lookupRelation(tableIdent)
    } catch {
      case tnfe: TableNotFoundException => throw tnfe
    }

    if(sessionCatalog.isTemporaryTable(tableIdent)) {
      throw new AnalysisException("alter table not supported for temp tables")
    }
    plan match {
      case LogicalRelation(_: ColumnFormatRelation, _, _) =>
        throw new AnalysisException("alter table not supported for column tables")
      case _ =>
    }

    SnappyContext.getClusterMode(sc) match {
      case ThinClientConnectorMode(_, _) =>
          sessionCatalog.invalidateTable(tableIdent)
          sessionCatalog.asInstanceOf[ConnectorCatalog].connectorHelper
            .alterTable(tableIdent, isAddColumn, column)
          return
      case _ =>
    }

    plan match {
      case LogicalRelation(ar: AlterableRelation, _, _) =>
        sessionCatalog.invalidateTable(tableIdent)
        ar.alterTable(tableIdent, isAddColumn, column)
        SnappyStoreHiveCatalog.registerRelationDestroy()
        SnappySession.clearAllCache()
      case _ =>
        throw new AnalysisException("alter table not supported for external tables")
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

  def getCurrentSchema: String = sessionCatalog.currentSchema

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
    // normalize index column names for API calls
    val columnsWithDirection = indexColumns.map(p => sessionCatalog.formatName(p._1) -> p._2)
    createIndex(indexIdent, tableIdent, columnsWithDirection, options)
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
    new QualifiedTableName(indexIdent.schemaName,
      Constant.COLUMN_TABLE_INDEX_PREFIX + indexIdent.table)
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
    val jdbcOptions = new JDBCOptions(connProperties.url, "",
      connProperties.connProps.asScala.toMap)
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
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
   * {{{
   *        snSession.insert(tableName, dataDF.collect(): _*)
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
   * {{{
   *        java.util.ArrayList[java.util.ArrayList[_] rows = ...    *
   *         snSession.insert(tableName, rows)
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
   * {{{
   *        snSession.put(tableName, dataDF.collect(): _*)
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
   * {{{
   *        java.util.ArrayList[java.util.ArrayList[_] rows = ...    *
   *         snSession.put(tableName, rows)
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
    snappyParser.setPreparedQuery(preparePhase, paramSet)

  private[sql] def getParameterValue(questionMarkCounter: Int, pvs: Any): (Any, DataType) = {
    val parameterValueSet = pvs.asInstanceOf[ParameterValueSet]
    if (questionMarkCounter > parameterValueSet.getParameterCount) {
      assert(assertion = false, s"For Prepared Statement, Got more number of" +
          s" placeholders = $questionMarkCounter" +
          s" than given number of parameter" +
          s" constants = ${parameterValueSet.getParameterCount}")
    }
    val dvd = parameterValueSet.getParameter(questionMarkCounter - 1)
    val scalaTypeVal = SnappySession.getValue(dvd)
    val storeType = dvd.getTypeFormatId
    val storePrecision = dvd match {
      case d: stypes.SQLDecimal => d.getDecimalValuePrecision
      case _ => -1
    }
    val storeScale = dvd match {
      case d: stypes.SQLDecimal => d.getDecimalValueScale
      case _ => -1
    }
    (scalaTypeVal, SnappySession.getDataType(storeType, storePrecision, storeScale))
  }
}

private class FinalizeSession(session: SnappySession)
    extends FinalizeObject(session, true) {

  private var sessionId = session.id

  override def getHolder: FinalizeHolder = FinalizeObject.getServerHolder

  override protected def doFinalize(): Boolean = {
    if (sessionId != SnappySession.INVALID_ID) {
      val session = SnappySession.clearSessionCache(sessionId)
      sessionId = SnappySession.INVALID_ID
      if (session ne null) {
        session.sessionCatalog.close()
      }
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
  private[sql] val CACHED_PUTINTO_UPDATE_PLAN = "cached_putinto_logical_plan"

  private[sql] var tokenize: Boolean = _

  lazy val isEnterpriseEdition: Boolean = {
    GemFireCacheImpl.setGFXDSystem(true)
    GemFireVersion.getInstance(classOf[GemFireXDVersion], SharedUtils.GFXD_VERSION_PROPERTIES)
    GemFireVersion.isEnterpriseEdition
  }

  private lazy val aqpSessionStateClass: Option[Class[_]] = {
    if (isEnterpriseEdition) {
      try {
        Some(org.apache.spark.util.Utils.classForName(
          "org.apache.spark.sql.internal.SnappyAQPSessionState"))
      } catch {
        case NonFatal(e) =>
          // Let the user know if it failed to load AQP classes.
          logWarning(s"Failed to load AQP classes in Enterprise edition: $e")
          None
      }
    } else None
  }

  private[sql] def findShuffleDependencies(rdd: RDD[_]): List[Int] = {
    rdd.dependencies.toList.flatMap {
      case s: ShuffleDependency[_, _, _] => if (s.rdd ne rdd) {
        s.shuffleId :: findShuffleDependencies(s.rdd)
      } else s.shuffleId :: Nil

      case d => if (d.rdd ne rdd) findShuffleDependencies(d.rdd) else Nil
    }
  }

  def getExecutedPlan(plan: SparkPlan): (SparkPlan, CodegenSparkFallback) = plan match {
    case cg@CodegenSparkFallback(WholeStageCodegenExec(p)) => (p, cg)
    case cg@CodegenSparkFallback(p) => (p, cg)
    case WholeStageCodegenExec(p) => (p, null)
    case _ => (plan, null)
  }

  /**
   * Snappy's execution happens in two phases. First phase the plan is executed
   * to create a rdd which is then used to create a CachedDataFrame.
   * In second phase, the CachedDataFrame is then used for further actions.
   * For accumulating the metrics for first phase,
   * SparkListenerSQLPlanExecutionStart is fired. This keeps the current
   * executionID in _executionIdToData but does not add it to the active
   * executions. This ensures that query is not shown in the UI but the
   * new jobs that are run while the plan is being executed are tracked
   * against this executionID. In the second phase, when the query is
   * actually executed, SparkListenerSQLPlanExecutionStart adds the execution
   * data to the active executions. SparkListenerSQLPlanExecutionEnd is
   * then sent with the accumulated time of both the phases.
   */
  private def planExecution(qe: QueryExecution, session: SnappySession, sqlText: String,
      executedPlan: SparkPlan, paramLiterals: Array[ParamLiteral], paramsId: Int)
      (f: => RDD[InternalRow]): (RDD[InternalRow], String, SparkPlanInfo,
      String, SparkPlanInfo, Long, Long, Long) = {
    // Right now the CachedDataFrame is not getting used across SnappySessions
    val executionId = CachedDataFrame.nextExecutionIdMethod.
      invoke(SQLExecution).asInstanceOf[Long]
    session.sparkContext.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, executionId.toString)
    val start = System.currentTimeMillis()
    try {
      // get below two with original "ParamLiteral(" tokens that will be replaced
      // by actual values before every execution
      val queryExecutionStr = qe.toString
      val queryPlanInfo = PartitionedPhysicalScan.getSparkPlanInfo(executedPlan)
      // post with proper values in event which will show up in GUI
      val postQueryExecutionStr = replaceParamLiterals(queryExecutionStr, paramLiterals, paramsId)
      val postQueryPlanInfo = PartitionedPhysicalScan.updatePlanInfo(queryPlanInfo,
        paramLiterals, paramsId)
      session.sparkContext.listenerBus.post(SparkListenerSQLPlanExecutionStart(
        executionId, CachedDataFrame.queryStringShortForm(sqlText),
        sqlText, postQueryExecutionStr, postQueryPlanInfo, start))
      val rdd = f
      (rdd, queryExecutionStr, queryPlanInfo, postQueryExecutionStr, postQueryPlanInfo,
          executionId, start, System.currentTimeMillis())
    } finally {
      session.sparkContext.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, null)
    }
  }

  private def evaluatePlan(qe: QueryExecution, session: SnappySession, sqlText: String,
      paramLiterals: Array[ParamLiteral], paramsId: Int): CachedDataFrame = {
    val (executedPlan, withFallback) = getExecutedPlan(qe.executedPlan)
    var planCaching = session.planCaching

    val (cachedRDD, execution, origExecutionString, origPlanInfo, executionString, planInfo,
    rddId, noSideEffects, executionId, planStartTime, planEndTime) = executedPlan match {
      case _: ExecutedCommandExec | _: ExecutePlan =>
        // TODO add caching for point updates/deletes; a bit of complication
        // because getPlan will have to do execution with all waits/cleanups
        // normally done in CachedDataFrame.collectWithHandler/withCallback
        /*
        val cachedRDD = plan match {
          case p: ExecutePlan => p.child.execute()
          case _ => null
        }
        */
        // post with proper values in event which will show up in GUI
        val origExecutionStr = qe.toString
        val origPlanInfo = PartitionedPhysicalScan.getSparkPlanInfo(executedPlan)
        val executionStr = replaceParamLiterals(origExecutionStr, paramLiterals, paramsId)
        val planInfo = PartitionedPhysicalScan.updatePlanInfo(origPlanInfo,
          paramLiterals, paramsId)
        // different Command types will post their own plans in toRdd evaluation
        val isCommand = executedPlan.isInstanceOf[ExecutedCommandExec]
        var rdd = if (isCommand) qe.toRdd else null
        // post final execution immediately (collect for these plans will post nothing)
        CachedDataFrame.withNewExecutionId(session, sqlText, sqlText, executionStr, planInfo) {
          // create new LogicalRDD plan so that plan does not get re-executed
          // (e.g. just toRdd is not enough since further operators like show will pass
          //   around the LogicalPlan and not the executedPlan; it works for plans using
          //   ExecutedCommandExec though because Spark layer has special check for it in
          //   Dataset hasSideEffects)
          if (!isCommand) rdd = qe.toRdd
          val newPlan = LogicalRDD(qe.analyzed.output, rdd)(session)
          val execution = session.sessionState.executePlan(newPlan)
          (null, execution, origExecutionStr, origPlanInfo, executionStr, planInfo,
              rdd.id, false, -1L, 0L, -1L)
        }._1

      case plan: CollectAggregateExec =>
        val (childRDD, origExecutionStr, origPlanInfo, executionStr, planInfo, executionId,
        planStartTime, planEndTime) = planExecution(qe, session, sqlText, plan, paramLiterals,
          paramsId)(if (withFallback ne null) withFallback.execute(plan.child) else plan.childRDD)
        (childRDD, qe, origExecutionStr, origPlanInfo, executionStr, planInfo,
            childRDD.id, true, executionId, planStartTime, planEndTime)

      case plan =>
        val (rdd, origExecutionStr, origPlanInfo, executionStr, planInfo, executionId,
        planStartTime, planEndTime) = planExecution(qe, session, sqlText, plan,
          paramLiterals, paramsId) {
          plan match {
            case p: CollectLimitExec =>
              if (withFallback ne null) withFallback.execute(p.child) else p.child.execute()
            case _ => qe.executedPlan.execute()
          }
        }
        (rdd, qe, origExecutionStr, origPlanInfo, executionStr, planInfo,
            rdd.id, true, executionId, planStartTime, planEndTime)
    }

    logDebug(s"qe.executedPlan = ${qe.executedPlan}")

    // If this has in-memory caching then don't cache since plan can change
    // dynamically after caching due to unpersist etc. Disable for broadcasts
    // too since the RDDs cache broadcast result which can change in subsequent
    // execution with different ParamLiteral values. Also do not cache
    // if snappy tables are not there since the plans may be dependent on constant
    // literals in push down filters etc
    planCaching &&= (cachedRDD ne null) && executedPlan.find {
      case _: BroadcastHashJoinExec | _: BroadcastNestedLoopJoinExec |
           _: BroadcastExchangeExec | _: InMemoryTableScanExec => true
      case p if HiveClientUtil.isHiveExecPlan(p) => true
      case dsc: DataSourceScanExec => !dsc.relation.isInstanceOf[DependentRelation]
      case _ => false
    }.isEmpty

    // collect the query hints
    val queryHints = session.synchronized {
      val numHints = session.queryHints.size()
      val hints = if (numHints == 0) java.util.Collections.emptyMap[String, String]()
      else {
        val m = ObjectObjectHashMap.withExpectedSize[String, String](numHints)
        m.putAll(session.queryHints)
        m
      }
      hints
    }

    val (rdd, shuffleDependencies, shuffleCleanups) = if (planCaching) {
      val shuffleDeps = findShuffleDependencies(cachedRDD).toArray
      val cleanups = new Array[Future[Unit]](shuffleDeps.length)
      (cachedRDD, shuffleDeps, cleanups)
    } else (null, Array.emptyIntArray, Array.empty[Future[Unit]])
    new CachedDataFrame(session, execution, origExecutionString, origPlanInfo,
      executionString, planInfo, rdd, shuffleDependencies, RowEncoder(qe.analyzed.schema),
      shuffleCleanups, rddId, noSideEffects, queryHints,
      executionId, planStartTime, planEndTime)
  }

  private[this] lazy val planCache = {
    val env = SparkEnv.get
    val cacheSize = if (env ne null) {
      Property.PlanCacheSize.get(env.conf)
    } else Property.PlanCacheSize.defaultValue.get
    CacheBuilder.newBuilder().maximumSize(cacheSize).build[CachedKey, CachedDataFrame]()
  }

  def getPlanCache: Cache[CachedKey, CachedDataFrame] = planCache

  def sqlPlan(session: SnappySession, sqlText: String): CachedDataFrame = {
    val parser = session.sessionState.sqlParser
    val parsed = parser.parsePlan(sqlText)
    val planCaching = session.planCaching
    val plan = if (planCaching) session.sessionState.preCacheRules.execute(parsed) else parsed
    val paramLiterals = parser.sqlParser.getAllLiterals
    val paramsId = parser.sqlParser.getCurrentParamsId
    val key = CachedKey(session, plan, sqlText, paramLiterals, planCaching)
    var cachedDF: CachedDataFrame = if (planCaching) planCache.getIfPresent(key) else null
    if (cachedDF eq null) {
      // evaluate the plan and cache it if required
      key.currentLiterals = paramLiterals
      key.currentParamsId = paramsId
      session.currentKey = key
      try {
        val execution = session.executePlan(plan)
        cachedDF = evaluatePlan(execution, session, sqlText, paramLiterals, paramsId)
        // put in cache if the DF has to be cached
        if (planCaching && cachedDF.isCached) {
          if (isTraceEnabled) {
            logTrace(s"Caching the plan for: $sqlText :: ${cachedDF.queryExecutionString}")
          } else if (isDebugEnabled) {
            logDebug(s"Caching the plan for: $sqlText")
          }
          key.currentLiterals = null
          key.currentParamsId = -1
          planCache.put(key, cachedDF)
        }
      } finally {
        session.currentKey = null
      }
    } else {
      logDebug(s"Using cached plan for: $sqlText (existing: ${cachedDF.queryString})")
      cachedDF = cachedDF.duplicate()
    }
    handleCachedDataFrame(cachedDF, plan, session, sqlText, paramLiterals, paramsId)
  }

  private def handleCachedDataFrame(cachedDF: CachedDataFrame, plan: LogicalPlan,
      session: SnappySession, sqlText: String, paramLiterals: Array[ParamLiteral],
      paramsId: Int): CachedDataFrame = {
    cachedDF.queryString = sqlText
    if (cachedDF.isCached && (cachedDF.paramLiterals eq null)) {
      cachedDF.paramLiterals = paramLiterals
      cachedDF.paramsId = paramsId
    }
    // store the current tokenized constant values which will be replaced
    // before execution in CachedDataFrame
    cachedDF.currentLiterals = paramLiterals
    cachedDF
  }

  /**
   * Replace any ParamLiterals in a string with current values.
   */
  private[sql] def replaceParamLiterals(text: String,
      currentParamConstants: Array[ParamLiteral], paramsId: Int): String = {

    if ((currentParamConstants eq null) || currentParamConstants.length == 0) return text
    val paramStart = TokenLiteral.PARAMLITERAL_START
    var nextIndex = text.indexOf(paramStart)
    if (nextIndex != -1) {
      var lastIndex = 0
      val sb = new java.lang.StringBuilder(text.length)
      while (nextIndex != -1) {
        sb.append(text, lastIndex, nextIndex)
        nextIndex += paramStart.length
        val posEnd = text.indexOf(',', nextIndex)
        val pos = Integer.parseInt(text.substring(nextIndex, posEnd))
        // get the ID which created this ParamLiteral (e.g. a query on temporary table
        // for a previously cached table will have its own literals and cannot replace former)
        val idEnd = text.indexOf('#', posEnd + 1)
        val id = Integer.parseInt(text.substring(posEnd + 1, idEnd))
        val lenEnd = text.indexOf(',', idEnd + 1)
        val len = Integer.parseInt(text.substring(idEnd + 1, lenEnd))
        lastIndex = lenEnd + 1 + len
        // append the new value if matching ID else replace with embedded value
        if (paramsId == id) {
          sb.append(currentParamConstants(pos).valueString)
          // skip to end of value and continue searching
        } else {
          sb.append(text.substring(lenEnd + 1, lastIndex))
        }
        nextIndex = text.indexOf(paramStart, lastIndex)
      }
      // append any remaining
      if (lastIndex < text.length) {
        sb.append(text, lastIndex, text.length)
      }
      sb.toString
    } else text

  }

  private def newId(): Int = {
    val id = ID.incrementAndGet()
    if (id != INVALID_ID) id
    else ID.incrementAndGet()
  }

  private[spark] def clearSessionCache(sessionId: Long): SnappySession = {
    var foundSession: SnappySession = null
    val iter = planCache.asMap().keySet().iterator()
    while (iter.hasNext) {
      val session = iter.next().session
      if (session.id == sessionId) {
        foundSession = session
        iter.remove()
      }
    }
    foundSession
  }

  def clearAllCache(onlyQueryPlanCache: Boolean = false): Unit = {
    val sc = SnappyContext.globalSparkContext
    if (!SnappyTableStatsProviderService.suspendCacheInvalidation &&
        (sc ne null) && !sc.isStopped) {
      planCache.invalidateAll()
      if (!onlyQueryPlanCache) {
        CodeGeneration.clearAllCache()
        Utils.mapExecutors[Unit](sc, () => {
          CodeGeneration.clearAllCache()
          Iterator.empty
        })
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

      session = new SnappySession(sc)
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

  def getValue(dvd: stypes.DataValueDescriptor): Any = dvd match {
    case i: stypes.SQLInteger => i.getInt
    case si: stypes.SQLSmallint => si.getShort
    case ti: stypes.SQLTinyint => ti.getByte
    case d: stypes.SQLDouble => d.getDouble
    case li: stypes.SQLLongint => li.getLong
    case bid: stypes.BigIntegerDecimal => bid.getDouble
    case de: stypes.SQLDecimal => de.getBigDecimal
    case r: stypes.SQLReal => r.getFloat
    case b: stypes.SQLBoolean => b.getBoolean
    case cl: stypes.SQLClob =>
      val charArray = cl.getCharArray()
      if (charArray != null) {
        val str = String.valueOf(charArray)
        UTF8String.fromString(str)
      } else null
    case lvc: stypes.SQLLongvarchar => UTF8String.fromString(lvc.getString)
    case vc: stypes.SQLVarchar => UTF8String.fromString(vc.getString)
    case c: stypes.SQLChar => UTF8String.fromString(c.getString)
    case ts: stypes.SQLTimestamp => ts.getTimestamp(null)
    case t: stypes.SQLTime => t.getTime(null)
    case d: stypes.SQLDate =>
      val c: Calendar = null
      d.getDate(c)
    case _ => dvd.getObject
  }
}

final class CachedKey(val session: SnappySession, private val lp: LogicalPlan,
    val sqlText: String, val hintHashcode: Int) {

  private[sql] var currentLiterals: Array[ParamLiteral] = _
  private[sql] var currentParamsId: Int = -1

  override val hashCode: Int = {
    var h = ClientResolverUtils.addIntToHashOpt(session.hashCode(), 42)
    h = ClientResolverUtils.addIntToHashOpt(lp.hashCode(), h)
    ClientResolverUtils.addIntToHashOpt(hintHashcode, h)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case x: CachedKey =>
        x.hintHashcode == hintHashcode && (x.session eq session) && x.lp == lp
      case _ => false
    }
  }
}

object CachedKey {
  def apply(session: SnappySession, plan: LogicalPlan, sqlText: String,
      paramLiterals: Array[ParamLiteral], forCaching: Boolean): CachedKey = {

    def normalizeExprIds: PartialFunction[Expression, Expression] = {
      case a: AttributeReference =>
        AttributeReference(a.name, a.dataType, a.nullable)(exprId = ExprId(-1))
      case a: Alias =>
        val name = if (a.name == Utils.WEIGHTAGE_COLUMN_NAME ||
            a.name.startsWith(Utils.SKIP_ANALYSIS_PREFIX)) {
          a.name
        } else "none"
        Alias(a.child, name)(exprId = ExprId(-1))
      case ae: AggregateExpression => ae.copy(resultId = ExprId(-1))
      case s: ScalarSubquery =>
        throw new IllegalStateException("scalar subquery should not have been present")
      case e: Exists =>
        e.copy(plan = e.plan.transformAllExpressions(normalizeExprIds), exprId = ExprId(-1))
      case p: PredicateSubquery =>
        p.copy(plan = p.plan.transformAllExpressions(normalizeExprIds), exprId = ExprId(-1))
      case l: ListQuery =>
        l.copy(plan = l.plan.transformAllExpressions(normalizeExprIds), exprId = ExprId(-1))
    }

    def transformExprID: PartialFunction[LogicalPlan, LogicalPlan] = {
      case f@Filter(condition, child) => f.copy(
        condition = condition.transform(normalizeExprIds),
        child = child.transformAllExpressions(normalizeExprIds))
      case q: LogicalPlan => q.transformAllExpressions(normalizeExprIds)
    }

    // normalize lp so that two queries can be determined to be equal
    val normalizedPlan = if (forCaching) {
      // mark ParamLiterals as "tokenized" at this point so that comparison
      // in the plan is based on position rather than value
      for (l <- paramLiterals) l.tokenized = true
      plan.transform(transformExprID)
    } else plan
    new CachedKey(session, normalizedPlan, sqlText, session.queryHints.hashCode())
  }
}
