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
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Consumer

import com.gemstone.gemfire.cache.EntryExistsException
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor.Profile
import com.gemstone.gemfire.distributed.internal.ProfileListener
import com.gemstone.gemfire.internal.GemFireVersion
import com.gemstone.gemfire.internal.cache.{CacheDistributionAdvisee, ColocationHelper, GemFireCacheImpl, PartitionedRegion}
import com.gemstone.gemfire.internal.shared.{ClientResolverUtils, FinalizeHolder, FinalizeObject}
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.common.util.concurrent.UncheckedExecutionException
import com.pivotal.gemfirexd.internal.GemFireXDVersion
import com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet
import com.pivotal.gemfirexd.internal.iapi.types.SQLDecimal
import com.pivotal.gemfirexd.internal.shared.common.{SharedUtils, StoredFormatIds}
import io.snappydata.collection.ObjectObjectHashMap
import io.snappydata.{Constant, Property, SnappyDataFunctions, SnappyTableStatsProviderService}
import org.apache.spark._
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.internal.config.{ConfigEntry, TypedConfigBuilder}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.aqp.SnappyContextFunctions
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, NoSuchTableException}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGeneration, CodegenContext}
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, AttributeReference, Descending, Exists, ExprId, Expression, GenericRow, ListQuery, LiteralValue, ParamLiteral, ScalarSubquery, SortDirection}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisBarrier, Filter, LogicalPlan, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.{DefinedByConstructorParams, InternalRow, ScalaReflection, TableIdentifier}
import org.apache.spark.sql.collection.{Utils, WrappedInternalRow}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.CollectAggregateExec
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation
import org.apache.spark.sql.execution.columnar.{ExternalStoreUtils, InMemoryTableScanExec}
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReuseExchange}
import org.apache.spark.sql.execution.ui.SparkListenerSQLPlanExecutionStart
import org.apache.spark.sql.hive._
import org.apache.spark.sql.internal._
import org.apache.spark.sql.row.GemFireXDDialect
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.reflect.{ClassTag, classTag}
import scala.util.control.NonFatal


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
  override lazy val sharedState: SnappySharedState = {
    SnappyContext.sharedState(sparkContext)
  }

  val contextFunctions: SnappyContextFunctions = new SnappyContextFunctions

  private[sql] var disableStoreOptimizations: Boolean = false

  /**
   * State isolated across sessions, including SQL configurations, temporary tables, registered
   * functions, and everything else that accepts a [[org.apache.spark.sql.internal.SQLConf]].
   */
  @transient
  lazy override val sessionState: SessionState = {
    SnappySession.aqpSessionStateClass match {
      case Some(aqpClass) =>
        try {
          val ctor = aqpClass.getConstructors.head
          ctor.newInstance(self, None).asInstanceOf[SnappySessionStateBuilder].build()
      } catch {
        case NonFatal(e) =>
          throw new IllegalArgumentException(s"Error while instantiating '$aqpClass':", e)
      }
      case None =>
        val className = "org.apache.spark.sql.internal.SnappySessionStateBuilder"
        try {
          val clazz = Utils.classForName(className)
          val ctor = clazz.getConstructors.head
          ctor.newInstance(self, None).asInstanceOf[SnappySessionStateBuilder].build()
        } catch {
          case NonFatal(e) =>
            throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
        }
    }
  }

  @transient
  lazy val sessionCatalog: SnappyStoreHiveCatalog = {
    SnappyContext.getClusterMode(sc) match {
      case ThinClientConnectorMode(_, _) => sessionState.catalog.asInstanceOf[ConnectorCatalog]
      case _ => sessionState.catalog.asInstanceOf[SnappyStoreHiveCatalog]
    }
  }

  def snappyParser: ParserInterface = sessionState.sqlParser

  def snappyContextFunctions: SnappyContextFunctions = new SnappyContextFunctions

  SnappyContext.initGlobalSnappyContext(sparkContext, this)
  SnappyDataFunctions.registerSnappyFunctions(sessionState.functionRegistry)
  snappyContextFunctions.registerAQPErrorFunctions(this)

  /**
    * The partition mapping selected for the lead partitioned region in
    * a collocated chain for current execution
    */
  private[spark] val leaderPartitions = new ConcurrentHashMap[PartitionedRegion,
    Array[Partition]](16, 0.7f, 1)

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
    snappyContextFunctions.sql(SnappySession.getPlan(this, sqlText))

  def sqlUncached(sqlText: String): DataFrame =
    snappyContextFunctions.sql(super.sql(sqlText))

  final def prepareSQL(sqlText: String): LogicalPlan = {
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
            sessionCatalog.invalidateAll()
            throw e
          case _ =>
            throw e
        }
    }
  }

  def onlyParseSQL(sqLText: String): LogicalPlan = {
    sessionState.sqlParser.parsePlan(sqLText)
  }

  @transient
  private[sql] val queryHints = new ConcurrentHashMap[String, String](16, 0.7f, 1)

  def getPreviousQueryHints: java.util.Map[String, String] =
    java.util.Collections.unmodifiableMap(queryHints)

  @transient
  private val contextObjects = new ConcurrentHashMap[Any, Any](16, 0.7f, 1)

  @transient
  private[sql] var currentKey: SnappySession.CachedKey = _

  @transient
  private[sql] var planCaching: Boolean = Property.PlanCaching.get(sessionState.conf)

  @transient
  private[sql] var partitionPruning: Boolean = Property.PartitionPruning.get(sessionState.conf)

  @transient
  private[sql] var wholeStageEnabled: Boolean = sessionState.conf.wholeStageEnabled


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
    getContextObject[Boolean](StoreUtils.PROPERTY_PARTITION_BUCKET_LINKED)
        .getOrElse(false)
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

  private[sql] def getAllLiterals(key: SnappySession.CachedKey): Array[LiteralValue] = {
    var allLiterals: Array[LiteralValue] = Array.empty
    if (key != null && key.valid) {
      allLiterals = CachedPlanHelperExec.allLiterals(
        getContextObject[ArrayBuffer[ArrayBuffer[Any]]](
          SnappyParserConsts.REFERENCES_KEY).getOrElse(Nil)
      ).filter(!_.collectedForPlanCaching)

      allLiterals.foreach(_.collectedForPlanCaching = true)
    }
    allLiterals
  }

  private[sql] def clearContext(): Unit = synchronized {
    // println(s"clearing context")
    // new Throwable().printStackTrace()
    getContextObject[LogicalPlan](SnappySession.CACHED_PUTINTO_UPDATE_PLAN).
        map { cachedPlan =>
          sharedState.cacheManager.uncacheQuery(this, cachedPlan, true)
        }
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
    val plan = sessionCatalog.lookupRelation(tableIdent)
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
      case LogicalRelation(br, _, _, _) =>
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
          return sessionCatalog.lookupRelation(tableIdent)
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
          return sessionCatalog.lookupRelation(tableIdent)
        case _ =>
      }
    }

    val clusterMode = SnappyContext.getClusterMode(sc)
    val schema = userSpecifiedSchema.getOrElse(
      normalizeSchema(Dataset.ofRows(sqlContext.sparkSession, query).schema))
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
          return sessionCatalog.lookupRelation(tableIdent)
        case _ =>
          // Check if the specified data source match the data source
          // of the existing table.
          val plan = new PreprocessTableInsertOrPut(sessionState.conf).apply(
            sessionState.catalog.lookupRelation(tableIdent))
          EliminateSubqueryAliases(plan) match {
            case LogicalRelation(ir: InsertableRelation, _, _, _) => Some(ir)
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
        val ds = DataSource(self, className = source, userSpecifiedSchema = userSpecifiedSchema,
          partitionColumns = partitionColumns, options = params)
        runCommand("save") { ds.planForWriting(mode, AnalysisBarrier(df.logicalPlan)) }
        ds.copy(userSpecifiedSchema = Some(df.schema.asNullable)).resolveRelation()
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
    LogicalRelation(relation, tableIdent.getTable(this.sessionCatalog))
  }

  private def runCommand(name: String)(command: LogicalPlan): Unit = {
    val qe = sessionState.executePlan(command)
    try {
      val start = System.nanoTime()
      // call `QueryExecution.toRDD` to trigger the execution of commands.
      SQLExecution.withNewExecutionId(this, qe)(qe.toRdd)
      val end = System.nanoTime()
      listenerManager.onSuccess(name, qe, end - start)
    } catch {
      case e: Exception =>
        listenerManager.onFailure(name, qe, e)
        throw e
    }
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
            case Some(LogicalRelation(br, _, _, _)) =>
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
      case Some(plan@LogicalRelation(br, _, _, _)) =>
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
    alterTable(sessionCatalog.newQualifiedTableName(tableName), isAddColumn, column)
  }

  private[sql]  def alterTable(tableIdent: QualifiedTableName, isAddColumn: Boolean,
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
      case LogicalRelation(_: ColumnFormatRelation, _, _, _) =>
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
      case LogicalRelation(ar: AlterableRelation, _, _, _) =>
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
      case LogicalRelation(ir: IndexableRelation, _, _, _) =>
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
        case LogicalRelation(dr: DependentRelation, _, _, _) =>
          // Remove the index from the bse table props
          val baseTableIdent = sessionCatalog.newQualifiedTableName(dr.baseTable.get)
          sessionCatalog.lookupRelation(baseTableIdent) match {
            case LogicalRelation(cr: ColumnFormatRelation, _, _, _) =>
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
      case LogicalRelation(r: RowInsertableRelation, _, _, _) => r.insert(rows)
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
      case LogicalRelation(r: RowInsertableRelation, _, _, _) => r.insert(convertedRowSeq)
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
      case LogicalRelation(r: RowPutRelation, _, _, _) => r.put(rows)
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
      case LogicalRelation(u: UpdatableRelation, _, _, _) =>
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
      case LogicalRelation(u: UpdatableRelation, _, _, _) =>
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
      case LogicalRelation(r: RowPutRelation, _, _, _) =>
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
      case LogicalRelation(d: DeletableRelation, _, _, _) => d.delete(filterExpr)
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
    snappyParser.asInstanceOf[SnappyParser].setPreparedQuery(preparePhase, paramSet)

  private[sql] def getParameterValue(questionMarkCounter: Int, pvs: Any): (Any, DataType) = {
    val parameterValueSet = pvs.asInstanceOf[ParameterValueSet]
    if (questionMarkCounter > parameterValueSet.getParameterCount) {
      assert(assertion = false, s"For Prepared Statement, Got more number of" +
          s" placeholders = $questionMarkCounter" +
          s" than given number of parameter" +
          s" constants = ${parameterValueSet.getParameterCount}")
    }
    val dvd = parameterValueSet.getParameter(questionMarkCounter - 1)
    val scalaTypeVal = CachedPlanHelperExec.getValue(dvd)
    val storeType = dvd.getTypeFormatId
    val storePrecision = dvd match {
      case d: SQLDecimal => d.getDecimalValuePrecision
      case _ => -1
    }
    val storeScale = dvd match {
      case d: SQLDecimal => d.getDecimalValueScale
      case _ => -1
    }
    (scalaTypeVal, SnappySession.getDataType(storeType, storePrecision, storeScale))
  }

    protected[sql] def queryPreparations(topLevel: Boolean): Seq[Rule[SparkPlan]] = Seq(
      python.ExtractPythonUDFs,
      PlanSubqueries(self),
      EnsureRequirements(sessionState.conf),
      CollapseCollocatedPlans(self),
      CollapseCodegenStages(self.sessionState.conf),
      InsertCachedPlanHelper(self, topLevel),
      ReuseExchange(sessionState.conf))

    private[spark] def newQueryExecution(plan: LogicalPlan): QueryExecution = {
      new QueryExecution(self, plan) {

        addContextObject(SnappySession.ExecutionKey, () => newQueryExecution(plan))

        override protected def preparations: Seq[Rule[SparkPlan]] =
          queryPreparations(topLevel = true)
      }
    }

//    override def executePlan(plan: LogicalPlan): QueryExecution = {
//      clearExecutionData()
//      newQueryExecution(plan)
//    }

    private[spark] def prepareExecution(plan: SparkPlan): SparkPlan = {
      queryPreparations(topLevel = false).foldLeft(plan) {
        case (sp, rule) => rule.apply(sp)
      }
    }

    private[spark] def clearExecutionData(): Unit = {
      sessionState.conf.asInstanceOf[SnappyConf].refreshNumShufflePartitions()
      leaderPartitions.clear()
      clearContext()
    }

    def getTablePartitions(region: PartitionedRegion): Array[Partition] = {
      val leaderRegion = ColocationHelper.getLeaderRegion(region)
      leaderPartitions.computeIfAbsent(leaderRegion,
        new java.util.function.Function[PartitionedRegion, Array[Partition]] {
          override def apply(pr: PartitionedRegion): Array[Partition] = {
            val linkPartitionsToBuckets = hasLinkPartitionsToBuckets
            if (linkPartitionsToBuckets || preferPrimaries) {
              // also set the default shuffle partitions for this execution
              // to minimize exchange
              sessionState.conf.asInstanceOf[SnappyConf]
                .setExecutionShufflePartitions(region.getTotalNumberOfBuckets)
            }
            StoreUtils.getPartitionsPartitionedTable(self, pr,
              linkPartitionsToBuckets, preferPrimaries)
          }
        })
    }

    def getTablePartitions(region: CacheDistributionAdvisee): Array[Partition] =
      StoreUtils.getPartitionsReplicatedTable(self, region)
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
          "org.apache.spark.sql.internal.SnappyAQPSessionStateBuilder"))
      } catch {
        case NonFatal(e) =>
          // Let the user know if it failed to load AQP classes.
          logWarning(s"Failed to load AQP classes in Enterprise edition: $e")
          None
      }
    } else None
  }

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
  private def planExecution(df: DataFrame, session: SnappySession, sqlText: String,
    executedPlan: SparkPlan, allLiterals: Array[LiteralValue])
    (f: => RDD[InternalRow]): (Long, Long, RDD[InternalRow]) = {
    // Right now the CachedDataFrame is not getting used across SnappySessions
    val executionId = CachedDataFrame.nextExecutionIdMethod.
      invoke(SQLExecution).asInstanceOf[Long]
    session.sparkContext.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, executionId.toString)
    val start = System.currentTimeMillis()

    val rdd = try {
      session.sparkContext.listenerBus.post(SparkListenerSQLPlanExecutionStart(
        executionId, CachedDataFrame.queryStringShortForm(sqlText),
        sqlText, df.queryExecution.toString,
        CachedDataFrame.queryPlanInfo(executedPlan, allLiterals),
        start))
      f
    } finally {
      session.sparkContext.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, null)
    }
    val executionTime = System.currentTimeMillis() - start
    (executionTime, executionId, rdd)
  }
  private def evaluatePlan(df: DataFrame,
      session: SnappySession, sqlText: String,
      key: CachedKey = null): CachedDataFrame = {
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
        SnappyParserConsts.NOCACHING_KEY).isDefined
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
    // keep the broadcast join plans and their references as well
    /*
    val allBroadcastPlans = session.getContextObject[mutable.Map[SparkPlan,
        ArrayBuffer[Any]]](CachedPlanHelperExec.BROADCASTS_KEY).getOrElse(
      mutable.Map.empty[SparkPlan, ArrayBuffer[Any]])
    */
    val (cachedRDD, shuffleDeps, rddId, localCollect, executionId, executionTime) =
      executedPlan match {
      case _: ExecutedCommandExec | _: ExecutePlan =>
        // create new LogicalRDD plan so that plan does not get re-executed
        // (e.g. just toRdd is not enough since further operators like show will pass
        //   around the LogicalPlan and not the executedPlan; it works for plans using
        //   ExecutedCommandExec though because Spark layer has special check for it in
        //   Dataset hasSideEffects)
        // TODO: plan caching for point inserts/updates/deletes
        val newPlan = LogicalRDD(df.queryExecution.analyzed.output,
          df.queryExecution.toRdd)(session)
        val cdf = new CachedDataFrame(session, session.sessionState.executePlan(newPlan),
          df.exprEnc, sqlText, null, Array.empty, -1, false)
        throw new EntryExistsException("uncached plan", cdf) // don't cache
      case plan: CollectAggregateExec =>
        val (executionTime, executionId, childRDD) = planExecution(
        df, session, sqlText, plan, Array.empty[LiteralValue])({
            if (withFallback ne null) withFallback.execute(plan.child)
            else plan.childRDD
          })
        (null, findShuffleDependencies(childRDD).toArray, childRDD.id, true,
          Some(executionId), executionTime)
      case _: LocalTableScanExec =>
        (null, Array.empty[Int], -1, false, None, 0L) // cache plan but no cached RDD
      // case _ if allbroadcastplans.nonEmpty =>
        // (null, Array.empty[Int], -1, false) // cache plan but no cached RDD
      case _ =>
        val (executionTime, executionId, rdd) = planExecution(
          df, session, sqlText, executedPlan, Array.empty[LiteralValue])({
          executedPlan match {
            case plan: CollectLimitExec =>
              if (withFallback ne null) withFallback.execute(plan.child)
              else plan.child.execute()
            case _ => df.queryExecution.executedPlan.execute()
          }})
        SnappyContext.getClusterMode(session.sparkContext) match {
          case ThinClientConnectorMode(_, _) =>
          case _ =>
            // add profile listener for all regions that are using cached
            // partitions of their "leader" region
            if (rdd.getNumPartitions > 0) {
              session.leaderPartitions.keySet().forEach(
                new Consumer[PartitionedRegion] {
                  override def accept(pr: PartitionedRegion): Unit = {
                    addBucketProfileListener(pr)
                  }
                })
            }
        }
        (rdd, findShuffleDependencies(rdd).toArray, rdd.id, false,
          Some(executionId), executionTime)
    }

    logDebug(s"qe.executedPlan = ${df.queryExecution.executedPlan}")

    // keep references as well
    // filter unvisited literals. If the query is on a view for example the
    // modified tpch query no 15, It even picks those literal which we don't want.
    val allLiterals = session.getAllLiterals(key)

    // This part is the defensive coding for all those cases where Tokenization
    // support is not smart enough to deal with cases where the execution plan
    // is modified in such a way that we cannot track those constants which
    // need to be replaced in the subsequent execution
    if (key != null) {
      val nocaching = session.getContextObject[Boolean](
        SnappyParserConsts.NOCACHING_KEY).isDefined
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

    // collect the query hints
    val queryHints = session.synchronized {
      val numHints = session.queryHints.size()
      val hints = if (numHints == 0) java.util.Collections.emptyMap[String, String]()
      else {
        val m = ObjectObjectHashMap.withExpectedSize[String, String](numHints)
        m.putAll(session.queryHints)
        m
      }
      session.clearQueryData()
      hints
    }

    val cdf = new CachedDataFrame(df, sqlText, cachedRDD, shuffleDeps, rddId,
      localCollect, allLiterals, queryHints, executionTime, executionId)

    // if this has in-memory caching then don't cache since plan can change
    // dynamically after caching due to unpersist etc. Also do not cache
    // if snappy tables are no there
    if (executedPlan.find {
      case imse : InMemoryTableScanExec => true
      case dsc: DataSourceScanExec => !dsc.relation.isInstanceOf[DependentRelation]
      case _ => false
    }.isDefined) {
      throw new EntryExistsException("uncached plan", cdf)
    }
    cdf
  }

  private[this] lazy val planCache = {
    val loader = new CacheLoader[CachedKey, CachedDataFrame] {
      override def load(key: CachedKey): CachedDataFrame = {
        val session = key.session
        session.currentKey = key
        try {
          val df = session.executeSQL(key.sqlText)
          evaluatePlan(df, session, key.sqlText, key)
        } finally {
          session.currentKey = null
        }
      }
    }
    val env = SparkEnv.get
    val cacheSize = if (env ne null) {
      Property.PlanCacheSize.get(env.conf)
    } else Property.PlanCacheSize.defaultValue.get
    CacheBuilder.newBuilder().maximumSize(cacheSize).build(loader)
  }

  def getPlanCache: LoadingCache[CachedKey, CachedDataFrame] = planCache

  private[spark] def addBucketProfileListener(pr: PartitionedRegion): Unit = {
    val advisers = pr.getRegionAdvisor.getAllBucketAdvisorsHostedAndProxies
        .values().iterator()
    while (advisers.hasNext) {
      advisers.next().addProfileChangeListener(bucketProfileListener)
    }
  }

  final class CachedKey(
      val session: SnappySession, val lp: LogicalPlan,
      val sqlText: String, val hintHashcode: Int, val pls: Array[ParamLiteral]) {

    override lazy val hashCode: Int = {
      (session, lp, hintHashcode).hashCode()
    }

    override def equals(obj: Any): Boolean = {
      obj match {
        case x: CachedKey =>
          x.hintHashcode == hintHashcode && x.session == session && x.lp == lp
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
          // TODO_2.3_MERGE
//        case p: PredicateSubquery =>
//          p.copy(exprId = ExprId(0))
        case a: AttributeReference =>
          AttributeReference(a.name, a.dataType, a.nullable)(exprId = ExprId(0))
        case a: Alias =>
          Alias(a.child, a.name)(exprId = ExprId(0))
        case l@ListQuery(plan, _, _, _) =>
          l.copy(plan = plan.transformAllExpressions(normalizeExprIds),
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
    val lp = session.onlyParseSQL(sqlText)
    val currentWrappedConstants = session.getContextObject[mutable.ArrayBuffer[ParamLiteral]](
      SnappyParserConsts.WRAPPED_CONSTANTS_KEY) match {
      case Some(list) => list
      case None => mutable.ArrayBuffer.empty[ParamLiteral]
    }
    val key = CachedKey(session, lp, sqlText, currentWrappedConstants)
    try {
      var cachedDF = planCache.getUnchecked(key)
      if (!key.valid || !session.planCaching) {
        logDebug(s"Invalidating cached plan for sql: ${key.sqlText}")
        planCache.invalidate(key)
      }
      // if null has been returned, then evaluate
      if (cachedDF eq null) {
        val df = session.executeSQL(sqlText)
        cachedDF = evaluatePlan(df, session, sqlText)
        // default is enable caching
        if (session.planCaching) {
          planCache.put(key, cachedDF)
        }
      }
      handleCachedDataFrame(cachedDF, key, lp, currentWrappedConstants, session, sqlText)
    } catch {
      case e: UncheckedExecutionException => e.getCause match {
        case ee: EntryExistsException =>
          handleCachedDataFrame(ee.getOldValue.asInstanceOf[CachedDataFrame],
            key, lp, currentWrappedConstants, session, sqlText)
        case t => throw t
      }
      case ee: EntryExistsException =>
        handleCachedDataFrame(ee.getOldValue.asInstanceOf[CachedDataFrame],
          key, lp, currentWrappedConstants, session, sqlText)
    }
  }

  private def handleCachedDataFrame(cachedDF: CachedDataFrame, key: CachedKey,
      plan: LogicalPlan, currentWrappedConstants: ArrayBuffer[ParamLiteral],
      session: SnappySession, sqlText: String): CachedDataFrame = {
    cachedDF.waitForLastShuffleCleanup()
    cachedDF.reset()
    cachedDF.queryString = sqlText
    if (key.valid) {
      // logDebug(s"calling reprepare broadcast with new constants ${currentWrappedConstants}")
      // cachedDF.reprepareBroadcast(lp, currentWrappedConstants)
      logDebug(s"calling replace constants with new constants $currentWrappedConstants" +
          s" in Literal values = ${cachedDF.allLiterals.toSet}")
      CachedPlanHelperExec.replaceConstants(cachedDF.allLiterals, plan, currentWrappedConstants)
    }
    // set the query hints as would be set at the end of un-cached sql()
    session.synchronized {
      session.queryHints.clear()
      session.queryHints.putAll(cachedDF.queryHints)
    }
    cachedDF
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
      val item = iter.next()
      val session = item.asInstanceOf[CachedKey].session
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
        Utils.mapExecutors(sc, (_, _) => {
          CodeGeneration.clearAllCache()
          Iterator.empty
        }).count()
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
          // SparkSession.sqlListener.set(null)
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

class SnappyConf(@transient val session: SnappySession)
  extends SQLConf with Serializable {

  /** Pool to be used for the execution of queries from this session */
  @volatile private[this] var schedulerPool: String = Property.SchedulerPool.defaultValue.get

  /** If shuffle partitions is set by [[setExecutionShufflePartitions]]. */
  @volatile private[this] var executionShufflePartitions: Int = _

  /**
    * Records the number of shuffle partitions to be used determined on runtime
    * from available cores on the system. A value <= 0 indicates that it was set
    * explicitly by user and should not use a dynamic value.
    */
  @volatile private[this] var dynamicShufflePartitions: Int = _

  SQLConf.SHUFFLE_PARTITIONS.defaultValue match {
    case Some(d) if session != null && super.numShufflePartitions == d =>
      dynamicShufflePartitions = SnappyContext.totalCoreCount.get()
    case None if session != null =>
      dynamicShufflePartitions = SnappyContext.totalCoreCount.get()
    case _ =>
      executionShufflePartitions = -1
      dynamicShufflePartitions = -1
  }

  private def keyUpdateActions(key: String, value: Option[Any], doSet: Boolean): Unit = key match {
    // clear plan cache when some size related key that effects plans changes
    case SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key |
         Property.HashJoinSize.name |
         Property.HashAggregateSize.name |
         Property.ForceLinkPartitionsToBuckets.name => session.clearPlanCache()
    case SQLConf.SHUFFLE_PARTITIONS.key =>
      // stop dynamic determination of shuffle partitions
      if (doSet) {
        executionShufflePartitions = -1
        dynamicShufflePartitions = -1
      } else {
        dynamicShufflePartitions = SnappyContext.totalCoreCount.get()
      }
    case Property.SchedulerPool.name =>
      schedulerPool = value match {
        case None => Property.SchedulerPool.defaultValue.get
        case Some(pool) if session.sparkContext.getAllPools.exists(_.name == pool) =>
          pool.toString
        case Some(pool) => throw new IllegalArgumentException(s"Invalid Pool $pool")
      }

    case Property.PartitionPruning.name => value match {
      case Some(b) => session.partitionPruning = b.toString.toBoolean
      case None => session.partitionPruning = Property.PartitionPruning.defaultValue.get
    }

    case Property.PlanCaching.name =>
      value match {
        case Some(boolVal) =>
          if (boolVal.toString.toBoolean) {
            session.clearPlanCache()
          }
          session.planCaching = boolVal.toString.toBoolean
        case None => session.planCaching = Property.PlanCaching.defaultValue.get
      }

    case Property.PlanCachingAll.name =>
      value match {
        case Some(boolVal) =>
          val clearCache = !boolVal.toString.toBoolean
          if (clearCache) SnappySession.getPlanCache.asMap().clear()
        case None =>
      }

    case Property.Tokenize.name =>
      value match {
        case Some(boolVal) => SnappySession.tokenize = boolVal.toString.toBoolean
        case None => SnappySession.tokenize = Property.Tokenize.defaultValue.get
      }

    case SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key => value match {
      case Some(b) => session.wholeStageEnabled = b.toString.toBoolean
      case None => session.wholeStageEnabled = SQLConf.WHOLESTAGE_CODEGEN_ENABLED.defaultValue.get
    }
    case _ => // ignore others
  }

  private[sql] def refreshNumShufflePartitions(): Unit = synchronized {
    if (session ne null) {
      if (executionShufflePartitions != -1) {
        executionShufflePartitions = 0
      }
      if (dynamicShufflePartitions != -1) {
        dynamicShufflePartitions = SnappyContext.totalCoreCount.get()
      }
    }
  }

  private[sql] def setExecutionShufflePartitions(n: Int): Unit = synchronized {
    if (executionShufflePartitions != -1 && session != null) {
      executionShufflePartitions = math.max(n, executionShufflePartitions)
    }
  }

  override def numShufflePartitions: Int = {
    val partitions = this.executionShufflePartitions
    if (partitions > 0) partitions
    else {
      val partitions = this.dynamicShufflePartitions
      if (partitions > 0) partitions else super.numShufflePartitions
    }
  }

  def activeSchedulerPool: String = {
    schedulerPool
  }

  override def setConfString(key: String, value: String): Unit = {
    keyUpdateActions(key, Some(value), doSet = true)
    super.setConfString(key, value)
  }

  override def setConf[T](entry: ConfigEntry[T], value: T): Unit = {
    keyUpdateActions(entry.key, Some(value), doSet = true)
    require(entry != null, "entry cannot be null")
    require(value != null, s"value cannot be null for key: ${entry.key}")
    entry.defaultValue match {
      case Some(_) => super.setConf(entry, value)
      case None => super.setConf(entry.asInstanceOf[ConfigEntry[Option[T]]], Some(value))
    }
  }

  override def unsetConf(key: String): Unit = {
    keyUpdateActions(key, None, doSet = false)
    super.unsetConf(key)
  }

  override def unsetConf(entry: ConfigEntry[_]): Unit = {
    keyUpdateActions(entry.key, None, doSet = false)
    super.unsetConf(entry)
  }
}

class SQLConfigEntry private(private[sql] val entry: ConfigEntry[_]) {

  def key: String = entry.key

  def doc: String = entry.doc

  def isPublic: Boolean = entry.isPublic

  def defaultValue[T]: Option[T] = entry.defaultValue.asInstanceOf[Option[T]]

  def defaultValueString: String = entry.defaultValueString

  def valueConverter[T]: String => T =
    entry.asInstanceOf[ConfigEntry[T]].valueConverter

  def stringConverter[T]: T => String =
    entry.asInstanceOf[ConfigEntry[T]].stringConverter

  override def toString: String = entry.toString
}

object SQLConfigEntry {

  private def handleDefault[T](entry: TypedConfigBuilder[T],
                               defaultValue: Option[T]): SQLConfigEntry = defaultValue match {
    case Some(v) => new SQLConfigEntry(entry.createWithDefault(v))
    case None => new SQLConfigEntry(entry.createOptional)
  }

  def sparkConf[T: ClassTag](key: String, doc: String, defaultValue: Option[T],
                             isPublic: Boolean = true): SQLConfigEntry = {
    classTag[T] match {
      case ClassTag.Int => handleDefault[Int](SQLConf.buildConf(key)
        .doc(doc).intConf, defaultValue.asInstanceOf[Option[Int]])
      case ClassTag.Long => handleDefault[Long](SQLConf.buildConf(key)
        .doc(doc).longConf, defaultValue.asInstanceOf[Option[Long]])
      case ClassTag.Double => handleDefault[Double](SQLConf.buildConf(key)
        .doc(doc).doubleConf, defaultValue.asInstanceOf[Option[Double]])
      case ClassTag.Boolean => handleDefault[Boolean](SQLConf.buildConf(key)
        .doc(doc).booleanConf, defaultValue.asInstanceOf[Option[Boolean]])
      case c if c.runtimeClass == classOf[String] =>
        handleDefault[String](SQLConf.buildConf(key).doc(doc).stringConf,
          defaultValue.asInstanceOf[Option[String]])
      case c => throw new IllegalArgumentException(
        s"Unknown type of configuration key: $c")
    }
  }

  def apply[T: ClassTag](key: String, doc: String, defaultValue: Option[T],
                         isPublic: Boolean = true): SQLConfigEntry = {
    classTag[T] match {
      case ClassTag.Int => handleDefault[Int](SQLConf.buildConf(key)
        .doc(doc).intConf, defaultValue.asInstanceOf[Option[Int]])
      case ClassTag.Long => handleDefault[Long](SQLConf.buildConf(key)
        .doc(doc).longConf, defaultValue.asInstanceOf[Option[Long]])
      case ClassTag.Double => handleDefault[Double](SQLConf.buildConf(key)
        .doc(doc).doubleConf, defaultValue.asInstanceOf[Option[Double]])
      case ClassTag.Boolean => handleDefault[Boolean](SQLConf.buildConf(key)
        .doc(doc).booleanConf, defaultValue.asInstanceOf[Option[Boolean]])
      case c if c.runtimeClass == classOf[String] =>
        handleDefault[String](SQLConf.buildConf(key).doc(doc).stringConf,
          defaultValue.asInstanceOf[Option[String]])
      case c => throw new IllegalArgumentException(
        s"Unknown type of configuration key: $c")
    }
  }
}

trait AltName[T] {

  def name: String

  def altName: String

  def configEntry: SQLConfigEntry

  def defaultValue: Option[T] = configEntry.defaultValue[T]

  def getOption(conf: SparkConf): Option[String] = if (altName == null) {
    conf.getOption(name)
  } else {
    conf.getOption(name) match {
      case s: Some[String] => // check if altName also present and fail if so
        if (conf.contains(altName)) {
          throw new IllegalArgumentException(
            s"Both $name and $altName configured. Only one should be set.")
        } else s
      case None => conf.getOption(altName)
    }
  }

  private def get(conf: SparkConf, name: String,
                  defaultValue: String): T = {
    configEntry.entry.defaultValue match {
      case Some(_) => configEntry.valueConverter[T](
        conf.get(name, defaultValue))
      case None => configEntry.valueConverter[Option[T]](
        conf.get(name, defaultValue)).get
    }
  }

  def get(conf: SparkConf): T = if (altName == null) {
    get(conf, name, configEntry.defaultValueString)
  } else {
    if (conf.contains(name)) {
      if (!conf.contains(altName)) get(conf, name, configEntry.defaultValueString)
      else {
        throw new IllegalArgumentException(
          s"Both $name and $altName configured. Only one should be set.")
      }
    } else {
      get(conf, altName, configEntry.defaultValueString)
    }
  }

  def get(properties: Properties): T = {
    val propertyValue = getProperty(properties)
    if (propertyValue ne null) configEntry.valueConverter[T](propertyValue)
    else defaultValue.get
  }

  def getProperty(properties: Properties): String = if (altName == null) {
    properties.getProperty(name)
  } else {
    val v = properties.getProperty(name)
    if (v != null) {
      // check if altName also present and fail if so
      if (properties.getProperty(altName) != null) {
        throw new IllegalArgumentException(
          s"Both $name and $altName specified. Only one should be set.")
      }
      v
    } else properties.getProperty(altName)
  }

  def unapply(key: String): Boolean = name.equals(key) ||
    (altName != null && altName.equals(key))
}

trait SQLAltName[T] extends AltName[T] {

  private def get(conf: SQLConf, entry: SQLConfigEntry): T = {
    entry.defaultValue match {
      case Some(_) => conf.getConf(entry.entry.asInstanceOf[ConfigEntry[T]])
      case None => conf.getConf(entry.entry.asInstanceOf[ConfigEntry[Option[T]]]).get
    }
  }

  private def get(conf: SQLConf, name: String,
                  defaultValue: String): T = {
    configEntry.entry.defaultValue match {
      case Some(_) => configEntry.valueConverter[T](
        conf.getConfString(name, defaultValue))
      case None => configEntry.valueConverter[Option[T]](
        conf.getConfString(name, defaultValue)).get
    }
  }

  def get(conf: SQLConf): T = if (altName == null) {
    get(conf, configEntry)
  } else {
    if (conf.contains(name)) {
      if (!conf.contains(altName)) get(conf, configEntry)
      else {
        throw new IllegalArgumentException(
          s"Both $name and $altName configured. Only one should be set.")
      }
    } else {
      get(conf, altName, configEntry.defaultValueString)
    }
  }

  def getOption(conf: SQLConf): Option[T] = if (altName == null) {
    if (conf.contains(name)) Some(get(conf, name, "<undefined>"))
    else defaultValue
  } else {
    if (conf.contains(name)) {
      if (!conf.contains(altName)) Some(get(conf, name, ""))
      else {
        throw new IllegalArgumentException(
          s"Both $name and $altName configured. Only one should be set.")
      }
    } else if (conf.contains(altName)) {
      Some(get(conf, altName, ""))
    } else defaultValue
  }

  def set(conf: SQLConf, value: T, useAltName: Boolean = false): Unit = {
    if (useAltName) {
      conf.setConfString(altName, configEntry.stringConverter(value))
    } else {
      conf.setConf[T](configEntry.entry.asInstanceOf[ConfigEntry[T]], value)
    }
  }

  def remove(conf: SQLConf, useAltName: Boolean = false): Unit = {
    conf.unsetConf(if (useAltName) altName else name)
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
