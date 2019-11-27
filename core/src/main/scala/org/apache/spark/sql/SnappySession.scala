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

import java.sql.{Connection, SQLException, SQLWarning}
import java.util.Calendar
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Calendar, Properties}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Future
import scala.language.implicitConversions
import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.util.control.NonFatal

import com.gemstone.gemfire.internal.GemFireVersion
import com.gemstone.gemfire.internal.cache.PartitionedRegion.RegionLock
import com.gemstone.gemfire.internal.cache.{GemFireCacheImpl, PartitionedRegion}
import com.gemstone.gemfire.internal.shared.{ClientResolverUtils, FinalizeHolder, FinalizeObject}
import com.google.common.cache.{Cache, CacheBuilder}
import com.pivotal.gemfirexd.internal.GemFireXDVersion
import com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet
import com.pivotal.gemfirexd.internal.iapi.types.TypeId
import com.pivotal.gemfirexd.internal.iapi.{types => stypes}
import com.pivotal.gemfirexd.internal.shared.common.{SharedUtils, StoredFormatIds}
import io.snappydata.sql.catalog.{CatalogObjectType, SnappyExternalCatalog}
import io.snappydata.{Constant, Property, SnappyTableStatsProviderService}
import org.eclipse.collections.impl.map.mutable.UnifiedMap

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.jdbc.{ConnectionConf, ConnectionUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.{Analyzer, NoSuchTableException, UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, AttributeReference, Descending, Exists, ExprId, Expression, GenericRow, ListQuery, ParamLiteral, PredicateSubquery, ScalarSubquery, SortDirection, TokenLiteral}
import org.apache.spark.sql.catalyst.plans.logical.{Command, Filter, LogicalPlan, Union}
import org.apache.spark.sql.catalyst.{DefinedByConstructorParams, InternalRow, ScalaReflection, TableIdentifier}
import org.apache.spark.sql.collection.{Utils, WrappedInternalRow}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.CollectAggregateExec
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.execution.columnar.impl.{ColumnFormatRelation, IndexColumnFormatRelation}
import org.apache.spark.sql.execution.columnar.{ExternalStoreUtils, InMemoryTableScanExec}
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, ExecutedCommandExec, ShowCreateTableCommand, UncacheTableCommand}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.execution.datasources.{CreateTable, DataSource, LogicalRelation}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLPlanExecutionEnd, SparkListenerSQLPlanExecutionStart}
import org.apache.spark.sql.hive.{HiveClientUtil, SnappySessionState}
import org.apache.spark.sql.internal.StaticSQLConf.SCHEMA_STRING_LENGTH_THRESHOLD
import org.apache.spark.sql.internal.{BypassRowLevelSecurity, MarkerForCreateTableAsSelect, SnappySessionCatalog, SnappySharedState, StaticSQLConf}
import org.apache.spark.sql.row.{JDBCMutableRelation, SnappyStoreDialect}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{Logging, ShuffleDependency, SparkContext, SparkEnv}


class SnappySession(_sc: SparkContext) extends SparkSession(_sc) {

  self =>

  // initialize SnappyStoreDialect so that it gets registered

  SnappyStoreDialect.init()

  /* ----------------------- *
   |  Session-related state  |
   * ----------------------- */

  private[spark] val id = SnappySession.newId()

  private[this] val tempCacheIndex = new AtomicInteger(0)

  new FinalizeSession(this)

  /**
   * State shared across sessions, including the [[SparkContext]], cached data, listener,
   * and a catalog that interacts with external systems.
   */
  @transient
  override lazy val sharedState: SnappySharedState = SnappyContext.sharedState(sparkContext)

  /**
   * State isolated across sessions, including SQL configurations, temporary tables, registered
   * functions, and everything else that accepts a [[org.apache.spark.sql.internal.SQLConf]].
   */
  @transient
  override lazy val sessionState: SnappySessionState = {
    SnappySession.aqpSessionStateClass match {
      case Some(aqpClass) => aqpClass.getConstructor(classOf[SnappySession]).
          newInstance(self).asInstanceOf[SnappySessionState]
      case None => new SnappySessionState(self)
    }
  }

  def sessionCatalog: SnappySessionCatalog = sessionState.catalog

  def externalCatalog: SnappyExternalCatalog = sessionState.catalog.externalCatalog

  def snappyParser: SnappyParser = sessionState.sqlParser.sqlParser

  private[spark] def snappyContextFunctions = sessionState.contextFunctions

  SnappyContext.initGlobalSnappyContext(sparkContext, this)
  snappyContextFunctions.registerSnappyFunctions(this)

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
  override def newSession(): SnappySession = new SnappySession(sparkContext)

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

  private[sql] def overrideConfs: Map[String, String] = Map.empty

  override def sql(sqlText: String): DataFrame = {
    try {
      sqInternal(sqlText)
    } catch {
      // fallback to uncached flow for streaming queries
      case ae: AnalysisException
        if ae.message.contains(
          "Queries with streaming sources must be executed with writeStream.start()"
        ) => sqlUncached(sqlText)
    }
  }

   private[sql] def sqInternal(sqlText: String): CachedDataFrame = {
    snappyContextFunctions.sql(SnappySession.sqlPlan(this, sqlText))
  }

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

  final def prepareSQL(sqlText: String, skipPromote: Boolean = false): LogicalPlan = {
    val logical = sessionState.sqlParser.parsePlan(sqlText, clearExecutionData = true)
    SparkSession.setActiveSession(this)
    val ap: Analyzer = sessionState.analyzer
    // logInfo(s"KN: Batches ${ap.batches.filter(
    //  _.name.equalsIgnoreCase("Resolution")).mkString("_")}")
    ap.execute(logical)
  }

  private[sql] final def executePlan(plan: LogicalPlan, retryCnt: Int = 0): QueryExecution = {
    try {
      val execution = sessionState.executePlan(plan)
      execution.assertAnalyzed()
      execution
    } catch {
      case e: AnalysisException =>
        val unresolvedNodes = plan.expressions.filter(
          x => x.isInstanceOf[UnresolvedStar] | x.isInstanceOf[UnresolvedAttribute])
        if (e.getMessage().contains("cannot resolve") && unresolvedNodes.nonEmpty) {
          reAnalyzeForUnresolvedAttribute(plan, e, retryCnt) match {
            case Some(p) => return executePlan(p, retryCnt + 1)
            case None => //
          }
        }
        // in case of connector mode, exception can be thrown if
        // table form is changed (altered) and we have old table
        // object in SnappyExternalCatalog cache
        SnappyContext.getClusterMode(sparkContext) match {
          case ThinClientConnectorMode(_, _) =>
            var hasCommand = false
            val relations = plan.collect {
              case _: Command => hasCommand = true; null
              case u: UnresolvedRelation =>
                val tableIdent = sessionCatalog.resolveTableIdentifier(u.tableIdentifier)
                tableIdent.database.get -> tableIdent.table
            }
            if (hasCommand) externalCatalog.invalidateAll()
            else if (relations.nonEmpty) {
              relations.foreach(externalCatalog.invalidate)
            }
            throw e
          case _ =>
            throw e
        }
    }
  }

  // Hack to fix SNAP-2440 ( TODO: Will return after 1.1.0 for a better fix )
  private def reAnalyzeForUnresolvedAttribute(
    originalPlan: LogicalPlan, e: AnalysisException,
    retryCount: Int): Option[LogicalPlan] = {

    if (!e.getMessage().contains("cannot resolve")) return None
    val unresolvedNodes = originalPlan.expressions.filter(
      x => x.isInstanceOf[UnresolvedStar] | x.isInstanceOf[UnresolvedAttribute])
    if (retryCount > unresolvedNodes.size) return None

    val errMsg = e.getMessage().split('\n').map(_.trim.filter(_ >= ' ')).mkString
    val newPlan = originalPlan transformExpressions {
      case us@UnresolvedStar(option) if option.isDefined =>
        val targetString = option.get.mkString(".")
        var matched = false
        errMsg match {
          case SnappySession.unresolvedStarRegex(_, schema, table, _) =>
            if (sessionCatalog.tableExists(tableIdentifier(s"$schema.$table"))) {
              val qname = s"$schema.$table"
              if (qname.equalsIgnoreCase(targetString)) {
                matched = true
              }
            }
          case _ => matched = false
        }
        if (matched) UnresolvedStar(None) else us

      case ua@UnresolvedAttribute(nameparts) =>
        val targetString = nameparts.mkString(".")
        var uqc = ""
        var matched = false
        errMsg match {
          case SnappySession.unresolvedColRegex(_, schema, table, col, _) =>
            if (sessionCatalog.tableExists(tableIdentifier(s"$schema.$table"))) {
              val qname = s"$schema.$table.$col"
              if (qname.equalsIgnoreCase(targetString)) matched = true
              uqc = col
            }
          case _ => matched = false
        }
        if (matched) UnresolvedAttribute(uqc) else ua
    }
    if (!newPlan.equals(originalPlan)) Some(newPlan) else None
  }

  @transient
  private[sql] val queryHints = new ConcurrentHashMap[String, String](4, 0.7f, 1)

  @transient
  private val contextObjects = new ConcurrentHashMap[Any, Any](16, 0.7f, 1)

  @transient
  private[sql] var currentKey: CachedKey = _

  @transient
  private[sql] var planCaching: Boolean = Property.PlanCaching.get(sessionState.conf)

  @transient
  private[sql] var tokenize: Boolean = Property.Tokenize.get(sessionState.conf)

  @transient
  private[sql] var partitionPruning: Boolean = Property.PartitionPruning.get(sessionState.conf)

  @transient
  private[sql] var disableHashJoin: Boolean = Property.DisableHashJoin.get(sessionState.conf)

  @transient
  private[sql] var enableHiveSupport: Boolean =
    isHiveSupportEnabled(sessionState.conf.getConf(StaticSQLConf.CATALOG_IMPLEMENTATION))

  @transient
  private var sqlWarnings: SQLWarning = _

  private[sql] var hiveInitializing: Boolean = _

  private[sql] def isHiveSupportEnabled(v: String): Boolean = Utils.toLowerCase(v) match {
    case "hive" => true
    case "in-memory" => false
    case _ => throw new IllegalArgumentException(
      s"Unexpected value '$v' for ${StaticSQLConf.CATALOG_IMPLEMENTATION.key}. " +
          "Allowed values are: in-memory and hive")
  }

  def getPreviousQueryHints: java.util.Map[String, String] =
    java.util.Collections.unmodifiableMap(queryHints)

  def getWarnings: SQLWarning = sqlWarnings

  private[sql] def addWarning(warning: SQLWarning): Unit = {
    val warnings = sqlWarnings
    if (warnings eq null) sqlWarnings = warning
    else warnings.setNextWarning(warning)
  }

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
  private[sql] def removeContextObject(key: Any): Any = {
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

  def preferPrimaries: Boolean =
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

  private[sql] def cachePutInto(doCache: Boolean, updateSubQuery: LogicalPlan,
      table: String): Option[LogicalPlan] = {
    // first acquire the global lock for putInto
    val (schemaName: String, _) =
      JdbcExtendedUtils.getTableWithSchema(table, conn = null, Some(sqlContext.sparkSession))
    val lockOption = if (Property.SerializeWrites.get(sessionState.conf)) {
      grabLock(table, schemaName, defaultConnectionProps)
    } else None

    var newUpdateSubQuery: Option[LogicalPlan] = None
    try {
      val cachedTable = if (doCache) {
        val tableName = s"snappyDataInternalTempPutIntoCache${tempCacheIndex.incrementAndGet()}"
        val tableIdent = new TableIdentifier(tableName)
        val cacheCommandString = if (currentKey ne null) s"CACHE FOR (${currentKey.sqlText})"
        else s"CACHE FOR (PUT INTO $table <plan>)"
        // use cache table command to display full plan
        val count = SnappyCacheTableCommand(tableIdent, cacheCommandString, Some(updateSubQuery),
          isLazy = false).run(this).head.getLong(0)
        if (count > 0) {
          newUpdateSubQuery = Some(this.table(tableIdent).logicalPlan)
          Some(tableIdent)
        } else {
          dropPutIntoCacheTable(tableIdent)
          None
        }
      } else {
        // assume that there are updates
        newUpdateSubQuery = Some(updateSubQuery)
        None
      }
      addContextObject(SnappySession.CACHED_PUTINTO_LOGICAL_PLAN, cachedTable)
      newUpdateSubQuery
    } finally {
      lockOption match {
        case Some(lock) => {
          logDebug(s"Adding the lock object $lock to the context")
          addContextObject(SnappySession.PUTINTO_LOCK, lock)
        }
        case None => // do nothing
      }
    }
  }

  private def dropPutIntoCacheTable(tableIdent: TableIdentifier): Unit = {
    UncacheTableCommand(tableIdent, ifExists = false).run(this)
    dropTable(tableIdent, ifExists = false, isView = false)
  }

  private[sql] def clearPutInto(): Unit = {
    contextObjects.remove(SnappySession.PUTINTO_LOCK) match {
      case null =>
      case lock => releaseLock(lock)
    }
    contextObjects.remove(SnappySession.CACHED_PUTINTO_LOGICAL_PLAN) match {
      case null =>
      case cachedTable: Option[_] =>
        if (cachedTable.isDefined) {
          dropPutIntoCacheTable(cachedTable.get.asInstanceOf[TableIdentifier])
        }
    }
  }

  private[sql] def clearWriteLockOnTable(): Unit = {
    contextObjects.remove(SnappySession.BULKWRITE_LOCK) match {
      case null =>
      case lock => releaseLock(lock)
    }
  }

  private[sql] def grabLock(table: String, schemaName: String,
      connProperties: ConnectionProperties): Option[Any] = {
    SnappyContext.getClusterMode(sparkContext) match {
      case _: ThinClientConnectorMode =>
        if (!sparkContext.isLocal) {
          SnappyContext.resourceLock.synchronized {
            while (!SnappyContext.executorAssigned && sparkContext.getExecutorIds().isEmpty) {
              if (!SnappyContext.executorAssigned) {
                try {
                  SnappyContext.resourceLock.wait(100)
                }
                catch {
                  case _: InterruptedException =>
                    logWarning("Interrupted while waiting for executor.")
                }
              }
              // Don't expect this case usually unless lots of
              // applications are submitted in parallel
              logDebug(s"grabLock waiting for resources to be " +
                s"allocated ${SnappyContext.executorAssigned}")
            }
          }
        }
        val conn = ConnectionPool.getPoolConnection(table, connProperties.dialect,
          connProperties.poolProps, connProperties.connProps, connProperties.hikariCP)
        var locked = false
        var retrycount = 0
        do {
          try {
            logDebug(s" Going to take lock on server for table $table," +
                s" current Thread ${Thread.currentThread().getId} and " +
              s"app ${sqlContext.sparkContext.appName}")

            val ps = conn.prepareCall(s"VALUES sys.ACQUIRE_REGION_LOCK(?,?)")
            ps.setString(1, SnappySession.WRITE_LOCK_PREFIX + table)
            ps.setInt(2, Property.SerializedWriteLockTimeOut.get(sessionState.conf) * 1000)
            val rs = ps.executeQuery()
            rs.next()
            locked = rs.getBoolean(1)
            ps.close()
            logDebug(s"Took lock on server. for string " +
              s"${SnappySession.WRITE_LOCK_PREFIX + table} and " +
              s"app ${sqlContext.sparkContext.appName}")
          }
          catch {
            case sqle: SQLException => {
              logDebug("Got exception while taking lock", sqle)
              if (sqle.getMessage.contains("Couldn't acquire lock")) {
                throw sqle
              } else {
                if (retrycount == 2) {
                  throw sqle
                }
              }
            }
            case e: Throwable => {
              logDebug("Got exception while taking lock", e)
              if (retrycount == 2) {
                throw e
              }
            }
          }
          finally {
            retrycount = retrycount + 1
            // conn.close()
          }
        } while (!locked)
        Some(conn, new TableIdentifier(table, Some(schemaName)))
      case _ =>
        logDebug(s"Taking lock in " +
            s" ${Thread.currentThread().getId} and " +
          s"app ${sqlContext.sparkContext.appName}")
        val regionLock = PartitionedRegion.getRegionLock(SnappySession.WRITE_LOCK_PREFIX + table,
          GemFireCacheImpl.getExisting)
        regionLock.lock(Property.SerializedWriteLockTimeOut.get(sessionState.conf) * 1000)
        Some(regionLock)
    }
  }

  private[sql] def releaseLock(lock: Any): Unit = {
    logInfo(s"Releasing the lock : $lock")
    lock match {
      case lock: RegionLock =>
        if (lock != null) {
          logInfo(s"Going to unlock the lock object bulkOp $lock and " +
            s"app ${sqlContext.sparkContext.appName}")
          lock.asInstanceOf[PartitionedRegion.RegionLock].unlock()
        }
      case (conn: Connection, id: TableIdentifier) =>
        var unlocked = false
        try {
          logDebug(s"Going to unlock the lock on the server. ${id.table} for " +
            s"app ${sqlContext.sparkContext.appName}")
          val ps = conn.prepareStatement(s"VALUES sys.RELEASE_REGION_LOCK(?)")
          ps.setString(1, SnappySession.WRITE_LOCK_PREFIX + id.table)
          val rs = ps.executeQuery()
          rs.next()
          unlocked = rs.getBoolean(1)
          ps.close()
        } catch {
          case t: Throwable => {
            logWarning(s"Caught exception while unlocking the $lock", t)
            throw t
          }
        }
        finally {
          conn.close()
        }
    }
  }

  private[sql] def clearContext(): Unit = synchronized {
    clearPutInto()
    clearWriteLockOnTable()
    contextObjects.clear()
    planCaching = Property.PlanCaching.get(sessionState.conf)
    sqlWarnings = null
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
    externalCatalog.close()
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
      transformer: Option[RDD[T] => RDD[Row]])(implicit v: TypeTag[T]): Unit = {
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

  def tableIdentifier(table: String): TableIdentifier = {
    // hive meta-store is case-insensitive so always use upper case names for object names
    val fullName = sessionCatalog.formatTableName(table)
    val dotIndex = fullName.indexOf('.')
    if (dotIndex > 0) {
      new TableIdentifier(fullName.substring(dotIndex + 1),
        Some(fullName.substring(0, dotIndex)))
    } else new TableIdentifier(fullName, None)
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
    val tableIdent = tableIdentifier(table)
    if (!sessionCatalog.isTemporaryTable(tableIdent)) {
      throw new AnalysisException(s"Schema specified for temporary table '$tableIdent'")
    }
    val plan = sessionCatalog.lookupRelation(tableIdent)
    // cache the new DataFrame
    df.persist(storageLevel)
    // trigger an Action to materialize 'cached' batch
    if (df.count() > 0) {
      // create a union of the two plans and store that in catalog
      val union = Union(plan, df.logicalPlan)
      if (sessionCatalog.isLocalTemporaryView(tableIdent)) {
        sessionCatalog.createTempView(table, union, overrideIfExists = true)
      } else {
        sessionCatalog.createGlobalTempView(table, union, overrideIfExists = true)
      }
    }
  }

  /**
   * Empties the contents of the table without deleting the catalog entry.
   *
   * @param table    full table name to be truncated
   * @param ifExists attempt truncate only if the table exists
   */
  def truncateTable(table: String, ifExists: Boolean = false): Unit = {
    sessionState.executePlan(TruncateManagedTableCommand(ifExists, tableIdentifier(table))).toRdd
  }

  override def createDataset[T: Encoder](data: RDD[T]): Dataset[T] = {
    val encoder = encoderFor[T]
    val output = encoder.schema.toAttributes
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

  override def internalCreateDataFrame(catalystRows: RDD[InternalRow],
      schema: StructType): DataFrame = super.internalCreateDataFrame(catalystRows, schema)

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
    createTableInternal(tableIdentifier(tableName), SnappyContext.SAMPLE_SOURCE,
      userSpecifiedSchema = None, schemaDDL = None,
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      addBaseTableOption(baseTable, samplingOptions), isBuiltIn = true)
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
    createTableInternal(tableIdentifier(tableName), SnappyContext.SAMPLE_SOURCE,
      Some(JdbcExtendedUtils.normalizeSchema(schema)), schemaDDL = None,
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      addBaseTableOption(baseTable, samplingOptions), isBuiltIn = true)
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
    createTableInternal(tableIdentifier(topKName), SnappyContext.TOPK_SOURCE,
      Some(JdbcExtendedUtils.normalizeSchema(inputDataSchema)), schemaDDL = None,
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      addBaseTableOption(baseTable, topkOptions) +
          ("key" -> keyColumnName), isBuiltIn = true)
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
    createTableInternal(tableIdentifier(topKName), SnappyContext.TOPK_SOURCE,
      userSpecifiedSchema = None, schemaDDL = None,
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      addBaseTableOption(baseTable, topkOptions) +
          ("key" -> keyColumnName), isBuiltIn = true)
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
   * Creates a SnappyData managed table. Any of the table types
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
   * @param provider      Provider name such as 'COLUMN', 'ROW' etc.
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
    createTableInternal(tableIdentifier(tableName), provider, userSpecifiedSchema = None,
      schemaDDL = None, if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      options, isBuiltIn = true)
  }

  /**
   * Create an external table from the given path based on a data source, a schema and
   * a set of options. For inbuilt relation providers like row or column tables, use createTable.
   *
   * @param tableName     Name of the table
   * @param provider      Provider name such as 'CSV', 'PARQUET' etc.
   * @param options       Properties for table creation
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   * @return DataFrame for the table
   */
  def createExternalTable(
      tableName: String,
      provider: String,
      options: Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createTableInternal(tableIdentifier(tableName), provider, userSpecifiedSchema = None,
      schemaDDL = None, if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      options, isBuiltIn = false)
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
   * @param provider      Provider name such as 'COLUMN', 'ROW' etc.
   * @param options       Properties for table creation
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   * @return DataFrame for the table
   */
  def createTable(
      tableName: String,
      provider: String,
      options: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createTable(tableName, provider, options.asScala.toMap, allowExisting)
  }

  /**
   * Create an external table from the given path based on a data source, a schema and
   * a set of options. For inbuilt relation providers like row or column tables, use createTable.
   *
   * @param tableName     Name of the table
   * @param provider      Provider name such as 'CSV', 'PARQUET' etc.
   * @param options       Properties for table creation
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   * @return DataFrame for the table
   */
  def createExternalTable(
      tableName: String,
      provider: String,
      options: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createExternalTable(tableName, provider, options.asScala.toMap, allowExisting)
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
   * @param provider      Provider name such as 'COLUMN', 'ROW' etc.
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
    createTableInternal(tableIdentifier(tableName), provider,
      Some(JdbcExtendedUtils.normalizeSchema(schema)), schemaDDL = None,
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists, options, isBuiltIn = true)
  }

  /**
   * Create an external table from the given path based on a data source, a schema and
   * a set of options. For inbuilt relation providers like row or column tables, use createTable.
   *
   * @param tableName     Name of the table
   * @param provider      Provider name such as 'CSV', 'PARQUET' etc.
   * @param schema        Table schema
   * @param options       Properties for table creation. See options list for different tables.
   *                      https://github
   *                      .com/SnappyDataInc/snappydata/blob/master/docs/rowAndColumnTables.md
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   * @return DataFrame for the table
   */
  def createExternalTable(
      tableName: String,
      provider: String,
      schema: StructType,
      options: Map[String, String],
      allowExisting: Boolean = false): DataFrame = {
    createTableInternal(tableIdentifier(tableName), provider, Some(schema), schemaDDL = None,
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists, options, isBuiltIn = false)
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
   * @param provider      Provider name such as 'COLUMN', 'ROW' etc.
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
      options: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createTable(tableName, provider, schema, options.asScala.toMap, allowExisting)
  }

  /**
   * Create an external table from the given path based on a data source, a schema and
   * a set of options. For inbuilt relation providers like row or column tables, use createTable.
   *
   * @param tableName     Name of the table
   * @param provider      Provider name such as 'CSV', 'PARQUET' etc.
   * @param schema        Table schema
   * @param options       Properties for table creation. See options list for different tables.
   *                      https://github
   *                      .com/SnappyDataInc/snappydata/blob/master/docs/rowAndColumnTables.md
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   * @return DataFrame for the table
   */
  def createExternalTable(
      tableName: String,
      provider: String,
      schema: StructType,
      options: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createExternalTable(tableName, provider, schema, options.asScala.toMap, allowExisting)
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
   * @param provider      Provider name such as 'COLUMN', 'ROW' etc.
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
    createTableInternal(tableIdentifier(tableName), provider, userSpecifiedSchema = None,
      Some(schemaStr), if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      options, isBuiltIn = true)
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
   * @param provider      Provider name such as 'COLUMN', 'ROW' etc.
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
      options: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createTable(tableName, provider, schemaDDL, options.asScala.toMap,
      allowExisting)
  }

  /**
   * Create a table with given name, provider, optional schema DDL string, optional schema.
   * and other options.
   */
  private[sql] def createTableInternal(
      tableIdent: TableIdentifier,
      provider: String,
      userSpecifiedSchema: Option[StructType],
      schemaDDL: Option[String],
      mode: SaveMode,
      options: Map[String, String],
      isBuiltIn: Boolean,
      partitionColumns: Array[String] = Utils.EMPTY_STRING_ARRAY,
      bucketSpec: Option[BucketSpec] = None,
      query: Option[LogicalPlan] = None): DataFrame = {
    val providerIsBuiltIn = SnappyContext.isBuiltInProvider(provider)
    if (providerIsBuiltIn) {
      if (!isBuiltIn) {
        throw new AnalysisException(s"CREATE EXTERNAL TABLE or createExternalTable API " +
            s"used for inbuilt provider '$provider'")
      }
      // for builtin tables, never use partitionSpec or bucketSpec since that has different
      // semantics and implies support for add/drop/recover partitions which is not possible
      if (partitionColumns.length > 0) {
        throw new AnalysisException(s"CREATE TABLE ... USING '$provider' does not support " +
            "PARTITIONED BY clause.")
      }
      if (bucketSpec.isDefined) {
        throw new AnalysisException(s"CREATE TABLE ... USING '$provider' does not support " +
            s"CLUSTERED BY clause. Use '${ExternalStoreUtils.PARTITION_BY}' as an option.")
      }
    }
    // check for permissions in the schema which should be done before the session catalog
    // createTable call since store table will be created by that time
    val resolvedIdentifier = sessionCatalog.resolveTableIdentifier(tableIdent)
    sessionCatalog.checkSchemaPermission(resolvedIdentifier.database.get, resolvedIdentifier.table,
      defaultUser = null, ignoreIfNotExists = true)

    val schema = userSpecifiedSchema match {
      case Some(s) =>
        if (query.isDefined) {
          throw new AnalysisException(
            "Schema may not be specified in a Create Table As Select (CTAS) statement")
        }
        s
      // CreateTable plan execution will resolve schema before adding to externalCatalog
      case None => new StructType()
    }
    var fullOptions = schemaDDL match {
      case None => options
      case Some(ddl) =>
        // check that the DataSource should implement ExternalSchemaRelationProvider
        if (!ExternalStoreUtils.isExternalSchemaRelationProvider(provider)) {
          throw new AnalysisException(s"Provider '$provider' should implement " +
              s"ExternalSchemaRelationProvider to use a custom schema string in CREATE TABLE")
        }
        JdbcExtendedUtils.addSplitProperty(ddl,
          SnappyExternalCatalog.SCHEMADDL_PROPERTY, options,
          sparkContext.conf.get(SCHEMA_STRING_LENGTH_THRESHOLD)).toMap
    }
    // add baseTable for colocated table
    val parameters = new CaseInsensitiveMutableHashMap[String](fullOptions)
    if (!parameters.contains(SnappyExternalCatalog.BASETABLE_PROPERTY)) {
      parameters.get(StoreUtils.COLOCATE_WITH) match {
        case None =>
        case Some(b) => fullOptions += SnappyExternalCatalog.BASETABLE_PROPERTY ->
            sessionCatalog.resolveExistingTable(b).unquotedString
      }
    }
    val orgSqlText = getContextObject[String]("orgSqlText") match {
      case Some(s) => s
      case None => {
        // case when create api was used - sql text cannot be captured in this case.
        userSpecifiedSchema match {
          case Some(schema) => s"create table ${resolvedIdentifier.table} (${getDDLSchema(schema)})"
          case None => ""
        }
      }
    }
    // if there is no path option for external DataSources, then mark as MANAGED except for JDBC
    val storage = DataSource.buildStorageFormatFromOptions(fullOptions)
    val tableType = if (!providerIsBuiltIn && storage.locationUri.isEmpty &&
        !Utils.toLowerCase(provider).contains("jdbc")) {
      CatalogTableType.MANAGED
    } else CatalogTableType.EXTERNAL
    val orgSqlTextParts = orgSqlText.grouped(3500).toSeq
    val sqlTextMap = mutable.Map(s"numPartsOrgSqlText_${System.currentTimeMillis()}"
        -> orgSqlTextParts.size.toString)
    orgSqlText.grouped(3500).toSeq.zipWithIndex.foreach{
      case(part, index) => sqlTextMap += (s"sqlTextpart.$index" -> s"$part")
    }

    val tableDesc = CatalogTable(
      identifier = resolvedIdentifier,
      tableType = tableType,
      storage = storage,
      schema = schema,
      provider = Some(provider),
      properties = sqlTextMap.toMap,
      partitionColumnNames = partitionColumns,
      bucketSpec = bucketSpec)
    val plan = CreateTable(tableDesc, mode, query.map(MarkerForCreateTableAsSelect))
    val tableAlreadyExisted = sessionState.catalog.tableExists(resolvedIdentifier)
    sessionState.executePlan(plan).toRdd
    val df = table(resolvedIdentifier)
    val relation = df.queryExecution.analyzed.collectFirst {
      case l: LogicalRelation => l.relation
    }
    snappyContextFunctions.postRelationCreation(relation, this)
    if (!tableAlreadyExisted) {
      updatePrimaryKeyDetails(resolvedIdentifier)
    }
    df
  }

  private def updatePrimaryKeyDetails(resolvedIdentifier: TableIdentifier): Unit = {
    val catalogTable = externalCatalog.getTable(resolvedIdentifier.database.get,
      resolvedIdentifier.table)
    val primaryKeys = getPrimaryKeys(resolvedIdentifier.database.get, resolvedIdentifier.table)
    if (primaryKeys.nonEmpty) {
      sessionCatalog.alterTable(catalogTable.copy(properties =
          catalogTable.properties + (s"primary_keys" -> primaryKeys.mkString(","))))
    }
  }

  /**
   * Queries sys.indexes table and returns csv string of primary column names of given table
   *
   * @param schema schema  that the table belongs to
   * @param table  table for which primary keys are to be found
   * @return primary keys of given table as csv string
   */
  def getPrimaryKeys(schema: String, table: String): Array[String] = {
    var primaryKeys = Array.empty[String]
    val conn = getConnection
    try {
      val sql = s"""SELECT COLUMNS_AND_ORDER FROM sys.indexes WHERE
                   |INDEXTYPE = 'PRIMARY KEY' AND schemaname = '${schema.toUpperCase}' AND
                   |tablename = '${table.toUpperCase}'""".stripMargin
      val ps = conn.prepareStatement(sql)
      val rs = ps.executeQuery()
      if (rs.next()) {
        val keys = rs.getString(1)
        if (keys != null && !keys.eq(""))
          // keys for table with c1, c2 as primary key looks like '+c1+c2'
          primaryKeys = keys.split("\\+").filter(!_.equals(""))
      }
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
    primaryKeys
  }

  def getDDLSchema(schema: StructType): String = {
    schema.fields.map(f => s"${f.name} ${getSQLType(f.dataType)}").mkString(", ")
  }

  def getSQLType(dataType: DataType): String = {
    dataType match {
      case IntegerType => "integer"
      case LongType => "long"
      case StringType => "string"
      case FloatType => "float"
      case DoubleType => "double"
      case t: DecimalType => s"decimal(${t.precision},${t.scale})"
      case DateType => "date"
      case TimestampType => "timestamp"
      case BooleanType => "boolean"
      case ByteType => "byte"
      case ShortType => "short"
      case BinaryType => "binary"
      case v: VarcharType => s"varchar(${v.length})"
      case a: ArrayType => s"array<${a.elementType}>"
      case m: MapType => s"map<${m.keyType}, ${m.valueType}>"
      case s: StructType => {
        val structFields = s.fields.map(f => s" ${f.name}:${f.dataType} ").mkString(",")
        s"struct<${structFields}>"
      }
    }
  }

  private[sql] def addBaseTableOption(baseTable: Option[String],
      options: Map[String, String]): Map[String, String] = baseTable match {
    case Some(t) => options + (SnappyExternalCatalog.BASETABLE_PROPERTY ->
        sessionCatalog.resolveExistingTable(t).unquotedString)
    case _ => options
  }

  /**
   * Drop a table created by a call to createTable or createExternalTable.
   *
   * @param tableName table to be dropped
   * @param ifExists  attempt drop only if the table exists
   */
  def dropTable(tableName: String, ifExists: Boolean = false): Unit =
    dropTable(tableIdentifier(tableName), ifExists, isView = false)

  /**
   * Drop a view.
   *
   * @param viewName name of the view to be dropped
   * @param ifExists attempt drop only if the view exists
   */
  def dropView(viewName: String, ifExists: Boolean = false): Unit =
    dropTable(tableIdentifier(viewName), ifExists, isView = true)

  /**
   * Drop a table created by a call to createTable or createExternalTable.
   *
   * @param tableIdent table to be dropped
   * @param ifExists   attempt drop only if the table exists
   */
  private[sql] def dropTable(tableIdent: TableIdentifier, ifExists: Boolean,
      isView: Boolean): Unit = {
    val plan = DropTableOrViewCommand(tableIdent, ifExists, isView, purge = false)
    sessionState.executePlan(plan).toRdd
  }

  /**
   * Drop a SnappyData Policy created by a call to [[createPolicy]].
   *
   * @param policyName Policy to be dropped
   * @param ifExists   attempt drop only if the Policy exists
   */
  def dropPolicy(policyName: String, ifExists: Boolean = false): Unit =
    dropPolicy(tableIdentifier(policyName), ifExists)

  /**
   * Drop a SnappyData Policy created by a call to [[createPolicy]].
   *
   * @param policyIdent Policy to be dropped
   * @param ifExists    attempt drop only if the Policy exists
   */
  private[sql] def dropPolicy(policyIdent: TableIdentifier, ifExists: Boolean): Unit = {
    try {
      dropTable(policyIdent, ifExists, isView = false)
    } catch {
      case _: NoSuchTableException if !ifExists =>
        throw new PolicyNotFoundException(policyIdent.database.getOrElse(getCurrentSchema),
          policyIdent.table)
    }
  }

  def alterTable(tableName: String, isAddColumn: Boolean, column: StructField,
      extensions: String): Unit = {
    val tableIdent = tableIdentifier(tableName)
    alterTable(tableIdent, isAddColumn, column, extensions)
  }

  private[sql] def alterTable(tableIdent: TableIdentifier, isAddColumn: Boolean,
      column: StructField, extensions: String): Unit = {
    if (sessionCatalog.isTemporaryTable(tableIdent)) {
      throw new AnalysisException("ALTER TABLE not supported for temporary tables")
    }
    val orgSqlText = getContextObject[String]("orgSqlText") match {
      case Some(str) => str
      case None => {
        if (isAddColumn) {
          s"alter table ${tableIdent.table} add column ${column.name}" +
              s" ${getSQLType(column.dataType)}"
        } else {
          s"alter table ${tableIdent.table} drop column ${column.name}"
        }
      }
    }
    sessionCatalog.resolveRelation(tableIdent) match {
      case LogicalRelation(ar: AlterableRelation, _, _) =>
        ar.alterTable(tableIdent, isAddColumn, column, extensions)
        val metadata = sessionCatalog.getTableMetadata(tableIdent)
        sessionCatalog.alterTable(metadata.copy(schema = ar.schema,
          properties = metadata.properties +
              (s"altTxt_${System.currentTimeMillis()}" -> orgSqlText)))
      case _ => throw new AnalysisException(
        s"ALTER TABLE ${tableIdent.unquotedString} supported only for row tables")
    }
  }

  private[sql] def alterTableToggleRLS(table: TableIdentifier, enableRls: Boolean): Unit = {
    val tableIdent = sessionCatalog.resolveTableIdentifier(table)
    val plan = sessionCatalog.resolveRelation(tableIdent)
    if (sessionCatalog.isTemporaryTable(tableIdent)) {
      throw new AnalysisException("ALTER TABLE enable/disable Row Level Security " +
          "not supported for temporary tables")
    }

    SnappyContext.getClusterMode(sparkContext) match {
      case ThinClientConnectorMode(_, _) =>
        throw new AnalysisException("ALTER TABLE enable/disable Row Level Security " +
            "not supported for smart connector mode")
      case _ =>
    }

    plan match {
      case LogicalRelation(rls: RowLevelSecurityRelation, _, _) =>
        rls.enableOrDisableRowLevelSecurity(tableIdent, enableRls)
        externalCatalog.invalidateCaches(tableIdent.database.get -> tableIdent.table :: Nil)
      case _ =>
        throw new AnalysisException("ALTER TABLE enable/disable Row Level Security " +
            s"not supported for ${tableIdent.unquotedString}")
    }
  }

  private[sql] def alterTableMisc(tableIdent: TableIdentifier, sql: String): Unit = {
    if (sessionCatalog.isTemporaryTable(tableIdent)) {
      throw new AnalysisException("ALTER TABLE not supported for temporary tables")
    }
    sessionCatalog.resolveRelation(tableIdent) match {
      case LogicalRelation(r: JDBCMutableRelation, _, _) =>
        r.executeUpdate(sql, JdbcExtendedUtils.toUpperCase(getCurrentSchema))

        // when "alter table add constraint" is fired, it directly fires the sql
        // as in on gemfire layer... to capture the sql string in catalog properties for ddls
        // export, we can we just add sql string to properties in CatalogTable
        // and update it into the metastore.

        val metadata = sessionCatalog.getTableMetadata(tableIdent)
        val primaryKeys = getPrimaryKeys(metadata.identifier.database.get, metadata.identifier.table)
        sessionCatalog.alterTable(metadata.copy(
          properties = metadata.properties +
              (s"altTxt_${System.currentTimeMillis()}" -> sql) +
              (s"primary_keys" -> primaryKeys.mkString(","))
        ))
      case _ => throw new AnalysisException(
        s"ALTER TABLE ${tableIdent.unquotedString} variant only supported for row tables")
    }
  }

  /**
   * Set current schema for the session.
   *
   * @param schema schema name which goes in the catalog
   */
  def setCurrentSchema(schema: String): Unit = setCurrentSchema(schema, createIfNotExists = false)

  /**
   * Set current schema for the session.
   *
   * @param schema            schema name which goes in the catalog
   * @param createIfNotExists create the schema if it does not exist
   */
  private[sql] def setCurrentSchema(schema: String, createIfNotExists: Boolean): Unit = {
    val schemaName = sessionCatalog.formatDatabaseName(schema)
    if (schemaName != getCurrentSchema) {
      if (createIfNotExists) {
        sessionCatalog.createSchema(schemaName, ignoreIfExists = true, createInStore = false)
      }
      sessionCatalog.setCurrentSchema(schemaName, force = true)
    }
  }

  def getCurrentSchema: String = sessionCatalog.getCurrentSchema

  /**
   * Create an index on a table.
   *
   * @param indexName    Index name which goes in the catalog
   * @param baseTable    Fully qualified name of table on which the index is created.
   * @param indexColumns Columns on which the index has to be created along with the
   *                     sorting direction.
   * @param sortOrders   Sorting direction for indexColumns. The direction of index will be
   *                     ascending if value is true and descending when value is false.
   *                     The values in this list must exactly match indexColumns list.
   *                     Direction can be specified as null in which case ascending is used.
   * @param options      Options for indexes. For e.g.
   *                     column table index - ("COLOCATE_WITH"->"CUSTOMER").
   *                     row table index - ("INDEX_TYPE"->"GLOBAL HASH") or ("INDEX_TYPE"->"UNIQUE")
   */
  def createIndex(indexName: String,
      baseTable: String,
      indexColumns: java.util.List[String],
      sortOrders: java.util.List[java.lang.Boolean],
      options: java.util.Map[String, String]): Unit = {

    val numIndexes = indexColumns.size()
    if (numIndexes != sortOrders.size()) {
      throw new IllegalArgumentException("Number of index columns should match the sorting orders")
    }
    val indexCols = for (i <- 0 until numIndexes) yield {
      val sortDirection = sortOrders.get(i) match {
        case null => None
        case java.lang.Boolean.TRUE => Some(Ascending)
        case java.lang.Boolean.FALSE => Some(Descending)
      }
      indexColumns.get(i) -> sortDirection
    }

    createIndex(indexName, baseTable, indexCols, options.asScala.toMap)
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
      indexColumns: Seq[(String, Option[SortDirection])],
      options: Map[String, String]): Unit = {
    createIndex(tableIdentifier(indexName), tableIdentifier(baseTable), indexColumns, options)
  }

  private[sql] def createPolicy(policyName: TableIdentifier, tableName: TableIdentifier,
      policyFor: String, applyTo: Seq[String], expandedPolicyApplyTo: Seq[String],
      currentUser: String, filterStr: String, filter: BypassRowLevelSecurity): Unit = {

    /*
    if (!SecurityUtils.allowPolicyOp(currentUser, tableName, this)) {
      throw new SQLException("Only Table Owner can create the policy", "01548", null)
    }
    */

    if (!policyFor.equalsIgnoreCase(SnappyParserConsts.SELECT.lower)) {
      throw new AnalysisException("Currently Policy only For Select is supported")
    }

    /*
    if (isTargetExternalRelation) {
      val targetAttributes = this.sessionState.catalog.lookupRelation(tableName).output
      def checkForValidFilter(filter: BypassRowLevelSecurity): Unit = {
        def checkExpression(expression: Expression): Unit = {
          expression match {
            case _: Attribute =>  // ok
            case _: Literal =>  // ok
            case _: TokenizedLiteral => // ok
            case br: BinaryComparison => {
              checkExpression(br.left)
              checkExpression(br.right)
            }
            case logicalOr(left, right) => {
              checkExpression(left)
              checkExpression(right)
            }
            case logicalAnd(left, right) => {
              checkExpression(left)
              checkExpression(right)
            }
            case logicalIn(value, list) => {
              checkExpression(value)
              list.foreach(checkExpression(_))
            }
            case _ => // for any other type of expression
              // it should not contain any attribute of target external relation
              expression.foreach(x => x match {
                case ne: NamedExpression => targetAttributes.find(_.exprId == ne.exprId).
                    foreach( _ => throw new AnalysisException("Filter for external " +
                        "relation cannot have functions " +
                        "or dependent subquery involving external table's attribute") )
              })

          }
        }
        checkExpression(filter.child.condition)

      }
      checkForValidFilter(filter)

    }
    */

    sessionCatalog.createPolicy(policyName, tableName, policyFor, applyTo, expandedPolicyApplyTo,
      currentUser, filterStr)
  }

  /**
   * Create an index on a table.
   */
  private[sql] def createIndex(indexIdent: TableIdentifier,
      tableIdent: TableIdentifier,
      indexColumns: Seq[(String, Option[SortDirection])],
      options: Map[String, String]): Unit = {

    if (indexIdent.database != tableIdent.database) {
      throw new AnalysisException(
        s"Index and table have different schemas " +
            s"specified ${indexIdent.database} and ${tableIdent.database}")
    }
    if (!sessionCatalog.tableExists(tableIdent)) {
      throw new AnalysisException(
        s"Could not find $tableIdent in catalog")
    }
    sessionCatalog.resolveRelation(tableIdent) match {
      case LogicalRelation(ir: IndexableRelation, _, _) =>
        ir.createIndex(indexIdent,
          tableIdent,
          indexColumns,
          options)

      case _ => throw new AnalysisException(
        s"$tableIdent is not an indexable table")
    }
  }

  private[sql] def getIndexTable(indexIdent: TableIdentifier): TableIdentifier = {
    val schema = indexIdent.database match {
      case None => Some(getCurrentSchema)
      case s => s
    }
    TableIdentifier(Constant.COLUMN_TABLE_INDEX_PREFIX + indexIdent.table, schema)
  }

  private def constructDropSQL(indexName: String,
      ifExists: Boolean): String = {
    val ifExistsClause = if (ifExists) " IF EXISTS" else ""
    s"DROP INDEX$ifExistsClause ${JdbcExtendedUtils.quotedName(indexName)}"
  }

  /**
   * Drops an index on a table
   *
   * @param indexName Index name which goes in catalog
   * @param ifExists  Drop if exists, else exit gracefully
   */
  def dropIndex(indexName: String, ifExists: Boolean): Unit = {
    dropIndex(tableIdentifier(indexName), ifExists)
  }

  /**
   * Drops an index on a table
   */
  private[sql] def dropIndex(indexName: TableIdentifier, ifExists: Boolean): Unit = {
    val indexIdent = getIndexTable(indexName)
    // Since the index does not exist in catalog, it may be a row table index.
    if (!sessionCatalog.tableExists(indexIdent)) {
      dropRowStoreIndex(sessionCatalog.resolveTableIdentifier(indexName).unquotedString, ifExists)
    } else {
      sessionCatalog.resolveRelation(indexIdent) match {
        case LogicalRelation(ir: IndexColumnFormatRelation, _, _) =>
          // Remove the index from the bse table props
          val baseTableIdent = tableIdentifier(ir.baseTable.get)
          sessionCatalog.resolveRelation(baseTableIdent) match {
            case LogicalRelation(cr: ColumnFormatRelation, _, _) =>
              cr.dropIndex(indexIdent, baseTableIdent, ifExists)
            case _ => throw new AnalysisException(
              s"No index ${indexName.unquotedString} on ${baseTableIdent.unquotedString}")
          }

        case _ => if (!ifExists) {
          throw new AnalysisException(s"No index found for ${indexName.unquotedString}")
        }
      }
    }
  }

  /**
   * creates a internal connection to snappydata and returns it
   * @return connection to snappydata
   */
  private def getConnection: Connection = {
    val connProperties = ExternalStoreUtils.validateAndGetAllProps(
      Some(this), ExternalStoreUtils.emptyCIMutableMap)
    val jdbcOptions = new JDBCOptions(connProperties.url, "",
      connProperties.connProps.asScala.toMap)
    JdbcUtils.createConnectionFactory(jdbcOptions)()
  }

  private def dropRowStoreIndex(indexName: String, ifExists: Boolean): Unit = {
    val conn = getConnection
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
   *
   * @param tableName table name for the insert operation
   * @param rows      list of rows to be inserted into the table
   * @return number of rows inserted
   */
  @DeveloperApi
  def insert(tableName: String, rows: Row*): Int = {
    sessionCatalog.resolveRelation(tableIdentifier(tableName)) match {
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
    sessionCatalog.resolveRelation(tableIdentifier(tableName)) match {
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
    sessionCatalog.resolveRelation(tableIdentifier(tableName)) match {
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
    sessionCatalog.resolveRelation(tableIdentifier(tableName)) match {
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
    sessionCatalog.resolveRelation(tableIdentifier(tableName)) match {
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
    sessionCatalog.resolveRelation(tableIdentifier(tableName)) match {
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
    sessionCatalog.resolveRelation(tableIdentifier(tableName)) match {
      case LogicalRelation(d: DeletableRelation, _, _) => d.delete(filterExpr)
      case _ => throw new AnalysisException(
        s"$tableName is not a deletable table")
    }
  }

  private def convertListToRow(row: java.util.ArrayList[_]): Row = {
    val rowAsArray: Array[Any] = row.asScala.toArray
    new GenericRow(rowAsArray)
  }

  private[sql] def defaultConnectionProps: ConnectionProperties =
    ExternalStoreUtils.validateAndGetAllProps(Some(this), ExternalStoreUtils.emptyCIMutableMap)

  private[sql] def defaultPooledConnection(name: String): java.sql.Connection =
    ConnectionUtil.getPooledConnection(name, new ConnectionConf(defaultConnectionProps))

  private[sql] def getPooledConnectionToServer(name: String): java.sql.Connection = {
    ConnectionUtil.getPooledConnection(name, new ConnectionConf(defaultConnectionProps))
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
    snappyContextFunctions.queryTopK(this, topKName, startTime, endTime, k)

  def queryApproxTSTopK(topK: String,
      startTime: Long, endTime: Long, k: Int): DataFrame =
    snappyContextFunctions.queryTopK(this, topK, startTime, endTime, k)

  def setPreparedQuery(preparePhase: Boolean, paramSet: Option[ParameterValueSet]): Unit =
    snappyParser.setPreparedQuery(preparePhase, paramSet)

  def setPreparedParamsTypeInfo(info: Array[Int]): Unit =
    snappyParser.setPrepareParamsTypesInfo(info)

  private[sql] def getParameterValue(
      questionMarkCounter: Int, pvs: Any, preparedParamsTypesInfo: Option[Array[Int]]) = {
    val parameterValueSet = pvs.asInstanceOf[ParameterValueSet]
    if (questionMarkCounter > parameterValueSet.getParameterCount) {
      assert(assertion = false, s"For Prepared Statement, Got more number of" +
          s" placeholders = $questionMarkCounter" +
          s" than given number of parameter" +
          s" constants = ${parameterValueSet.getParameterCount}")
    }
    val dvd = parameterValueSet.getParameter(questionMarkCounter - 1)
    var scalaTypeVal = SnappySession.getValue(dvd)
    val storeType = dvd.getTypeFormatId
    val (storePrecision, storeScale) = dvd match {
      case _: stypes.SQLDecimal =>
        // try to normalize parameter value into target column's scale/precision
        val index = (questionMarkCounter - 1) * 4 + 1
        // actual scale of the target column
        val scale = preparedParamsTypesInfo.map(a => a(index + 2)).getOrElse(-1)

        val decimalValue = new com.pivotal.gemfirexd.internal.iapi.types.SQLDecimal()
        val typeId = TypeId.getBuiltInTypeId(java.sql.Types.DECIMAL)
        val dtd = new com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor(
          typeId, DecimalType.MAX_PRECISION, scale, true, typeId.getMaximumMaximumWidth)
        decimalValue.normalize(dtd, dvd)
        scalaTypeVal = decimalValue.getBigDecimal
        (decimalValue.getDecimalValuePrecision, scale)

      case _ => (-1, -1)
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
  private[sql] val PUTINTO_LOCK = "putinto_lock"
  private[sql] val CACHED_PUTINTO_LOGICAL_PLAN = "cached_putinto_logical_plan"
  private[sql] val BULKWRITE_LOCK = "bulkwrite_lock"
  private[sql] val WRITE_LOCK_PREFIX = "BULKWRITE_"

  private val unresolvedStarRegex =
    """(cannot resolve ')(\w+).(\w+).*(' given input columns.*)""".r
  private val unresolvedColRegex =
    """(cannot resolve '`)(\w+).(\w+).(\w+)(.*given input columns.*)""".r

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
    case cg@CodegenSparkFallback(WholeStageCodegenExec(p), _) => (p, cg)
    case cg@CodegenSparkFallback(p, _) => (p, cg)
    case WholeStageCodegenExec(p) => (p, null)
    case _ => (plan, null)
  }

  private[sql] def setExecutionProperties(localProperties: Properties,
      executionIdStr: String, queryLongForm: String): Unit = {
    localProperties.setProperty(SQLExecution.EXECUTION_ID_KEY, executionIdStr)
    // trim query string to 10K to keep its UTF8 form always < 32K which is the limit
    // for DataOutput.writeUTF used during task serialization
    localProperties.setProperty(SparkContext.SPARK_JOB_DESCRIPTION,
      CachedDataFrame.queryStringShortForm(queryLongForm, 10240))
    localProperties.setProperty(SparkContext.SPARK_JOB_GROUP_ID, executionIdStr)
  }

  private[sql] def clearExecutionProperties(localProperties: Properties): Unit = {
    localProperties.remove(SparkContext.SPARK_JOB_GROUP_ID)
    localProperties.remove(SparkContext.SPARK_JOB_DESCRIPTION)
    localProperties.remove(SQLExecution.EXECUTION_ID_KEY)
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
  private def planExecution(qe: QueryExecution, session: SnappySession, sqlShortText: String,
      sqlText: String, executedPlan: SparkPlan, paramLiterals: Array[ParamLiteral], paramsId: Int)
      (f: => RDD[InternalRow]): (RDD[InternalRow], String, SparkPlanInfo,
      String, SparkPlanInfo, Long, Long, Long) = {
    // Right now the CachedDataFrame is not getting used across SnappySessions
    val executionId = Utils.nextExecutionIdMethod.invoke(SQLExecution).asInstanceOf[Long]
    val executionIdStr = java.lang.Long.toString(executionId)
    val context = session.sparkContext
    val localProperties = context.getLocalProperties
    setExecutionProperties(localProperties, executionIdStr, sqlText)
    var success = false
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
      context.listenerBus.post(SparkListenerSQLPlanExecutionStart(
        executionId, CachedDataFrame.queryStringShortForm(sqlText),
        sqlText, postQueryExecutionStr, postQueryPlanInfo, start))
      val rdd = f
      success = true
      (rdd, queryExecutionStr, queryPlanInfo, postQueryExecutionStr, postQueryPlanInfo,
          executionId, start, System.currentTimeMillis())
    } finally {
      clearExecutionProperties(localProperties)
      if (success) {
        // post the end of "plan" phase which will remove this execution from active list
        context.listenerBus.post(SparkListenerSQLPlanExecutionEnd(executionId))
      } else {
        // post the end of SQL since body of `f` failed
        context.listenerBus.post(SparkListenerSQLExecutionEnd(
          executionId, System.currentTimeMillis()))
      }
    }
  }

  private def handleCTAS(tableType: CatalogObjectType.Type): (Boolean, Boolean) = {
    if (CatalogObjectType.isTableBackedByRegion(tableType)) {
      if (tableType == CatalogObjectType.Sample) false -> true else true -> false
    }
    // most CTAS writers (FileFormatWriter, KafkaWriter) post their own plans for insert
    // but it does not include SQL string or the top-level plan for ExecuteCommand
    // so post the GUI plans but evaluate the toRdd outside else it will be nested
    // withNewExecutionId() that will fail
    else true -> true
  }

  private def evaluatePlan(qe: QueryExecution, session: SnappySession, sqlShortText: String,
      sqlText: String, paramLiterals: Array[ParamLiteral], paramsId: Int): CachedDataFrame = {
    val (executedPlan, withFallback) = getExecutedPlan(qe.executedPlan)
    var planCaching = session.planCaching

    val (cachedRDD, execution, origExecutionString, origPlanInfo, executionString, planInfo, rddId,
    noSideEffects, executionId, planStartTime: Long, planEndTime: Long) = executedPlan match {
      case _: ExecutedCommandExec | _: ExecutePlan | UnionCommands(_) =>
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
        // don't post separate plan for CTAS since it already has posted one for the insert
        val (eagerToRDD, postGUIPlans) = executedPlan.collectFirst {
          case ExecutedCommandExec(c: CreateTableUsingCommand) if c.query.isDefined =>
            handleCTAS(SnappyContext.getProviderType(c.provider))
          case ExecutedCommandExec(c: CreateDataSourceTableAsSelectCommand) =>
            handleCTAS(CatalogObjectType.getTableType(c.table))
          case ExecutedCommandExec(c: SnappyCacheTableCommand) if !c.isLazy => true -> false
          // other commands may have their own withNewExecutionId but still post GUI
          // plans to see the command with proper SQL string in the GUI
          case _: ExecutedCommandExec => true -> true
        } match {
          case None => false -> true
          case Some(p) => p
        }
        var rdd = if (eagerToRDD) qe.toRdd else null

        // post final execution immediately (collect for these plans will post nothing)
        CachedDataFrame.withNewExecutionId(session, sqlShortText, sqlText, executionStr, planInfo,
          postGUIPlans = postGUIPlans) {
          // create new LogicalRDD plan so that plan does not get re-executed
          // (e.g. just toRdd is not enough since further operators like show will pass
          //   around the LogicalPlan and not the executedPlan; it works for plans using
          //   ExecutedCommandExec though because Spark layer has special check for it in
          //   Dataset hasSideEffects)
          if (rdd eq null) rdd = qe.toRdd
          val newPlan = LogicalRDD(qe.analyzed.output, rdd)(session)
          val execution = session.sessionState.executePlan(newPlan)
          (null, execution, origExecutionStr, origPlanInfo, executionStr, planInfo,
              rdd.id, false, -1L, 0L, -1L)
        }._1

      case plan: CollectAggregateExec =>
        val (childRDD, origExecutionStr, origPlanInfo, executionStr, planInfo, executionId,
        planStartTime, planEndTime) = planExecution(qe, session, sqlShortText, sqlText, plan,
          paramLiterals, paramsId)(
          if (withFallback ne null) withFallback.execute(plan.child) else plan.childRDD)
        (childRDD, qe, origExecutionStr, origPlanInfo, executionStr, planInfo,
            childRDD.id, true, executionId, planStartTime, planEndTime)

      case plan =>
        val (rdd, origExecutionStr, origPlanInfo, executionStr, planInfo, executionId,
        planStartTime, planEndTime) = planExecution(qe, session, sqlShortText, sqlText, plan,
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
           _: BroadcastExchangeExec | _: InMemoryTableScanExec |
           _: RangeExec | _: LocalTableScanExec | _: RDDScanExec => true
      case p if HiveClientUtil.isHiveExecPlan(p) => true
      case dsc: DataSourceScanExec => !dsc.relation.isInstanceOf[PartitionedDataSourceScan]
      case _ => false
    }.isEmpty

    // collect the query hints
    val queryHints = session.synchronized {
      val numHints = session.queryHints.size()
      val hints = if (numHints == 0) java.util.Collections.emptyMap[String, String]()
      else {
        val m = new UnifiedMap[String, String](numHints)
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
      executionId, planStartTime, planEndTime, session.hasLinkPartitionsToBuckets)
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
    val sqlShortText = CachedDataFrame.queryStringShortForm(sqlText)
    val plan = parser.parsePlan(sqlText, clearExecutionData = true)
    val planCaching = session.planCaching
    val paramLiterals = parser.sqlParser.getAllLiterals
    val paramsId = parser.sqlParser.getCurrentParamsId
    val key = CachedKey(session, session.getCurrentSchema,
      plan, sqlText, paramLiterals, planCaching)
    var cachedDF: CachedDataFrame = if (planCaching) planCache.getIfPresent(key) else null
    if (cachedDF eq null) {
      // evaluate the plan and cache it if required
      key.currentLiterals = paramLiterals
      key.currentParamsId = paramsId
      session.currentKey = key
      try {
        val execution = session.executePlan(plan)
        session.addContextObject[String]("orgSqlText", sqlText)
        cachedDF = evaluatePlan(execution, session, sqlShortText, sqlText, paramLiterals, paramsId)
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
    handleCachedDataFrame(cachedDF, plan, session, sqlShortText, sqlText, paramLiterals, paramsId)
  }

  private def handleCachedDataFrame(cachedDF: CachedDataFrame, plan: LogicalPlan,
      session: SnappySession, sqlShortText: String, sqlText: String,
      paramLiterals: Array[ParamLiteral], paramsId: Int): CachedDataFrame = {
    cachedDF.queryShortString = sqlShortText
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
    if (!SnappyTableStatsProviderService.TEST_SUSPEND_CACHE_INVALIDATION &&
        (sc ne null) && !sc.isStopped) {
      planCache.invalidateAll()
      if (!onlyQueryPlanCache) {
        RefreshMetadata.executeOnAll(sc, RefreshMetadata.CLEAR_CODEGEN_CACHE, args = null)
      }
    }
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

  def getValue(dvd: stypes.DataValueDescriptor, returnUTF8String: Boolean = true): Any =
    dvd match {
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
        if (returnUTF8String) {
          UTF8String.fromString(str)
        } else str
      } else null
    case lvc: stypes.SQLLongvarchar => if (returnUTF8String) {
      UTF8String.fromString(lvc.getString)
    } else lvc.getString
    case vc: stypes.SQLVarchar => if (returnUTF8String) {
      UTF8String.fromString(vc.getString)
    } else vc.getString
    case c: stypes.SQLChar => if (returnUTF8String) {
      UTF8String.fromString(c.getString)
    } else c.getString
    case ts: stypes.SQLTimestamp => ts.getTimestamp(null)
    case t: stypes.SQLTime => t.getTime(null)
    case d: stypes.SQLDate =>
      val c: Calendar = null
      d.getDate(c)
    case _ => dvd.getObject
  }

  var jarServerFiles: Array[String] = Array.empty

  def getJarURIs: Array[String] = {
    SnappySession.synchronized({
      jarServerFiles
    })
  }

  def addJarURIs(uris: Array[String]): Unit = {
    SnappySession.synchronized({
      jarServerFiles = jarServerFiles ++ uris
    })
  }
}

final class CachedKey(val session: SnappySession,
    val currSchema: String, private val lp: LogicalPlan,
    val sqlText: String, val hintHashcode: Int) {

  private[sql] var currentLiterals: Array[ParamLiteral] = _
  private[sql] var currentParamsId: Int = -1

  override val hashCode: Int = {
    var h = ClientResolverUtils.addIntToHashOpt(session.hashCode(), 42)
    h = ClientResolverUtils.addIntToHashOpt(currSchema.hashCode, h)
    h = ClientResolverUtils.addIntToHashOpt(lp.hashCode(), h)
    ClientResolverUtils.addIntToHashOpt(hintHashcode, h)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case x: CachedKey =>
        x.hintHashcode == hintHashcode && (x.session eq session) &&
            (x.currSchema == currSchema) && x.lp == lp
      case _ => false
    }
  }
}

object CachedKey {
  def apply(session: SnappySession, currschema: String, plan: LogicalPlan, sqlText: String,
      paramLiterals: Array[ParamLiteral], forCaching: Boolean): CachedKey = {

    def normalizeExprIds: PartialFunction[Expression, Expression] = {
      case a: AttributeReference =>
        AttributeReference(a.name, a.dataType, a.nullable)(exprId = ExprId(-1))
      case a: Alias =>
        Alias(a.child, a.name)(exprId = ExprId(-1))
      case ae: AggregateExpression => ae.copy(resultId = ExprId(-1))
      case _: ScalarSubquery =>
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
    new CachedKey(session, currschema, normalizedPlan, sqlText, session.queryHints.hashCode())
  }
}

private object UnionCommands {
  def unapply(plan: SparkPlan): Option[Boolean] = plan match {
    case union: UnionExec if union.children.nonEmpty && union.children.forall {
      case _: ExecutedCommandExec | _: ExecutePlan => true
      case _ => false
    } => Some(true)
    case _ => None
  }
}
