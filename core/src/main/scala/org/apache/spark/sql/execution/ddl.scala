/*
 * Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
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

package org.apache.spark.sql.execution

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.Map.Entry
import java.util.function.Consumer

import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.SystemFailure
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore
import com.pivotal.gemfirexd.internal.iapi.reference.{Property => GemXDProperty}
import com.pivotal.gemfirexd.internal.impl.jdbc.Util
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import io.snappydata.util.ServiceUtils
import io.snappydata.{Constant, Property}

import org.apache.spark.SparkContext
import org.apache.spark.deploy.SparkSubmitUtils
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Cast, Expression, GenericInternalRow, SortDirection}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.collection.ToolsCallbackInit
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.command.{DescribeTableCommand, DropTableCommand, RunnableCommand, SetCommand, ShowTablesCommand}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.{BypassRowLevelSecurity, ContextJarUtils, StaticSQLConf}
import org.apache.spark.sql.sources.DestroyRelation
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, SnappyStreamingContext}


/**
 * Allow execution of adhoc scala code on the Lead node.
 * Creates a new Scala interpreter for a Snappy Session. But, cached for the life of the
 * session. Subsequent invocations of the 'interpret' command will resuse the cached
 * interpreter. Allowing any variables (e.g. dataframe) to be preserved across invocations.
 * State will not be preserved during Lead node failover.
 * <p> Application is injected (1) The SnappySession in variable called 'session' and
 * (2) The Options in a variable called 'intp_options'.
 * <p> To return values set a variable called 'intp_return' - a Seq[Row].
 */
case class InterpretCodeCommand(
     code: String,
     snappySession: SnappySession,
     options: Map[String, String] = Map.empty) extends RunnableCommand {

  lazy val df: Dataset[Row] = {
    val tcb = ToolsCallbackInit.toolsCallback
    if (tcb != null) {
      // supported in embedded mode only
      tcb.getScalaCodeDF(code, snappySession, options)
    } else {
      null
    }
  }


  // This is handled directly by Remote Interpreter code
  override def run(sparkSession: SparkSession): Seq[Row] = df.collect()

  override def output: Seq[Attribute] = df.schema.fields.map(
    x => AttributeReference(x.name, x.dataType, x.nullable)())
}

case class GrantRevokeIntpCommand(
  isGrant: Boolean, users: String) extends RunnableCommand {

  // This is handled directly by Remote Interpreter code
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val tcb = ToolsCallbackInit.toolsCallback
    if (tcb == null) {
      throw new AnalysisException("Granting/Revoking" +
        " of INTP not supported from smart connector mode")
    }
    val session = sparkSession.asInstanceOf[SnappySession]
    val user = session.conf.get(com.pivotal.gemfirexd.Attribute.USERNAME_ATTR)
    tcb.updateIntpGrantRevoke(user, isGrant, users)
    Nil
  }
}

case class GrantRevokeOnExternalTable(
   isGrant: Boolean, table: TableIdentifier, users: String) extends RunnableCommand {

  // This is handled directly by Remote Interpreter code
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val tcb = ToolsCallbackInit.toolsCallback
    if (tcb == null) {
      throw new AnalysisException("Granting/Revoking" +
        " on external table not supported from smart connector mode")
    }
    val session = sparkSession.asInstanceOf[SnappySession]
    val ct = session.sessionCatalog.getTableMetadata(table)
    val user = session.conf.get(com.pivotal.gemfirexd.Attribute.USERNAME_ATTR)
    tcb.updateGrantRevokeOnExternalTable(user, isGrant, table, users, ct)
    Nil
  }
}

object GrantRevokeOnExternalTable {
  val META_REGION_KEY_PREFIX = "##_EXTERNAL__GRANT__REVOKE_##"
  def getMetaRegionKey(fqtn: String): String = {
    META_REGION_KEY_PREFIX + "####" + fqtn
  }
}

case class CreateTableUsingCommand(
    tableIdent: TableIdentifier,
    baseTable: Option[String],
    userSpecifiedSchema: Option[StructType],
    schemaDDL: Option[String],
    provider: String,
    mode: SaveMode,
    options: Map[String, String],
    partitionColumns: Array[String],
    bucketSpec: Option[BucketSpec],
    query: Option[LogicalPlan],
    isBuiltIn: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val session = sparkSession.asInstanceOf[SnappySession]
    val allOptions = session.addBaseTableOption(baseTable, options)
    session.createTableInternal(tableIdent, provider, userSpecifiedSchema,
      schemaDDL, mode, allOptions, isBuiltIn, partitionColumns, bucketSpec, query)
    Nil
  }
}

/**
 * Like Spark's DropTableCommand but checks for non-existent table case upfront to avoid
 * unnecessary warning logs from Spark's DropTableCommand.
 */
case class DropTableOrViewCommand(
    tableIdent: TableIdentifier,
    ifExists: Boolean,
    isView: Boolean,
    purge: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.asInstanceOf[SnappySession].sessionCatalog

    if (!catalog.isTemporaryTable(tableIdent) && !catalog.tableExists(tableIdent)) {
      val resolved = catalog.resolveTableIdentifier(tableIdent)
      if (ifExists) return Nil
      else throw new TableNotFoundException(resolved.database.get, resolved.table)
    }

    DropTableCommand(tableIdent, ifExists, isView, purge).run(sparkSession)
  }
}

case class CreateSchemaCommand(ifNotExists: Boolean, schemaName: String,
    authId: Option[(String, Boolean)]) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val session = sparkSession.asInstanceOf[SnappySession]
    val catalog = session.sessionCatalog
    val schema = catalog.formatDatabaseName(schemaName)
    catalog.createSchema(schema, ifNotExists, authId)
    Nil
  }
}

case class DropSchemaCommand(schemaName: String, ignoreIfNotExists: Boolean,
    cascade: Boolean) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val session = sparkSession.asInstanceOf[SnappySession]
    session.sessionCatalog.dropSchema(schemaName, ignoreIfNotExists, cascade)
    Nil
  }
}

case class DropPolicyCommand(ifExists: Boolean,
    policyIdentifer: TableIdentifier) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snappySession = session.asInstanceOf[SnappySession]
    snappySession.dropPolicy(policyIdentifer, ifExists)
    Nil
  }
}

case class TruncateManagedTableCommand(ifExists: Boolean,
    table: TableIdentifier) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val catalog = session.asInstanceOf[SnappySession].sessionCatalog
    // skip if "ifExists" is true and table does not exist
    if (!(ifExists && !catalog.tableExists(table))) {
      catalog.resolveRelation(table) match {
        case lr: LogicalRelation if lr.relation.isInstanceOf[DestroyRelation] =>
          lr.relation.asInstanceOf[DestroyRelation].truncate()
        case plan => throw new AnalysisException(
          s"Table '$table' must be a DestroyRelation for truncate. Found plan: $plan")
      }
      session.sharedState.cacheManager.uncacheQuery(session.table(table))
    }
    Nil
  }
}

case class AlterTableAddColumnCommand(tableIdent: TableIdentifier,
    addColumn: StructField, extensions: String) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snappySession = session.asInstanceOf[SnappySession]
    snappySession.alterTable(tableIdent, isAddColumn = true, addColumn, extensions)
    Nil
  }
}

case class AlterTableToggleRowLevelSecurityCommand(tableIdent: TableIdentifier,
    enableRls: Boolean) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snappySession = session.asInstanceOf[SnappySession]
    snappySession.alterTableToggleRLS(tableIdent, enableRls)
    Nil
  }
}

case class AlterTableDropColumnCommand(tableIdent: TableIdentifier, column: String,
    extensions: String) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snappySession = session.asInstanceOf[SnappySession]
    // drop column doesn't need anything apart from name so fill dummy values
    snappySession.alterTable(tableIdent, isAddColumn = false,
      StructField(column, NullType), extensions)
    Nil
  }
}

case class AlterTableMiscCommand(tableIdent: TableIdentifier, sql: String)
    extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snappySession = session.asInstanceOf[SnappySession]
    snappySession.alterTableMisc(tableIdent, sql)
    Nil
  }
}

case class CreateIndexCommand(indexName: TableIdentifier,
    baseTable: TableIdentifier,
    indexColumns: Seq[(String, Option[SortDirection])],
    options: Map[String, String]) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snappySession = session.asInstanceOf[SnappySession]
    snappySession.createIndex(indexName, baseTable, indexColumns, options)
    Nil
  }
}

case class CreatePolicyCommand(policyIdent: TableIdentifier,
    tableIdent: TableIdentifier,
    policyFor: String, applyTo: Seq[String], expandedPolicyApplyTo: Seq[String],
    currentUser: String, filterStr: String,
    filter: BypassRowLevelSecurity) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    if (!Misc.isSecurityEnabled && !GemFireStore.ALLOW_RLS_WITHOUT_SECURITY) {
      throw Util.generateCsSQLException(SQLState.SECURITY_EXCEPTION_ENCOUNTERED,
        null, new IllegalStateException("CREATE POLICY failed: Security (" +
            com.pivotal.gemfirexd.Attribute.AUTH_PROVIDER + ") not enabled in the system"))
    }
    if (!Misc.getMemStoreBooting.isRLSEnabled) {
      throw Util.generateCsSQLException(SQLState.SECURITY_EXCEPTION_ENCOUNTERED,
        null, new IllegalStateException("CREATE POLICY failed: Row level security (" +
            GemXDProperty.SNAPPY_ENABLE_RLS + ") not enabled in the system"))
    }
    val snappySession = session.asInstanceOf[SnappySession]
    SparkSession.setActiveSession(snappySession)
    snappySession.createPolicy(policyIdent, tableIdent, policyFor, applyTo, expandedPolicyApplyTo,
      currentUser, filterStr, filter)
    Nil
  }
}

case class DropIndexCommand(ifExists: Boolean,
    indexName: TableIdentifier) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snappySession = session.asInstanceOf[SnappySession]
    snappySession.dropIndex(indexName, ifExists)
    Nil
  }
}

case class SetSchemaCommand(schemaName: String) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.asInstanceOf[SnappySession].setCurrentSchema(schemaName)
    Nil
  }
}

case class SnappyStreamingActionsCommand(action: Int,
    batchInterval: Option[Duration]) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {

    def creatingFunc(): SnappyStreamingContext = {
      // batchInterval will always be defined when action == 0
      new SnappyStreamingContext(session.sparkContext, batchInterval.get)
    }

    action match {
      case 0 =>
        val ssc = SnappyStreamingContext.getInstance()
        ssc match {
          case Some(_) => // TODO .We should create a named Streaming
          // Context and check if the configurations match
          case None => SnappyStreamingContext.getActiveOrCreate(creatingFunc)
        }
      case 1 =>
        val ssc = SnappyStreamingContext.getInstance()
        ssc match {
          case Some(x) => x.start()
          case None => throw new AnalysisException(
            "Streaming Context has not been initialized")
        }
      case 2 =>
        val ssc = SnappyStreamingContext.getActive
        ssc match {
          case Some(strCtx) => strCtx.stop(stopSparkContext = false,
            stopGracefully = true)
          case None => // throw new AnalysisException(
          // "There is no running Streaming Context to be stopped")
        }
    }
    Nil
  }
}

/**
 * Alternative to Spark's CacheTableCommand that shows the plan being cached
 * in the GUI rather than count() plan for InMemoryRelation.
 */
case class SnappyCacheTableCommand(tableIdent: TableIdentifier, queryString: String,
    plan: Option[LogicalPlan], isLazy: Boolean) extends RunnableCommand {

  require(plan.isEmpty || tableIdent.database.isEmpty,
    "Schema name is not allowed in CACHE TABLE AS SELECT")

  override def output: Seq[Attribute] = AttributeReference(
    "batchCount", LongType)() :: Nil

  override protected def innerChildren: Seq[QueryPlan[_]] = plan match {
    case None => Nil
    case Some(p) => p :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val session = sparkSession.asInstanceOf[SnappySession]
    val df = plan match {
      case None => session.table(tableIdent)
      case Some(lp) =>
        val df = Dataset.ofRows(session, lp)
        df.createTempView(tableIdent.quotedString)
        df
    }

    val isOffHeap = ServiceUtils.isOffHeapStorageAvailable(session)

    if (isLazy) {
      if (isOffHeap) df.persist(StorageLevel.OFF_HEAP) else df.persist()
      Nil
    } else {
      val queryShortString = CachedDataFrame.queryStringShortForm(queryString)
      val localProperties = session.sparkContext.getLocalProperties
      val previousJobDescription = localProperties.getProperty(SparkContext.SPARK_JOB_DESCRIPTION)
      localProperties.setProperty(SparkContext.SPARK_JOB_DESCRIPTION, queryShortString)
      try {
        session.sessionState.enableExecutionCache = true
        // Get the actual QueryExecution used by InMemoryRelation so that
        // "withNewExecutionId" runs on the same and shows proper metrics in GUI.
        val cachedExecution = try {
          if (isOffHeap) df.persist(StorageLevel.OFF_HEAP) else df.persist()
          session.sessionState.getExecution(df.logicalPlan)
        } finally {
          session.sessionState.enableExecutionCache = false
          session.sessionState.clearExecutionCache()
        }
        val memoryPlan = df.queryExecution.executedPlan.collectFirst {
          case plan: InMemoryTableScanExec => plan.relation
        }.get
        // cachedExecution can be null if query was already cached
        val (cachedPlan, cachedPlanStr) =
          if (cachedExecution ne null) cachedExecution.executedPlan -> cachedExecution.toString
          else {
            val executedPlan = session.sharedState.cacheManager().lookupCachedData(df.logicalPlan)
                .getOrElse(throw new AnalysisException(s"Cached data for [ $queryString ] after " +
                    "successful persist() call no longer present.")).cachedRepresentation.child
            executedPlan ->
                s"""== Physical Plan previously cached ==
                   |${executedPlan.treeString(verbose = false)}
                """.stripMargin.trim
          }
        val planInfo = PartitionedPhysicalScan.getSparkPlanInfo(cachedPlan)
        Row(CachedDataFrame.withCallback(session, df = null, cachedExecution, "cache")(_ =>
          CachedDataFrame.withNewExecutionId(session, queryShortString, queryString,
            cachedPlanStr, planInfo, if (cachedExecution ne null) cachedPlan else null,
            removeBroadcastsFromDriver = true)({
            val start = System.nanoTime()
            // Dummy op to materialize the cache. This does the minimal job of count on
            // the actual cached data (RDD[CachedBatch]) to force materialization of cache
            // while avoiding creation of any new SparkPlan.
            val count = memoryPlan.cachedColumnBuffers.count()
            (count, System.nanoTime() - start)
          }))._1) :: Nil
      } finally {
        if (previousJobDescription ne null) {
          localProperties.setProperty(SparkContext.SPARK_JOB_DESCRIPTION, previousJobDescription)
        } else {
          localProperties.remove(SparkContext.SPARK_JOB_DESCRIPTION)
        }
      }
    }
  }
}

/**
 * Changes the name of "database" column to "schemaName" over Spark's ShowTablesCommand.
 * Also when hive compatibility is turned on, then this does not include the schema name
 * or "isTemporary" to return hive compatible result.
 */
class ShowSnappyTablesCommand(session: SnappySession, schemaOpt: Option[String],
    tablePattern: Option[String]) extends ShowTablesCommand(schemaOpt, tablePattern) {

  private val hiveCompatible = Property.HiveCompatibility.get(
    session.sessionState.conf).equalsIgnoreCase("full")

  override val output: Seq[Attribute] = {
    if (hiveCompatible) AttributeReference("name", StringType, nullable = false)() :: Nil
    else {
      AttributeReference("schemaName", StringType, nullable = false)() ::
          AttributeReference("tableName", StringType, nullable = false)() ::
          AttributeReference("isTemporary", BooleanType, nullable = false)() :: Nil
    }
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (!hiveCompatible) {
      return super.run(sparkSession)
    }

    val catalog = sparkSession.sessionState.catalog
    val schemaName = schemaOpt match {
      case None => catalog.getCurrentDatabase
      case Some(s) => s
    }
    val tables = tableIdentifierPattern match {
      case None => catalog.listTables(schemaName)
      case Some(p) => catalog.listTables(schemaName, p)
    }
    tables.map(tableIdent => Row(tableIdent.table))
  }
}

case class ShowViewsCommand(session: SnappySession, schemaOpt: Option[String],
    viewPattern: Option[String]) extends RunnableCommand {

  private val hiveCompatible = Property.HiveCompatibility.get(
    session.sessionState.conf).equalsIgnoreCase("full")

  // The result of SHOW VIEWS has four columns: schemaName, tableName, isTemporary and isGlobal.
  override val output: Seq[Attribute] = {
    if (hiveCompatible) AttributeReference("viewName", StringType, nullable = false)() :: Nil
    else {
      AttributeReference("schemaName", StringType, nullable = false)() ::
          AttributeReference("viewName", StringType, nullable = false)() ::
          AttributeReference("isTemporary", BooleanType, nullable = false)() ::
          AttributeReference("isGlobal", BooleanType, nullable = false)() :: Nil
    }
  }

  private def getViewType(table: TableIdentifier,
      session: SnappySession): Option[(Boolean, Boolean)] = {
    val catalog = session.sessionCatalog
    if (catalog.isTemporaryTable(table)) Some(true -> !catalog.isLocalTemporaryView(table))
    else if (catalog.getTableMetadata(table).tableType != CatalogTableType.VIEW) None
    else Some(false -> false)
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val session = sparkSession.asInstanceOf[SnappySession]
    val catalog = session.sessionCatalog
    val schemaName = schemaOpt match {
      case None => catalog.getCurrentDatabase
      case Some(s) => s
    }
    val tables = viewPattern match {
      case None => catalog.listTables(schemaName)
      case Some(p) => catalog.listTables(schemaName, p)
    }
    tables.map(tableIdent => tableIdent -> getViewType(tableIdent, session)).collect {
      case (viewIdent, Some((isTemp, isGlobalTemp))) =>
        if (hiveCompatible) Row(viewIdent.table)
        else {
          val viewSchema = viewIdent.database match {
            case None => ""
            case Some(s) => s
          }
          Row(viewSchema, viewIdent.table, isTemp, isGlobalTemp)
        }
    }
  }
}

/**
 * This extends Spark's describe to add support for CHAR and VARCHAR types.
 */
class DescribeSnappyTableCommand(table: TableIdentifier,
    partitionSpec: TablePartitionSpec, isExtended: Boolean, isFormatted: Boolean)
    extends DescribeTableCommand(table, partitionSpec, isExtended, isFormatted) {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.asInstanceOf[SnappySession].sessionCatalog
    catalog.synchronized {
      // set the flag to return CharType/VarcharType if present
      catalog.convertCharTypesInMetadata = true
      try {
        super.run(sparkSession)
      } finally {
        catalog.convertCharTypesInMetadata = false
      }
    }
  }
}

class SetSnappyCommand(kv: Option[(String, Option[String])]) extends SetCommand(kv) {

  override def run(sparkSession: SparkSession): Seq[Row] = kv match {
    // SnappySession allows attaching external hive catalog at runtime
    case Some((k, Some(v))) if k.equalsIgnoreCase(StaticSQLConf.CATALOG_IMPLEMENTATION.key) =>
      sparkSession.sessionState.conf.setConfString(k, v)
      Row(k, v) :: Nil
    case _ => super.run(sparkSession)
  }
}

case class DeployCommand(
    coordinates: String,
    alias: String,
    repos: Option[String],
    jarCache: Option[String],
    restart: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    try {
      val jarsstr = SparkSubmitUtils.resolveMavenCoordinates(coordinates, repos, jarCache)
      if (jarsstr.nonEmpty) {
        val jars = jarsstr.split(",")
        val sc = sparkSession.sparkContext
        val uris = jars.map(j => sc.env.rpcEnv.fileServer.addFile(new File(j)))
        SnappySession.addJarURIs(uris)
        RefreshMetadata.executeOnAll(sc, RefreshMetadata.ADD_URIS_TO_CLASSLOADER, uris)
        val deployCmd = s"$coordinates|${repos.getOrElse("")}|${jarCache.getOrElse("")}"
        ToolsCallbackInit.toolsCallback.addURIs(alias, jars, deployCmd)
      }
      Nil
    } catch {
      case ex: Throwable =>
        ex match {
          case err: Error =>
            if (SystemFailure.isJVMFailureError(err)) {
              SystemFailure.initiateFailure(err)
              // If this ever returns, rethrow the error. We're poisoned
              // now, so don't let this thread continue.
              throw err
            }
          case _ =>
        }
        Misc.checkIfCacheClosing(ex)
        if (restart) {
          logWarning(s"Following mvn coordinate" +
              s" could not be resolved during restart: $coordinates", ex)
        }
        throw ex
    }
  }
}

case class DeployJarCommand(
    alias: String,
    paths: String,
    restart: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (paths.nonEmpty) {
      val jars = paths.split(",")
      val (availableUris, unavailableUris) = jars.partition(f => Files.isReadable(Paths.get(f)))
      if (unavailableUris.nonEmpty) {
        logWarning(s"Following jars are unavailable" +
            s" for deployment during restart: ${unavailableUris.deep.mkString(",")}")
        if (restart) {
          throw new IllegalStateException(
            s"Could not find deployed jars: ${unavailableUris.mkString(",")}")
        }
        throw new IllegalArgumentException(s"jars not readable: ${unavailableUris.mkString(",")}")
      }
      val sc = sparkSession.sparkContext
      val uris = availableUris.map(j => sc.env.rpcEnv.fileServer.addFile(new File(j)))
      SnappySession.addJarURIs(uris)
      RefreshMetadata.executeOnAll(sc, RefreshMetadata.ADD_URIS_TO_CLASSLOADER, uris)
      ToolsCallbackInit.toolsCallback.addURIs(alias, jars, paths, isPackage = false)
    }
    Nil
  }
}

case class ListPackageJarsCommand(isJar: Boolean) extends RunnableCommand {
  override val output: Seq[Attribute] = {
    AttributeReference("alias", StringType, nullable = false)() ::
        AttributeReference("coordinate", StringType, nullable = false)() ::
        AttributeReference("isPackage", BooleanType, nullable = false)() :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val commands = ToolsCallbackInit.toolsCallback.getGlobalCmndsSet
    val rows = new ArrayBuffer[Row]
    commands.forEach(new Consumer[Entry[String, Object]] {
      override def accept(t: Entry[String, Object]): Unit = {
        if (!(t.getKey.equals(Constant.CLUSTER_ID) ||
            t.getKey.startsWith(Constant.MEMBER_ID_PREFIX))) {
          var alias = t.getKey
          // Skip dropped functions entry
          if (alias.contains(ContextJarUtils.droppedFunctionsKey)) return
          // Explicitly mark functions as UDF while listing jars/packages.
          alias = alias.replace(ContextJarUtils.functionKeyPrefix, "[UDF]")
          if (t.getValue.isInstanceOf[String]) {
            val value = t.getValue.toString
            val indexOf = value.indexOf('|')
            if (indexOf > 0) {
              // It is a package
              val pkg = value.substring(0, indexOf)
              rows += Row(alias, pkg, true)
            }
            else {
              // It is a jar
              val jars = value.split(',')
              val jarfiles = jars.map(f => {
                val lastIndexOf = f.lastIndexOf('/')
                val length = f.length
                if (lastIndexOf > 0) f.substring(lastIndexOf + 1, length)
                else {
                  f
                }
              })
              rows += Row(alias, jarfiles.mkString(","), false)
            }
          }
        }
      }
    })
    rows
  }
}

case class UnDeployCommand(alias: String) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    var value = ""
    val sc = sparkSession.sparkContext
    if (alias != null) {
      val cmndsSet = ToolsCallbackInit.toolsCallback.getGlobalCmndsSet
      cmndsSet.forEach(new Consumer[Entry[String, Object]] {
        override def accept(t: Entry[String, Object]): Unit = {
          val alias1 = t.getKey
          if (alias == alias1) {
            value = t.getValue match {
              case s: String => s
              case _ => null
            }
          }
        }
      })
      if (value != null) {
        val indexOf = value.indexOf("|")
        val lastIndexOf = value.lastIndexOf("|")
        if (indexOf > 0) {
          val coordinates = value.substring(0, indexOf)
          val repos = Option(value.substring(indexOf + 1, lastIndexOf))
          val jarCache = Option(value.substring(lastIndexOf + 1, value.length))
          val jarsstr = SparkSubmitUtils.resolveMavenCoordinates(coordinates,
            repos, jarCache)
          if (jarsstr.nonEmpty) {
            val pkgs = jarsstr.split(",")
            RefreshMetadata.executeOnAll(sc, RefreshMetadata.REMOVE_URIS_FROM_CLASSLOADER, pkgs)
            ToolsCallbackInit.toolsCallback.removeURIs(pkgs)
          }
        }
        else {
          if (value.nonEmpty) {
            val jars = value.split(',')
            RefreshMetadata.executeOnAll(sc, RefreshMetadata.REMOVE_URIS_FROM_CLASSLOADER, jars)
            ToolsCallbackInit.toolsCallback.removeURIs(jars)
          }
        }
      }
    }
    ToolsCallbackInit.toolsCallback.removePackage(alias)
    Nil
  }
}

case class PutIntoValuesColumnTable(table: CatalogTable,
    colNames: Option[Seq[String]],
    values: Seq[Seq[Expression]])
    extends RunnableCommand {

  if (values.isEmpty) {
    throw new IllegalArgumentException(s"PUT: empty value list passed")
  }

  override lazy val output: Seq[Attribute] = AttributeReference("count", LongType)() :: Nil

  private def cast(from: Expression, to: StructField): Expression = {
    val fromType = from.dataType
    val toType = to.dataType
    if (fromType == toType) from
    else if (Cast.canCast(fromType, toType)) {
      Cast(from, toType)
    } else if (Cast.canCast(StringType, toType)) {
      Cast(Cast(from, StringType), toType)
    } else {
      throw new AnalysisException(
        s"Cannot cast '$from' to target type '${toType.sql}' for column '${to.name}'")
    }
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val snc = sparkSession.asInstanceOf[SnappySession]
    val tableName = table.identifier.unquotedString
    val schema = table.schema
    import snappy._
    if (colNames.isEmpty) {
      val rows = values.zipWithIndex.map { case (exprs, i) =>
        if (exprs.length != schema.length) {
          throw new AnalysisException(
            s"PUT: mismatch in the length of value list and table schema at index $i")
        }
        new GenericInternalRow(
          exprs.zip(schema).map(p => cast(p._1, p._2).eval()).toArray)
      }
      Row(snc.internalCreateDataFrame(rows, schema).write.putInto(tableName)) :: Nil
    }
    else {
      val columnNames = colNames.get
      val resolver = snc.sessionState.analyzer.resolver
      val indexMapping = columnNames.map { column =>
        val index = schema.fields.indexWhere(f => resolver(f.name, column))
        if (index == -1) {
          throw new AnalysisException(s"Field '$column' does not exist in " +
              s"${table.identifier.quotedString} with schema=${schema.map(_.name)}")
        }
        index
      }
      val rows = values.zipWithIndex.map { case (exprs, i) =>
        if (exprs.length != columnNames.length) {
          throw new AnalysisException(
            s"PUT: mismatch in the length of value list and column names at index $i")
        }
        val rowData = new Array[Any](schema.length)
        exprs.zip(indexMapping).foreach(p =>
          rowData(p._2) = cast(p._1, schema(p._2)).eval())
        new GenericInternalRow(rowData)
      }
      Row(snc.internalCreateDataFrame(rows, schema).write.putInto(tableName)) :: Nil
    }
  }
}
