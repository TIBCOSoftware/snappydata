package org.apache.spark.sql

import java.sql.{Connection, SQLException}

import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => u}
import scala.util.{Failure, Success, Try}

import io.snappydata.{Constant, Property}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.aqp.{AQPContext, AQPDefault}
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, ScalaReflection}
import org.apache.spark.sql.collection.{ToolsCallbackInit, UUIDRegionKey, Utils}
import org.apache.spark.sql.columnar.{CachedBatch, ExternalStoreRelation, ExternalStoreUtils, InMemoryAppendableRelation}
import org.apache.spark.sql.execution.datasources.{LogicalRelation, ResolvedDataSource}
import org.apache.spark.sql.execution.{ConnectionPool, ExtractPythonUDFs, LogicalRDD, SparkPlan}
import org.apache.spark.sql.hive.{ExternalTableType, QualifiedTableName, SnappyStoreHiveCatalog}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.row.GemFireXDDialect
import org.apache.spark.sql.snappy.RDDExtensions
import org.apache.spark.sql.sources.{DeletableRelation, DestroyRelation, IndexableRelation, JdbcExtendedUtils, RowInsertableRelation, UpdatableRelation}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{execution => sparkexecution}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{Logging, SparkContext, SparkException}

/**
 * Main entry point for SnappyData extensions to Spark. A SnappyContext
 * extends Spark's [[org.apache.spark.sql.SQLContext]] to work with Row and
 * Column tables. Any DataFrame can be managed as SnappyData tables and any
 * table can be accessed as a DataFrame. This is similar to
 * [[org.apache.spark.sql.hive.HiveContext HiveContext]] - integrates the
 * SQLContext functionality with the Snappy store.
 *
 * When running in the '''embedded ''' mode (i.e. Spark executor collocated
 * with Snappy data store), Applications typically submit Jobs to the
 * Snappy-JobServer
 * (provide link) and do not explicitly create a SnappyContext. A single
 * shared context managed by SnappyData makes it possible to re-use Executors
 * across client connections or applications.
 *
 * @see document describing the various URL options to create
 *      Snappy/Spark Context
 * @see document describing the Job server API
 * @todo Provide links to above descriptions
 *
 */

class SnappyContext protected[spark] (@transient sc: SparkContext)
    extends SQLContext(sc) with Serializable with Logging {

  self =>

  val aqpContext = Try {
    val mirror = u.runtimeMirror(getClass.getClassLoader)
    val cls = mirror.classSymbol(Class.forName(SnappyContext.aqpContextImplClass))
    val clsType = cls.toType
    val classMirror = mirror.reflectClass(clsType.typeSymbol.asClass)
    val defaultCtor = clsType.member(u.nme.CONSTRUCTOR)
    val runtimeCtr = classMirror.reflectConstructor(defaultCtor.asMethod)
    runtimeCtr().asInstanceOf[AQPContext]
  }  match {
    case Success(v) => v
    case Failure(_) => AQPDefault
  }

  // initialize GemFireXDDialect so that it gets registered

  GemFireXDDialect.init()
  GlobalSnappyInit.initGlobalSnappyContext(sc)

  protected[sql] override lazy val conf: SQLConf = new SQLConf {
    override def caseSensitiveAnalysis: Boolean =
      getConf(SQLConf.CASE_SENSITIVE, false)

    override def unsafeEnabled: Boolean = if(aqpContext.isTungstenEnabled) {
      super.unsafeEnabled
    }else {
      false
    }
  }

  @transient
  override protected[sql] val ddlParser = this.aqpContext.getSnappyDDLParser(sqlParser.parse)


  override protected[sql] def dialectClassName = if (conf.dialect == "sql") {
    this.aqpContext.getSQLDialectClassName
  } else {
    conf.dialect
  }
  override protected[sql] def executePlan(plan: LogicalPlan) =   aqpContext.executePlan(this, plan)

  @transient
  override lazy val catalog = this.aqpContext.getSnappyCatalogue(this)

  @transient
  override protected[sql] val cacheManager =  this.aqpContext.getSnappyCacheManager

  /**
   * :: DeveloperApi ::
   * @todo do we need this anymore? If useful functionality, make this
   *       private to sql package ... SchemaDStream should use the data source
   *       API?
   *              Tagging as developer API, for now
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
    val transfrmr = transformer match {
      case Some(x) => x
      case None =>  if ( !(v.tpe =:= u.typeOf[Row])) {
        //check if the stream type is already a Row
        throw new IllegalStateException(" Transformation to Row type needs to be supplied")
      }else {
        null
      }
    }
    stream.foreachRDD((rdd: RDD[T], time: Time) => {

      val rddRows = if( transfrmr != null) {
        transfrmr(rdd)
      }else {
        rdd.asInstanceOf[RDD[Row]]
      }
      aqpContext.collectSamples(this, rddRows, aqpTables, time.milliseconds)
    })
  }

  /**
   * @todo remove its reference from SnappyImplicits ... use DataSource API
   *       instead
   * @param df
   * @param aqpTables
   */
  def saveTable(df: DataFrame,  aqpTables: Seq[String]): Unit= this
    .aqpContext.collectSamples(this, df.rdd,
    aqpTables, System.currentTimeMillis())


  /**
   * Append dataframe to cache table in Spark.
   * @todo should this be renamed to appendToTempTable(...) ?
   *
   * @param df
   * @param table
   * @param storageLevel default storage level is MEMORY_AND_DISK
   * @return  @todo -> return type?
   */
  def appendToCache(df: DataFrame, table: String,
                    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK) = {
    val useCompression = conf.useCompression
    val columnBatchSize = conf.columnBatchSize

    val tableIdent = catalog.newQualifiedTableName(table)
    val plan = catalog.lookupRelation(tableIdent, None)
    val relation = cacheManager.lookupCachedData(plan).getOrElse {
      cacheManager.cacheQuery(DataFrame(self, plan),
        Some(tableIdent.table), storageLevel)

      cacheManager.lookupCachedData(plan).getOrElse {
        sys.error(s"couldn't cache table $tableIdent")
      }
    }

    val (schema, output) = (df.schema, df.logicalPlan.output)

    val cached = df.rdd.mapPartitionsPreserve { rowIterator =>

      val batches = ExternalStoreRelation(useCompression, columnBatchSize,
        tableIdent, schema, relation.cachedRepresentation, output)

      val converter = CatalystTypeConverters.createToCatalystConverter(schema)

      rowIterator.map(converter(_).asInstanceOf[InternalRow])
        .foreach(batches.appendRow((), _))
      batches.forceEndOfBatch().iterator
    }.persist(storageLevel)

    // trigger an Action to materialize 'cached' batch
    if (cached.count() > 0) {
      relation.cachedRepresentation match {
        case externalStore: ExternalStoreRelation =>
          externalStore.appendUUIDBatch(cached.asInstanceOf[RDD[UUIDRegionKey]])
        case appendable: InMemoryAppendableRelation =>
          appendable.appendBatch(cached.asInstanceOf[RDD[CachedBatch]])
      }
    }
  }


  /**
   * @param rdd
   * @param table
   * @param schema
   * @param storageLevel
   */
  private[sql] def appendToCacheRDD(rdd: RDD[_], table: String, schema:
    StructType,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK) {
    val useCompression = conf.useCompression
    val columnBatchSize = conf.columnBatchSize

    val tableIdent = catalog.newQualifiedTableName(table)
    val plan = catalog.lookupRelation(tableIdent, None)
    val relation = cacheManager.lookupCachedData(plan).getOrElse {
      cacheManager.cacheQuery(DataFrame(this, plan),
        Some(tableIdent.table), storageLevel)

      cacheManager.lookupCachedData(plan).getOrElse {
        sys.error(s"couldn't cache table $tableIdent")
      }
    }

    val cached = rdd.mapPartitionsPreserve { rowIterator =>

      val batches = ExternalStoreRelation(useCompression, columnBatchSize,
        tableIdent, schema, relation.cachedRepresentation , schema.toAttributes)
      val converter = CatalystTypeConverters.createToCatalystConverter(schema)
      rowIterator.map(converter(_).asInstanceOf[InternalRow])
          .foreach(batches.appendRow((), _))
      batches.forceEndOfBatch().iterator

    }.persist(storageLevel)

    // trigger an Action to materialize 'cached' batch
    if (cached.count() > 0) {
      relation.cachedRepresentation match {
        case externalStore: ExternalStoreRelation =>
          externalStore.appendUUIDBatch(cached.asInstanceOf[RDD[UUIDRegionKey]])
        case appendable: InMemoryAppendableRelation =>
          appendable.appendBatch(cached.asInstanceOf[RDD[CachedBatch]])
      }
    }
  }

  /**
   * :: DeveloperApi ::
   * Empties the contents of the table without deleting the catalog entry.
   * @todo truncateTable should work for cached temporary tables or Snappy
   *       tables ... remove DeveloperApi annotation later ..
   * @param tableName
   */
  @DeveloperApi
  def truncateTable(tableName: String): Unit = {
    cacheManager.lookupCachedData(catalog.lookupRelation(
      tableName)).foreach(_.cachedRepresentation.
        asInstanceOf[InMemoryAppendableRelation].truncate())
  }

  /**
   * :: DeveloperApi ::
   * Empties the contents of the table without deleting the catalog entry.
   * @todo No need for truncateExternalTable just truncateTable ?
   * @param tableName
   */
  @DeveloperApi
  def truncateExternalTable(tableName: String): Unit = {
    val qualifiedTable = catalog.newQualifiedTableName(tableName)
    val plan = catalog.lookupRelation(qualifiedTable, None)
    snappy.unwrapSubquery(plan) match {
      case LogicalRelation(br, _) =>
        cacheManager.tryUncacheQuery(DataFrame(self, plan))
        br match {
          case d: DestroyRelation => d.truncate()
        }
      case _ => throw new AnalysisException(
        s"truncateExternalTable: Table $tableName not an external table")
    }
  }

  /**
   * :: DeveloperApi ::
   * @todo remove later. Use the DataSource API instead .. or, just createTable
   * @param tableName
   * @tparam A
   */
  @DeveloperApi
  def registerTable[A <: Product : u.TypeTag](tableName: String): Unit = {
    if (u.typeOf[A] =:= u.typeOf[Nothing]) {
      sys.error("Type of case class object not mentioned. " +
          "Mention type information for e.g. registerSampleTableOn[<class>]")
    }

    SparkPlan.currentContext.set(self)
    val schema = ScalaReflection.schemaFor[A].dataType
        .asInstanceOf[StructType]

    val plan: LogicalRDD = LogicalRDD(schema.toAttributes,
      new DummyRDD(self))(self)

    catalog.registerTable(catalog.newQualifiedTableName(tableName), plan)
  }

  /**
   * :: DeveloperApi ::
   * @todo Remove ... use DataSource API
   * @param tableName
   * @param schema
   * @param samplingOptions
   * @param streamTable
   * @param jdbcSource
   * @return
   */
  @DeveloperApi
  def registerSampleTable(tableName: String, schema: StructType,
      samplingOptions: Map[String, Any], streamTable: Option[String] = None,
      jdbcSource: Option[Map[String, String]] = None): SampleDataFrame =
       aqpContext.registerSampleTable(self,  tableName, schema,
         samplingOptions, streamTable,
       jdbcSource)


  /**
   * :: DeveloperApi ::
   * @todo not needed any more
   * @param tableName
   * @param samplingOptions
   * @param streamTable
   * @param jdbcSource
   * @tparam A
   * @return
   */
  @DeveloperApi
  def registerSampleTableOn[A <: Product : u.TypeTag](tableName: String,
      samplingOptions: Map[String, Any], streamTable: Option[String] = None,
      jdbcSource: Option[Map[String, String]] = None): DataFrame =
      aqpContext.registerSampleTableOn(self, tableName,
        samplingOptions, streamTable,
      jdbcSource)


  /**
   * @todo rename to createApproxTSTopK .. it is approximate and time series
   * @todo provide lot more details and examples to explain creating and
   *       using TopK with time series
   * @param topKName
   * @param keyColumnName
   * @param inputDataSchema
   * @param topkOptions
   * @param isStreamSummary
   */
  def createTopK(topKName: String, keyColumnName: String,
                 inputDataSchema: StructType,
      topkOptions: Map[String, Any], isStreamSummary: Boolean): Unit =
      aqpContext.createTopK(self, topKName, keyColumnName, inputDataSchema,
        topkOptions, isStreamSummary)


  /**
   * Create external tables like parquet or tables in external database like
   * MySQL. For creating tables in SnappyData Store use createTable
   * @param tableName
   * @param provider
   * @param options
   * @return
   */
  override def createExternalTable(
      tableName: String,
      provider: String,
      options: Map[String, String]): DataFrame = {
    val plan = createTable(catalog.newQualifiedTableName(tableName), provider,
      userSpecifiedSchema = None, schemaDDL = None,
      SaveMode.ErrorIfExists, options)
    DataFrame(self, plan)
  }

  override def createExternalTable(
      tableName: String,
      provider: String,
      schema: StructType,
      options: Map[String, String]): DataFrame = {
    val plan = createTable(catalog.newQualifiedTableName(tableName), provider,
      Some(schema), schemaDDL = None, SaveMode.ErrorIfExists, options)
    DataFrame(self, plan)
  }

  /**
   * @todo Jags: Recommend we change behavior so createExternal is used only to
   * create non-snappy managed tables. 'createTable' should create snappy
   * managed tables.
   */


  /**
    * Create an external table with given options.
    */
  private[sql] def createTable(
      tableIdent: QualifiedTableName,
      provider: String,
      userSpecifiedSchema: Option[StructType],
      schemaDDL: Option[String],
      mode: SaveMode,
      options: Map[String, String]): LogicalPlan = {

    if (catalog.tableExists(tableIdent)) {
      mode match {
        case SaveMode.ErrorIfExists =>
          throw new AnalysisException(
            s"createExternalTable: Table $tableIdent already exists.")
        case _ =>
          return catalog.lookupRelation(tableIdent, None)
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

    val source = SnappyContext.getProvider(provider)
    val resolved = schemaDDL match {
      case Some(schema) => JdbcExtendedUtils.externalResolvedDataSource(self,
        schema, source, mode, params)

      case None =>
        // add allowExisting in properties used by some implementations
        ResolvedDataSource(self, userSpecifiedSchema, Array.empty[String],
          source, params + (JdbcExtendedUtils.ALLOW_EXISTING_PROPERTY ->
              (mode != SaveMode.ErrorIfExists).toString))
    }

    catalog.registerExternalTable(tableIdent, userSpecifiedSchema,
      Array.empty[String], source, params,
      ExternalTableType.getTableType(resolved.relation))
    LogicalRelation(resolved.relation)
  }

  /**
    * Create an external table with given options.
    */
  private[sql] def createTable(
      tableIdent: QualifiedTableName,
      provider: String,
      partitionColumns: Array[String],
      mode: SaveMode,
      options: Map[String, String],
      query: LogicalPlan): LogicalPlan = {

    var data = DataFrame(self, query)
    if (catalog.tableExists(tableIdent)) {
      mode match {
        case SaveMode.ErrorIfExists =>
          throw new AnalysisException(s"Table $tableIdent already exists. " +
              "If using SQL CREATE TABLE, you need to use the " +
              s"APPEND or OVERWRITE mode, or drop $tableIdent first.")
        case SaveMode.Ignore => return catalog.lookupRelation(tableIdent, None)
        case _ =>
          // existing table schema could have nullable columns
          val schema = data.schema
          if (schema.exists(!_.nullable)) {
            data = internalCreateDataFrame(data.queryExecution.toRdd,
              schema.asNullable)
          }
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

    // this gives the provider..

    val source = SnappyContext.getProvider(provider)
    val resolved = ResolvedDataSource(self, source, partitionColumns,
      mode, params, data)

    if (catalog.tableExists(tableIdent) && mode == SaveMode.Overwrite) {
      // uncache the previous results and don't register again
      cacheManager.tryUncacheQuery(data)
    }
    else {
      catalog.registerExternalTable(tableIdent, Some(data.schema),
        partitionColumns, source, params, ExternalTableType.getTableType(resolved.relation))
    }
    LogicalRelation(resolved.relation)
  }

  /**
   * @todo should be renamed to dropTable
   * Drop an external table created by a call to createExternalTable.
   * @param tableName table to be dropped
   * @param ifExists  attempt drop only if the table exists
   */
  def dropExternalTable(tableName: String, ifExists: Boolean = false): Unit = {
    val qualifiedTable = catalog.newQualifiedTableName(tableName)
    val plan = try {
      catalog.lookupRelation(qualifiedTable, None)
    } catch {
      case ae: AnalysisException =>
        if (ifExists) return else throw ae
    }
    // additional cleanup for external tables, if required
    snappy.unwrapSubquery(plan) match {
      case LogicalRelation(br, _) =>
        cacheManager.tryUncacheQuery(DataFrame(self, plan))
        catalog.unregisterExternalTable(qualifiedTable)
        br match {
          case d: DestroyRelation => d.destroy(ifExists)
          case _ => // Do nothing
        }
      case _ => throw new AnalysisException(
        s"dropExternalTable: Table $tableName not an external table")
    }
  }

  /**
   * Create Index on an external table (created by a call to createExternalTable).
   * @todo how can the user invoke this? sql?
   */
  private[sql] def createIndexOnExternalTable(tableName: String, sql: String):
    Unit = {
    //println("create-index" + " tablename=" + tableName    + " ,sql=" + sql)

    if (!catalog.tableExists(tableName)) {
      throw new AnalysisException(
        s"$tableName is not an indexable table")
    }

    val qualifiedTable = catalog.newQualifiedTableName(tableName)
    //println("qualifiedTable=" + qualifiedTable)
    snappy.unwrapSubquery(catalog.lookupRelation(qualifiedTable, None)) match {
      case LogicalRelation(i: IndexableRelation, _) =>
        i.createIndex(tableName, sql)
      case _ => throw new AnalysisException(
        s"$tableName is not an indexable table")
    }
  }

  /**
   * Create Index on an external table (created by a call to createExternalTable).
   */
  private[sql] def dropIndexOnExternalTable(sql: String): Unit = {
    //println("drop-index" + " sql=" + sql)

    var conn: Connection = null
    try {
      val connProperties =
        ExternalStoreUtils.validateAndGetAllProps(sc, new mutable.HashMap[String, String])
      conn = ExternalStoreUtils.getConnection(connProperties.url, connProperties.connProps,
        JdbcDialects.get(connProperties.url), Utils.isLoner(sc))
      JdbcExtendedUtils.executeUpdate(sql, conn)
    } catch {
      case sqle: java.sql.SQLException =>
        if (sqle.getMessage.contains("No suitable driver found")) {
          throw new AnalysisException(s"${sqle.getMessage}\n" +
            "Ensure that the 'driver' option is set appropriately and " +
            "the driver jars available (--jars option in spark-submit).")
        } else {
          throw sqle
        }
    } finally {
      if (conn != null) {
        conn.close()
      }
    }
  }

  /**
   * Drop a temporary table from spark memory
   */
  def dropTempTable(tableName: String, ifExists: Boolean = false): Unit = {
    val qualifiedTable = catalog.newQualifiedTableName(tableName)
    val plan = try {
      catalog.lookupRelation(qualifiedTable, None)
    } catch {
      case ae: AnalysisException =>
        if (ifExists) return else throw ae
    }
    cacheManager.tryUncacheQuery(DataFrame(self, plan))
    catalog.unregisterTable(qualifiedTable)
  }

  /**
   * :: DeveloperApi ::
   * @todo why can't this be done using dropTable ?
   */
  @DeveloperApi
  def dropSampleTable(tableName: String, ifExists: Boolean = false): Unit = {

    val qualifiedTable = catalog.newQualifiedTableName(tableName)
    val plan = try {
      catalog.lookupRelation(qualifiedTable, None)
    } catch {
      case ae: AnalysisException =>
        if (ifExists) return else throw ae
    }
    cacheManager.tryUncacheQuery(DataFrame(self, plan))
    catalog.unregisterTable(qualifiedTable)
    this.aqpContext.dropSampleTable(tableName, ifExists)

  }

  /**
   * Insert one or more [[org.apache.spark.sql.Row]] into an existing table
   * @todo provide an example : insert a DF using foreachPartition...
   *       {{{
   *         someDataFrame.foreachPartition (x => snappyContext.insert
   *            ("MyTable", x.toSeq)
   *         )
   *       }}}
   * @param tableName
   * @param rows
   * @return
   */
  def insert(tableName: String, rows: Row*): Int = {
    val plan = catalog.lookupRelation(tableName)
    snappy.unwrapSubquery(plan) match {
      case LogicalRelation(r: RowInsertableRelation, _) => r.insert(rows)
      case _ => throw new AnalysisException(
        s"$tableName is not a row insertable table")
    }
  }

  /**
   * Update all rows in table that match passed filter expression
   * @todo provide an example
   * @param tableName
   * @param filterExpr
   * @param newColumnValues  A single Row containing all updated column
   *                         values. They MUST match the updateColumn list
   *                         passed
   * @param updateColumns   List of all column names being updated
   * @return
   */
  def update(tableName: String, filterExpr: String, newColumnValues: Row,
      updateColumns: String*): Int = {
    val plan = catalog.lookupRelation(tableName)
    snappy.unwrapSubquery(plan) match {
      case LogicalRelation(u: UpdatableRelation, _) =>
        u.update(filterExpr, newColumnValues, updateColumns)
      case _ => throw new AnalysisException(
        s"$tableName is not an updatable table")
    }
  }

  /**
   * Delete all rows in table that match passed filter expression
   *
   * @param tableName
   * @param filterExpr
   * @return
   */
  def delete(tableName: String, filterExpr: String): Int = {
    val plan = catalog.lookupRelation(tableName)
    snappy.unwrapSubquery(plan) match {
      case LogicalRelation(d: DeletableRelation, _) => d.delete(filterExpr)
      case _ => throw new AnalysisException(
        s"$tableName is not a deletable table")
    }
  }

  // end of insert/update/delete operations

  /**
   * :: DeveloperApi ::
   */
  @DeveloperApi
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      resultHandler: (Int, U) => Unit): Unit = {
    self.sc.runJob(rdd, processPartition, resultHandler)
  }

  @transient
  override protected[sql] lazy val analyzer: Analyzer =
    new Analyzer(catalog, functionRegistry, conf) {
      override val extendedResolutionRules =
        ExtractPythonUDFs ::
            sparkexecution.datasources.PreInsertCastAndRename ::
         //   ReplaceWithSampleTable ::
          //  WeightageRule ::
            //TestRule::
            Nil

      override val extendedCheckRules = Seq(
        sparkexecution.datasources.PreWriteCheck(catalog))
    }

  @transient
  override protected[sql] val planner = this.aqpContext.getPlanner(this)





  /**
    * Fetch the topK entries in the Approx TopK synopsis for the specified
   * time interval. See _createTopK_ for how to create this data structure
   * and associate this to a base table (i.e. the full data set). The time
   * interval specified here should not be less than the minimum time interval
   * used when creating the TopK synopsis.
   * @todo provide an example and explain the returned DataFrame. Key is the
   *       attribute stored but the value is a struct containing
   *       count_estimate, and lower, upper bounds? How many elements are
   *       returned if K is not specified?
    *
    * @param topKName - The topK structure that is to be queried.
    * @param startTime start time as string of the format "yyyy-mm-dd hh:mm:ss".
    *                  If passed as null, oldest interval is considered as the start interval.
    * @param endTime  end time as string of the format "yyyy-mm-dd hh:mm:ss".
    *                 If passed as null, newest interval is considered as the last interval.
    * @param k Optional. Number of elements to be queried.
    *          This is to be passed only for stream summary
    * @return returns the top K elements with their respective frequencies between two time
    */
  def queryTopK[T: ClassTag](topKName: String,
      startTime: String = null, endTime: String = null,
      k: Int = -1): DataFrame =
      aqpContext.queryTopK[T](this, topKName,
        startTime, endTime, k)


  /**
   * @todo why do we need this method? K is optional in the above method
   */
  def queryTopK[T: ClassTag](topKName: String,
      startTime: Long, endTime: Long): DataFrame =
    queryTopK[T](topKName, startTime, endTime, -1)

  def queryTopK[T: ClassTag](topK: String,
                             startTime: Long, endTime: Long, k: Int): DataFrame =
    aqpContext.queryTopK[T](this, topK, startTime, endTime, k)


}

/**
 * @todo document me
 */
object GlobalSnappyInit {
  @volatile private[this] var _globalSNContextInitialized: Boolean = false
  private[this] val contextLock = new AnyRef

  private[sql] def initGlobalSnappyContext(sc: SparkContext) = {
    if (!_globalSNContextInitialized ) {
      contextLock.synchronized {
        if (!_globalSNContextInitialized) {
          invokeServices(sc)
          _globalSNContextInitialized = true
        }
      }
    }
  }

  private[sql] def resetGlobalSNContext(): Unit =
    _globalSNContextInitialized = false

  private def invokeServices(sc: SparkContext): Unit = {
    SnappyContext.getClusterMode(sc) match {
      case SnappyEmbeddedMode(_, _) =>
        // NOTE: if Property.jobServer.enabled is true
        // this will trigger SnappyContext.apply() method
        // prior to `new SnappyContext(sc)` after this
        // method ends.
        ToolsCallbackInit.toolsCallback.invokeLeadStartAddonService(sc)
      case SnappyShellMode(_, _) =>
        ToolsCallbackInit.toolsCallback.invokeStartFabricServer(sc,
          hostData = false)
      case ExternalEmbeddedMode(_, url) =>
        SnappyContext.urlToConf(url, sc)
        ToolsCallbackInit.toolsCallback.invokeStartFabricServer(sc,
          hostData = false)
      case LocalMode(_, url) =>
        SnappyContext.urlToConf(url, sc)
        ToolsCallbackInit.toolsCallback.invokeStartFabricServer(sc,
          hostData = true)
      case _ => // ignore
    }
  }
}

object SnappyContext extends Logging {

  @volatile private[this] var _anySNContext: SnappyContext = _
  @volatile private[this] var _clusterMode: ClusterMode = _

  private val aqpContextImplClass = "org.apache.spark.sql.execution.AQPContextImpl"

  var SnappySC:SnappyContext = null

  private[this] val contextLock = new AnyRef

  val DEFAULT_SOURCE = "row"

  private val builtinSources = Map(
    "jdbc" -> classOf[row.DefaultSource].getCanonicalName,
    "column" -> classOf[columnar.DefaultSource].getCanonicalName,
    "row" -> "org.apache.spark.sql.rowtable.DefaultSource",
    "socket_stream" -> classOf[SocketStreamSource].getCanonicalName,
    "file_stream" -> classOf[FileStreamSource].getCanonicalName,
    "kafka_stream" -> classOf[KafkaStreamSource].getCanonicalName,
    "directkafka_stream" -> classOf[DirectKafkaStreamSource].getCanonicalName,
    "twitter_stream" -> classOf[TwitterStreamSource].getCanonicalName
  )

  def globalSparkContext: SparkContext = SparkContext.activeContext.get()

  private def newSnappyContext(sc: SparkContext) = {
    val snc = new SnappyContext(sc)
    // No need to synchronize. any occurrence would do
    if (_anySNContext == null) {
      _anySNContext = snc
    }
    snc
  }

  /**
   * @todo document me
   * @return
   */
  def apply(): SnappyContext = {
    val gc = globalSparkContext
    if (gc != null) {
      newSnappyContext(gc)
    } else {
      null
    }
  }

  /**
   * @todo document me
   * @param sc
   * @return
   */
  def apply(sc: SparkContext): SnappyContext = {
    if (sc != null) {
      newSnappyContext(sc)
    } else {
      apply()
    }
  }

  /**
   * @todo document me
   * @param sc
   * @return
   */
  def getOrCreate(sc: SparkContext): SnappyContext = {
    val gnc = _anySNContext
    if (gnc != null) gnc
    else contextLock.synchronized {
      val gnc = _anySNContext
      if (gnc != null) gnc
      else {
        apply(sc)
      }
    }
  }


  /**
   * @todo document me
   * @param url
   * @param sc
   */
  def urlToConf(url: String, sc: SparkContext): Unit = {
    val propValues = url.split(';')
    propValues.foreach { s =>
      val propValue = s.split('=')
      // propValue should always give proper result since the string
      // is created internally by evalClusterMode
      sc.conf.set(Constant.STORE_PROPERTY_PREFIX + propValue(0),
        propValue(1))
    }
  }

  /**
   * @todo document me
   * @param sc
   * @return
   */
  def getClusterMode(sc: SparkContext): ClusterMode = {
    val mode = _clusterMode
    if ((mode != null && mode.sc == sc) || sc == null) {
      mode
    } else if (mode != null) {
      evalClusterMode(sc)
    } else contextLock.synchronized {
      val mode = _clusterMode
      if ((mode != null && mode.sc == sc) || sc == null) {
        mode
      } else if (mode != null) {
        evalClusterMode(sc)
      } else {
        _clusterMode = evalClusterMode(sc)
        _clusterMode
      }
    }
  }

  private def evalClusterMode(sc: SparkContext): ClusterMode = {
    if (sc.master.startsWith(Constant.JDBC_URL_PREFIX)) {
      if (ToolsCallbackInit.toolsCallback == null) {
        throw new SparkException("Missing 'io.snappydata.ToolsCallbackImpl$'" +
            " from SnappyData tools package")
      }
      SnappyEmbeddedMode(sc,
        sc.master.substring(Constant.JDBC_URL_PREFIX.length))
    } else if (ToolsCallbackInit.toolsCallback != null) {
      val conf = sc.conf
      val local = Utils.isLoner(sc)
      val embedded = conf.getOption(Property.embedded).exists(_.toBoolean)
      conf.getOption(Property.locators).collectFirst {
        case s if !s.isEmpty =>
          val url = "locators=" + s + ";mcast-port=0"
          if (local) LocalMode(sc, url)
          else if (embedded) ExternalEmbeddedMode(sc, url)
          else SnappyShellMode(sc, url)
      }.orElse(conf.getOption(Property.mcastPort).collectFirst {
        case s if s.toInt > 0 =>
          val url = "mcast-port=" + s
          if (local) LocalMode(sc, url)
          else if (embedded) ExternalEmbeddedMode(sc, url)
          else SnappyShellMode(sc, url)
      }).getOrElse {
        if (local) LocalMode(sc, "mcast-port=0")
        else ExternalClusterMode(sc, sc.master)
      }
    } else {
      ExternalClusterMode(sc, sc.master)
    }
  }

  /**
   * @todo document me
   */
  def stop(): Unit = {
    val sc = globalSparkContext
    if (sc != null && !sc.isStopped) {
      // clean up the connection pool on executors first
      Utils.mapExecutors(sc, { (tc, p) =>
        ConnectionPool.clear()
        Iterator.empty
      }).count()
      // then on the driver
      ConnectionPool.clear()
      // clear current hive catalog connection
      SnappyStoreHiveCatalog.closeCurrent()
      if (ExternalStoreUtils.isShellOrLocalMode(sc)) {
        try {
          ToolsCallbackInit.toolsCallback.invokeStopFabricServer(sc)
        } catch {
          case se: SQLException if se.getCause.getMessage.indexOf(
            "No connection to the distributed system") != -1 => // ignore
        }
      }
      sc.stop()
    }
    _clusterMode = null
    _anySNContext = null
    GlobalSnappyInit.resetGlobalSNContext()
  }

  /**
   * Checks if the passed provider is recognized
   * @param providerName
   * @return
   */
  def getProvider(providerName: String): String =
    builtinSources.getOrElse(providerName, providerName)
}

// end of SnappyContext

abstract class ClusterMode {
  val sc: SparkContext
  val url: String
}

case class SnappyEmbeddedMode(override val sc: SparkContext,
    override val url: String) extends ClusterMode

case class SnappyShellMode(override val sc: SparkContext,
    override val url: String) extends ClusterMode

case class ExternalEmbeddedMode(override val sc: SparkContext,
    override val url: String) extends ClusterMode

case class LocalMode(override val sc: SparkContext,
    override val url: String) extends ClusterMode

case class ExternalClusterMode(override val sc: SparkContext,
    override val url: String) extends ClusterMode

/*
private[sql] case class SnappyOperations(context: SnappyContext,
    df: DataFrame) {

  /**
    * Creates stratified sampled data from given DataFrame
    * {{{
    *   peopleDf.stratifiedSample(Map("qcs" -> Array(1,2), "fraction" -> 0.01))
    * }}}
    */
  def stratifiedSample(options: Map[String, Any]): SampleDataFrame =
    new SampleDataFrame(context,
      context.aqpContext.convertToStratifiedSample(options, df.logicalPlan) )

  def createTopK(ident: String, options: Map[String, Any]): Unit =
    context.aqpContext.createTopK(df, context, ident, options)


  /**
    * Table must be registered using #registerSampleTable.
    */
  def insertIntoSampleTables(sampleTableName: String*): Unit =
    context.aqpContext.collectSamples(context, df.rdd, sampleTableName, System.currentTimeMillis())




  /**
    * Append to an existing cache table.
    * Automatically uses #cacheQuery if not done already.
    */
  def appendToCache(tableName: String): Unit =  context.appendToCache(df, tableName)

}

private[sql] case class SnappyDStreamOperations[T: ClassTag](
    context: SnappyContext, ds: DStream[T]) {

  def saveStream(sampleTab: Seq[String],
      formatter: (RDD[T], StructType) => RDD[Row],
      schema: StructType,
      transform: RDD[Row] => RDD[Row] = null): Unit =
      context.aqpContext.saveStream(context, ds, sampleTab, formatter, schema, transform)



  def saveToExternalTable[A <: Product : Ty1peTag](externalTable: String,
      jdbcSource: Map[String, String]): Unit = {
    val schema: StructType = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    saveStreamToExternalTable(externalTable, schema, jdbcSource)
  }

  def saveToExternalTable(externalTable: String, schema: StructType,
      jdbcSource: Map[String, String]): Unit = {
    saveStreamToExternalTable(externalTable, schema, jdbcSource)
  }

  private def saveStreamToExternalTable(externalTable: String,
      schema: StructType, jdbcSource: Map[String, String]): Unit = {
    require(externalTable != null && externalTable.length > 0,
      "saveToExternalTable: expected non-empty table name")

    val tableIdent = context.catalog.newQualifiedTableName(externalTable)
    val externalStore = context.catalog.getExternalTable(jdbcSource)
    context.catalog.createExternalTableForCachedBatches(tableIdent.table,
      externalStore)
    val attributeSeq = schema.toAttributes

    val dummyDF = {
      val plan: LogicalRDD = LogicalRDD(attributeSeq,
        new DummyRDD(context))(context)
      DataFrame(context, plan)
    }

    context.catalog.tables.put(tableIdent, dummyDF.logicalPlan)
    context.cacheManager.cacheQuery_ext(dummyDF, Some(tableIdent.table),
      externalStore)

    ds.foreachRDD((rdd: RDD[T], time: Time) => {
      context.appendToCacheRDD(rdd, tableIdent.table, schema)
    })
  }
}*/

