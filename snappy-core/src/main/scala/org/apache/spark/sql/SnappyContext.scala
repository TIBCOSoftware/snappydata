package org.apache.spark.sql

import java.sql.Connection

import org.apache.spark.util.ShutdownHookManager

import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.{universe => u}

import io.snappydata.{Constant, Property}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.LockUtils.ReadWriteLock
import org.apache.spark.sql.approximate.TopKUtil
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, ScalaReflection}
import org.apache.spark.sql.collection.{ToolsCallbackInit, UUIDRegionKey, Utils}
import org.apache.spark.sql.columnar._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.{LogicalRelation, ResolvedDataSource}
import org.apache.spark.sql.execution.streamsummary.StreamSummaryAggregation
import org.apache.spark.sql.hive.{ExternalTableType, QualifiedTableName, SnappyStoreHiveCatalog}
import org.apache.spark.sql.row.GemFireXDDialect
import org.apache.spark.sql.snappy.RDDExtensions
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.{Logging, SparkContext, SparkException, TaskContext}

/**
  * An instance of the Spark SQL execution engine that delegates to supplied
  * SQLContext offering additional capabilities.
  *
  * Created by Soubhik on 5/13/15.
  */
class SnappyContext private(sc: SparkContext)
    extends SQLContext(sc) with Serializable with Logging {

  self =>

  // initialize GemFireXDDialect so that it gets registered
  GemFireXDDialect.init()
  GlobalSnappyInit.initGlobalSnappyContext(sc)

  @transient
  override protected[sql] val ddlParser = new SnappyDDLParser(sqlParser.parse)

  @transient
  val topKLocks = scala.collection.mutable.Map[String, ReadWriteLock]()

  override protected[sql] def dialectClassName = if (conf.dialect == "sql") {
    classOf[SnappyParserDialect].getCanonicalName
  } else {
    conf.dialect
  }

  @transient
  override lazy val catalog = new SnappyStoreHiveCatalog(self)

  @transient
  override protected[sql] val cacheManager = new SnappyCacheManager(self)


  def saveStream[T: ClassTag](stream: DStream[T],
      aqpTables: Seq[String],
      formatter: (RDD[T], StructType) => RDD[Row],
      schema: StructType,
      transform: RDD[Row] => RDD[Row] = null) {
    stream.foreachRDD((rdd: RDD[T], time: Time) => {

      val rddRows = formatter(rdd, schema)

      val rows = if (transform != null) {
        transform(rddRows)
      } else rddRows

      collectSamples(rows, aqpTables, time.milliseconds)
    })
  }

  protected[sql] def collectSamples(rows: RDD[Row], aqpTables: Seq[String],
      time: Long,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK) {
    val useCompression = conf.useCompression
    val columnBatchSize = conf.columnBatchSize
    val aqpTableNames = mutable.Set(aqpTables.map(
      catalog.newQualifiedTableName): _*)

    val sampleTables = catalog.tables.collect {
      case (name, sample: StratifiedSample) if aqpTableNames.contains(name) =>
        aqpTableNames.remove(name)
        (name, sample.options, sample.schema, sample.output,
            cacheManager.lookupCachedData(sample).getOrElse(sys.error(
              s"SnappyContext.saveStream: failed to lookup cached plan for " +
                  s"sampling table $name")).cachedRepresentation)
    }

    val topKWrappers = catalog.topKStructures.filter {
      case (name, topkstruct) => aqpTableNames.remove(name)
    }

    if (aqpTableNames.nonEmpty) {
      throw new IllegalArgumentException("collectSamples: no sampling or " +
          s"topK structures for ${aqpTableNames.mkString(", ")}")
    }

    // TODO: this iterates rows multiple times
    val rdds = sampleTables.map {
      case (name, samplingOptions, schema, output, relation) =>
        (relation, rows.mapPartitionsPreserve(rowIterator => {
          val sampler = StratifiedSampler(samplingOptions, Array.emptyIntArray,
            nameSuffix = "", columnBatchSize, schema, cached = true)
          // create a new holder for set of CachedBatches
          val batches = ExternalStoreRelation(useCompression,
            columnBatchSize, name, schema, relation, output)
          sampler.append(rowIterator, (), batches.appendRow, batches.endRows)
          batches.forceEndOfBatch().iterator
        }))
    }
    // TODO: A different set of job is created for topK structure

    topKWrappers.foreach {
      case (name, (topKWrapper, topkRDD)) =>
        val clazz = Utils.getInternalType(
          topKWrapper.schema(topKWrapper.key.name).dataType)
        val ct = ClassTag(clazz)
        TopKUtil.populateTopK(rows, topKWrapper, self,
          name, topkRDD, time)(ct)
    }

    // add to list in relation
    // TODO: avoid a separate job for each RDD and instead try to do it
    // TODO: using a single UnionRDD or something
    rdds.foreach {
      case (relation, rdd) =>
        val cached = rdd.persist(storageLevel)
        if (cached.count() > 0) {
          relation match {
            case externalStore: ExternalStoreRelation =>
              externalStore.appendUUIDBatch(cached.asInstanceOf[RDD[UUIDRegionKey]])
            case appendable: InMemoryAppendableRelation =>
              appendable.appendBatch(cached.asInstanceOf[RDD[CachedBatch]])
          }
        }
    }
  }

  def appendToCache(df: DataFrame, table: String,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK) {
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

  def appendToCacheRDD(rdd: RDD[_], table: String, schema: StructType,
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
        tableIdent, schema, relation.cachedRepresentation, schema.toAttributes)

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

  def truncateTable(tableName: String): Unit = {
    cacheManager.lookupCachedData(catalog.lookupRelation(
      tableName)).foreach(_.cachedRepresentation.
        asInstanceOf[InMemoryAppendableRelation].truncate())
  }

  def truncateExternalTable(tableName: String): Unit = {
    val qualifiedTable = catalog.newQualifiedTableName(tableName)
    val plan = catalog.lookupRelation(qualifiedTable, None)
    snappy.unwrapSubquery(plan) match {
      case LogicalRelation(br) =>
        cacheManager.tryUncacheQuery(DataFrame(self, plan))
        br match {
          case d: DestroyRelation => d.truncate()
        }
      case _ => throw new AnalysisException(
        s"truncateExternalTable: Table $tableName not an external table")
    }
  }

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

    catalog.registerTable(Seq(tableName), plan)
  }

  def registerSampleTable(tableName: String, schema: StructType,
      samplingOptions: Map[String, Any], streamTable: Option[String] = None,
      jdbcSource: Option[Map[String, String]] = None): SampleDataFrame = {
    catalog.registerSampleTable(tableName, schema, samplingOptions,
      None, streamTable.map(catalog.newQualifiedTableName), jdbcSource)
  }

  def registerSampleTableOn[A <: Product : u.TypeTag](tableName: String,
      samplingOptions: Map[String, Any], streamTable: Option[String] = None,
      jdbcSource: Option[Map[String, String]] = None): DataFrame = {
    if (u.typeOf[A] =:= u.typeOf[Nothing]) {
      sys.error("Type of case class object not mentioned. " +
          "Mention type information for e.g. registerSampleTableOn[<class>]")
    }
    SparkPlan.currentContext.set(self)
    val schemaExtract = ScalaReflection.schemaFor[A].dataType
        .asInstanceOf[StructType]
    registerSampleTable(tableName, schemaExtract, samplingOptions,
      streamTable, jdbcSource)
  }

  def registerTopK(tableName: String, streamTableName: String,
      topkOptions: Map[String, Any], isStreamSummary: Boolean): Unit = {
    val topKRDD = TopKUtil.createTopKRDD(tableName, self.sc, isStreamSummary)
    catalog.registerTopK(tableName, streamTableName,
      catalog.getStreamTableRelation(streamTableName).schema, topkOptions, topKRDD)
  }

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
    * Drop an external table created by a call to createExternalTable.
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
      case LogicalRelation(br) =>
        cacheManager.tryUncacheQuery(DataFrame(self, plan))
        catalog.unregisterExternalTable(qualifiedTable)
        br match {
          case d: DestroyRelation => d.destroy(ifExists)
        }
      case _ => throw new AnalysisException(
        s"dropExternalTable: Table $tableName not an external table")
    }
  }

  /**
   * Create Index on an external table (created by a call to createExternalTable).
   */
  def createIndexOnExternalTable(tableName: String, sql: String): Unit = {
    //println("create-index" + " tablename=" + tableName    + " ,sql=" + sql)

    if (!catalog.tableExists(tableName)) {
      throw new AnalysisException(
        s"$tableName is not an indexable table")
    }

    val qualifiedTable = catalog.newQualifiedTableName(tableName)
    //println("qualifiedTable=" + qualifiedTable)
    snappy.unwrapSubquery(catalog.lookupRelation(qualifiedTable, None)) match {
      case LogicalRelation(i: IndexableRelation) =>
        i.createIndex(tableName, sql)
      case _ => throw new AnalysisException(
        s"$tableName is not an indexable table")
    }
  }

  /**
   * Create Index on an external table (created by a call to createExternalTable).
   */
  def dropIndexOnExternalTable(sql: String): Unit = {
    //println("drop-index" + " sql=" + sql)

    var conn: Connection = null
    try {
      val (url, _, _, connProps, _) =
        ExternalStoreUtils.validateAndGetAllProps(sc, new mutable.HashMap[String, String])
      conn = ExternalStoreUtils.getConnection(url, connProps)
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
   * Drop a temporary table.
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

  // insert/update/delete operations on an external table

  def insert(tableName: String, rows: Row*): Int = {
    catalog.lookupRelation(tableName) match {
      case LogicalRelation(r: RowInsertableRelation) => r.insert(rows)
      case _ => throw new AnalysisException(
        s"$tableName is not a row insertable table")
    }
  }

  def update(tableName: String, filterExpr: String, newColumnValues: Row,
      updateColumns: String*): Int = {
    catalog.lookupRelation(tableName) match {
      case LogicalRelation(u: UpdatableRelation) =>
        u.update(filterExpr, newColumnValues, updateColumns)
      case _ => throw new AnalysisException(
        s"$tableName is not an updatable table")
    }
  }

  def delete(tableName: String, filterExpr: String): Int = {
    catalog.lookupRelation(tableName) match {
      case LogicalRelation(d: DeletableRelation) => d.delete(filterExpr)
      case _ => throw new AnalysisException(
        s"$tableName is not a deletable table")
    }
  }

  // end of insert/update/delete operations

  @transient
  override protected[sql] lazy val analyzer: Analyzer =
    new Analyzer(catalog, functionRegistry, conf) {
      override val extendedResolutionRules =
        ExtractPythonUDFs ::
            datasources.PreInsertCastAndRename ::
            WeightageRule ::
            Nil

      override val extendedCheckRules = Seq(
        datasources.PreWriteCheck(catalog))
    }

  @transient
  override protected[sql] val planner = new execution.SparkPlanner(this) {
    val snappyContext = self

    override def strategies: Seq[Strategy] = Seq(
      SnappyStrategies, StreamStrategy, StoreStrategy) ++ super.strategies

    object SnappyStrategies extends Strategy {
      def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
        case s@StratifiedSample(options, child, _) =>
          s.getExecution(planLater(child)) :: Nil
        case PhysicalOperation(projectList, filters,
        mem: columnar.InMemoryAppendableRelation) =>
          pruneFilterProject(
            projectList,
            filters,
            identity[Seq[Expression]], // All filters still need to be evaluated
            InMemoryAppendableColumnarTableScan(_, filters, mem)) :: Nil
        case _ => Nil
      }
    }

  }

  /**
    * Queries the topK structure between two points in time. If the specified
    * time lies between a topK interval the whole interval is considered
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
      k: Int = -1): DataFrame = {
    val stime = if (startTime == null) 0L
    else CastLongTime.getMillis(java.sql.Timestamp.valueOf(startTime))

    val etime = if (endTime == null) Long.MaxValue
    else CastLongTime.getMillis(java.sql.Timestamp.valueOf(endTime))

    queryTopK[T](topKName, stime, etime, k)
  }

  def queryTopK[T: ClassTag](topKName: String,
      startTime: Long, endTime: Long): DataFrame =
    queryTopK[T](topKName, startTime, endTime, -1)

  def queryTopK[T: ClassTag](topK: String,
      startTime: Long, endTime: Long, k: Int): DataFrame = {
    val topKIdent = catalog.newQualifiedTableName(topK)
    topKLocks(topKIdent.toString()).executeInReadLock {
      val (topkWrapper, rdd) = catalog.topKStructures(topKIdent)
      // requery the catalog to obtain the TopKRDD

      val size = if (k > 0) k else topkWrapper.size

      val topKName = topKIdent.table
      if (topkWrapper.stsummary) {
        queryTopkStreamSummary(topKName, startTime, endTime, topkWrapper, size, rdd)
      } else {
        queryTopkHokusai(topKName, startTime, endTime, topkWrapper, rdd, size)

      }
    }
  }

  def queryTopkStreamSummary[T: ClassTag](topKName: String,
      startTime: Long, endTime: Long,
      topkWrapper: TopKWrapper, k: Int, topkRDD: RDD[(Int, TopK)]): DataFrame = {
    val rdd = topkRDD.mapPartitionsPreserve[(T, Approximate)] { iter => {
      iter.next()._2 match {
        case x: StreamSummaryAggregation[_] =>
          val arrayTopK = x.asInstanceOf[StreamSummaryAggregation[T]]
              .getTopKBetweenTime(startTime, endTime, x.capacity)
          arrayTopK.map(_.toIterator).getOrElse(Iterator.empty)
        case _ => Iterator.empty
      }
    }
    }
    val topKRDD = rdd.reduceByKey(_ + _).mapPreserve {
      case (key, approx) =>
        Row(key, approx.estimate, approx.lowerBound)
    }

    val aggColumn = "EstimatedValue"
    val errorBounds = "DeltaError"
    val topKSchema = StructType(Array(topkWrapper.key,
      StructField(aggColumn, LongType),
      StructField(errorBounds, LongType)))

    val df = createDataFrame(topKRDD, topKSchema)
    df.sort(df.col(aggColumn).desc).limit(k)
  }

  def queryTopkHokusai[T: ClassTag](topKName: String,
      startTime: Long, endTime: Long,
      topkWrapper: TopKWrapper, topkRDD: RDD[(Int, TopK)], k: Int): DataFrame = {

    // TODO: perhaps this can be done more efficiently via a shuffle but
    // using the straightforward approach for now

    // first collect keys from across the cluster
    val rdd = topkRDD.mapPartitionsPreserve[(T, Approximate)] { iter =>
      iter.next()._2 match {
        case x: TopKHokusai[_] =>
          val arrayTopK = if (x.windowSize == Long.MaxValue) {
            Some(x.asInstanceOf[TopKHokusai[T]].getTopKInCurrentInterval)
          }
          else {
            x.asInstanceOf[TopKHokusai[T]].getTopKBetweenTime(startTime,
              endTime)
          }

          arrayTopK.map(_.toIterator).getOrElse(Iterator.empty)
        case _ => Iterator.empty
      }
    }
    val topKRDD = rdd.reduceByKey(_ + _).mapPreserve {
      case (key, approx) =>
        Row(key, approx.estimate, approx)
    }

    val aggColumn = "EstimatedValue"
    val errorBounds = "ErrorBoundsInfo"
    val topKSchema = StructType(Array(topkWrapper.key,
      StructField(aggColumn, LongType),
      StructField(errorBounds, ApproximateType)))

    val df = createDataFrame(topKRDD, topKSchema)
    df.sort(df.col(aggColumn).desc).limit(k)
  }

  private var storeConfig: Map[String, String] = _

  def setExternalStoreConfig(conf: Map[String, String]): Unit = {
    self.storeConfig = conf
  }

  def getExternalStoreConfig: Map[String, String] = {
    storeConfig
  }

  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      resultHandler: (Int, U) => Unit): Unit = {
    self.sc.runJob(rdd, processPartition, resultHandler)
  }
}

// scalastyle:off
object snappy extends Serializable {
  // scalastyle:on

  implicit def snappyOperationsOnDataFrame(df: DataFrame): SnappyOperations = {
    df.sqlContext match {
      case sc: SnappyContext => SnappyOperations(sc, df)
      case sc => throw new AnalysisException("Extended snappy operations " +
          s"require SnappyContext and not ${sc.getClass.getSimpleName}")
    }
  }

  def unwrapSubquery(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case Subquery(_, child) => unwrapSubquery(child)
      case _ => plan
    }
  }

  implicit def samplingOperationsOnDataFrame(df: DataFrame): SampleDataFrame = {
    df.sqlContext match {
      case sc: SnappyContext =>
        unwrapSubquery(df.logicalPlan) match {
          case ss: StratifiedSample =>
            new SampleDataFrame(sc, ss)
          case s => throw new AnalysisException("Stratified sampling " +
              "operations require stratifiedSample plan and not " +
              s"${s.getClass.getSimpleName}")
        }
      case sc => throw new AnalysisException("Extended snappy operations " +
          s"require SnappyContext and not ${sc.getClass.getSimpleName}")
    }
  }

  implicit def snappyOperationsOnDStream[T: ClassTag](
      ds: DStream[T]): SnappyDStreamOperations[T] =
    SnappyDStreamOperations(SnappyContext(ds.context.sparkContext), ds)

  implicit class SparkContextOperations(val s: SparkContext) {
    def getOrCreateStreamingContext(batchInterval: Int = 2): StreamingContext = {
      StreamingCtxtHolder(s, batchInterval)
    }
  }

  implicit class RDDExtensions[T: ClassTag](rdd: RDD[T]) extends Serializable {

    /**
      * Return a new RDD by applying a function to all elements of this RDD.
      */
    def mapPreserve[U: ClassTag](f: T => U): RDD[U] = rdd.withScope {
      val cleanF = rdd.sparkContext.clean(f)
      new MapPartitionsPreserveRDD[U, T](rdd,
        (context, pid, iter) => iter.map(cleanF))
    }

    /**
      * Return a new RDD by applying a function to each partition of given RDD.
      * This variant also preserves the preferred locations of parent RDD.
      *
      * `preservesPartitioning` indicates whether the input function preserves
      * the partitioner, which should be `false` unless this is a pair RDD and
      * the input function doesn't modify the keys.
      */
    def mapPartitionsPreserve[U: ClassTag](
        f: Iterator[T] => Iterator[U],
        preservesPartitioning: Boolean = false): RDD[U] = rdd.withScope {
      val cleanedF = rdd.sparkContext.clean(f)
      new MapPartitionsPreserveRDD(rdd,
        (context: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(iter),
        preservesPartitioning)
    }
  }

}

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

  private[sql] def  resetGlobalSNContext:Unit = _globalSNContextInitialized=false

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

  private[this] val contextLock = new AnyRef

  private val builtinSources = Map(
    "jdbc" -> classOf[row.DefaultSource].getCanonicalName,
    "row" -> "org.apache.spark.sql.rowtable.DefaultSource",
    "column" -> classOf[columnar.DefaultSource].getCanonicalName
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

  def apply(): SnappyContext = {
    val gc = globalSparkContext
    if (gc != null) {
      newSnappyContext(gc)
    } else {
      null
    }
  }

  def apply(sc: SparkContext): SnappyContext = {
    if (sc != null) {
      newSnappyContext(sc)
    } else {
      apply()
    }
  }

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
        throw new SparkException(
          "Missing 'io.snappydata.ToolsCallbackImpl$' from SnappyData tools package")
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
      if (ExternalStoreUtils.isExternalShellMode(sc)) {
        ToolsCallbackInit.toolsCallback.invokeStopFabricServer(sc)
      }
      sc.stop()
    }
    _clusterMode = null
    _anySNContext = null
    GlobalSnappyInit.resetGlobalSNContext
  }

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

private[sql] case class SnappyOperations(context: SnappyContext,
    df: DataFrame) {

  /**
    * Creates stratified sampled data from given DataFrame
    * {{{
    *   peopleDf.stratifiedSample(Map("qcs" -> Array(1,2), "fraction" -> 0.01))
    * }}}
    */
  def stratifiedSample(options: Map[String, Any]): SampleDataFrame =
    new SampleDataFrame(context, StratifiedSample(options, df.logicalPlan)())

  def createTopK(ident: String, options: Map[String, Any]): Unit = {
    val name = context.catalog.newQualifiedTableName(ident)
    val schema = df.logicalPlan.schema

    // Create a very long timeInterval when the topK is being created
    // on a DataFrame.

    val topKWrapper = TopKWrapper(name, options, schema)

    val clazz = Utils.getInternalType(
      topKWrapper.schema(topKWrapper.key.name).dataType)
    val ct = ClassTag(clazz)
    context.topKLocks += name.toString() -> new ReadWriteLock()
    val topKRDD = TopKUtil.createTopKRDD(name.toString,
      context.sparkContext, topKWrapper.stsummary)
    context.catalog.topKStructures.put(name, topKWrapper -> topKRDD)
    TopKUtil.populateTopK(df.rdd, topKWrapper, context,
      name, topKRDD, System.currentTimeMillis())(ct)

  }

  /**
    * Table must be registered using #registerSampleTable.
    */
  def insertIntoSampleTables(sampleTableName: String*): Unit =
    context.collectSamples(df.rdd, sampleTableName, System.currentTimeMillis())

  /**
    * Append to an existing cache table.
    * Automatically uses #cacheQuery if not done already.
    */
  def appendToCache(tableName: String): Unit = context.appendToCache(df, tableName)
}

private[sql] case class SnappyDStreamOperations[T: ClassTag](
    context: SnappyContext, ds: DStream[T]) {

  def saveStream(sampleTab: Seq[String],
      formatter: (RDD[T], StructType) => RDD[Row],
      schema: StructType,
      transform: RDD[Row] => RDD[Row] = null): Unit =
    context.saveStream(ds, sampleTab, formatter, schema, transform)

  def saveToExternalTable[A <: Product : TypeTag](externalTable: String,
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
}
