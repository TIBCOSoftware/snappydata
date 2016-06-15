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

import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListener}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal
import scala.language.implicitConversions
import scala.reflect.runtime.{universe => u}

import io.snappydata.Constant

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.annotation.{Experimental, DeveloperApi}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{GenericRow, SortDirection, Descending, Ascending}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Union}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.hive.{SnappyStoreHiveCatalog, QualifiedTableName}
import org.apache.spark.sql.internal.{SnappySessionState, SnappySharedState}
import org.apache.spark.sql.row.GemFireXDDialect
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream


class SnappySession(@transient private val sc: SparkContext,
    @transient private val existingSharedState: Option[SnappySharedState])
    extends SparkSession(sc, existingSharedState) with Logging {
  self =>


  // initialize GemFireXDDialect so that it gets registered

  GemFireXDDialect.init()
  SnappyContext.initGlobalSnappyContext(sparkContext)


  /* ----------------------- *
   |  Session-related state  |
   * ----------------------- */

  /**
   * State shared across sessions, including the [[SparkContext]], cached data, listener,
   * and a catalog that interacts with external systems.
   */
  @transient
  private[spark] lazy override val sharedState: SnappySharedState = {
    existingSharedState.getOrElse(new SnappySharedState(sc))

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
      case NonFatal(e) =>
        new SnappySessionState(this)
    }

  }

  lazy val sessionCatalog = sessionState.catalog.asInstanceOf[SnappyStoreHiveCatalog]

  private[spark] val snappyContextFunctions = sessionState.contextFunctions

  snappyContextFunctions.registerAQPErrorFunctions(this)

  /**
   * A wrapped version of this session in the form of a [[SQLContext]], for backward compatibility.
   */
  @transient
  private[spark] val snappyContext: SnappyContext = new SnappyContext(this)

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

  private[sql] val queryHints: mutable.Map[String, String] = mutable.Map.empty

  def getPreviousQueryHints: Map[String, String] = Utils.immutableMap(queryHints)

  def clear(): Unit = {
    snappyContextFunctions.clear()
  }

  /**
   * :: DeveloperApi ::
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
        //check if the stream type is already a Row
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
      snappyContextFunctions.collectSamples(this, rddRows, aqpTables, time.milliseconds)
    })
  }

  /**
   * Append dataframe to cache table in Spark.
   *
   * @param df
   * @param table
   * @param storageLevel default storage level is MEMORY_AND_DISK
   * @return  @todo -> return type?
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
   * @param tableName full table name to be truncated
   */
  def truncateTable(tableName: String): Unit =
    truncateTable(sessionCatalog.newQualifiedTableName(tableName))

  /**
   * Empties the contents of the table without deleting the catalog entry.
   * @param tableIdent qualified name of table to be truncated
   */
  private[sql] def truncateTable(tableIdent: QualifiedTableName,
      ignoreIfUnsupported: Boolean = false): Unit = {
    val plan = sessionCatalog.lookupRelation(tableIdent)
    cacheManager.tryUncacheQuery(Dataset.ofRows(this, plan))
    plan match {
      case LogicalRelation(br, _, _) =>
        br match {
          case d: DestroyRelation => d.truncate()
        }
      case _ => if (!ignoreIfUnsupported) {
        throw new AnalysisException(s"Table $tableIdent cannot be truncated")
      }
    }
  }


  /**
   * Create a stratified sample table.
   * @todo provide lot more details and examples to explain creating and
   *       using sample tables with time series and otherwise
   * @param tableName the qualified name of the table
   * @param samplingOptions sampling options like QCS, reservoir size etc.
   * @param allowExisting When set to true it will ignore if a table with the same name is
   *                      present , else it will throw table exist exception
   */
  def createSampleTable(tableName: String,
      samplingOptions: Map[String, String],
      allowExisting: Boolean): DataFrame = {
    val plan = createTable(sessionCatalog.newQualifiedTableName(tableName),
      SnappyContext.SAMPLE_SOURCE, None, schemaDDL = None,
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists, samplingOptions,
      onlyBuiltIn = true, onlyExternal = false)
    Dataset.ofRows(this, plan)
  }

  /**
   * Create a stratified sample table. Java friendly version.
   * @todo provide lot more details and examples to explain creating and
   *       using sample tables with time series and otherwise
   * @param tableName the qualified name of the table
   * @param samplingOptions sampling options like QCS, reservoir size etc.
   * @param allowExisting When set to true it will ignore if a table with the same name is
   *                      present , else it will throw table exist exception
   */
  def createSampleTable(tableName: String,
      samplingOptions: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createSampleTable(tableName, samplingOptions.asScala.toMap, allowExisting)
  }


  /**
   * Create a stratified sample table.
   * @todo provide lot more details and examples to explain creating and
   *       using sample tables with time series and otherwise
   * @param tableName the qualified name of the table
   * @param schema schema of the table
   * @param samplingOptions sampling options like QCS, reservoir size etc.
   * @param allowExisting When set to true it will ignore if a table with the same name is
   *                      present , else it will throw table exist exception
   */
  def createSampleTable(tableName: String,
      schema: StructType,
      samplingOptions: Map[String, String],
      allowExisting: Boolean = false): DataFrame = {
    val plan = createTable(sessionCatalog.newQualifiedTableName(tableName),
      SnappyContext.SAMPLE_SOURCE, Some(schema), schemaDDL = None,
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists, samplingOptions,
      onlyBuiltIn = true, onlyExternal = false)
    Dataset.ofRows(this, plan)
  }

  /**
   * Create a stratified sample table. Java friendly version.
   * @todo provide lot more details and examples to explain creating and
   *       using sample tables with time series and otherwise
   * @param tableName the qualified name of the table
   * @param schema schema of the table
   * @param samplingOptions sampling options like QCS, reservoir size etc.
   * @param allowExisting When set to true it will ignore if a table with the same name is
   *                      present , else it will throw table exist exception
   */
  def createSampleTable(tableName: String,
      schema: StructType,
      samplingOptions: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createSampleTable(tableName, schema, samplingOptions.asScala.toMap, allowExisting)
  }


  /**
   * Create approximate structure to query top-K with time series support.
   * @todo provide lot more details and examples to explain creating and
   *       using TopK with time series
   * @param topKName the qualified name of the top-K structure
   * @param keyColumnName
   * @param inputDataSchema
   * @param topkOptions
   * @param allowExisting When set to true it will ignore if a table with the same name is
   *                      present , else it will throw table exist exception
   */
  def createApproxTSTopK(topKName: String, keyColumnName: String,
      inputDataSchema: StructType, topkOptions: Map[String, String],
      allowExisting: Boolean = false): DataFrame = {
    val plan = createTable(sessionCatalog.newQualifiedTableName(topKName),
      SnappyContext.TOPK_SOURCE, Some(inputDataSchema), schemaDDL = None,
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      topkOptions + ("key" -> keyColumnName),
      onlyBuiltIn = true, onlyExternal = false)
    Dataset.ofRows(this, plan)
  }

  /**
   * Create approximate structure to query top-K with time series support.
   * Java friendly api.
   * @todo provide lot more details and examples to explain creating and
   *       using TopK with time series
   * @param topKName the qualified name of the top-K structure
   * @param keyColumnName
   * @param inputDataSchema
   * @param topkOptions
   * @param allowExisting When set to true it will ignore if a table with the same name is
   *                      present , else it will throw table exist exception
   */
  def createApproxTSTopK(topKName: String, keyColumnName: String,
      inputDataSchema: StructType, topkOptions: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createApproxTSTopK(topKName, keyColumnName, inputDataSchema,
      topkOptions.asScala.toMap, allowExisting)
  }

  /**
   * Create approximate structure to query top-K with time series support.
   * @todo provide lot more details and examples to explain creating and
   *       using TopK with time series
   * @param topKName the qualified name of the top-K structure
   * @param keyColumnName
   * @param topkOptions
   * @param allowExisting When set to true it will ignore if a table with the same name is
   *                      present , else it will throw table exist exception
   */
  def createApproxTSTopK(topKName: String, keyColumnName: String,
      topkOptions: Map[String, String], allowExisting: Boolean): DataFrame = {
    val plan = createTable(sessionCatalog.newQualifiedTableName(topKName),
      SnappyContext.TOPK_SOURCE, None, schemaDDL = None,
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      topkOptions + ("key" -> keyColumnName),
      onlyBuiltIn = true, onlyExternal = false)
    Dataset.ofRows(this, plan)
  }

  /**
   * Create approximate structure to query top-K with time series support. Java
   * friendly api.
   * @todo provide lot more details and examples to explain creating and
   *       using TopK with time series
   * @param topKName the qualified name of the top-K structure
   * @param keyColumnName
   * @param topkOptions
   * @param allowExisting When set to true it will ignore if a table with the same name is
   *                      present , else it will throw table exist exception
   */
  def createApproxTSTopK(topKName: String, keyColumnName: String,
      topkOptions: java.util.Map[String, String], allowExisting: Boolean): DataFrame = {
    createApproxTSTopK(topKName, keyColumnName, topkOptions.asScala.toMap, allowExisting)
  }

  /**
   * Creates a Snappy managed table. Any relation providers (e.g. parquet, jdbc etc)
   * supported by Spark & Snappy can be created here. Unlike SqlContext.createExternalTable this
   * API creates a persistent catalog entry.
   *
   * {{{
   *
   * val airlineDF = snappyContext.createTable(stagingAirline, "parquet", Map("path" -> airlinefilePath))
   *
   * }}}
   * @param tableName Name of the table
   * @param provider  Provider name such as 'COLUMN', 'ROW', 'JDBC', 'PARQUET' etc.
   * @param options Properties for table creation
   * @param allowExisting When set to true it will ignore if a table with the same name is
   *                      present , else it will throw table exist exception
   * @return DataFrame for the table
   */
  def createTable(
      tableName: String,
      provider: String,
      options: Map[String, String],
      allowExisting: Boolean): DataFrame = {
    val plan = createTable(sessionCatalog.newQualifiedTableName(tableName), provider,
      userSpecifiedSchema = None, schemaDDL = None,
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      options,
      onlyBuiltIn = true, onlyExternal = false)
    Dataset.ofRows(this, plan)
  }

  /**
   * Creates a Snappy managed table. Any relation providers (e.g. parquet, jdbc etc)
   * supported by Spark & Snappy can be created here. Unlike SqlContext.createExternalTable this
   * API creates a persistent catalog entry.
   *
   * {{{
   *
   * val airlineDF = snappyContext.createTable(stagingAirline, "parquet", Map("path" -> airlinefilePath))
   *
   * }}}
   *
   * @param tableName Name of the table
   * @param provider  Provider name such as 'COLUMN', 'ROW', 'JDBC', 'PARQUET' etc.
   * @param options Properties for table creation
   * @param allowExisting When set to true it will ignore if a table with the same name is
   *                      present , else it will throw table exist exception
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
   * Creates a Snappy managed table. Any relation providers (e.g. parquet, jdbc etc)
   * supported by Spark & Snappy can be created here. Unlike SqlContext.createExternalTable this
   * API creates a persistent catalog entry.
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
   * @param tableName Name of the table
   * @param provider Provider name such as 'COLUMN', 'ROW', 'JDBC', 'PARQUET' etc.
   * @param schema   Table schema
   * @param options  Properties for table creation. See options list for different tables.
   *                 https://github.com/SnappyDataInc/snappydata/blob/master/docs/rowAndColumnTables.md
   * @param allowExisting When set to true it will ignore if a table with the same name is
   *                      present , else it will throw table exist exception
   * @return DataFrame for the table
   */
  def createTable(
      tableName: String,
      provider: String,
      schema: StructType,
      options: Map[String, String],
      allowExisting: Boolean = false): DataFrame = {
    val plan = createTable(sessionCatalog.newQualifiedTableName(tableName), provider,
      Some(schema), schemaDDL = None,
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      options,
      onlyBuiltIn = true, onlyExternal = false)
    Dataset.ofRows(this, plan)
  }

  /**
   * Creates a Snappy managed table. Any relation providers (e.g. parquet, jdbc etc)
   * supported by Spark & Snappy can be created here. Unlike SqlContext.createExternalTable this
   * API creates a persistent catalog entry.
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
   * @param tableName Name of the table
   * @param provider Provider name such as 'COLUMN', 'ROW', 'JDBC', 'PARQUET' etc.
   * @param schema   Table schema
   * @param options  Properties for table creation. See options list for different tables.
   *                 https://github.com/SnappyDataInc/snappydata/blob/master/docs/rowAndColumnTables.md
   * @param allowExisting When set to true it will ignore if a table with the same name is
   *                      present , else it will throw table exist exception
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
   * Creates a Snappy managed JDBC table which takes a free format ddl string. The ddl string
   * should adhere to syntax of underlying JDBC store. SnappyData ships with inbuilt JDBC store ,
   * which can be accessed by Row format data store.
   * The option parameter can take connection details.
   * Unlike SqlContext.createExternalTable this API creates a persistent catalog entry.
   *
   * {{{
   *    val props = Map(
   * "url" -> s"jdbc:derby:$path",
   * "driver" -> "org.apache.derby.jdbc.EmbeddedDriver",
   * "poolImpl" -> "tomcat",
   * "user" -> "app",
   * "password" -> "app"
   * )
   *
   *
   * val schemaDDL = "(OrderId INT NOT NULL PRIMARY KEY,ItemId INT, ITEMREF INT)"
   * snappyContext.createTable("jdbcTable", "jdbc", schemaDDL, props)
   *
   * Any DataFrame of the same schema can be inserted into the JDBC table using
   * DataFrameWriter Api.
   *
   * e.g.
   *
   * case class Data(col1: Int, col2: Int, col3: Int)
   *
   * val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
   * val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
   * val dataDF = snc.createDataFrame(rdd)
   * dataDF.write.format("jdbc").mode(SaveMode.Append).saveAsTable("jdbcTable")
   *
   * }}}
   *
   * @param tableName Name of the table
   * @param provider  Provider name 'ROW' and 'JDBC'.
   * @param schemaDDL Table schema as a string interpreted by provider
   * @param options   Properties for table creation. See options list for different tables.
   * https://github.com/SnappyDataInc/snappydata/blob/master/docs/rowAndColumnTables.md
   * @param allowExisting When set to true it will ignore if a table with the same name is
   * present , else it will throw table exist exception
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
    val plan = createTable(sessionCatalog.newQualifiedTableName(tableName), provider,
      userSpecifiedSchema = None, Some(schemaStr),
      if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists,
      options,
      onlyBuiltIn = true, onlyExternal = false)
    Dataset.ofRows(this, plan)
  }

  /**
   * Creates a Snappy managed JDBC table which takes a free format ddl string. The ddl string
   * should adhere to syntax of underlying JDBC store. SnappyData ships with inbuilt JDBC store ,
   * which can be accessed by Row format data store.
   * The option parameter can take connection details.
   * Unlike SqlContext.createExternalTable this API creates a persistent catalog entry.
   *
   * {{{
   *    val props = Map(
   * "url" -> s"jdbc:derby:$path",
   * "driver" -> "org.apache.derby.jdbc.EmbeddedDriver",
   * "poolImpl" -> "tomcat",
   * "user" -> "app",
   * "password" -> "app"
   * )
   *
   *
   * val schemaDDL = "(OrderId INT NOT NULL PRIMARY KEY,ItemId INT, ITEMREF INT)"
   * snappyContext.createTable("jdbcTable", "jdbc", schemaDDL, props)
   *
   * Any DataFrame of the same schema can be inserted into the JDBC table using
   * DataFrameWriter Api.
   *
   * e.g.
   *
   * case class Data(col1: Int, col2: Int, col3: Int)
   *
   * val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
   * val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
   * val dataDF = snc.createDataFrame(rdd)
   * dataDF.write.format("jdbc").mode(SaveMode.Append).saveAsTable("jdbcTable")
   *
   * }}}
   *
   * @param tableName Name of the table
   * @param provider  Provider name 'ROW' and 'JDBC'.
   * @param schemaDDL Table schema as a string interpreted by provider
   * @param options   Properties for table creation. See options list for different tables.
   * https://github.com/SnappyDataInc/snappydata/blob/master/docs/rowAndColumnTables.md
   * @param allowExisting When set to true it will ignore if a table with the same name is
   * present , else it will throw table exist exception
   * @return DataFrame for the table
   */

  @Experimental
  def createTable(
      tableName: String,
      provider: String,
      schemaDDL: String,
      options: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createTable(tableName, provider, schemaDDL, options.asScala.toMap, allowExisting)
  }

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
      onlyBuiltIn: Boolean,
      onlyExternal: Boolean): LogicalPlan = {

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

    val schema = userSpecifiedSchema.map(sessionCatalog.normalizeSchema)
    val source = if (onlyExternal) provider
    else SnappyContext.getProvider(provider, onlyBuiltIn)

    val relation = schemaDDL match {
      case Some(cols) => JdbcExtendedUtils.externalResolvedDataSource(self,
        cols, source, mode, params)

      case None =>
        // add allowExisting in properties used by some implementations
        DataSource(
          self,
          userSpecifiedSchema = schema,
          className = source,
          options = params + (JdbcExtendedUtils.ALLOW_EXISTING_PROPERTY ->
              (mode != SaveMode.ErrorIfExists).toString)).resolveRelation(true)
    }

    val plan = LogicalRelation(relation)
    sessionCatalog.registerDataSourceTable(tableIdent, schema, Array.empty[String],
      source, params, relation)
    snappyContextFunctions.postRelationCreation(relation, this)
    plan
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
      query: LogicalPlan,
      onlyBuiltIn: Boolean,
      onlyExternal: Boolean): LogicalPlan = {

    var data = Dataset.ofRows(this, query)
    if (sessionCatalog.tableExists(tableIdent)) {
      mode match {
        case SaveMode.ErrorIfExists =>
          throw new AnalysisException(s"Table $tableIdent already exists. " +
              "If using SQL CREATE TABLE, you need to use the " +
              s"APPEND or OVERWRITE mode, or drop $tableIdent first.")
        case SaveMode.Ignore => return sessionCatalog.lookupRelation(tableIdent, None)
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

    val source = if (onlyExternal) provider
    else SnappyContext.getProvider(provider, onlyBuiltIn)



    val relation = DataSource(
      self,
      partitionColumns = partitionColumns,
      className = source,
      options = params).write(mode, data)

    if (sessionCatalog.tableExists(tableIdent) && mode == SaveMode.Overwrite) {
      // uncache the previous results and don't register again
      cacheManager.tryUncacheQuery(data)
    }
    else {
      sessionCatalog.registerDataSourceTable(tableIdent, Some(data.schema),
        partitionColumns, source, params, relation)
    }
    snappyContextFunctions.postRelationCreation(relation, this)
    LogicalRelation(relation)
  }

  /**
   * Drop a SnappyData table created by a call to SnappyContext.createTable
   * @param tableName table to be dropped
   * @param ifExists  attempt drop only if the table exists
   */
  def dropTable(tableName: String, ifExists: Boolean = false): Unit =
    dropTable(sessionCatalog.newQualifiedTableName(tableName), ifExists)

  /**
   * Drop a SnappyData table created by a call to SnappyContext.createTable
   * @param tableIdent table to be dropped
   * @param ifExists  attempt drop only if the table exists
   */
  private[sql] def dropTable(tableIdent: QualifiedTableName,
      ifExists: Boolean): Unit = {
    val plan = try {
      sessionCatalog.lookupRelation(tableIdent)
    } catch {
      case tnfe: TableNotFoundException =>
        if (ifExists) return else throw tnfe
      case NonFatal(_) =>
        // table loading may fail due to an initialization exception
        // in relation, so try to remove from hive catalog in any case
        try {
          sessionCatalog.unregisterDataSourceTable(tableIdent, None)
          return
        } catch {
          case NonFatal(e) =>
            if (ifExists) return
            else throw new TableNotFoundException(
              s"Table Not Found: $tableIdent", e)
        }
    }
    // additional cleanup for external tables, if required
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
        cacheManager.tryUncacheQuery(Dataset.ofRows(this, plan))
        sessionCatalog.unregisterDataSourceTable(tableIdent, Some(br))
        br match {
          case d: DestroyRelation => d.destroy(ifExists)
          case _ => // Do nothing
        }
      case _ =>
        // assume a temporary table
        cacheManager.tryUncacheQuery(Dataset.ofRows(this, plan))
        sessionCatalog.unregisterTable(tableIdent)
    }
  }

  /**
   * Create an index on a table.
   * @param indexName Index name which goes in the catalog
   * @param baseTable Fully qualified name of table on which the index is created.
   * @param indexColumns Columns on which the index has to be created along with the
   *                     sorting direction.The direction of index will be ascending
   *                     if value is true and descending when value is false.
   *                     Direction can be specified as null
   * @param options Options for indexes. For e.g.
   *                column table index - ("COLOCATE_WITH"->"CUSTOMER").
   *                row table index - ("INDEX_TYPE"->"GLOBAL HASH") or ("INDEX_TYPE"->"UNIQUE")
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
   * @param indexName Index name which goes in the catalog
   * @param baseTable Fully qualified name of table on which the index is created.
   * @param indexColumns Columns on which the index has to be created with the
   *                     direction of sorting. Direction can be specified as None.
   * @param options Options for indexes. For e.g.
   *                column table index - ("COLOCATE_WITH"->"CUSTOMER").
   *                row table index - ("INDEX_TYPE"->"GLOBAL HASH") or ("INDEX_TYPE"->"UNIQUE")
   */
  def createIndex(indexName: String,
      baseTable: String,
      indexColumns: Map[String, Option[SortDirection]],
      options: Map[String, String]): Unit = {

    val tableIdent = sessionCatalog.newQualifiedTableName(baseTable)
    val indexIdent = sessionCatalog.newQualifiedTableName(indexName)

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
  }

  private[sql] def getIndexTable(indexIdent: QualifiedTableName): QualifiedTableName = {
    new QualifiedTableName(Some(Constant.INTERNAL_SCHEMA_NAME),
      indexIdent.table)
  }

  private def constructDropSQL(indexName: String,
      ifExists: Boolean): String = {

    val ifExistsClause = if (ifExists) "IF EXISTS" else ""

    return s"DROP INDEX $ifExistsClause $indexName"
  }

  /**
   * Drops an index on a table
   * @param indexName Index name which goes in catalog
   * @param ifExists Drop if exists, else exit gracefully
   */
  def dropIndex(indexName: String, ifExists: Boolean): Unit = {

    val indexIdent = getIndexTable(sessionCatalog.newQualifiedTableName(indexName))

    // Since the index does not exist in catalog, it may be a row table index.
    if (!sessionCatalog.tableExists(indexIdent)) {
      dropRowStoreIndex(indexName, ifExists)
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
  }

  private def dropRowStoreIndex(indexName: String, ifExists: Boolean): Unit = {
    val connProperties = ExternalStoreUtils.validateAndGetAllProps(
      sparkContext, new mutable.HashMap[String, String])
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
   * @param tableName
   * @param rows
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
   * @param tableName
   * @param rows
   * @return number of rows inserted
   */
  @Experimental
  def insert(tableName: String, rows: java.util.ArrayList[java.util.ArrayList[_]]): Int = {
    val convertedRowSeq: Seq[Row] = rows.asScala.map(row => convertListToRow(row)).toSeq
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
   * @param tableName
   * @param rows
   * @return
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
   * @param tableName    table name which needs to be updated
   * @param filterExpr    SQL WHERE criteria to select rows that will be updated
   * @param newColumnValues  A single Row containing all updated column
   *                         values. They MUST match the updateColumn list
   *                         passed
   * @param updateColumns   List of all column names being updated
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
        u.update(filterExpr, convertListToRow(newColumnValues), updateColumns.asScala.toSeq)
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
   * @param tableName
   * @param rows
   * @return
   */
  @Experimental
  def put(tableName: String, rows: java.util.ArrayList[java.util.ArrayList[_]]): Int = {
    sessionCatalog.lookupRelation(sessionCatalog.newQualifiedTableName(tableName)) match {
      case LogicalRelation(r: RowPutRelation, _, _) =>
        r.put(rows.asScala.map(row => convertListToRow(row)).toSeq)
      case _ => throw new AnalysisException(
        s"$tableName is not a row upsertable table")
    }
  }


  /**
   * Delete all rows in table that match passed filter expression
   *
   * @param tableName  table name
   * @param filterExpr SQL WHERE criteria to select rows that will be updated
   * @return  number of rows deleted
   */
  @DeveloperApi
  def delete(tableName: String, filterExpr: String): Int = {
    sessionCatalog.lookupRelation(sessionCatalog.newQualifiedTableName(tableName)) match {
      case LogicalRelation(d: DeletableRelation, _ , _) => d.delete(filterExpr)
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


}

object SnappySession {
  // TODO: hemant : Its clumsy design to use where SnappySession.Builder
  // is using the SparkSession object's functions. This needs to be revisited
  // once we decide on how our Builder API should be. We have made few variables
  // in SparkSession as protected from private. We should revert them if they are
  // not needed with the final API.
  class Builder extends SparkSession.Builder {
    /**
      * Gets an existing [[SparkSession]] or, if there is no existing one, creates a new
      * one based on the options set in this builder.
      *
      * This method first checks whether there is a valid thread-local SparkSession,
      * and if yes, return that one. It then checks whether there is a valid global
      * default SparkSession, and if yes, return that one. If no valid global default
      * SparkSession exists, the method creates a new SparkSession and assigns the
      * newly created SparkSession as the global default.
      *
      * In case an existing SparkSession is returned, the config options specified in
      * this builder will be applied to the existing SparkSession.
      *
      * @since 2.0.0
      */
    override def getOrCreate(): SnappySession = synchronized {
      // TODO: hemant - Is this needed for Snappy. Snappy should always use newSession.
      // Get the session from current thread's active session.
      var session = SparkSession.getActiveSession.get
      if ((session ne null) && !session.sparkContext.isStopped) {
        options.foreach { case (k, v) => session.conf.set(k, v) }
        if (options.nonEmpty) {
          logWarning("Use an existing SparkSession, some configuration may not take effect.")
        }
        return session.asInstanceOf[SnappySession]
      }

      // Global synchronization so we will only set the default session once.
      SparkSession.synchronized {
        // If the current thread does not have an active session, get it from the global session.
        session = SparkSession.getDefaultSession.get
        if ((session ne null) && !session.sparkContext.isStopped) {
          options.foreach { case (k, v) => session.conf.set(k, v) }
          if (options.nonEmpty) {
            logWarning("Use an existing SparkSession, some configuration may not take effect.")
          }
          return session.asInstanceOf[SnappySession]
        }

        // No active nor global default session. Create a new one.
        val sparkContext = userSuppliedContext.getOrElse {
          // set app name if not given
          if (!options.contains("spark.app.name")) {
            options += "spark.app.name" -> java.util.UUID.randomUUID().toString
          }

          val sparkConf = new SparkConf()
          options.foreach { case (k, v) => sparkConf.set(k, v) }
          val sc = SparkContext.getOrCreate(sparkConf)
          // maybe this is an existing SparkContext, update its SparkConf which maybe used
          // by SparkSession
          options.foreach { case (k, v) => sc.conf.set(k, v) }
          sc
        }
        session = new SnappySession(sparkContext, None)
        options.foreach { case (k, v) => session.conf.set(k, v) }
        SparkSession.setDefaultSession(session)

        // Register a successfully instantiated context to the singleton. This should be at the
        // end of the class definition so that the singleton is updated only if there is no
        // exception in the construction of the instance.
        sparkContext.addSparkListener(new SparkListener {
          override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
            SparkSession.setDefaultSession(null)
            SparkSession.sqlListener.set(null)
          }
        })
      }

      return session.asInstanceOf[SnappySession]
    }
  }

  def builder(): Builder = new SnappySession.Builder
}