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

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.lang.reflect.Method

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.language.implicitConversions
import scala.reflect.runtime.{universe => u}

import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.util.ServiceUtils
import io.snappydata.{Constant, Property, StoreTableValueSizeProviderService}

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.catalyst.expressions.SortDirection
import org.apache.spark.sql.collection.{ToolsCallbackInit, Utils}
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.datasources.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.joins.HashedRelationCache
import org.apache.spark.sql.execution.ui.SnappyStatsTab
import org.apache.spark.sql.hive.{ExternalTableType, QualifiedTableName, SnappyStoreHiveCatalog}
import org.apache.spark.sql.internal.SnappySessionState
import org.apache.spark.sql.sources.SamplingRelation
import org.apache.spark.sql.store.CodeGeneration
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.{BlockManagerId, StorageLevel}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext, SparkEnv, SparkException}

/**
 * Main entry point for SnappyData extensions to Spark. A SnappyContext
 * extends Spark's [[org.apache.spark.sql.SQLContext]] to work with Row and
 * Column tables. Any DataFrame can be managed as SnappyData tables and any
 * table can be accessed as a DataFrame. This integrates the SQLContext
 * functionality with the Snappy store.
 *
 * When running in the '''embedded ''' mode (i.e. Spark executor collocated
 * with Snappy data store), Applications typically submit Jobs to the
 * Snappy-JobServer
 * (provide link) and do not explicitly create a SnappyContext. A single
 * shared context managed by SnappyData makes it possible to re-use Executors
 * across client connections or applications.
 *
 * SnappyContext uses a HiveMetaStore for catalog , which is
 * persistent. This enables table metadata info recreated on driver restart.
 *
 * User should use obtain reference to a SnappyContext instance as below
 * val snc: SnappyContext = SnappyContext.getOrCreate(sparkContext)
 *
 * @see https://github.com/SnappyDataInc/snappydata#step-1---start-the-snappydata-cluster
 * @see https://github.com/SnappyDataInc/snappydata#interacting-with-snappydata
 * @todo document describing the Job server API
 * @todo Provide links to above descriptions
 *
 */
class SnappyContext protected[spark](val snappySession: SnappySession)
    extends SQLContext(snappySession)
    with Serializable with Logging {

  self =>

  protected[spark] def this(sc: SparkContext) {
    this(new SnappySession(sc, None))
  }

  override def newSession(): SnappyContext =
    snappySession.newSession().snappyContext

  override def sessionState: SnappySessionState = snappySession.sessionState

  def clear(): Unit = {
    snappySession.clear()
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
    snappySession.saveStream(stream, aqpTables, transformer)
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
    snappySession.appendToTempTableCache(df, table, storageLevel)
  }

  /**
   * Empties the contents of the table without deleting the catalog entry.
   * @param tableName full table name to be truncated
   */
  def truncateTable(tableName: String): Unit = snappySession.truncateTable(tableName)


  /**
   * Empties the contents of the table without deleting the catalog entry.
   * @param tableIdent qualified name of table to be truncated
   */
  private[sql] def truncateTable(tableIdent: QualifiedTableName,
      ignoreIfUnsupported: Boolean = false): Unit = {
    snappySession.truncateTable(tableIdent, ignoreIfUnsupported)
  }


  /**
   * Create a stratified sample table.
   * @todo provide lot more details and examples to explain creating and
   *       using sample tables with time series and otherwise
   * @param tableName the qualified name of the table
   * @param baseTable the base table of the sample table, if any
   * @param samplingOptions sampling options like QCS, reservoir size etc.
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   */
  def createSampleTable(tableName: String,
      baseTable: Option[String],
      samplingOptions: Map[String, String],
      allowExisting: Boolean): DataFrame = {
    snappySession.createSampleTable(tableName, baseTable, samplingOptions,
      allowExisting)
  }

  /**
   * Create a stratified sample table. Java friendly version.
   * @todo provide lot more details and examples to explain creating and
   *       using sample tables with time series and otherwise
   * @param tableName the qualified name of the table
   * @param baseTable the base table of the sample table, if any, or null
   * @param samplingOptions sampling options like QCS, reservoir size etc.
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   */
  def createSampleTable(tableName: String,
      baseTable: String,
      samplingOptions: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createSampleTable(tableName, Option(baseTable),
      samplingOptions.asScala.toMap, allowExisting)
  }

  /**
   * Create a stratified sample table.
   * @todo provide lot more details and examples to explain creating and
   *       using sample tables with time series and otherwise
   * @param tableName the qualified name of the table
   * @param baseTable the base table of the sample table, if any
   * @param schema schema of the table
   * @param samplingOptions sampling options like QCS, reservoir size etc.
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   */
  def createSampleTable(tableName: String,
      baseTable: Option[String],
      schema: StructType,
      samplingOptions: Map[String, String],
      allowExisting: Boolean = false): DataFrame = {
    snappySession.createSampleTable(tableName, baseTable, schema,
      samplingOptions, allowExisting)
  }

  /**
   * Create a stratified sample table. Java friendly version.
   * @todo provide lot more details and examples to explain creating and
   *       using sample tables with time series and otherwise
   * @param tableName the qualified name of the table
   * @param baseTable the base table of the sample table, if any, or null
   * @param schema schema of the table
   * @param samplingOptions sampling options like QCS, reservoir size etc.
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   */
  def createSampleTable(tableName: String,
      baseTable: String,
      schema: StructType,
      samplingOptions: java.util.Map[String, String],
      allowExisting: Boolean): DataFrame = {
    createSampleTable(tableName, Option(baseTable), schema,
      samplingOptions.asScala.toMap, allowExisting)
  }

  /**
   * Create approximate structure to query top-K with time series support.
   * @todo provide lot more details and examples to explain creating and
   *       using TopK with time series
   * @param topKName the qualified name of the top-K structure
   * @param baseTable the base table of the top-K structure, if any
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
    snappySession.createApproxTSTopK(topKName, baseTable, keyColumnName,
      inputDataSchema, topkOptions, allowExisting)
  }

  /**
   * Create approximate structure to query top-K with time series support.
   * Java friendly api.
   * @todo provide lot more details and examples to explain creating and
   *       using TopK with time series
   * @param topKName the qualified name of the top-K structure
   * @param baseTable the base table of the top-K structure, if any, or null
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
   * @todo provide lot more details and examples to explain creating and
   *       using TopK with time series
   * @param topKName the qualified name of the top-K structure
   * @param baseTable the base table of the top-K structure, if any
   * @param keyColumnName
   * @param topkOptions
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   */
  def createApproxTSTopK(topKName: String, baseTable: Option[String],
      keyColumnName: String, topkOptions: Map[String, String],
      allowExisting: Boolean): DataFrame = {
    snappySession.createApproxTSTopK(topKName, baseTable,
      keyColumnName, topkOptions, allowExisting)
  }

  /**
   * Create approximate structure to query top-K with time series support. Java
   * friendly api.
   * @todo provide lot more details and examples to explain creating and
   *       using TopK with time series
   * @param topKName the qualified name of the top-K structure
   * @param baseTable the base table of the top-K structure, if any, or null
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
   * @param tableName Name of the table
   * @param provider  Provider name such as 'COLUMN', 'ROW', 'JDBC', 'PARQUET' etc.
   * @param options Properties for table creation
   * @param allowExisting When set to true it will ignore if a table with the same
   *                      name is present, else it will throw table exist exception
   * @return DataFrame for the table
   */
  def createTable(
      tableName: String,
      provider: String,
      options: Map[String, String],
      allowExisting: Boolean): DataFrame = {
    snappySession.createTable(tableName, provider, options, allowExisting)
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
   * @param tableName Name of the table
   * @param provider  Provider name such as 'COLUMN', 'ROW', 'JDBC', 'PARQUET' etc.
   * @param options Properties for table creation
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
   * @param tableName Name of the table
   * @param provider Provider name such as 'COLUMN', 'ROW', 'JDBC' etc.
   * @param schema   Table schema
   * @param options  Properties for table creation. See options list for different tables.
   *              https://github.com/SnappyDataInc/snappydata/blob/master/docs/rowAndColumnTables.md
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
    snappySession.createTable(tableName, provider, schema, options, allowExisting)
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
   * @param tableName Name of the table
   * @param provider Provider name such as 'COLUMN', 'ROW', 'JDBC' etc.
   * @param schema   Table schema
   * @param options  Properties for table creation. See options list for different tables.
   * https://github.com/SnappyDataInc/snappydata/blob/master/docs/rowAndColumnTables.md
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
   * @param tableName Name of the table
   * @param provider  Provider name 'ROW' or 'JDBC'.
   * @param schemaDDL Table schema as a string interpreted by provider
   * @param options   Properties for table creation. See options list for different tables.
   * https://github.com/SnappyDataInc/snappydata/blob/master/docs/rowAndColumnTables.md
   * @param allowExisting When set to true it will ignore if a table with the same
   * name is present, else it will throw table exist exception
   * @return DataFrame for the table
   */
  def createTable(
      tableName: String,
      provider: String,
      schemaDDL: String,
      options: Map[String, String],
      allowExisting: Boolean): DataFrame = {
    snappySession.createTable(tableName, provider, schemaDDL, options, allowExisting)
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
   * @param tableName Name of the table
   * @param provider  Provider name 'ROW' or 'JDBC'.
   * @param schemaDDL Table schema as a string interpreted by provider
   * @param options   Properties for table creation. See options list for different tables.
   * https://github.com/SnappyDataInc/snappydata/blob/master/docs/rowAndColumnTables.md
   * @param allowExisting When set to true it will ignore if a table with the same
   * name is present, else it will throw table exist exception
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
   * Drop a SnappyData table created by a call to SnappyContext.createTable,
   * createExternalTable or registerTempTable.
   *
   * @param tableName table to be dropped
   * @param ifExists  attempt drop only if the table exists
   */
  def dropTable(tableName: String, ifExists: Boolean = false): Unit =
    snappySession.dropTable(tableName, ifExists)

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
    snappySession.createIndex(indexName, baseTable, indexColumns, options)
  }

  /**
   * Set current database/schema.
   * @param schemaName schema name which goes in the catalog
   */
  def setSchema(schemaName: String): Unit = {
    snappySession.setSchema(schemaName)
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
    snappySession.createIndex(indexName, baseTable, indexColumns, options)
  }

  /**
   * Drops an index on a table
   * @param indexName Index name which goes in catalog
   * @param ifExists Drop if exists, else exit gracefully
   */
  def dropIndex(indexName: String, ifExists: Boolean): Unit = {
    snappySession.dropIndex(indexName, ifExists)
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
    snappySession.insert(tableName, rows: _*)
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
    snappySession.insert(tableName, rows)
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
    snappySession.put(tableName, rows: _*)
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
    snappySession.update(tableName, filterExpr, newColumnValues, updateColumns: _*)
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
    snappySession.update(tableName, filterExpr, newColumnValues, updateColumns)
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
    snappySession.put(tableName, rows)
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
    snappySession.delete(tableName, filterExpr)
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
    snappySession.queryApproxTSTopK(topKName,
      startTime, endTime, k)

  /**
   * @todo why do we need this method? K is optional in the above method
   */
  def queryApproxTSTopK(topKName: String,
      startTime: Long, endTime: Long): DataFrame =
    queryApproxTSTopK(topKName, startTime, endTime, -1)

  def queryApproxTSTopK(topK: String,
      startTime: Long, endTime: Long, k: Int): DataFrame =
    snappySession.queryApproxTSTopK(topK, startTime, endTime, k)

}


object SnappyContext extends Logging {

  @volatile private[this] var _anySNContext: SnappyContext = _
  @volatile private[this] var _clusterMode: ClusterMode = _

  @volatile private[this] var _globalSNContextInitialized: Boolean = false
  private[this] val contextLock = new AnyRef

  val COLUMN_SOURCE = "column"
  val ROW_SOURCE = "row"
  val SAMPLE_SOURCE = "column_sample"
  val TOPK_SOURCE = "approx_topk"

  val DEFAULT_SOURCE = ROW_SOURCE

  private val builtinSources = new CaseInsensitiveMap(Map(
    "jdbc" -> classOf[row.DefaultSource].getCanonicalName,
    COLUMN_SOURCE -> classOf[execution.columnar.DefaultSource].getCanonicalName,
    ROW_SOURCE -> classOf[execution.row.DefaultSource].getCanonicalName,
    SAMPLE_SOURCE -> "org.apache.spark.sql.sampling.DefaultSource",
    TOPK_SOURCE -> "org.apache.spark.sql.topk.DefaultSource",
    "socket_stream" -> classOf[SocketStreamSource].getCanonicalName,
    "file_stream" -> classOf[FileStreamSource].getCanonicalName,
    "kafka_stream" -> classOf[KafkaStreamSource].getCanonicalName,
    "directkafka_stream" -> classOf[DirectKafkaStreamSource].getCanonicalName,
    "twitter_stream" -> classOf[TwitterStreamSource].getCanonicalName,
    "raw_socket_stream" -> classOf[RawSocketStreamSource].getCanonicalName,
    "text_socket_stream" -> classOf[TextSocketStreamSource].getCanonicalName,
    "rabbitmq_stream" -> classOf[RabbitMQStreamSource].getCanonicalName,
    "com.databricks.spark.csv" -> classOf[CSVFileFormat].getCanonicalName
  ))

  private[this] val INVALID_CONF = new SparkConf(loadDefaults = false) {
    override def getOption(key: String): Option[String] =
      throw new IllegalStateException("Invalid SparkConf")
  }

  private[this] val storeToBlockMap: TrieMap[String, BlockAndExecutorId] =
    TrieMap.empty[String, BlockAndExecutorId]

  def containsBlockId(executorId: String): Boolean = {
    storeToBlockMap.get(executorId) match {
      case Some(b) => b.blockId != null
      case None => false
    }
  }

  def getBlockId(executorId: String): Option[BlockAndExecutorId] = {
    storeToBlockMap.get(executorId) match {
      case s@Some(b) if b.blockId != null => s
      case None => None
    }
  }

  private[spark] def getBlockIdIfNull(
      executorId: String): Option[BlockAndExecutorId] =
    storeToBlockMap.get(executorId)

  private[spark] def addBlockId(executorId: String,
      blockId: BlockAndExecutorId): Option[BlockAndExecutorId] =
    storeToBlockMap.put(executorId, blockId)

  private[spark] def removeBlockId(executorId: String): Option[BlockAndExecutorId] =
    storeToBlockMap.remove(executorId)

  def getAllBlockIds: scala.collection.Map[String, BlockAndExecutorId] = {
    storeToBlockMap.filter(_._2.blockId != null)
  }

  private[spark] def clearBlockIds(): Unit = storeToBlockMap.clear()

  /** Returns the current SparkContext or null */
  def globalSparkContext: SparkContext = try {
    SparkContext.getOrCreate(INVALID_CONF)
  } catch {
    case _: IllegalStateException => null
  }

  private def newSnappyContext(sc: SparkContext) = {
    val snc = new SnappyContext(sc)
    // No need to synchronize. any occurrence would do
    if (_anySNContext == null) {
      _anySNContext = snc
    }
    snc
  }

  private def initMemberBlockMap(sc: SparkContext): Unit = {
    val cache = Misc.getGemFireCacheNoThrow
    if (cache != null && Utils.isLoner(sc)) {
      storeToBlockMap(cache.getMyId.toString) =
          new BlockAndExecutorId(SparkEnv.get.blockManager.blockManagerId,
            sc.schedulerBackend.defaultParallelism())
    }
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
   * @param jsc
   * @return
   */
  def apply(jsc: JavaSparkContext): SnappyContext = {
    if (jsc != null) {
      apply(jsc.sc)
    } else {
      apply()
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
      resolveClusterMode(sc)
    } else contextLock.synchronized {
      val mode = _clusterMode
      if ((mode != null && mode.sc == sc) || sc == null) {
        mode
      } else if (mode != null) {
        resolveClusterMode(sc)
      } else {
        _clusterMode = resolveClusterMode(sc)
        _clusterMode
      }
    }
  }

  private def resolveClusterMode(sc: SparkContext): ClusterMode = {
    if (sc.master.startsWith(Constant.JDBC_URL_PREFIX)) {
      if (ToolsCallbackInit.toolsCallback == null) {
        throw new SparkException("Missing 'io.snappydata.ToolsCallbackImpl$'" +
            " from SnappyData tools package")
      }
      SnappyEmbeddedMode(sc,
        sc.master.substring(Constant.JDBC_URL_PREFIX.length))
    } else {
      val conf = sc.conf
      val embedded = Property.Embedded.getOption(conf).exists(_.toBoolean)
      Property.Locators.getOption(conf).collectFirst {
        case s if !s.isEmpty =>
          val url = "locators=" + s + ";mcast-port=0"
          if (embedded) ExternalEmbeddedMode(sc, url)
          else SplitClusterMode(sc, url)
      }.orElse(Property.McastPort.getOption(conf).collectFirst {
        case s if s.toInt > 0 =>
          val url = "mcast-port=" + s
          if (embedded) ExternalEmbeddedMode(sc, url)
          else SplitClusterMode(sc, url)
      }).getOrElse {
        if (Utils.isLoner(sc)) LocalMode(sc, "mcast-port=0")
        else ExternalClusterMode(sc, sc.master)
      }
    }
  }

  private[sql] def initGlobalSnappyContext(sc: SparkContext) = {
    if (!_globalSNContextInitialized) {
      contextLock.synchronized {
        if (!_globalSNContextInitialized) {
          invokeServices(sc)
          sc.addSparkListener(new SparkContextListener)
          sc.ui.foreach(new SnappyStatsTab(_))
          initMemberBlockMap(sc)
          _globalSNContextInitialized = true
        }
      }
    }
  }

  private class SparkContextListener extends SparkListener {
    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
      stopSnappyContext
    }
  }

  private def invokeServices(sc: SparkContext): Unit = {
    SnappyContext.getClusterMode(sc) match {
      case SnappyEmbeddedMode(_, _) =>
        // NOTE: if Property.jobServer.enabled is true
        // this will trigger SnappyContext.apply() method
        // prior to `new SnappyContext(sc)` after this
        // method ends.
        ToolsCallbackInit.toolsCallback.invokeLeadStartAddonService(sc)
        StoreTableValueSizeProviderService.start(sc)
      case SplitClusterMode(_, _) =>
        ServiceUtils.invokeStartFabricServer(sc, hostData = false)
        StoreTableValueSizeProviderService.start(sc)
      case ExternalEmbeddedMode(_, url) =>
        SnappyContext.urlToConf(url, sc)
        ServiceUtils.invokeStartFabricServer(sc, hostData = false)
        StoreTableValueSizeProviderService.start(sc)
      case LocalMode(_, url) =>
        SnappyContext.urlToConf(url, sc)
        ServiceUtils.invokeStartFabricServer(sc, hostData = true)
        StoreTableValueSizeProviderService.start(sc)
      case _ => // ignore
    }
  }

  private def stopSnappyContext(): Unit = {
    val sc = globalSparkContext
    if (_globalSNContextInitialized) {
      // then on the driver
      clearStaticArtifacts()
      // clear current hive catalog connection
      SnappyStoreHiveCatalog.closeCurrent()
      if (ExternalStoreUtils.isSplitOrLocalMode(sc)) {
        ServiceUtils.invokeStopFabricServer(sc)
      }
    }
    _clusterMode = null
    _anySNContext = null
    _globalSNContextInitialized = false
  }

  /** Cleanup static artifacts on this lead/executor. */
  def clearStaticArtifacts(): Unit = {
    ConnectionPool.clear()
    CodeGeneration.clearCache()
    HashedRelationCache.close()
    _clusterMode match {
      case m: ExternalClusterMode =>
      case _ => ServiceUtils.clearStaticArtifacts()
    }
  }

  /**
   * Checks if the passed provider is recognized
   *
   * @param providerName
   * @param onlyBuiltIn
   * @return
   */
  def getProvider(providerName: String, onlyBuiltIn: Boolean): String = {
    builtinSources.getOrElse(providerName,
      if (onlyBuiltIn) throw new AnalysisException(
        s"Failed to find a builtin provider $providerName") else providerName)
  }


  def flushSampleTables(): Unit = {
    val sampleRelations = _anySNContext.sessionState.catalog.
      getDataSourceRelations[AnyRef](Seq(ExternalTableType.Sample), None)
    val clazz = org.apache.spark.util.Utils.classForName(
      "org.apache.spark.sql.sampling.ColumnFormatSamplingRelation")
    val method: Method = clazz.getDeclaredMethod("flushReservior")
    for (s <- sampleRelations) {
      method.setAccessible(true)
      method.invoke(s)
    }

  }
}

// end of SnappyContext

abstract class ClusterMode {
  val sc: SparkContext
  val url: String
}

final class BlockAndExecutorId(private[spark] var _blockId: BlockManagerId,
    private[spark] var _executorCores: Int) extends Externalizable {

  def blockId: BlockManagerId = _blockId

  def executorCores: Int = _executorCores

  override def hashCode: Int = if (blockId != null) blockId.hashCode() else 0

  override def equals(that: Any): Boolean = that match {
    case b: BlockAndExecutorId => b.blockId == blockId
    case b: BlockManagerId => b == blockId
    case _ => false
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    _blockId.writeExternal(out)
    out.writeInt(_executorCores)
  }

  override def readExternal(in: ObjectInput): Unit = {
    _blockId.readExternal(in)
    _executorCores = in.readInt()
  }

  override def toString: String = if (blockId != null) {
    s"BlockAndExecutorId(${blockId.executorId}," +
        s" ${blockId.host}, ${blockId.port}, cores=$executorCores)"
  } else "BlockAndExecutor()"
}

/**
 * The regular snappy cluster where each node is both a Spark executor
 * as well as GemFireXD data store. There is a "lead node" which is the
 * Spark driver that also hosts a job-server and GemFireXD accessor.
 */
case class SnappyEmbeddedMode(override val sc: SparkContext,
    override val url: String) extends ClusterMode

/**
 * This is for the two cluster mode: one is the normal snappy cluster, and
 * this one is a separate local/Spark/Yarn/Mesos cluster fetching data from
 * the snappy cluster on demand that just remains like an external datastore.
 */
case class SplitClusterMode(override val sc: SparkContext,
    override val url: String) extends ClusterMode

/**
 * This is for the "old-way" of starting GemFireXD inside an existing
 * Spark/Yarn cluster where cluster nodes themselves boot up as GemXD cluster.
 */
case class ExternalEmbeddedMode(override val sc: SparkContext,
    override val url: String) extends ClusterMode

/**
 * The local mode which hosts the data, executor, driver
 * (and optionally even jobserver) all in the same node.
 */
case class LocalMode(override val sc: SparkContext,
    override val url: String) extends ClusterMode

/**
 * A regular Spark/Yarn/Mesos or any other non-snappy cluster.
 */
case class ExternalClusterMode(override val sc: SparkContext,
    override val url: String) extends ClusterMode

class TableNotFoundException(message: String, cause: Option[Throwable] = None)
    extends AnalysisException(message) with Serializable
