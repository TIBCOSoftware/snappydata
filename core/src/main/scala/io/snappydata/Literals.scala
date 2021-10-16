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
package io.snappydata

import scala.reflect.ClassTag

import com.gemstone.gemfire.internal.shared.unsafe.DirectBufferAllocator
import com.pivotal.gemfirexd.internal.engine.GfxdConstants

import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.internal.{AltName, SQLAltName, SQLConfigEntry}

/**
 * Property names should be as per naming convention
 * http://docs.scala-lang.org/style/naming-conventions.html
 * i.e. upper camel case.
 */
object Property extends Enumeration {

  case class SparkValue[T](name: String, altName: String,
      configEntry: SQLConfigEntry) extends Property.Val(name) with AltName[T] {

    override def toString(): String =
      if (altName == null) name else name + '/' + altName
  }

  case class SQLValue[T](name: String, altName: String,
      configEntry: SQLConfigEntry) extends Property.Val(name) with SQLAltName[T] {

    override def toString(): String =
      if (altName == null) name else name + '/' + altName
  }

  protected final def Val[T: ClassTag](name: String, doc: String,
      defaultValue: Option[T], prefix: String = null,
      isPublic: Boolean = true): SparkValue[T] = {
    SparkValue(name, if (prefix == null) null else prefix + name,
      SQLConfigEntry.sparkConf(name, doc, defaultValue, isPublic))
  }

  protected final def SQLVal[T: ClassTag](name: String, doc: String,
      defaultValue: Option[T], prefix: String = null,
      isPublic: Boolean = true): SQLValue[T] = {
    SQLValue(name, if (prefix == null) null else prefix + name,
      SQLConfigEntry(name, doc, defaultValue, isPublic))
  }

  def getPropertyValue(propertyName: String): Option[String] = {
    if (propertyName.startsWith(Constant.PROPERTY_PREFIX) &&
        !propertyName.startsWith(Constant.STORE_PROPERTY_PREFIX)) {
      Some(propertyName.substring(Constant.PROPERTY_PREFIX.length))
    } else None
  }

  def getSnappyPropertyValue(propertyName: String): Option[String] = {
    if (propertyName.startsWith(Constant.SPARK_SNAPPY_PREFIX) &&
        !propertyName.startsWith(Constant.SPARK_STORE_PREFIX)) {
      Some(propertyName.substring(Constant.SPARK_SNAPPY_PREFIX.length))
    } else None
  }

  val Locators: SparkValue[String] = Val[String](s"${Constant.STORE_PROPERTY_PREFIX}locators",
    "The list of locators as comma-separated host:port values that have been " +
        "configured in the SnappyData cluster.", None, Constant.SPARK_PREFIX)

  val McastPort: SparkValue[Int] = Val[Int](s"${Constant.STORE_PROPERTY_PREFIX}mcastPort",
    "[Deprecated] The multicast port configured in the SnappyData cluster " +
        "when locators are not being used. This mode is no longer supported.",
    None, Constant.SPARK_PREFIX)

  val JobServerEnabled: SparkValue[Boolean] = Val(s"${Constant.JOBSERVER_PROPERTY_PREFIX}enabled",
    "If true then REST API access via Spark jobserver will be available in " +
        "the SnappyData cluster", Some(true), prefix = null, isPublic = false)

  val JobServerWaitForInit: SparkValue[Boolean] = Val(
    s"${Constant.JOBSERVER_PROPERTY_PREFIX}waitForInitialization",
    "If true then cluster startup will wait for Spark jobserver to be fully initialized " +
        "before marking lead as 'RUNNING'. Default is false.", Some(false), prefix = null)

  val HiveServerEnabled: SparkValue[Boolean] = Val(
    s"${Constant.PROPERTY_PREFIX}hiveServer.enabled", "If true on a lead node, then an " +
        "embedded HiveServer2 with thrift access will be started in foreground. Default is true " +
        "but starts the service in background.", Some(true), prefix = null)

  val HiveCompatibility: SQLValue[String] = SQLVal(
    s"${Constant.PROPERTY_PREFIX}sql.hiveCompatibility", "Property on SnappySession to make " +
        "alter the hive compatibility level. The 'default' level is Spark compatible except for " +
        "CREATE TABLE which defaults to row tables. A value of 'spark' makes it fully Spark " +
        s"compatible where CREATE TABLE defaults to hive tables when catalogImplementation is " +
        "'hive' for the session.  When set to 'full' then in addition to the behaviour " +
        "with 'spark', it makes the behavior hive compatible for statements like SHOW TABLES " +
        "rather than being compatible with Spark SQL. Default is 'default'.",
    Some("default"), prefix = null)

  val HiveServerUseHiveSession: SparkValue[Boolean] = Val(
    s"${Constant.PROPERTY_PREFIX}hiveServer.useHiveSession", "If true, then the session " +
        "created in embedded HiveServer2 will be a hive session else a SnappySession",
    Some(false), prefix = null)

  val SnappyConnection: SparkValue[String] = Val[String](Constant.CONNECTION_PROPERTY,
     "Host and client port combination in the form [host:clientPort]. This " +
     "is used by smart connector to connect to SnappyData cluster using " +
     "JDBC driver. This will be used to form a JDBC URL of the form " +
     "\"jdbc:snappydata://host:clientPort/\" (or use the form \"host[clientPort]\"). " +
     "It is recommended that hostname and client port of the locator " +
     "be specified for this.", None, Constant.SPARK_PREFIX)

  val PlanCacheSize: SparkValue[Int] = Val[Int](s"${Constant.PROPERTY_PREFIX}sql.planCacheSize",
    s"Number of query plans that will be cached.", Some(3000))

  val CatalogCacheSize: SparkValue[Int] = Val[Int](
    s"${Constant.PROPERTY_PREFIX}sql.catalogCacheSize",
    s"Number of catalog tables whose meta-data will be cached.", Some(2000))

  val ColumnBatchSize: SQLValue[String] = SQLVal[String](
    s"${Constant.PROPERTY_PREFIX}column.batchSize",
    "The default size of blocks to use for storage in SnappyData column " +
        "store. When inserting data into the column storage this is the unit " +
        "(in bytes or k/m/g suffixes for unit) that will be used to split the data " +
        "into chunks for efficient storage and retrieval. It can also be set for each " +
        s"table using the ${ExternalStoreUtils.COLUMN_BATCH_SIZE} option in " +
        "create table DDL. Maximum allowed size is 2GB.", Some("24m"))

  val ColumnMaxDeltaRows: SQLValue[Int] = SQLVal[Int](
    s"${Constant.PROPERTY_PREFIX}column.maxDeltaRows",
    "The maximum number of rows that can be in the delta buffer of a column table. " +
        s"The size of delta buffer is already limited by $ColumnBatchSize but " +
        "this allows a lower limit on number of rows for better scan performance. " +
        "So the delta buffer will be rolled into the column store whichever of " +
        s"$ColumnBatchSize and this property is hit first. It can also be set for " +
        s"each table using the ${ExternalStoreUtils.COLUMN_MAX_DELTA_ROWS} option in " +
        s"create table DDL else this setting is used for the create table.", Some(10000))

  val ColumnCompactionRatio: SparkValue[Double] = Val[Double](
    s"${Constant.PROPERTY_PREFIX}column.compactionRatio",
    "Proportion of deleted rows in a column batch after which the batch will be " +
        "compacted. This should be a double value between 0 (exclusive) and 1 (inclusive). " +
        "A value of 1 will disable compaction. This property has to be set at spark " +
        "configuration level or as system property (latter on all the servers) and cannot be " +
        "changed at the session level. Default is 0.1", Some(0.1))

  val ColumnUpdateCompactionRatio: SparkValue[Double] = Val[Double](
    s"${Constant.PROPERTY_PREFIX}column.updateCompactionRatio",
    "Proportion of updated rows in a column batch after which the batch will be " +
        "compacted. This should be a double value between 0 (exclusive) and 1 (inclusive). " +
        "A value of 1 will disable compaction. This property has to be set at spark " +
        "configuration level or as system property (latter on all the servers) and cannot be " +
        "changed at the session level. Default is 0.2", Some(0.2))

  val MaxMemoryResultSize: SparkValue[String] = Val[String](
    s"${Constant.SPARK_PREFIX}sql.maxMemoryResultSize",
    "Maximum size of results from a JDBC/ODBC/SQL query in a partition that will be held " +
        "in memory beyond which the results will be written to disk " +
        "(value in bytes or k/m/g suffixes for unit, min 1k). Default is 4MB.", Some("4m"))

  val ResultPersistenceTimeout: SparkValue[Long] = Val[Long](
    s"${Constant.SPARK_PREFIX}sql.resultPersistenceTimeout",
    s"Maximum duration in seconds for which results larger than ${MaxMemoryResultSize.name}" +
        "are held on disk after which they are cleaned up. This is to handle cases where a " +
        "client does not consume all the results. Default is 21600 (6h).", Some(21600L))

  val DisableHashJoin: SQLValue[Boolean] = SQLVal[Boolean](
    s"${Constant.PROPERTY_PREFIX}sql.disableHashJoin",
    "Disable hash joins completely including those for replicated row tables. Default is false.",
    Some(false))

  val HashJoinSize: SQLValue[String] = SQLVal[String](
    s"${Constant.PROPERTY_PREFIX}sql.hashJoinSize",
    "The join would be converted into a hash join if the table is of size less " +
        "than hashJoinSize. The limit specifies an estimate on the input data size " +
        "(in bytes or k/m/g/t suffixes for unit). Note that replicated row tables always use " +
        s"local hash joins regardless of this property. Use ${DisableHashJoin.name} to disable " +
        s"all hash joins. Default value is 100MB.", Some("100m"))

  val HashAggregateSize: SQLValue[String] = SQLVal[String](
    s"${Constant.PROPERTY_PREFIX}sql.hashAggregateSize",
    "Aggregation will use optimized hash aggregation plan but one that does not " +
        "overflow to disk and can cause OOME if the result of aggregation is large. " +
        "The limit specifies the input data size (in bytes or k/m/g/t suffixes for unit) " +
        "and not the output size. Set this only if there are known to be queries " +
        "that can return very large number of rows in aggregation results. " +
        "Default value is 0 meaning no limit on the size so the optimized " +
        "hash aggregation is always used.", Some("0"))

  val ForceLinkPartitionsToBuckets: SQLValue[Boolean] = SQLVal[Boolean](
    s"${Constant.PROPERTY_PREFIX}linkPartitionsToBuckets",
    "Property to always treat each bucket as separate partition in column/row table scans. " +
        "When unset or set to false, SnappyData will try to create only " +
        "as many partitions as executor cores clubbing multiple buckets " +
        "into each partition when possible.", Some(false), Constant.SPARK_PREFIX)

  val PreferPrimariesInQuery: SQLValue[Boolean] = SQLVal[Boolean](
    s"${Constant.PROPERTY_PREFIX}preferPrimaries",
    "Property to prefer using primary buckets in queries. This reduces " +
        "scalability of queries in the interest of reduced memory usage for " +
        "secondary buckets. Default is false.", Some(false), Constant.SPARK_PREFIX)

  val PartitionPruning: SQLValue[Boolean] = SQLVal[Boolean](
    s"${Constant.PROPERTY_PREFIX}sql.partitionPruning",
    "Property to set/unset partition pruning of queries", Some(true))

  val PlanCaching: SQLValue[Boolean] = SQLVal[Boolean](
    s"${Constant.PROPERTY_PREFIX}sql.planCaching",
    "Property to set/unset plan caching", Some(false))

  val SerializeWrites: SQLValue[Boolean] = SQLVal[Boolean](
    s"${Constant.PROPERTY_PREFIX}sql.serializeWrites",
    "Property to set/unset serialized writes on column table." +
      "There will be a global lock which will ensure that at a time only" +
      "one write operation is active on the column table.", Some(true))

  val SerializedWriteLockTimeOut: SQLValue[Int] = SQLVal[Int](
    s"${Constant.PROPERTY_PREFIX}sql.serializedWriteLockTimeOut",
    "Property to specify the lock timeout for write ops in seconds. If the" +
      " write operation doesn't get lock for write within this time period" +
      s" then operation will fail. Default value is ${GfxdConstants.MAX_LOCKWAIT_DEFAULT/1000} sec",
    Some(GfxdConstants.MAX_LOCKWAIT_DEFAULT/1000))

  val Tokenize: SQLValue[Boolean] = SQLVal[Boolean](
    s"${Constant.PROPERTY_PREFIX}sql.tokenize",
    "Property to enable/disable tokenization", Some(true))

  val ParserTraceError: SQLValue[Boolean] = SQLVal[Boolean](
    s"${Constant.PROPERTY_PREFIX}sql.parser.traceError",
    "Property to enable detailed rule tracing for parse errors", Some(false))

  val EnableExperimentalFeatures: SQLValue[Boolean] = SQLVal[Boolean](
    s"${Constant.PROPERTY_PREFIX}enable-experimental-features",
    "SQLConf property that enables snappydata experimental features like distributed index " +
        "optimizer choice during query planning. Default is turned off.",
    Some(false), Constant.SPARK_PREFIX)

  val SchedulerPool: SQLValue[String] = SQLVal[String](
    s"${Constant.PROPERTY_PREFIX}scheduler.pool",
    "Property to set the scheduler pool for the current session. This property can " +
      "be used to assign queries to different pools for improving " +
      "throughput of specific queries.", Some("default"))

  val FlushReservoirThreshold: SQLValue[Int] = SQLVal[Int](
    s"${Constant.PROPERTY_PREFIX}flushReservoirThreshold",
    "Reservoirs of sample table will be flushed and stored in columnar format if sampling is done" +
        " on baset table of size more than flushReservoirThreshold." +
        " Default value is 10,000.", Some(10000))

  val NumBootStrapTrials: SQLValue[Int] = SQLVal[Int](
    s"${Constant.SPARK_PREFIX}sql.aqp.numBootStrapTrials",
    "Number of bootstrap trials to do for calculating error bounds. Default value is 100.",
    Some(100))

  val MaxErrorAllowed: SQLValue[Double] = SQLVal[Double](
    s"${Constant.SPARK_PREFIX}sql.aqp.maxErrorAllowed",
    "Maximum relative error tolerable in the approximate value calculation. It should be a " +
      "fractional value not exceeding 1. Default value is 1.0",
    Some(1.0d), prefix = null, isPublic = false)

  val Error: SQLValue[Double] = SQLVal[Double](s"${Constant.SPARK_PREFIX}sql.aqp.error",
    "Maximum relative error tolerable in the approximate value calculation. It should be a " +
      s"fractional value not exceeding 1. Default value is ${Constant.DEFAULT_ERROR}",
    Some(Constant.DEFAULT_ERROR))

  val Confidence: SQLValue[Double] = SQLVal[Double](s"${Constant.SPARK_PREFIX}sql.aqp.confidence",
    "Confidence with which the error bounds are calculated for the approximate value. It should " +
    s"be a fractional value not exceeding 1. Default value is ${Constant.DEFAULT_CONFIDENCE}",
    None)

  val Behavior: SQLValue[String] = SQLVal[String](s"${Constant.SPARK_PREFIX}sql.aqp.behavior",
    s" The action to be taken if the error computed goes oustide the error tolerance limit. " +
      s"Default value is ${Constant.BEHAVIOR_DO_NOTHING}", None)

  val AqpDebug: SQLValue[Boolean] = SQLVal[Boolean](s"${Constant.SPARK_PREFIX}sql.aqp.debug",
    s" Boolean if true tells engine to do  bootstrap analysis in debug mode returning an array of" +
      s" values for the iterations. Default is false", Some(false), prefix = null, isPublic = false)

  val AqpDebugFixedSeed: SQLValue[Boolean] = SQLVal[Boolean](
    s"${Constant.SPARK_PREFIX}sql.aqp.debug.fixedSeed",
    s" Boolean if true tells engine to initialize the seed for poisson value calculation with a " +
      s"fixed  number 123456789L. Default is false.", Some(false), prefix = null, isPublic = false)

  val AQPDebugPoissonType: SQLValue[String] = SQLVal[String](
    s"${Constant.SPARK_PREFIX}sql.aqp.debug.poissonType",
    s" If aqp debugging is enbaled, this property can be used to set different types of algorithm" +
      s" to generate bootstrap multiplicity numbers. Default is Real",
    None, prefix = null, isPublic = false)

  val ClosedFormEstimates: SQLValue[Boolean] = SQLVal[Boolean](
    s"${Constant.SPARK_PREFIX}sql.aqp.closedFormEstimates",
    s"Boolean if false tells engine to use bootstrap analysis for error calculation for all cases" +
      s". Default is true.", Some(true), null, isPublic = false)

  val PutIntoInnerJoinCacheSize: SQLValue[String] =
    SQLVal[String](s"${Constant.PROPERTY_PREFIX}cache.putIntoInnerJoinResultSize",
      "The putInto inner join would be cached if the result of " +
          "join with incoming Dataset is of size less than this limit (value is in bytes or " +
          "k/m/g/t suffixes for unit). Default value is -1 which indicates no limit.", Some("-1"))

  val PutIntoInnerJoinLocalCache: SQLValue[Boolean] =
    SQLVal[Boolean](s"${Constant.PROPERTY_PREFIX}cache.putIntoInnerJoinLocalCache",
      "Cache the putInto inner join locally at the driver node. Use only if the " +
          "size of the putInto data and the resulting updates is small. Default is do local " +
          "caching only if the data being put is a local relation created from a list of Rows " +
          s"and its size is within the limit specified by ${HashJoinSize.name}.", Some(false))

  val TestExplodeComplexDataTypeInSHA: SQLValue[Boolean] = SQLVal[Boolean](
    s"${Constant.PROPERTY_PREFIX}sql.explodeStructInSHA",
    "Explodes the Struct or Array Field in Group By Keys even if the struct object is " +
      "UnsafeRow or UnsafeArrayData", Some(false))

  val UseOptimzedHashAggregate: SQLValue[Boolean] = SQLVal[Boolean](
    s"${Constant.PROPERTY_PREFIX}sql.optimizedHashAggregate",
    "Enables the use of ByteBufferMap based SnappyHashAggregateExec",
    Some(true))

  val UseOptimizedHashAggregateForSingleKey: SQLValue[Boolean] = SQLVal[Boolean](
    s"${Constant.PROPERTY_PREFIX}sql.useOptimizedHashAggregateForSingleKey",
    "Use the new ByteBufferMap based SnappyHashAggregateExec even for single column group by." +
        "The default value is true since the number of groups can be very large causing the " +
        "older implementation to run out of memory much for easily despite it being " +
        "substantially faster for most single column group by cases.", Some(true))

  val UseDriverCollectForGroupBy: SQLValue[Boolean] = SQLVal[Boolean](
    s"${Constant.PROPERTY_PREFIX}sql.useDriverCollectForGroupBy",
    "Enable direct collect for partial grouping results avoiding the EXCHANGE at the last step." +
        "Use this with caution since it can cause memory problems in case the final result is " +
        "large, so its best to turn it on for specific queries and disable when done.", Some(false))

  val ApproxMaxCapacityOfBBMap: SQLValue[Int] = SQLVal[Int](
    s"${Constant.PROPERTY_PREFIX}sql.approxMaxCapacityOfBBMap",
    s"The max capacity of value byte array in ByteBufferHashMap. " +
      s"Default value is ${Integer.MAX_VALUE}",
    Some(((Integer.MAX_VALUE -DirectBufferAllocator.DIRECT_OBJECT_OVERHEAD - 7) >>> 3) << 3))

  val initialCapacityOfSHABBMap: SQLValue[Int] = SQLVal[Int](
    s"${Constant.PROPERTY_PREFIX}sql.initialCapacityOfSHABBMap",
    s"The initial capacity of SHAMap. " +
      s"Default value is 8192", Some(8192))

  val TestDisableCodeGenFlag: SQLValue[Boolean] = SQLVal[Boolean](
    s"${Constant.PROPERTY_PREFIX}sql.disableCodegenFallback",
    s"The test flag if set to true will throw Exception instead of creating CodegenSparkFallback " +
      s"Default value is false", Some(false))

  val TestCodeSplitThresholdInSHA: SQLValue[Int] = SQLVal[Int](
    s"${Constant.PROPERTY_PREFIX}sql.codeSplitThresholdInSHA",
    s"The maximum number of group by keys or aggregates which can generate inline " +
      s"code in SnappyHashAggregateExec. Beyond the threshold value" +
      s", code splitting occurs through functions. Default value is 75", Some(75))

  val TestCodeSplitFunctionParamsSizeInSHA: SQLValue[Int] = SQLVal[Int](
    s"${Constant.PROPERTY_PREFIX}sql.codeSplitFunctionParamsSizeInSHA",
    s"The number of group by keys or aggregates which should be used as parameters at a time" +
      s" for code splitting function", Some(5))

  // By default it will be same as MAX_TASK_FAILURES
  // User should set it to 0, if they want insert to fail on any failure.
  // this is used in TaskSchedulerImpl.SNAPPY_WRITE_RETRY_PROP Any change here
  // should be done reflected there too.
  val MaxRetryAttemptsForWrite: SQLValue[Int] = SQLVal[Int](
    s"${Constant.PROPERTY_PREFIX}maxRetryAttemptsForWrite",
    s"The number of times a write task should be retried before all tasks failing." , Some(4))
}

// extractors for properties

object SparkProperty {
  def unapply(property: Property.SparkValue[_]): Option[String] =
    Property.getPropertyValue(property.name)
}

object SparkSQLProperty {
  def unapply(property: Property.SQLValue[_]): Option[String] =
    Property.getPropertyValue(property.name)
}

object SnappySparkProperty {
  def unapply(property: Property.SparkValue[_]): Option[String] =
    Property.getSnappyPropertyValue(property.name)
}

object SnappySparkSQLProperty {
  def unapply(property: Property.SQLValue[_]): Option[String] =
    Property.getSnappyPropertyValue(property.name)
}

/**
 * SQL query hints as interpreted by the SnappyData SQL parser. The format
 * mirrors closely the format used by Hive,Oracle query hints with a comment
 * followed immediately by a '+' and then "key(value)" for the hint. Example:
 * <p>
 * SELECT * /`*`+ hint(value) *`/` FROM t1
 */
object QueryHint extends Enumeration {

  type Type = Value

  import scala.language.implicitConversions

  implicit def toStr(h: Type): String = h.toString

  /**
   * Query hint for SQL queries to serialize complex types (ARRAY, MAP, STRUCT)
   * as CLOBs in JSON format for routed JDBC/ODBC queries (default) to display better
   * in external tools else if set to false/0 then display as serialized blobs.
   *
   * Possible values are 'false/0' or 'true/1' (default is true)
   *
   * Example:<br>
   * SELECT * FROM t1 --+ complexTypeAsJson(0)
   */
  val ComplexTypeAsJson: Type = Value(Constant.COMPLEX_TYPE_AS_JSON_HINT)

  /**
   * Query hint followed by table to override optimizer choice of index per table.
   *
   * Possible values are valid indexes defined on the table.
   *
   * Example:<br>
   * SELECT * FROM t1 /`*`+ index(xxx) *`/`, t2 --+ withIndex(yyy)
   */
  val Index: Type = Value("index")

  /**
   * Query hint after FROM clause to indicate following tables have join order fixed and
   * optimizer shouldn't try to re-order joined tables.
   *
   * Possible comma separated values are [[io.snappydata.JOS]].
   *
   * Example:<br>
   * SELECT * FROM /`*`+ joinOrder(fixed) *`/` t1, t2
   */
  val JoinOrder: Type = Value("joinOrder")

  /**
   * Query hint to force a join type for the current join. This should appear after
   * the required table/plan in FROM where the specific join type has to be forced.
   * Note that this will enable the specific join type only if it is possible
   * for that table in the join and silently ignore otherwise.
   *
   * Possible values are [[Constant.JOIN_TYPE_BROADCAST]], [[Constant.JOIN_TYPE_HASH]],
   * [[Constant.JOIN_TYPE_SORT]].
   *
   * Example:<br>
   * SELECT * FROM t1 /`*`+ joinType(broadcast) -- broadcast t1 *`/`, t2 where ...
   */
  val JoinType: Type = Value("joinType")

  /**
   * Query hint for SQL queries to serialize STRING type as CLOB rather than
   * as VARCHAR.
   *
   * Possible values are valid column names in the tables/schema. Multiple
   * column names to be comma separated.
   * One can also provide '*' for serializing all the STRING columns as CLOB.
   *
   * Example:<br>
   * SELECT id, name, addr, medical_history FROM t1 --+ columnsAsClob(addr)
   * SELECT id, name, addr, medical_history FROM t1 --+ columnsAsClob(*)
   */
  val ColumnsAsClob: Type = Value("columnsAsClob")
}

/**
 * List of possible values for Join Order QueryHint.
 *
 * `Note:` Ordering is applicable only when index choice is left to the optimizer. By default,
 * if user specifies explicit index hint like "select * from t1 --+ index()", optimizer will just
 * honor the hint and skip everything mentioned in joinOrder. In other words, a blank index()
 * hint for any table disables choice of index and its associated following rules.
 */
object JOS extends Enumeration {
  type Type = Value

  import scala.language.implicitConversions

  implicit def toStr(h: Type): String = h.toString

  /**
   * Continue to attempt optimization choices of index for colocated joins even if user have
   * specified explicit index hints for some tables.
   *
   * `Note:` user specified index hint will be honored and optimizer will only attempt for
   * other tables in the query.
   */
  val ContinueOptimizations: Type = Value("continueOpts")

  /**
   * By default if query have atleast one colocated join conditions mentioned between a pair of
   * partitiioned tables, optimizer won't try to derive colocation possibilities with replicated
   * tables in between. This switch tells the optimizer to include partition -> replicated ->
   * partition like indirect colocation possibilities even if partition -> partition join
   * conditions are mentioned.
   */
  val IncludeGeneratedPaths: Type = Value("includeGeneratedPaths")

  /**
   * Don't alter the join order provided by the user.
   */
  val Fixed: Type = Value("fixed")
}
