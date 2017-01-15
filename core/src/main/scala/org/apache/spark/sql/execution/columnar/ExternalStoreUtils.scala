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
package org.apache.spark.sql.execution.columnar

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import scala.collection.mutable

import io.snappydata.Constant
import io.snappydata.util.ServiceUtils

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.row.{GemFireXDClientDialect, GemFireXDDialect}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.CodeGeneration
import org.apache.spark.sql.types._

/**
 * Utility methods used by external storage layers.
 */
object ExternalStoreUtils {

  final val DEFAULT_TABLE_BUCKETS = "113"
  final val DEFAULT_SAMPLE_TABLE_BUCKETS = "79"
  final val DEFAULT_TABLE_BUCKETS_LOCAL_MODE = "19"
  final val DEFAULT_SAMPLE_TABLE_BUCKETS_LOCAL_MODE = "11"
  final val INDEX_TYPE = "INDEX_TYPE"
  final val INDEX_NAME = "INDEX_NAME"
  final val DEPENDENT_RELATIONS = "DEPENDENT_RELATIONS"


  def lookupName(tableName: String, schema: String): String = {
    if (tableName.indexOf('.') <= 0) {
      schema + '.' + tableName
    } else tableName
  }

  private def addProperty(props: Map[String, String], key: String,
      default: String): Map[String, String] = {
    if (props.contains(key)) props
    else props + (key -> default)
  }

  private def defaultMaxExternalPoolSize: String =
    String.valueOf(math.max(32, Runtime.getRuntime.availableProcessors() * 4))

  private def defaultMaxEmbeddedPoolSize: String =
    String.valueOf(math.max(64, Runtime.getRuntime.availableProcessors() * 6))

  def getAllPoolProperties(url: String, driver: String,
      poolProps: Map[String, String], hikariCP: Boolean,
      isEmbedded: Boolean): Map[String, String] = {
    // setup default pool properties
    var props = poolProps
    if (driver != null && !driver.isEmpty) {
      props = addProperty(props, "driverClassName", driver)
    }
    val defaultMaxPoolSize = if (isEmbedded) defaultMaxEmbeddedPoolSize
    else defaultMaxExternalPoolSize
    if (hikariCP) {
      props = props + ("jdbcUrl" -> url)
      props = addProperty(props, "maximumPoolSize", defaultMaxPoolSize)
      props = addProperty(props, "minimumIdle", "4")
    } else {
      props = props + ("url" -> url)
      props = addProperty(props, "maxActive", defaultMaxPoolSize)
      props = addProperty(props, "maxIdle", defaultMaxPoolSize)
      props = addProperty(props, "initialSize", "4")
    }
    props
  }

  def getDriver(url: String, dialect: JdbcDialect): String = {
    dialect match {
      case GemFireXDDialect => "io.snappydata.jdbc.EmbeddedDriver"
      case GemFireXDClientDialect => "io.snappydata.jdbc.ClientDriver"
      case _ => Utils.getDriverClassName(url)
    }
  }

  class CaseInsensitiveMutableHashMap[T](map: scala.collection.Map[String, T])
      extends mutable.Map[String, T] with Serializable {

    val baseMap = new mutable.HashMap[String, T]
    baseMap ++= map.map(kv => kv.copy(_1 = kv._1.toLowerCase))

    override def get(k: String): Option[T] = baseMap.get(k.toLowerCase)

    override def remove(k: String): Option[T] = baseMap.remove(k.toLowerCase)

    override def iterator: Iterator[(String, T)] = baseMap.iterator

    override def +=(kv: (String, T)): this.type = {
      baseMap += kv.copy(_1 = kv._1.toLowerCase)
      this
    }

    override def -=(key: String): this.type = {
      baseMap -= key.toLowerCase
      this
    }
  }

  def removeInternalProps(parameters: mutable.Map[String, String]): String = {
    val dbtableProp = JdbcExtendedUtils.DBTABLE_PROPERTY
    val table = parameters.remove(dbtableProp)
        .getOrElse(sys.error(s"Option '$dbtableProp' not specified"))
    parameters.remove(JdbcExtendedUtils.ALLOW_EXISTING_PROPERTY)
    parameters.remove(JdbcExtendedUtils.SCHEMA_PROPERTY)
    parameters.remove("serialization.format")
    table
  }

  def removeSamplingOptions(
      parameters: mutable.Map[String, String]): Map[String, String] = {

    val optSequence = Seq("qcs", "fraction", "strataReservoirSize",
      "errorLimitColumn", "errorLimitPercent", "timeSeriesColumn",
      "timeInterval", "aqp.debug.byPassSampleOperator")

    val optMap = new mutable.HashMap[String, String]

    optSequence.map(key => {
      val value = parameters.remove(key)
      value match {
        case Some(v) => optMap += (Utils.toLowerCase(key) -> v)
        case None => // Do nothing
      }
    })
    optMap.toMap
  }

  def defaultStoreURL(sc: SparkContext): String = {
    SnappyContext.getClusterMode(sc) match {
      case SnappyEmbeddedMode(_, _) =>
        // Already connected to SnappyData in embedded mode.
        Constant.DEFAULT_EMBEDDED_URL + ";host-data=false;mcast-port=0"
      case SplitClusterMode(_, _) =>
        ServiceUtils.getLocatorJDBCURL(sc) + ";route-query=false"
      case ExternalEmbeddedMode(_, url) =>
        Constant.DEFAULT_EMBEDDED_URL + ";host-data=false;" + url
      case LocalMode(_, url) =>
        Constant.DEFAULT_EMBEDDED_URL + ';' + url
      case ExternalClusterMode(_, url) =>
        throw new AnalysisException("Option 'url' not specified for cluster " +
            url)
    }
  }

  def isSplitOrLocalMode(sparkContext: SparkContext): Boolean = {
    SnappyContext.getClusterMode(sparkContext) match {
      case SplitClusterMode(_, _) | LocalMode(_, _) => true
      case _ => false
    }
  }


  def validateAndGetAllProps(sc: SparkContext,
      parameters: mutable.Map[String, String]): ConnectionProperties = {

    val url = parameters.remove("url").getOrElse(defaultStoreURL(sc))

    val dialect = JdbcDialects.get(url)
    val driver = parameters.remove("driver").getOrElse(getDriver(url, dialect))

    DriverRegistry.register(driver)

    val poolImpl = parameters.remove("poolimpl")
    val poolProperties = parameters.remove("poolproperties")

    val hikariCP = poolImpl.map(Utils.toLowerCase) match {
      case Some("hikari") => true
      case Some("tomcat") => false
      case Some(p) =>
        throw new IllegalArgumentException("ExternalStoreUtils: " +
            s"unsupported pool implementation '$p' " +
            s"(supported values: tomcat, hikari)")
      case None => Constant.DEFAULT_USE_HIKARICP
    }
    val poolProps = poolProperties.map(p => Map(p.split(",").map { s =>
      val eqIndex = s.indexOf('=')
      if (eqIndex >= 0) {
        (s.substring(0, eqIndex).trim, s.substring(eqIndex + 1).trim)
      } else {
        // assume a boolean property to be enabled
        (s.trim, "true")
      }
    }: _*)).getOrElse(Map.empty)

    val isLoner = Utils.isLoner(sc)
    // remaining parameters are passed as properties to getConnection
    val connProps = new Properties()
    val executorConnProps = new Properties()
    parameters.foreach { kv =>
      connProps.setProperty(kv._1, kv._2)
      executorConnProps.setProperty(kv._1, kv._2)
    }
    connProps.remove("poolProperties")
    executorConnProps.remove("poolProperties")
    connProps.setProperty("driver", driver)
    executorConnProps.setProperty("driver", driver)
    val isEmbedded = dialect match {
      case GemFireXDDialect =>
        GemFireXDDialect.addExtraDriverProperties(isLoner, connProps)
        true
      case GemFireXDClientDialect =>
        GemFireXDClientDialect.addExtraDriverProperties(isLoner, connProps)
        connProps.setProperty("route-query", "false")
        executorConnProps.setProperty("route-query", "false")
        false
      case d: JdbcExtendedDialect =>
        d.addExtraDriverProperties(isLoner, connProps)
        false
      case _ => false
    }
    val allPoolProps = getAllPoolProperties(url, driver,
      poolProps, hikariCP, isEmbedded)
    ConnectionProperties(url, driver, dialect, allPoolProps,
      connProps, executorConnProps, hikariCP)
  }

  def getConnection(id: String, connProperties: ConnectionProperties,
      forExecutor: Boolean): Connection = {
    Utils.registerDriver(connProperties.driver)
    val connProps = if (forExecutor) connProperties.executorConnProps
    else connProperties.connProps
    ConnectionPool.getPoolConnection(id, connProperties.dialect,
      connProperties.poolProps, connProps, connProperties.hikariCP)
  }

  def getConnector(id: String, connProperties: ConnectionProperties,
      forExecutor: Boolean): () => Connection = () => {
    getConnection(id, connProperties, forExecutor)
  }

  def getConnectionType(dialect: JdbcDialect): ConnectionType.Value = {
    dialect match {
      case GemFireXDDialect => ConnectionType.Embedded
      case GemFireXDClientDialect => ConnectionType.Net
      case _ => ConnectionType.Unknown
    }
  }

  def getJDBCType(dialect: JdbcDialect, dataType: DataType): Int = {
    dialect.getJDBCType(dataType).map(_.jdbcNullType).getOrElse(
      dataType match {
        case IntegerType => java.sql.Types.INTEGER
        case LongType => java.sql.Types.BIGINT
        case DoubleType => java.sql.Types.DOUBLE
        case FloatType => java.sql.Types.REAL
        case ShortType => java.sql.Types.INTEGER
        case ByteType => java.sql.Types.INTEGER
        // need to keep below mapping to BIT instead of BOOLEAN for MySQL
        case BooleanType => java.sql.Types.BIT
        case StringType => java.sql.Types.CLOB
        case BinaryType => java.sql.Types.BLOB
        case TimestampType => java.sql.Types.TIMESTAMP
        case DateType => java.sql.Types.DATE
        case _: DecimalType => java.sql.Types.DECIMAL
        case NullType => java.sql.Types.NULL
        case _ => throw new IllegalArgumentException(
          s"Can't translate to JDBC value for type $dataType")
      })
  }

  // This should match JDBCRDD.compileFilter for best performance
  def unhandledFilter(f: Filter): Boolean = f match {
    case EqualTo(_, _) => false
    case LessThan(_, _) => false
    case GreaterThan(_, _) => false
    case LessThanOrEqual(_, _) => false
    case GreaterThanOrEqual(_, _) => false
    case _ => true
  }

  val SOME_TRUE = Some(true)
  val SOME_FALSE = Some(false)

  private def checkIndexedColumn(col: String,
      indexedCols: scala.collection.Set[String]): Option[Boolean] =
    if (indexedCols.contains(col)) SOME_TRUE else None

  // below should exactly match RowFormatScanRDD.compileFilter
  def handledFilter(f: Filter,
      indexedCols: scala.collection.Set[String]): Option[Boolean] = f match {
    // only pushdown filters if there is an index on the column;
    // keeping a bit conservative and not pushing other filters because
    // Spark execution engine is much faster at filter apply (though
    //   its possible that not all indexed columns will be used for
    //   index lookup still push down all to keep things simple)
    case EqualTo(col, _) => checkIndexedColumn(col, indexedCols)
    case LessThan(col, _) => checkIndexedColumn(col, indexedCols)
    case GreaterThan(col, _) => checkIndexedColumn(col, indexedCols)
    case LessThanOrEqual(col, _) => checkIndexedColumn(col, indexedCols)
    case GreaterThanOrEqual(col, _) => checkIndexedColumn(col, indexedCols)
    case StringStartsWith(col, _) => checkIndexedColumn(col, indexedCols)
    case In(col, _) => checkIndexedColumn(col, indexedCols)
    // At least one column should be indexed for the AND condition to be
    // evaluated efficiently
    case And(left, right) =>
      val v = handledFilter(left, indexedCols)
      if (v ne None) v
      else handledFilter(right, indexedCols)
    // ORList optimization requires all columns to have indexes
    // which is ensured by the condition below
    case Or(left, right) => if ((handledFilter(left, indexedCols) eq
        SOME_TRUE) && (handledFilter(right, indexedCols) eq SOME_TRUE)) {
      SOME_TRUE
    } else SOME_FALSE
    case _ => SOME_FALSE
  }

  def unhandledFilter(f: Filter,
      indexedCols: scala.collection.Set[String]): Boolean =
    handledFilter(f, indexedCols) ne SOME_TRUE

  /**
   * Prune all but the specified columns from the specified Catalyst schema.
   *
   * @param fieldMap - The Catalyst column name to metadata of the master table
   * @param columns  - The list of desired columns
   * @return A Catalyst schema corresponding to columns in the given order.
   */
  def pruneSchema(fieldMap: Map[String, StructField],
      columns: Array[String]): StructType = {
    new StructType(columns.map { col =>
      fieldMap.getOrElse(col, fieldMap.getOrElse(col,
        throw new AnalysisException("Cannot resolve " +
            s"""column name "$col" among (${fieldMap.keys.mkString(", ")})""")
      ))
    })
  }

  def columnIndicesAndDataTypes(requestedSchema: StructType,
      schema: StructType): Seq[(Int, StructField)] = {
    if (requestedSchema.isEmpty) {
      val (narrowestOrdinal, narrowestField) =
        schema.fields.zipWithIndex.map(f => f._2 -> f._1).minBy { f =>
          ColumnType(f._2.dataType).defaultSize
        }
      Seq(narrowestOrdinal -> narrowestField)
    } else {
      requestedSchema.map { a =>
        schema.fieldIndex(Utils.fieldName(a)) -> a
      }
    }
  }

  def getInsertString(table: String, userSchema: StructType): String = {
    val sb = new mutable.StringBuilder("INSERT INTO ")
    sb.append(table).append(" VALUES (")
    (1 until userSchema.length).foreach { _ =>
      sb.append("?,")
    }
    sb.append("?)").toString()
  }

  def getPutString(table: String, userSchema: StructType): String = {
    val sb = new mutable.StringBuilder("PUT INTO ")
    sb.append(table).append(" VALUES (")
    (1 until userSchema.length).foreach { _ =>
      sb.append("?,")
    }
    sb.append("?)").toString()
  }

  def getInsertStringWithColumnName(table: String,
      rddSchema: StructType): String = {
    val sb = new StringBuilder(s"INSERT INTO $table (")
    val schemaFields = rddSchema.fields
    (0 until (schemaFields.length - 1)).foreach { i =>
      sb.append(schemaFields(i).name).append(',')
    }
    sb.append(schemaFields(schemaFields.length - 1).name)
    sb.append(") VALUES (")

    (1 until rddSchema.length).foreach { _ =>
      sb.append("?,")
    }
    sb.append("?)").toString()
  }

  def setStatementParameters(stmt: PreparedStatement,
      row: mutable.ArrayBuffer[Any]): Unit = {
    var col = 1
    val len = row.length
    while (col <= len) {
      val colVal = row(col - 1)
      if (colVal != null) {
        colVal match {
          case s: String => stmt.setString(col, s)
          case i: Int => stmt.setInt(col, i)
          case l: Long => stmt.setLong(col, l)
          case d: Double => stmt.setDouble(col, d)
          case f: Float => stmt.setFloat(col, f)
          case s: Short => stmt.setInt(col, s)
          case b: Byte => stmt.setInt(col, b)
          case b: Boolean => stmt.setBoolean(col, b)
          case b: Array[Byte] => stmt.setBytes(col, b)
          case ts: java.sql.Timestamp => stmt.setTimestamp(col, ts)
          case d: java.sql.Date => stmt.setDate(col, d)
          case t: java.sql.Time => stmt.setTime(col, t)
          case d: Decimal => stmt.setBigDecimal(col, d.toJavaBigDecimal)
          case bd: java.math.BigDecimal => stmt.setBigDecimal(col, bd)
          case _ => stmt.setObject(col, colVal)
        }
      } else {
        stmt.setNull(col, java.sql.Types.NULL)
      }
      col += 1
    }
  }

  final val PARTITION_BY = "PARTITION_BY"
  final val REPLICATE = "REPLICATE"
  final val BUCKETS = "BUCKETS"

  def getTotalPartitions(sc: SparkContext,
      parameters: mutable.Map[String, String],
      forManagedTable: Boolean, forColumnTable: Boolean = true,
      forSampleTable: Boolean = false): Int = {
    parameters.getOrElse(BUCKETS, {
      val partitions = SnappyContext.getClusterMode(sc) match {
        case LocalMode(_, _) =>
          if (forSampleTable) DEFAULT_SAMPLE_TABLE_BUCKETS_LOCAL_MODE
          else DEFAULT_TABLE_BUCKETS_LOCAL_MODE
        case _ =>
          if (forSampleTable)  DEFAULT_SAMPLE_TABLE_BUCKETS
          else DEFAULT_TABLE_BUCKETS
      }
      if (forManagedTable) {
        if (forColumnTable) {
          // column tables are always partitioned
          parameters += BUCKETS -> partitions
        } else if (parameters.contains(PARTITION_BY) &&
            !parameters.contains(REPLICATE)) {
          parameters += BUCKETS -> partitions
        }
      }
      partitions
    }).toInt
  }

  final def cachedBatchesToRows(
      cachedBatches: Iterator[CachedBatch],
      requestedColumns: Array[String],
      schema: StructType, forScan: Boolean): Iterator[InternalRow] = {
    // check and compare with InMemoryColumnarTableScan
    val numColumns = requestedColumns.length
    val columnIndices = new Array[Int](numColumns)
    val columnDataTypes = new Array[DataType](numColumns)
    for (index <- 0 until numColumns) {
      val fieldIndex = schema.fieldIndex(requestedColumns(index))
      columnIndices(index) = index
      schema.fields(fieldIndex).dataType match {
        case udt: UserDefinedType[_] => columnDataTypes(index) = udt.sqlType
        case other => columnDataTypes(index) = other
      }
    }
    val columnarIterator = GenerateColumnAccessor.generate(columnDataTypes)
    columnarIterator.initialize(cachedBatches, columnDataTypes, columnIndices)
    columnarIterator
  }

  def removeCachedObjects(sqlContext: SQLContext, table: String,
      registerDestroy: Boolean = false): Unit = {
    // clean up the connection pool and caches on executors first
    Utils.mapExecutors(sqlContext,
      removeCachedObjects(table)
    ).count()
    // then on the driver
    removeCachedObjects(table)()
    if (registerDestroy) {
      SnappyStoreHiveCatalog.registerRelationDestroy()
    }
  }

  def removeCachedObjects(table: String): () => Iterator[Unit] = () => {
    ConnectionPool.removePoolReference(table)
    CodeGeneration.removeCache(table)
    Iterator.empty
  }
}

object ConnectionType extends Enumeration {
  type ConnectionType = Value
  val Embedded, Net, Unknown = Value
}

private[sql] final class ArrayBufferForRows(externalStore: ExternalStore,
    colTableName: String,
    schema: StructType,
    useCompression: Boolean,
    bufferSize: Int,
    reservoirInRegion: Boolean, columnBatchSize: Int) {

  private var holder = getCachedBatchHolder(-1)

  def getCachedBatchHolder(bucketId: Int): CachedBatchHolder =
    new CachedBatchHolder(columnBuilders, 0,
      Int.MaxValue, schema, (c: CachedBatch) =>
        externalStore.storeCachedBatch(colTableName, c, bucketId))

  def columnBuilders: Array[ColumnBuilder] = schema.map {
    attribute =>
      val columnType = ColumnType(attribute.dataType)
      val initialBufferSize = columnType.defaultSize * bufferSize
      ColumnBuilder(attribute.dataType, initialBufferSize,
        attribute.name, useCompression)
  }.toArray

  def endRows(u: Unit): Unit = {
    holder.forceEndOfBatch()
    if (!reservoirInRegion) {
      holder = getCachedBatchHolder(-1)
    }
  }

  def startRows(u: Unit, bucketId: Int): Unit = {
    if (reservoirInRegion) {
      holder = getCachedBatchHolder(bucketId)
    }
  }

  def appendRow(u: Unit, row: InternalRow): Unit = {
    holder.appendRow(row)
  }

  def forceEndOfBatch(): Unit = endRows(())
}
