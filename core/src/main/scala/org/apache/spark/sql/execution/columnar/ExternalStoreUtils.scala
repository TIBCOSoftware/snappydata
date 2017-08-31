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
package org.apache.spark.sql.execution.columnar

import java.sql.{Connection, PreparedStatement}
import java.util.Properties
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.gemstone.gemfire.internal.cache.ExternalTableMetaData
import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor
import com.pivotal.gemfirexd.internal.shared.common.reference.Limits
import com.pivotal.gemfirexd.jdbc.ClientAttribute
import io.snappydata.thrift.snappydataConstants
import io.snappydata.{Constant, Property}

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodegenContext}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.impl.JDBCSourceAsColumnarStore
import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry, JdbcUtils}
import org.apache.spark.sql.execution.ui.SQLListener
import org.apache.spark.sql.execution.{BufferedRowIterator, CodegenSupport, CodegenSupportOnExecutor, ConnectionPool}
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.row.{GemFireXDClientDialect, GemFireXDDialect}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.CodeGeneration
import org.apache.spark.sql.types._
import org.apache.spark.util.{Utils => SparkUtils}

/**
 * Utility methods used by external storage layers.
 */
object ExternalStoreUtils {

  private[spark] final lazy val (defaultTableBuckets, defaultSampleTableBuckets) =
    Option(SnappyContext.globalSparkContext).map(_.schedulerBackend) match {
      case Some(local: LocalSchedulerBackend) =>
        // apply a max limit of 64 in local mode since there is not much
        // scaling to be had beyond that on most processors
        val result = math.min(64, local.totalCores << 1).toString
        // use same number of partitions for sample table in local mode
        (result, result)
      case _ => ("128", "64")
    }

  final val INDEX_TYPE = "INDEX_TYPE"
  final val INDEX_NAME = "INDEX_NAME"
  final val DEPENDENT_RELATIONS = "DEPENDENT_RELATIONS"
  final val COLUMN_BATCH_SIZE = "COLUMN_BATCH_SIZE"
  final val COLUMN_BATCH_SIZE_TRANSIENT = "COLUMN_BATCH_SIZE_TRANSIENT"
  final val COLUMN_MAX_DELTA_ROWS = "COLUMN_MAX_DELTA_ROWS"
  final val COLUMN_MAX_DELTA_ROWS_TRANSIENT = "COLUMN_MAX_DELTA_ROWS_TRANSIENT"
  final val COMPRESSION_CODEC = "COMPRESSION_CODEC"
  final val RELATION_FOR_SAMPLE = "RELATION_FOR_SAMPLE"
  // internal properties stored as hive table parameters
  final val USER_SPECIFIED_SCHEMA = "USER_SCHEMA"

  val ddlOptions: Seq[String] = Seq(INDEX_NAME, COLUMN_BATCH_SIZE,
    COLUMN_BATCH_SIZE_TRANSIENT, COLUMN_MAX_DELTA_ROWS,
    COLUMN_MAX_DELTA_ROWS_TRANSIENT, COMPRESSION_CODEC, RELATION_FOR_SAMPLE)

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
    String.valueOf(math.max(256, Runtime.getRuntime.availableProcessors() * 8))

  private def defaultMaxEmbeddedPoolSize: String =
    String.valueOf(math.max(256, Runtime.getRuntime.availableProcessors() * 16))

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
      props = addProperty(props, "minimumIdle", "10")
      props = addProperty(props, "idleTimeout", "120000")
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

  def defaultStoreURL(sparkContext: Option[SparkContext]): String = {
    sparkContext match {
      case None => Constant.DEFAULT_EMBEDDED_URL +
          ";host-data=false;mcast-port=0;internal-connection=true"

      case Some(sc) =>
        SnappyContext.getClusterMode(sc) match {
          case SnappyEmbeddedMode(_, _) =>
            // Already connected to SnappyData in embedded mode.
            Constant.DEFAULT_EMBEDDED_URL +
                ";host-data=false;mcast-port=0;internal-connection=true"
          case ThinClientConnectorMode(_, url) =>
            url + ";route-query=false;internal-connection=true"
          case ExternalEmbeddedMode(_, url) =>
            Constant.DEFAULT_EMBEDDED_URL + ";host-data=false;" + url
          case LocalMode(_, url) =>
            Constant.DEFAULT_EMBEDDED_URL + ";" + url + ";internal-connection=true"
          case ExternalClusterMode(_, url) =>
            throw new AnalysisException("Option 'url' not specified for cluster " +
                url)
        }
    }
  }

  def isLocalMode(sparkContext: SparkContext): Boolean = {
    SnappyContext.getClusterMode(sparkContext) match {

      case LocalMode(_, _) => true
      case _ => false
    }
  }

  def validateAndGetAllProps(session: Option[SparkSession],
      parameters: mutable.Map[String, String]): ConnectionProperties = {

    val url = parameters.remove("url").getOrElse(defaultStoreURL(
      session.map(_.sparkContext)))

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

    val isLoner = session match {
      case None => false
      case Some(ss) => Utils.isLoner(ss.sparkContext)
    }

    // remaining parameters are passed as properties to getConnection
    val connProps = new Properties()
    val executorConnProps = new Properties()
    parameters.foreach { kv =>
      if (!ddlOptions.contains(Utils.toUpperCase(kv._1))) {
        connProps.setProperty(kv._1, kv._2)
        executorConnProps.setProperty(kv._1, kv._2)
      }
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
        connProps.setProperty(ClientAttribute.ROUTE_QUERY, "false")
        executorConnProps.setProperty(ClientAttribute.ROUTE_QUERY, "false")
        // increase the lob-chunk-size to match/exceed column batch size
        val batchSize = parameters.get(COLUMN_BATCH_SIZE.toLowerCase) match {
          case Some(s) => Integer.parseInt(s)
          case None => session.map(defaultColumnBatchSize).getOrElse(
            sizeAsBytes(Property.ColumnBatchSize.defaultValue.get, Property.ColumnBatchSize.name))
        }
        val columnBatchSize = math.max((batchSize << 2) / 3,
          snappydataConstants.DEFAULT_LOB_CHUNKSIZE)
        executorConnProps.setProperty(ClientAttribute.THRIFT_LOB_CHUNK_SIZE,
          Integer.toString(columnBatchSize))
        false
      case d: JdbcExtendedDialect =>
        d.addExtraDriverProperties(isLoner, connProps)
        false
      case _ => false
    }
    val allPoolProps = getAllPoolProperties(url, driver,
      poolProps, hikariCP, isEmbedded)
    getConnectionProperties(session, url, driver, dialect, allPoolProps,
      connProps, executorConnProps, hikariCP)
  }

  def getConnectionProperties(session: Option[SparkSession], url: String, driver: String,
      dialect: JdbcDialect, poolProps: Map[String, String], connProps: Properties,
      executorConnProps: Properties, hikariCP: Boolean): ConnectionProperties = {
    session match {
      case Some(_) => getConnProps(session.get, url, driver, dialect, poolProps, connProps,
        executorConnProps, hikariCP)
      case None => ConnectionProperties(url, driver, dialect, poolProps, connProps,
        executorConnProps, hikariCP)
    }
  }

  def getConnProps(session: SparkSession, url: String, driver: String, dialect: JdbcDialect,
      poolProps: Map[String, String], connProps: Properties, executorConnProps: Properties,
      hikariCP: Boolean): ConnectionProperties = {
    val (user, password) = getCredentials(session)

    if (!user.isEmpty && !password.isEmpty) {
      def secureProps(props: Properties): Properties = {
        props.setProperty(Attribute.USERNAME_ATTR, user)
        props.setProperty(Attribute.PASSWORD_ATTR, password)
        props
      }

      // Hikari only take 'username'. So does Tomcat
      def securePoolProps(props: Map[String, String]): Map[String, String] = {
        props + (Attribute.USERNAME_ALT_ATTR.toLowerCase -> user) + (Attribute.PASSWORD_ATTR ->
            password)
      }

      ConnectionProperties(url, driver, dialect, securePoolProps(poolProps),
        secureProps(connProps), secureProps(executorConnProps), hikariCP)
    } else {
      ConnectionProperties(url, driver, dialect, poolProps, connProps, executorConnProps,
        hikariCP)
    }
  }

  def getCredentials(session: SparkSession, prefix: String = ""): (String, String) = {
    val prefix = SnappyContext.getClusterMode(session.sparkContext) match {
      case ThinClientConnectorMode(_, _) => Constant.SPARK_STORE_PREFIX
      case _ => ""
    }
    (session.conf.get(prefix + Attribute.USERNAME_ATTR, ""),
        session.conf.get(prefix + Attribute.PASSWORD_ATTR, ""))
  }

  def getConnection(id: String, connProperties: ConnectionProperties,
      forExecutor: Boolean): Connection = {
    Utils.registerDriver(connProperties.driver)
    val connProps = if (forExecutor) connProperties.executorConnProps
    else connProperties.connProps
    ConnectionPool.getPoolConnection(id, connProperties.dialect,
      connProperties.poolProps, connProps, connProperties.hikariCP)
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

  def getAndSetTotalPartitions(parameters: java.util.Map[String, String],
      forManagedTable: Boolean): Int = {
    // noinspection RedundantDefaultArgument
    getAndSetTotalPartitions(None, parameters.asScala,
      forManagedTable, forColumnTable = true, forSampleTable = false)
  }

  def getAndSetTotalPartitions(sparkContext: Option[SparkContext],
      parameters: mutable.Map[String, String],
      forManagedTable: Boolean, forColumnTable: Boolean = true,
      forSampleTable: Boolean = false): Int = {

    parameters.getOrElse(BUCKETS, {
      val partitions = if (forSampleTable) defaultSampleTableBuckets else defaultTableBuckets
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

  /**
   * Generates code for this subtree.
   *
   * Adapted from WholeStageCodegenExec to allow running on executors.
   *
   * @return the tuple of the codegen context and the actual generated source.
   */
  def codeGenOnExecutor(plan: CodegenSupport,
      child: CodegenSupportOnExecutor): (CodegenContext, CodeAndComment) = {
    val ctx = new CodegenContext
    val code = child.produceOnExecutor(ctx, plan)
    val source =
      s"""
      public Object generate(Object[] references) {
        return new GeneratedIterator(references);
      }

      ${ctx.registerComment(s"""Codegend pipeline for\n${child.treeString.trim}""")}
      final class GeneratedIterator extends ${classOf[BufferedRowIterator].getName} {

        private Object[] references;
        private scala.collection.Iterator[] inputs;
        ${ctx.declareMutableStates()}

        public GeneratedIterator(Object[] references) {
          this.references = references;
        }

        public void init(int index, scala.collection.Iterator inputs[]) {
          partitionIndex = index;
          this.inputs = inputs;
          ${ctx.initMutableStates()}
        }

        ${ctx.declareAddedFunctions()}

        protected void processNext() throws java.io.IOException {
          ${code.trim}
        }
      }
      """.trim

    // try to compile, helpful for debug
    val cleanedSource = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(CodeFormatter.stripExtraNewLines(source),
        ctx.getPlaceHolderToComments()))

    CodeGeneration.logDebug(s"\n${CodeFormatter.format(cleanedSource)}")
    (ctx, cleanedSource)
  }

  def getExternalStoreOnExecutor(parameters: java.util.Map[String, String],
      partitions: Int, tableName: String, schema: StructType): ExternalStore = {
    val connProperties: ConnectionProperties =
      ExternalStoreUtils.validateAndGetAllProps(None, parameters.asScala)
    new JDBCSourceAsColumnarStore(connProperties, partitions, tableName, schema)
  }

  def getTableSchema(
      tableProps: java.util.Map[String, String]): Option[StructType] =
    getTableSchema(tableProps.asScala)

  def getTableSchema(
      tableProps: scala.collection.Map[String, String]): Option[StructType] =
    JdbcExtendedUtils.readSplitProperty(SnappyStoreHiveCatalog.HIVE_SCHEMA_PROP,
      tableProps).map(StructType.fromString)

  def getColumnMetadata(
      schema: Option[StructType]): java.util.List[ExternalTableMetaData.Column] = {
    schema.toList.flatMap(_.map { f =>
      val (dataType, typeName) = f.dataType match {
        case u: UserDefinedType[_] =>
          (Utils.getSQLDataType(u.sqlType), Some(u.userClass.getName))
        case t => (t, None)
      }
      val (prec, scale) = dataType match {
        case d: DecimalType => (d.precision, d.scale)
        case StringType => if (f.metadata.contains(Constant.CHAR_TYPE_SIZE_PROP)) {
          val p = math.min(f.metadata.getLong(Constant.CHAR_TYPE_SIZE_PROP),
            Int.MaxValue).toInt
          (p, -1)
        } else (Limits.DB2_VARCHAR_MAXWIDTH, -1)
        case _: NumericType => (-1, 0)
        case _ => (-1, -1)
      }
      val jdbcTypeOpt = if (dataType eq StringType) {
        Some(org.apache.spark.sql.jdbc.JdbcType("VARCHAR", java.sql.Types.VARCHAR))
      } else { GemFireXDDialect.getJDBCType(dataType).orElse(
        JdbcUtils.getCommonJDBCType(dataType))
      }
      jdbcTypeOpt match {
        case Some(jdbcType) =>
          val (precision, width) = if (prec == -1) {
            val dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(
              jdbcType.jdbcNullType, f.nullable)
            if (dtd ne null) {
              (dtd.getPrecision, dtd.getMaximumWidth)
            } else (dataType.defaultSize, dataType.defaultSize)
          } else (prec, prec)
          new ExternalTableMetaData.Column(f.name, jdbcType.jdbcNullType,
            typeName.getOrElse(jdbcType.databaseTypeDefinition),
            precision, scale, width, f.nullable)
        case None =>
          val precision = if (prec == -1) dataType.defaultSize else prec
          new ExternalTableMetaData.Column(f.name, java.sql.Types.OTHER,
            typeName.getOrElse(dataType.simpleString),
            precision, scale, precision, f.nullable)
      }
    }).asJava
  }

  def getExternalTableMetaData(schema: String, table: String): ExternalTableMetaData = {
    val region = Misc.getRegion(Misc.getRegionPath(schema, table, null), true, false)
    region.getUserAttribute.asInstanceOf[GemFireContainer] match {
      case null =>
        throw new IllegalStateException(s"Table $schema.$table not found in containers")
      case c => c.fetchHiveMetaData(false)
    }
  }

  def sizeAsBytes(str: String, propertyName: String): Int = {
    val size = SparkUtils.byteStringAsBytes(str)
    if (size > 0 && size <= Int.MaxValue) size.toInt
    else {
      throw new IllegalArgumentException(
        s"$propertyName should be > 0 and < 2GB (provided = $str)")
    }
  }

  def defaultColumnBatchSize(session: SparkSession): Int = {
    sizeAsBytes(Property.ColumnBatchSize.get(session.sessionState.conf),
      Property.ColumnBatchSize.name)
  }

  def defaultColumnMaxDeltaRows(session: SparkSession): Int = {
    Property.ColumnMaxDeltaRows.get(session.sessionState.conf)
  }

  def defaultCompressionCodec(session: SparkSession): String = {
    Property.CompressionCodec.get(session.sessionState.conf)
  }

  def getSQLListener: AtomicReference[SQLListener] = {
    SparkSession.sqlListener
  }
}

object ConnectionType extends Enumeration {
  type ConnectionType = Value
  val Embedded, Net, Unknown = Value
}
