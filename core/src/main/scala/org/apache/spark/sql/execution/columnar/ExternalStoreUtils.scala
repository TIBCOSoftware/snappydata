/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

import java.sql.{Connection, PreparedStatement, Types}
import java.util.Properties
import java.util.concurrent.atomic.AtomicReference
import javax.naming.NameNotFoundException

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal
import com.gemstone.gemfire.internal.cache.ExternalTableMetaData
import com.pivotal.gemfirexd.auth.callback.UserAuthenticator
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor
import com.pivotal.gemfirexd.internal.impl.jdbc.authentication.{AuthenticationServiceBase, LDAPAuthenticationSchemeImpl}
import com.pivotal.gemfirexd.internal.impl.sql.execute.GranteeIterator
import com.pivotal.gemfirexd.jdbc.ClientAttribute
import io.snappydata.sql.catalog.SnappyExternalCatalog
import io.snappydata.thrift.snappydataConstants
import io.snappydata.{Constant, Property}
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodegenContext}
import org.apache.spark.sql.catalyst.expressions.{Attribute, BinaryExpression, Expression, TokenLiteral}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.impl.JDBCSourceAsColumnarStore
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.execution.ui.SQLListener
import org.apache.spark.sql.execution.{BufferedRowIterator, CodegenSupport, CodegenSupportOnExecutor, ConnectionPool, RefreshMetadata}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.row.SnappyStoreDialect
import org.apache.spark.sql.sources.{ConnectionProperties, ExternalSchemaRelationProvider, JdbcExtendedDialect, JdbcExtendedUtils}
import org.apache.spark.sql.store.CodeGeneration
import org.apache.spark.sql.types._
import org.apache.spark.util.{Utils => SparkUtils}

/**
 * Utility methods used by external storage layers.
 */
object ExternalStoreUtils {

  private[spark] final lazy val (defaultTableBuckets, defaultSampleTableBuckets) = {
    val sc = Option(SnappyContext.globalSparkContext)
    sc.map(_.schedulerBackend) match {
      case Some(local: LocalSchedulerBackend) =>
        // apply a max limit of 32 in local mode since there is not much
        // scaling to be had beyond that on most machines
        val result = math.min(32, math.max(local.totalCores << 1, 8)).toString
        // use same number of partitions for sample table in local mode
        (result, result)
      case _ => sc.flatMap(s => Property.Locators.getOption(s.conf).map(Utils.toLowerCase)) match {
        // reduce defaults for localhost-only cluster too
        case Some(s) if s.startsWith("localhost:") || s.startsWith("localhost[") ||
            s.startsWith("127.0.0.1") || s.startsWith("::1[") =>
          val result = math.min(32, math.max(SnappyContext.totalCoreCount.get() << 1, 8)).toString
          (result, result)
        case _ => ("128", "64")
      }
    }
  }

  final val INDEX_TYPE = "INDEX_TYPE"
  final val INDEX_NAME = "INDEX_NAME"
  final val COLUMN_BATCH_SIZE = "COLUMN_BATCH_SIZE"
  final val COLUMN_MAX_DELTA_ROWS = "COLUMN_MAX_DELTA_ROWS"
  final val COMPRESSION_CODEC = "COMPRESSION"
  final val RELATION_FOR_SAMPLE = "RELATION_FOR_SAMPLE"

  // inbuilt basic table properties
  final val PARTITION_BY = "PARTITION_BY"
  final val REPLICATE = "REPLICATE"
  final val BUCKETS = "BUCKETS"
  final val KEY_COLUMNS = "KEY_COLUMNS"

  // these two are obsolete column table properties only for backward compatibility
  final val COLUMN_BATCH_SIZE_TRANSIENT = "COLUMN_BATCH_SIZE_TRANSIENT"
  final val COLUMN_MAX_DELTA_ROWS_TRANSIENT = "COLUMN_MAX_DELTA_ROWS_TRANSIENT"

  val ddlOptions: Seq[String] = Seq(INDEX_NAME, COLUMN_BATCH_SIZE,
    COLUMN_BATCH_SIZE_TRANSIENT, COLUMN_MAX_DELTA_ROWS,
    COLUMN_MAX_DELTA_ROWS_TRANSIENT, COMPRESSION_CODEC, RELATION_FOR_SAMPLE, KEY_COLUMNS)

  registerBuiltinDrivers()

  def registerBuiltinDrivers(): Unit = {
    DriverRegistry.register(Constant.JDBC_EMBEDDED_DRIVER)
    DriverRegistry.register(Constant.JDBC_CLIENT_DRIVER)
  }

  private def addProperty(props: mutable.Map[String, String], key: String,
      default: String): Unit = {
    if (!props.contains(key)) props.put(key, default)
  }

  private def defaultMaxExternalPoolSize: String =
    String.valueOf(math.max(256, Runtime.getRuntime.availableProcessors() * 8))

  private def defaultMaxEmbeddedPoolSize: String =
    String.valueOf(math.max(256, Runtime.getRuntime.availableProcessors() * 16))

  def getAllPoolProperties(url: String, driver: String,
      poolProps: Map[String, String], hikariCP: Boolean,
      isEmbedded: Boolean): Map[String, String] = {
    // setup default pool properties
    val props = new mutable.HashMap[String, String]()
    if (poolProps.nonEmpty) props ++= poolProps
    if (driver != null && !driver.isEmpty) {
      addProperty(props, "driverClassName", driver)
    }
    val defaultMaxPoolSize = if (isEmbedded) defaultMaxEmbeddedPoolSize
    else defaultMaxExternalPoolSize
    if (hikariCP) {
      props.put("jdbcUrl", url)
      addProperty(props, "maximumPoolSize", defaultMaxPoolSize)
      addProperty(props, "minimumIdle", "10")
      addProperty(props, "idleTimeout", "120000")
    } else {
      props.put("url", url)
      addProperty(props, "maxActive", defaultMaxPoolSize)
      addProperty(props, "maxIdle", defaultMaxPoolSize)
      addProperty(props, "initialSize", "4")
      addProperty(props, "testOnBorrow", "true")
      // embedded validation check is cheap
      if (isEmbedded) addProperty(props, "validationInterval", "0")
      else addProperty(props, "validationInterval", "10000")
    }
    props.toMap
  }

  def getDriver(url: String, dialect: JdbcDialect): String = {
    dialect match {
      case SnappyStoreDialect => Constant.JDBC_EMBEDDED_DRIVER
      case SnappyStoreClientDialect => Constant.JDBC_CLIENT_DRIVER
      case SnappyDataPoolDialect => Constant.JDBC_CLIENT_POOL_DRIVER
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
    val dbtableProp = SnappyExternalCatalog.DBTABLE_PROPERTY
    val table = parameters.remove(dbtableProp)
        .getOrElse(sys.error(s"Option '$dbtableProp' not specified"))
    // obsolete property but has to be removed when recovering from old meta-stores
    parameters.remove("ALLOWEXISTING")
    // remove the "path" property added by Spark hive catalog
    parameters.remove("path")
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
    new CaseInsensitiveMap(optMap.toMap)
  }

  def getLdapGroupsForUser(userId: String): Array[String] = {
    val auth = Misc.getMemStoreBooting.getDatabase.getAuthenticationService.
      asInstanceOf[AuthenticationServiceBase].getAuthenticationScheme

    auth match {
      case x: LDAPAuthenticationSchemeImpl => x.getLdapGroupsOfUser(userId).
        toArray[String](Array.empty)
      case _ => throw new NameNotFoundException("Require LDAP authentication scheme for " +
        "LDAP group support but is " + auth)
    }
  }

  def getExpandedGranteesIterator(grantees: Seq[String]): Iterator[String] = {
    new GranteeIterator(grantees.asJava, null, true, -1, -1, -1, null, null).asScala
  }

  def defaultStoreURL(sparkContext: Option[SparkContext]): String = sparkContext match {
    case None => defaultStoreURL(SnappyContext.getClusterMode(SnappyContext.globalSparkContext))
    case Some(sc) => defaultStoreURL(SnappyContext.getClusterMode(sc))
  }

  def defaultStoreURL(clusterMode: ClusterMode): String = clusterMode match {
    case null | SnappyEmbeddedMode(_, _) =>
      // Already connected to SnappyData in embedded mode.
      Constant.DEFAULT_EMBEDDED_URL +
          ";host-data=false;mcast-port=0;internal-connection=true"
    case ThinClientConnectorMode(_, url) =>
      url + ";route-query=false;internal-connection=true"
    case LocalMode(_, url) =>
      Constant.DEFAULT_EMBEDDED_URL + ";" + url + ";internal-connection=true"
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
    val driver = parameters.remove("driver") match {
      case Some(d) => DriverRegistry.register(d); d
      case None => getDriver(url, dialect)
    }

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
      case SnappyStoreDialect =>
        SnappyStoreDialect.addExtraDriverProperties(isLoner, connProps)
        true
      case SnappyStoreClientDialect =>
        SnappyStoreClientDialect.addExtraDriverProperties(isLoner, connProps)
        connProps.setProperty(ClientAttribute.ROUTE_QUERY, "false")
        executorConnProps.setProperty(ClientAttribute.ROUTE_QUERY, "false")
        // increase the lob-chunk-size to match/exceed column batch size
        val batchSize = parameters.get(COLUMN_BATCH_SIZE.toLowerCase) match {
          case Some(s) => sizeAsBytes(s, COLUMN_BATCH_SIZE)
          case None => session.map(defaultColumnBatchSize).getOrElse(
            sizeAsBytes(Property.ColumnBatchSize.defaultValue.get, Property.ColumnBatchSize.name))
        }
        val columnBatchSize = math.min(Int.MaxValue, math.max((batchSize << 2) / 3,
          snappydataConstants.DEFAULT_LOB_CHUNKSIZE))
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
      case Some(s) => getConnProps(s, url, driver, dialect, poolProps, connProps,
        executorConnProps, hikariCP)
      case None => ConnectionProperties(url, driver, dialect, poolProps, connProps,
        executorConnProps, hikariCP)
    }
  }

  private def getConnProps(session: SparkSession, url: String, driver: String,
      dialect: JdbcDialect, poolProps: Map[String, String], connProps: Properties,
      executorConnProps: Properties, hikariCP: Boolean): ConnectionProperties = {
    val (user, password) = getCredentials(session)

    val isSnappy = dialect match {
      case _: SnappyDataBaseDialect => true
      case _ => false
    }

    if (!user.isEmpty && !password.isEmpty && isSnappy) {
      def secureProps(props: Properties): Properties = {
        props.setProperty(ClientAttribute.USERNAME, user)
        props.setProperty(ClientAttribute.PASSWORD, password)
        props
      }

      // Hikari only take 'username'. So does Tomcat
      def securePoolProps(props: Map[String, String]): Map[String, String] = {
        props + (ClientAttribute.USERNAME_ALT.toLowerCase -> user) + (ClientAttribute.PASSWORD ->
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
    (session.conf.get(prefix + ClientAttribute.USERNAME, ""),
        session.conf.get(prefix + ClientAttribute.PASSWORD, ""))
  }

  def getConnection(id: String, connProperties: ConnectionProperties,
      forExecutor: Boolean): Connection = {
    connProperties.driver match {
      case Constant.JDBC_EMBEDDED_DRIVER | Constant.JDBC_CLIENT_DRIVER => // ignore
      case driver => Utils.registerDriver(driver)
    }
    val connProps = if (forExecutor) connProperties.executorConnProps
    else connProperties.connProps
    ConnectionPool.getPoolConnection(id, connProperties.dialect,
      connProperties.poolProps, connProps, connProperties.hikariCP)
  }

  def getConnectionType(dialect: JdbcDialect): ConnectionType.Value = {
    dialect match {
      case SnappyStoreDialect => ConnectionType.Embedded
      case SnappyStoreClientDialect => ConnectionType.Net
      case _ => ConnectionType.Unknown
    }
  }

  /** check if the DataSource implements ExternalSchemaRelationProvider */
  def isExternalSchemaRelationProvider(provider: String): Boolean = {
    try {
      classOf[ExternalSchemaRelationProvider].isAssignableFrom(
        DataSource.lookupDataSource(provider))
    } catch {
      case NonFatal(_) => false
    }
  }

  // This should match JDBCRDD.compileFilter for best performance
  def unhandledFilter(f: Expression): Boolean = f match {
    case _: expressions.EqualTo | _: expressions.LessThan | _: expressions.GreaterThan |
         _: expressions.LessThanOrEqual | _: expressions.GreaterThanOrEqual =>
      val b = f.asInstanceOf[BinaryExpression]
      !((b.left.isInstanceOf[Attribute] && TokenLiteral.isConstant(b.right)) ||
          (TokenLiteral.isConstant(b.left) && b.right.isInstanceOf[Attribute]))
    case expressions.IsNull(_: Attribute) | expressions.IsNotNull(_: Attribute) => false
    case _: expressions.StartsWith | _: expressions.EndsWith | _: expressions.Contains =>
      val b = f.asInstanceOf[BinaryExpression]
      !(b.left.isInstanceOf[Attribute] && TokenLiteral.isConstant(b.right))
    case _ => true
  }

  private def checkIndexedColumn(a: Attribute,
      indexedCols: scala.collection.Set[String]): Option[Attribute] = {
    val col = a.name
    // quote identifiers when they could be case-sensitive
    if (indexedCols.contains(col)) Some(a.withName("\"" + col + '"'))
    else {
      // case-insensitive check
      val ucol = Utils.toUpperCase(col)
      if ((col ne ucol) && indexedCols.contains(ucol)) Some(a) else None
    }
  }

  // below should exactly match RowFormatScanRDD.compileFilter
  def handledFilter(f: Expression,
      indexedCols: scala.collection.Set[String]): Option[Expression] = f match {
    // only pushdown filters if there is an index on the column;
    // keeping a bit conservative and not pushing other filters because
    // Spark execution engine is much faster at filter apply (though
    //   its possible that not all indexed columns will be used for
    //   index lookup still push down all to keep things simple)
    case expressions.EqualTo(a: Attribute, v) =>
      checkIndexedColumn(a, indexedCols).map(expressions.EqualTo(_, v))
    case expressions.EqualTo(v, a: Attribute) =>
      checkIndexedColumn(a, indexedCols).map(expressions.EqualTo(v, _))
    case expressions.LessThan(a: Attribute, v) =>
      checkIndexedColumn(a, indexedCols).map(expressions.LessThan(_, v))
    case expressions.LessThan(v, a: Attribute) =>
      checkIndexedColumn(a, indexedCols).map(expressions.LessThan(v, _))
    case expressions.GreaterThan(a: Attribute, v) =>
      checkIndexedColumn(a, indexedCols).map(expressions.GreaterThan(_, v))
    case expressions.GreaterThan(v, a: Attribute) =>
      checkIndexedColumn(a, indexedCols).map(expressions.GreaterThan(v, _))
    case expressions.LessThanOrEqual(a: Attribute, v) =>
      checkIndexedColumn(a, indexedCols).map(expressions.LessThanOrEqual(_, v))
    case expressions.LessThanOrEqual(v, a: Attribute) =>
      checkIndexedColumn(a, indexedCols).map(expressions.LessThanOrEqual(v, _))
    case expressions.GreaterThanOrEqual(a: Attribute, v) =>
      checkIndexedColumn(a, indexedCols).map(expressions.GreaterThanOrEqual(_, v))
    case expressions.GreaterThanOrEqual(v, a: Attribute) =>
      checkIndexedColumn(a, indexedCols).map(expressions.GreaterThanOrEqual(v, _))
    case expressions.StartsWith(a: Attribute, v) =>
      checkIndexedColumn(a, indexedCols).map(expressions.StartsWith(_, v))
    case expressions.In(a: Attribute, v) =>
      checkIndexedColumn(a, indexedCols).map(expressions.In(_, v))
    // At least one column should be indexed for the AND condition to be
    // evaluated efficiently
    // Commenting out the below conditions for SNAP-2463. This needs to be fixed
    /* case expressions.And(left, right) => handledFilter(left, indexedCols) match {
      case None => handledFilter(right, indexedCols)
      case lf@Some(l) => handledFilter(right, indexedCols) match {
        case None => lf
        case Some(r) => Some(expressions.And(l, r))
      }
    }
    // ORList optimization requires all columns to have indexes
    // which is ensured by the condition below
    case expressions.Or(left, right) => handledFilter(left, indexedCols) match {
      case None => None
      case Some(l) => handledFilter(right, indexedCols) match {
        case None => None
        case Some(r) => Some(expressions.Or(l, r))
      }
    } */
    case _ => None
  }

  def unhandledFilter(f: Expression, indexedCols: scala.collection.Set[String]): Boolean =
    handledFilter(f, indexedCols) eq None

  /**
   * Prune all but the specified columns from the specified Catalyst schema.
   *
   * @param fieldMap - The Catalyst column name to metadata of the master table
   * @param columns  - The list of desired columns
   * @return A Catalyst schema corresponding to columns in the given order.
   */
  def pruneSchema(fieldMap: scala.collection.Map[String, StructField],
      columns: Array[String], columnType: String): StructType = {
    new StructType(columns.map { col =>
      fieldMap.get(col) match {
        case None => throw new AnalysisException("Cannot resolve " +
            s"""$columnType column name "$col" among (${fieldMap.keys.mkString(", ")})""")
        case Some(f) => f
      }
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
        stmt.setNull(col, Types.NULL)
      }
      col += 1
    }
  }

  def getAndSetTotalPartitions(session: SnappySession,
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

  def removeCachedObjects(sqlContext: SQLContext, table: String): Unit = {
    RefreshMetadata.executeOnAll(sqlContext.sparkContext,
      RefreshMetadata.REMOVE_CACHED_OBJECTS, table)
  }

  def removeCachedObjects(table: String): Unit = {
    ConnectionPool.removePoolReference(table)
    CodeGeneration.removeCache(table)
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

  def getExternalStoreOnExecutor(parameters: mutable.Map[String, String],
      partitions: Int, tableName: String, schema: StructType): ExternalStore = {
    val connProperties: ConnectionProperties =
      ExternalStoreUtils.validateAndGetAllProps(None, parameters)
    new JDBCSourceAsColumnarStore(connProperties, partitions, tableName, schema)
  }

  def getTableSchema(schemaAsJson: String): StructType = StructType.fromString(schemaAsJson)

  /**
   * Get the table schema from CatalogTable.properties if present.
   */
  def getTableSchema(props: Map[String, String], forView: Boolean): Option[StructType] = {
    (if (forView) {
      JdbcExtendedUtils.readSplitProperty(SnappyExternalCatalog.SPLIT_VIEW_SCHEMA, props) match {
        case None => JdbcExtendedUtils.readSplitProperty(SnappyExternalCatalog.TABLE_SCHEMA, props)
        case s => s
      }
    } else JdbcExtendedUtils.readSplitProperty(SnappyExternalCatalog.TABLE_SCHEMA, props)) match {
      case Some(s) => Some(StructType.fromString(s))
      case None => None
    }
  }

  def getColumnMetadata(schema: StructType): java.util.List[ExternalTableMetaData.Column] = {
    schema.toList.map { f =>
      val (dataType, typeName, jdbcType, prec, scale) = SnappyStoreDialect.getJDBCMetadata(
        f.dataType, f.metadata, forTableDefn = false)
      val (precision, width) = if (prec == -1) {
        val dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(jdbcType, f.nullable)
        if (dtd ne null) {
          (dtd.getPrecision, dtd.getMaximumWidth)
        } else (dataType.defaultSize, dataType.defaultSize)
      } else (prec, prec)
      new ExternalTableMetaData.Column(f.name, jdbcType, typeName,
        precision, scale, width, f.nullable)
    }.asJava
  }

  def getExternalTableMetaData(qualifiedTable: String): ExternalTableMetaData = {
    getExternalTableMetaData(qualifiedTable,
      GemFireXDUtils.getGemFireContainer(qualifiedTable, true), checkColumnStore = false)
  }

  def getExternalTableMetaData(qualifiedTable: String, container: GemFireContainer,
      checkColumnStore: Boolean): ExternalTableMetaData = {
    container match {
      case null =>
        throw new IllegalStateException(s"Table $qualifiedTable not found in containers")
      case c => c.fetchHiveMetaData(false) match {
        case null =>
          throw new IllegalStateException(s"Table $qualifiedTable not found in hive metadata")
        case m => if (checkColumnStore && !c.isColumnStore) {
          throw new IllegalStateException(s"Table $qualifiedTable not a column table")
        } else m
      }
    }
  }

  def sizeAsBytes(str: String, propertyName: String): Int =
    sizeAsBytes(str, propertyName, 1, Int.MaxValue).toInt

  def sizeAsBytes(str: String, propertyName: String, minSize: Long, maxSize: Long): Long = {
    val s = str.trim
    // if last character is a digit then it does not have a unit suffix
    val size =
      if (Character.isDigit(s.charAt(s.length - 1))) java.lang.Long.parseLong(s)
      else SparkUtils.byteStringAsBytes(s)
    if (size >= minSize && size <= maxSize) size
    else {
      throw new IllegalArgumentException(
        s"$propertyName should be >= $minSize and <= $maxSize (provided = $str)")
    }
  }

  def defaultColumnBatchSize(session: SparkSession): Int = {
    sizeAsBytes(Property.ColumnBatchSize.get(session.sessionState.conf),
      Property.ColumnBatchSize.name)
  }

  def checkPositiveNum(n: Int, propertyName: String): Int = {
    if (n > 0 && n < Int.MaxValue) n
    else {
      throw new IllegalArgumentException(
        s"$propertyName should be > 0 and < 2GB (provided = $n)")
    }
  }

  def defaultColumnMaxDeltaRows(session: SparkSession): Int = {
    checkPositiveNum(Property.ColumnMaxDeltaRows.get(session.sessionState.conf),
      Property.ColumnMaxDeltaRows.name)
  }

  def getSQLListener: AtomicReference[SQLListener] = {
    SparkSession.sqlListener
  }
}

object ConnectionType extends Enumeration {
  type ConnectionType = Value
  val Embedded, Net, Unknown = Value
}
