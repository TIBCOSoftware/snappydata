package org.apache.spark.sql.hive

import java.io.File
import java.net.{URL, URLClassLoader}

import com.google.common.cache.{CacheBuilder, CacheLoader}
import io.snappydata.{Constant, Property}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.{Hive, HiveException}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.collection.{ExecutorLocalPartition, Utils}
import org.apache.spark.sql.columnar.{ConnectionType, ExternalStoreUtils, JDBCAppendableRelation}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.execution.datasources.{CaseInsensitiveMap, LogicalRelation, ResolvedDataSource}
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog._
import org.apache.spark.sql.hive.client._
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.row.JDBCMutableRelation
import org.apache.spark.sql.sources.{JdbcExtendedUtils, JdbcExtendedDialect, BaseRelation}
import org.apache.spark.sql.store.ExternalStore
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.streaming._
import org.apache.spark.{Logging, Partition, TaskContext}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.implicitConversions

/**
 * Catalog using Hive for persistence and adding Snappy extensions like
 * stream/topK tables and returning LogicalPlan to materialize these entities.
 *
 * Created by Sumedh on 7/27/15.
 */
class SnappyStoreHiveCatalog(context: SnappyContext)
    extends Catalog with Logging {

  override val conf = context.conf

  val tables = new mutable.HashMap[QualifiedTableName, LogicalPlan]()



  /**
   * The version of the hive client that will be used to communicate
   * with the meta-store for catalog.
   */
  protected[sql] val hiveMetastoreVersion: String =
    context.getConf(HIVE_METASTORE_VERSION, hiveExecutionVersion)

  /**
   * The location of the jars that should be used to instantiate the Hive
   * meta-store client.  This property can be one of three options:
   *
   * a classpath in the standard format for both hive and hadoop.
   *
   * builtin - attempt to discover the jars that were used to load Spark SQL
   * and use those. This option is only valid when using the
   * execution version of Hive.
   *
   * maven - download the correct version of hive on demand from maven.
   */
  protected[sql] def hiveMetastoreJars(): String =
    context.getConf(HIVE_METASTORE_JARS)

  /**
   * A comma separated list of class prefixes that should be loaded using the
   * ClassLoader that is shared between Spark SQL and a specific version of
   * Hive. An example of classes that should be shared is JDBC drivers that
   * are needed to talk to the meta-store. Other classes that need to be
   * shared are those that interact with classes that are already shared.
   * For example, custom appender used by log4j.
   */
  protected[sql] def hiveMetastoreSharedPrefixes(): Seq[String] =
    context.getConf(HIVE_METASTORE_SHARED_PREFIXES, jdbcPrefixes())
        .filterNot(_ == "")

  private def jdbcPrefixes() = Seq("com.pivotal.gemfirexd", "com.mysql.jdbc",
    "org.postgresql", "com.microsoft.sqlserver", "oracle.jdbc")

  /**
   * A comma separated list of class prefixes that should explicitly be
   * reloaded for each version of Hive that Spark SQL is communicating with.
   * For example, Hive UDFs that are declared in a prefix that typically
   * would be shared (i.e. org.apache.spark.*)
   */
  protected[sql] def hiveMetastoreBarrierPrefixes(): Seq[String] =
    context.getConf(HIVE_METASTORE_BARRIER_PREFIXES).filterNot(_ == "")

  /**
   * Overridden by child classes that need to set configuration before
   * client init (but after hive-site.xml).
   */
  protected def configure(): Map[String, String] = Map.empty

  /**
   * Hive client that is used to retrieve metadata from the Hive MetaStore.
   * The version of the Hive client that is used here must match the
   * meta-store that is configured in the hive-site.xml file.
   */
  @transient
  protected[sql] var client: ClientInterface = newClient()

  private def newClient(): ClientInterface = {

    val metaVersion = IsolatedClientLoader.hiveVersion(hiveMetastoreVersion)

    // We instantiate a HiveConf here to read in the hive-site.xml file and
    // then pass the options into the isolated client loader
    val metadataConf = new HiveConf()
    var warehouse = metadataConf.get(
      HiveConf.ConfVars.METASTOREWAREHOUSE.varname)
    if (warehouse == null || warehouse.isEmpty ||
        warehouse == HiveConf.ConfVars.METASTOREWAREHOUSE.getDefaultExpr) {
      // append warehouse to current directory
      warehouse = new java.io.File("./warehouse").getCanonicalPath
      metadataConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, warehouse)
    }
    logInfo("Default warehouse location is " + warehouse)

    val (useSnappyStore, dbURL, dbDriver) = resolveMetaStoreDBProps()
    if (useSnappyStore) {
      logInfo(s"Using SnappyStore as metastore database, dbURL = $dbURL")
      metadataConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, dbURL)
      metadataConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER,
        dbDriver)
      metadataConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME,
        "HIVE_METASTORE")
    } else if (dbURL != null) {
      logInfo(s"Using specified metastore database, dbURL = $dbURL")
      metadataConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, dbURL)
      if (dbDriver != null) {
        metadataConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER,
          dbDriver)
      } else {
        metadataConf.unset(
          HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER.varname)
      }
      metadataConf.unset(
        HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME.varname)
    } else {
      logInfo("Using Hive metastore database, dbURL = " +
          metadataConf.getVar(HiveConf.ConfVars.METASTORECONNECTURLKEY))
    }

    val allConfig = metadataConf.asScala.map(e =>
      e.getKey -> e.getValue).toMap ++ configure

    val hiveMetastoreJars = this.hiveMetastoreJars()
    if (hiveMetastoreJars == "builtin") {
      if (hiveExecutionVersion != hiveMetastoreVersion) {
        throw new IllegalArgumentException("Builtin jars can only be used " +
            "when hive default version == hive metastore version. Execution: " +
            s"$hiveExecutionVersion != Metastore: $hiveMetastoreVersion. " +
            "Specify a vaild path to the correct hive jars using " +
            s"$HIVE_METASTORE_JARS or change " +
            s"$HIVE_METASTORE_VERSION to $hiveExecutionVersion.")
      }

      // We recursively find all jars in the class loader chain,
      // starting from the given classLoader.
      def allJars(classLoader: ClassLoader): Array[URL] = classLoader match {
        case null => Array.empty[URL]
        case urlClassLoader: URLClassLoader =>
          urlClassLoader.getURLs ++ allJars(urlClassLoader.getParent)
        case other => allJars(other.getParent)
      }

      val classLoader = org.apache.spark.util.Utils.getContextOrSparkClassLoader
      val jars = allJars(classLoader)
      if (jars.length == 0) {
        throw new IllegalArgumentException(
          "Unable to locate hive jars to connect to metastore. " +
              "Please set spark.sql.hive.metastore.jars.")
      }

      logInfo("Initializing HiveMetastoreConnection version " +
          s"$hiveMetastoreVersion using Spark classes.")
      //new ClientWrapper(metaVersion, allConfig, classLoader)
      new IsolatedClientLoader(
        version = metaVersion,
        execJars = jars.toSeq,
        config = allConfig,
        isolationOn = false,
        barrierPrefixes = hiveMetastoreBarrierPrefixes(),
        sharedPrefixes = hiveMetastoreSharedPrefixes()).createClient()
    } else if (hiveMetastoreJars == "maven") {
      logInfo("Initializing HiveMetastoreConnection version " +
          s"$hiveMetastoreVersion using maven.")
      IsolatedClientLoader.forVersion(
        version = hiveMetastoreVersion,
        config = allConfig,
        barrierPrefixes = hiveMetastoreBarrierPrefixes(),
        sharedPrefixes = hiveMetastoreSharedPrefixes()).createClient()
    } else {
      // Convert to files and expand any directories.
      val jars = hiveMetastoreJars.split(File.pathSeparator).flatMap {
        case path if new File(path).getName == "*" =>
          val files = new File(path).getParentFile.listFiles()
          if (files == null) {
            logWarning(s"Hive jar path '$path' does not exist.")
            Nil
          } else {
            files.filter(_.getName.toLowerCase.endsWith(".jar"))
          }
        case path =>
          new File(path) :: Nil
      }.map(_.toURI.toURL)

      logInfo("Initializing HiveMetastoreConnection version " +
          s"$hiveMetastoreVersion using $jars")
      new IsolatedClientLoader(
        version = metaVersion,
        execJars = jars.toSeq,
        config = allConfig,
        isolationOn = true,
        barrierPrefixes = hiveMetastoreBarrierPrefixes(),
        sharedPrefixes = hiveMetastoreSharedPrefixes()).createClient()
    }
  }

  private def resolveMetaStoreDBProps(): (Boolean, String, String) = {
    val sc = context.sparkContext
    val sparkConf = sc.conf
    val url = sparkConf.get(Property.metastoreDBURL, null)
    if (url != null) {
      val driver = sparkConf.get(Property.metastoreDriver, null)
      (false, url, driver)
    } else SnappyContext.getClusterMode(sc) match {
      case SnappyEmbeddedMode(_, _) | ExternalEmbeddedMode(_, _) |
           LocalMode(_, _) =>
        (true, ExternalStoreUtils.defaultStoreURL(sc) +
            ";disable-streaming=true;default-persistent=true",
            Constant.JDBC_EMBEDDED_DRIVER)
      case SnappyShellMode(_, props) =>
        (true, Constant.DEFAULT_EMBEDDED_URL +
            ";host-data=false;disable-streaming=true;default-persistent=true;" +
            props, Constant.JDBC_EMBEDDED_DRIVER)
      case ExternalClusterMode(_, _) =>
        (false, null, null)
    }
  }

  /** A cache of Spark SQL data source tables that have been accessed. */
  protected[sql] val cachedDataSourceTables = {
    val cacheLoader = new CacheLoader[QualifiedTableName, LogicalPlan]() {
      override def load(in: QualifiedTableName): LogicalPlan = {
        logDebug(s"Creating new cached data source for $in")
        val table = in.getTable(client)

        def schemaStringFromParts: Option[String] = {
          table.properties.get(HIVE_SCHEMA_NUMPARTS).map { numParts =>
            val parts = (0 until numParts.toInt).map { index =>
              val partProp = s"$HIVE_SCHEMA_PART.$index"
              table.properties.get(partProp) match {
                case Some(part) => part
                case None => throw new AnalysisException("Could not read " +
                    "schema from metastore because it is corrupted (missing " +
                    s"part $index of the schema, $numParts parts expected).")
              }
            }
            // Stick all parts back to a single schema string.
            parts.mkString
          }
        }

        // Originally, we used spark.sql.sources.schema to store the schema
        // of a data source table. After SPARK-6024, this flag was removed.
        // Still need to support the deprecated property.
        val schemaString = table.properties.get(HIVE_SCHEMA_OLD)
            .orElse(schemaStringFromParts)

        val userSpecifiedSchema =
          schemaString.map(s => DataType.fromJson(s).asInstanceOf[StructType])

        val partitionColumns = table.partitionColumns.map(_.name)
        val provider = table.properties(HIVE_PROVIDER)
        val options = table.serdeProperties

        val resolved = options.get(JdbcExtendedUtils.SCHEMA_PROPERTY) match {
          case Some(schema) => JdbcExtendedUtils.externalResolvedDataSource(
            context, schema, provider, SaveMode.Ignore, options)

          case None =>
            // add allowExisting in properties used by some implementations
            ResolvedDataSource(context, userSpecifiedSchema,
              partitionColumns.toArray, provider, options +
                  (JdbcExtendedUtils.ALLOW_EXISTING_PROPERTY -> "true"))
        }

        LogicalRelation(resolved.relation)
      }
    }

    CacheBuilder.newBuilder().maximumSize(1000).build(cacheLoader)
  }

  def processTableIdentifier(tableIdentifier: String): String = {
    SnappyStoreHiveCatalog.processTableIdentifier(tableIdentifier, conf)
  }

  def newQualifiedTableName(tableIdent: TableIdentifier): QualifiedTableName =
    new QualifiedTableName(tableIdent.database,
      SnappyStoreHiveCatalog.processTableIdentifier(tableIdent.table, conf))

  def newQualifiedTableName(tableIdent: String): QualifiedTableName = {
    val tableName = processTableIdentifier(tableIdent)
    val dotIndex = tableName.indexOf('.')
    if (dotIndex > 0 && tableName.indexOf('.', dotIndex + 1) > 0) {
      new QualifiedTableName(Some(tableName.substring(0, dotIndex)),
        tableName.substring(dotIndex + 1))
    } else {
      new QualifiedTableName(None, tableName)
    }
  }

  override def refreshTable(tableIdent: TableIdentifier): Unit = {
    // refreshTable does not eagerly reload the cache. It just invalidates
    // the cache. it is better at here to invalidate the cache to avoid
    // confusing warning logs from the cache loader (e.g. cannot find data
    // source provider, which is only defined for data source table).
    invalidateTable(tableIdent)
  }

  def invalidateTable(tableIdent: TableIdentifier): Unit = {
    cachedDataSourceTables.invalidate(newQualifiedTableName(tableIdent))
  }

  override def unregisterAllTables(): Unit = {
    tables.clear()
  }

  override def unregisterTable(tableIdentifier: TableIdentifier): Unit = {
    unregisterTable(newQualifiedTableName(tableIdentifier))
  }

  def unregisterTable(tableIdent: QualifiedTableName): Unit = {
    if (tables.contains(tableIdent)) {
      context.truncateTable(tableIdent.table)
      tables -= tableIdent
    }
  }

  def lookupRelation(tableIdent: QualifiedTableName,
      alias: Option[String]): LogicalPlan = {
    val plan = tables.getOrElse(tableIdent,
      tableIdent.getTableOption(client) match {
        case Some(table) =>
          if (table.properties.contains(HIVE_PROVIDER)) {
            cachedDataSourceTables(tableIdent)
          } else if (table.tableType == VirtualView) {
            val viewText = table.viewText
                .getOrElse(sys.error("Invalid view without text."))
            context.parseSql(viewText)
          } else {
            throw new IllegalStateException(
              s"Unsupported table type ${table.tableType}")
          }

        case None =>
          throw new AnalysisException(s"Table Not Found: $tableIdent")
      })
    // If an alias was specified by the lookup, wrap the plan in a
    // sub-query so that attributes are properly qualified with this alias
    Subquery(alias.getOrElse(tableIdent.table), plan)
  }

  def lookupRelation(tableIdentifier: String): LogicalPlan = {
    lookupRelation(newQualifiedTableName(tableIdentifier), None)
  }

  override def lookupRelation(tableIdentifier: TableIdentifier,
      alias: Option[String]): LogicalPlan = {
    lookupRelation(newQualifiedTableName(tableIdentifier), alias)
  }

  override def tableExists(tableIdentifier: TableIdentifier): Boolean = {
    tableExists(newQualifiedTableName(tableIdentifier))
  }

  def tableExists(tableIdentifier: String): Boolean = {
    tableExists(newQualifiedTableName(tableIdentifier))
  }

  def tableExists(tableName: QualifiedTableName): Boolean = {
    tables.contains(tableName) ||
        tableName.getTableOption(client).isDefined
  }

  override def registerTable(tableIdentifier: TableIdentifier,
      plan: LogicalPlan): Unit = {
    tables += (newQualifiedTableName(tableIdentifier) -> plan)
  }

  def registerTable(tableName: QualifiedTableName, plan: LogicalPlan): Unit = {
    tables += (tableName -> plan)
  }

  def registerExternalTable(tableName: QualifiedTableName,
      userSpecifiedSchema: Option[StructType],
      partitionColumns: Array[String], provider: String,
      options: Map[String, String],
      tableType: ExternalTableType.Type): Unit = {
    createDataSourceTable(tableName, tableType,
      userSpecifiedSchema, partitionColumns, provider, options)
  }

  def unregisterExternalTable(tableIdent: QualifiedTableName): Unit = {
    val dbName = tableIdent.getDatabase(client)
    try {
      client.dropTable(dbName, tableIdent.table)
    } catch {
      case he: HiveException if isDisconnectException(he) =>
        // stale GemXD connection
        Hive.closeCurrent()
        client = newClient()
        client.dropTable(dbName, tableIdent.table)
    }
  }

  /**
   * Creates a data source table (a table created with USING clause) in Hive's
   * meta-store. Returns true when the table has been created else false.
   *
   * @param tableType the type of external table: ROW, COLUMN, SAMPLE etc
   */
  def createDataSourceTable(
      tableIdent: QualifiedTableName,
      tableType: ExternalTableType.Type,
      userSpecifiedSchema: Option[StructType],
      partitionColumns: Array[String],
      provider: String,
      options: Map[String, String]): Unit = {
    val tableProperties = new mutable.HashMap[String, String]
    tableProperties.put(HIVE_PROVIDER, provider)

    // Saves optional user specified schema.  Serialized JSON schema string
    // may be too long to be stored into a single meta-store SerDe property.
    // In this case, we split the JSON string and store each part as a
    // separate SerDe property.
    if (userSpecifiedSchema.isDefined) {
      val threshold = conf.schemaStringLengthThreshold
      val schemaJsonString = userSpecifiedSchema.get.json
      // Split the JSON string.
      val parts = schemaJsonString.grouped(threshold).toSeq
      tableProperties.put(HIVE_SCHEMA_NUMPARTS, parts.size.toString)
      parts.zipWithIndex.foreach { case (part, index) =>
        tableProperties.put(s"$HIVE_SCHEMA_PART.$index", part)
      }
    }

    val metastorePartitionColumns = userSpecifiedSchema.map { schema =>
      val fields = Utils.getFields(partitionColumns, schema,
        "createDataSourceTable")
      fields.map { field =>
        HiveColumn(
          name = field.name,
          hiveType = HiveMetastoreTypes.toMetastoreType(field.dataType),
          comment = "")
      }.toSeq
    }.getOrElse {
      if (partitionColumns.length > 0) {
        // The table does not have a specified schema, which means that the
        // schema will be inferred when we load the table. So, we are not
        // expecting partition columns and we will discover partitions
        // when we load the table. However, if there are specified partition
        // columns, we simply ignore them and provide a warning message..
        logWarning(s"The schema and partitions of table " +
            s"${tableIdent.table} will be inferred when it is " +
            s"loaded. Specified partition columns " +
            s"(${partitionColumns.mkString(",")}) will be ignored.")
      }
      Seq.empty[HiveColumn]
    }

    tableProperties.put("EXTERNAL", tableType.toString)

    val hiveTable = HiveTable(
      specifiedDatabase = Option(tableIdent.getDatabase(client)),
      name = tableIdent.table,
      schema = Seq.empty,
      partitionColumns = metastorePartitionColumns,
      tableType = ExternalTable,
      properties = tableProperties.toMap,
      serdeProperties = options)
    try {
      client.createTable(hiveTable)
    } catch {
      case he: HiveException if isDisconnectException(he) =>
        // stale GemXD connection
        Hive.closeCurrent()
        client = newClient()
        client.createTable(hiveTable)
    }
  }

  private def isDisconnectException(t: Throwable): Boolean = {
    if (t != null) {
      val tClass = t.getClass.getName
      tClass.contains("DisconnectedException") ||
          tClass.contains("DisconnectException") ||
          isDisconnectException(t.getCause)
    } else {
      false
    }
  }

  def registerSampleTable(table: String, schema: StructType,
      samplingOptions: Map[String, Any], df: Option[SampleDataFrame] = None,
      streamTable: Option[QualifiedTableName] = None,
      jdbcSource: Option[Map[String, String]] = None): SampleDataFrame = {
    throw new UnsupportedOperationException("missing AQP jar")
  }

  /*def registerTopK(tableIdent: String,
      schema: StructType, topkOptions: Map[String, Any], rdd: RDD[(Int, TopK)]): Unit = {
    throw new UnsupportedOperationException("missing AQP jar")
  }*/

  // TODO: The JDBC source is currently reading a property jdbcStore
  // to find out the type of the jdbc store. This is a
  // temporary arrangement.
  def getExternalTable(jdbcSource: Map[String, String]): ExternalStore = {
    val options = new CaseInsensitiveMap(jdbcSource)
    val externalSource = options.get("jdbcstore") match {
      case Some(x) => x
      case None => "org.apache.spark.sql.store.JDBCSourceAsStore"
    }
    val constructor = org.apache.spark.util.Utils.getContextOrSparkClassLoader
        .loadClass(externalSource).getConstructor(classOf[Map[String, String]])
    constructor.newInstance(options).asInstanceOf[ExternalStore]
  }

  def createTable(externalStore: ExternalStore, tableStr: String,
      tableName: String, dropIfExists: Boolean): Unit = {
    val isLoner = Utils.isLoner(context.sparkContext)

    val rdd = new DummyRDD(context) {
      override def compute(split: Partition,
          taskContext: TaskContext): Iterator[InternalRow] = {
        DriverRegistry.register(externalStore.driver)
        JdbcDialects.get(externalStore.url) match {
          case d: JdbcExtendedDialect =>
            val extraProps = d.extraDriverProperties(isLoner).propertyNames
            while (extraProps.hasMoreElements) {
              val p = extraProps.nextElement()
              if (externalStore.connProps.get(p) != null) {
                sys.error(s"Master specific property $p " +
                    "shouldn't exist here in Executors")
              }
            }
          case _ =>
        }

        val conn = ExternalStoreUtils.getConnection(externalStore.url,
          externalStore.connProps, driverDialect = null, isLoner = false)
        conn.close()
        Iterator.empty
      }

      override protected def getPartitions: Array[Partition] = {
        //TODO : Find a cleaner way of starting all executors.
        val partitions = new Array[Partition](100)
        for (p <- 0 until 100) {
          partitions(p) = new ExecutorLocalPartition(p, null)
        }
        partitions
      }
    }

    rdd.collect()

    //val tableName = processTableIdentifier(tableIdent)
    val connProps = externalStore.connProps
    val dialect = JdbcDialects.get(externalStore.url)
    dialect match {
      case d: JdbcExtendedDialect =>
        connProps.putAll(d.extraDriverProperties(isLoner))
    }

    externalStore.tryExecute(tableName, {
      case conn =>
        if (dropIfExists) {
          JdbcExtendedUtils.dropTable(conn, tableName, dialect, context,
            ifExists = true)
        }
        JdbcExtendedUtils.executeUpdate(tableStr, conn)
        dialect match {
          case d: JdbcExtendedDialect => d.initializeTable(tableName,
            conf.caseSensitiveAnalysis, conn)
        }
    })
  }

  def createExternalTableForCachedBatches(tableName: String,
      externalStore: ExternalStore): Unit = {
    require(tableName != null && tableName.length > 0,
      "registerAndInsertIntoExternalStore: expected non-empty table name")

    //val tableName = processTableIdentifier(tableIdent)
    val (primarykey, partitionStrategy) = ExternalStoreUtils.getConnectionType(
      externalStore.url) match {
      case ConnectionType.Embedded =>
        (s"constraint ${tableName}_bucketCheck check (bucketId != -1), " +
            "primary key (uuid, bucketId)", "partition by column (bucketId)")
      // TODO: [sumedh] Neeraj, the partition clause should come from JdbcDialect or something
      case _ => ("primary key (uuid)", "partition by primary key")
    }

    createTable(externalStore, s"create table $tableName (uuid varchar(36) " +
        s"not null, bucketId integer, cachedBatch Blob not null, $primarykey) " +
        s"$partitionStrategy", tableName, dropIfExists = true)
  }

  /** tableName is assumed to be pre-normalized with processTableIdentifier*/
  private[sql] def getStreamTableSchema(
      tableIdentifier: String): StructType = {
    getStreamTableSchema(newQualifiedTableName(tableIdentifier))
  }

  /** tableName is assumed to be pre-normalized with processTableIdentifier */
  private[sql] def getStreamTableSchema[T](
      tableName: QualifiedTableName): StructType = {
    val plan: LogicalPlan = tables.getOrElse(tableName,
      throw new IllegalStateException(s"Plan for stream $tableName not found"))
    plan match {
      case LogicalRelation(sr: SocketStreamRelation, _) => sr.schema
      case LogicalRelation(kr: KafkaStreamRelation, _) => kr.schema
      case LogicalRelation(fr: FileStreamRelation, _) => fr.schema
      case LogicalRelation(tr: TwitterStreamRelation, _) => tr.schema
      case LogicalRelation(dkr: DirectKafkaStreamRelation, _) => dkr.schema
      case _ => throw new IllegalStateException(
        s"StreamRelation was expected for $tableName but got $plan")
    }
  }

  override def getTables(dbIdent: Option[String]): Seq[(String, Boolean)] = {
    val client = this.client
    val dbName = dbIdent.map(processTableIdentifier)
        .getOrElse(client.currentDatabase)
    tables.collect {
      case (tableIdent, _) if tableIdent.getDatabase(client) == dbName =>
        (tableIdent.table, true)
    }.toSeq ++ client.listTables(dbName).map((_, false))
  }
}

object SnappyStoreHiveCatalog {

  /** The version of hive used internally by Spark SQL. */
  val hiveExecutionVersion = HiveContext.hiveExecutionVersion

  val HIVE_METASTORE_VERSION = HiveContext.HIVE_METASTORE_VERSION
  val HIVE_METASTORE_JARS = HiveContext.HIVE_METASTORE_JARS
  val HIVE_METASTORE_SHARED_PREFIXES =
    HiveContext.HIVE_METASTORE_SHARED_PREFIXES
  val HIVE_METASTORE_BARRIER_PREFIXES =
    HiveContext.HIVE_METASTORE_BARRIER_PREFIXES

  val HIVE_PROVIDER = "spark.sql.sources.provider"
  val HIVE_SCHEMA_NUMPARTS = "spark.sql.sources.schema.numParts"
  val HIVE_SCHEMA_PART = "spark.sql.sources.schema.part"
  val HIVE_SCHEMA_OLD = "spark.sql.sources.schema"

  def processTableIdentifier(tableIdentifier: String, conf: SQLConf): String = {
    if (conf.caseSensitiveAnalysis) {
      tableIdentifier
    } else {
      Utils.normalizeId(tableIdentifier)
    }
  }

  def closeCurrent(): Unit = {
    Hive.closeCurrent()
  }
}

/** A fully qualified identifier for a table (i.e. [dbName.]schema.tableName) */
final class QualifiedTableName(_database: Option[String], _tableIdent: String)
    extends TableIdentifier(_tableIdent, _database) {

  @transient private[this] var _table: Option[HiveTable] = None

  def getDatabase(client: ClientInterface): String =
    database.getOrElse(client.currentDatabase)

  def getTableOption(client: ClientInterface) = _table.orElse {
    _table = client.getTableOption(getDatabase(client), table)
    _table
  }

  def getTable(client: ClientInterface) =
    getTableOption(client).getOrElse(throw new AnalysisException(
      s"Table Not Found: $table (in database: ${getDatabase(client)})"))

  override def toString: String =
    database.map(_ + '.').getOrElse("") + table
}

object ExternalTableType extends Enumeration {
  type Type = Value

  val Row = Value("ROW")
  val Columnar = Value("COLUMN")
  val Stream = Value("STREAM")
  val Sample = Value("SAMPLE")
  val TopK = Value("TOPK")

  def getTableType(relation: BaseRelation): ExternalTableType.Type = {
    relation match {
      case x: JDBCMutableRelation => ExternalTableType.Row
      case x: JDBCAppendableRelation => ExternalTableType.Columnar
      case _ => ExternalTableType.Row
    }
  }
}
