package org.apache.spark.sql.hive

import java.io.File
import java.net.{URL, URLClassLoader}

import scala.collection.convert.Wrappers
import scala.collection.mutable
import scala.language.implicitConversions

import com.gemstone.gemfire.cache.partition.PartitionRegionHelper
import com.gemstone.gemfire.cache.{CacheFactory, PartitionAttributesFactory, RegionExistsException}
import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}
import org.apache.spark.sql.collection.{ExecutorLocalPartition, UUIDRegionKey, Utils}
import org.apache.spark.sql.columnar.{CachedBatch, ConnectionType, ExternalStoreUtils}
import org.apache.spark.sql.execution.row.{GemFireXDDialect, JdbcExtendedDialect}
import org.apache.spark.sql.execution.{LogicalRDD, StratifiedSample, TopKWrapper}
import org.apache.spark.sql.hive.client._
import org.apache.spark.sql.jdbc.{DriverRegistry, JdbcDialects}
import org.apache.spark.sql.sources.{LogicalRelation, ResolvedDataSource}
import org.apache.spark.sql.store.ExternalStore
import org.apache.spark.sql.store.impl.{JDBCSourceAsStore, UUIDKeyResolver}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.streaming.StreamRelation
import org.apache.spark.{Logging, Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.TopK

/**
 * Catalog using Hive for persistence and adding Snappy extensions like
 * stream/topK tables and returning LogicalPlan to materialize these entities.
 *
 * Created by Sumedh on 7/27/15.
 */
final class SnappyStoreHiveCatalog(context: SnappyContext)
    extends Catalog with Logging {

  override val conf = context.conf

  val tables = new mutable.HashMap[QualifiedTableName, LogicalPlan]()

  val topKStructures =
    new mutable.HashMap[QualifiedTableName, (TopKWrapper, RDD[(Int, TopK)])]()

  /**
   * The version of the hive client that will be used to communicate
   * with the meta-store for catalog.
   */
  protected[sql] val hiveMetastoreVersion: String = context.getConf(
    SnappyContext.HIVE_METASTORE_VERSION, SnappyContext.hiveDefaultVersion)

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
    context.getConf(SnappyContext.HIVE_METASTORE_JARS, "builtin")

  /**
   * A comma separated list of class prefixes that should be loaded using the
   * ClassLoader that is shared between Spark SQL and a specific version of
   * Hive. An example of classes that should be shared is JDBC drivers that
   * are needed to talk to the meta-store. Other classes that need to be
   * shared are those that interact with classes that are already shared.
   * For example, custom appender used by log4j.
   */
  protected[sql] def hiveMetastoreSharedPrefixes(): Seq[String] =
    context.getConf("spark.sql.hive.metastore.sharedPrefixes", jdbcPrefixes())
        .split(",").filterNot(_ == "")

  private def jdbcPrefixes() = Seq("com.pivotal.gemfirexd", "com.mysql.jdbc",
    "org.postgresql", "com.microsoft.sqlserver", "oracle.jdbc").mkString(",")

  /**
   * A comma separated list of class prefixes that should explicitly be
   * reloaded for each version of Hive that Spark SQL is communicating with.
   * For example, Hive UDFs that are declared in a prefix that typically
   * would be shared (i.e. org.apache.spark.*)
   */
  protected[sql] def hiveMetastoreBarrierPrefixes(): Seq[String] =
    context.getConf("spark.sql.hive.metastore.barrierPrefixes", "")
        .split(",").filterNot(_ == "")

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
  protected[sql] val client: ClientInterface = {
    val metaVersion = IsolatedClientLoader.hiveVersion(hiveMetastoreVersion)

    // We instantiate a HiveConf here to read in the hive-site.xml file and
    // then pass the options into the isolated client loader
    val metadataConf = new HiveConf()
    // `configure` goes second to override other settings.
    val allConfig = Wrappers.JIteratorWrapper(metadataConf.iterator)
        .map(e => e.getKey -> e.getValue).toMap ++ configure()

    val hiveDefaultVersion = SnappyContext.hiveDefaultVersion
    val hiveMetastoreJars = this.hiveMetastoreJars()
    val isolatedLoader = if (hiveMetastoreJars == "builtin") {
      if (hiveDefaultVersion != hiveMetastoreVersion) {
        throw new IllegalArgumentException("Builtin jars can only be used " +
            "when hive default version == hive metastore version. Execution: " +
            s"$hiveDefaultVersion != Metastore: $hiveMetastoreVersion. " +
            "Specify a vaild path to the correct hive jars using " +
            s"${SnappyContext.HIVE_METASTORE_JARS} or change " +
            s"${SnappyContext.HIVE_METASTORE_VERSION} to $hiveDefaultVersion.")
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
      new IsolatedClientLoader(
        version = metaVersion,
        execJars = jars.toSeq,
        config = allConfig,
        isolationOn = true,
        barrierPrefixes = hiveMetastoreBarrierPrefixes(),
        sharedPrefixes = hiveMetastoreSharedPrefixes())
    } else if (hiveMetastoreJars == "maven") {
      logInfo("Initializing HiveMetastoreConnection version " +
          s"$hiveMetastoreVersion using maven.")
      IsolatedClientLoader.forVersion(hiveMetastoreVersion, allConfig)
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
        sharedPrefixes = hiveMetastoreSharedPrefixes())
    }
    isolatedLoader.client
  }

  /** A cache of Spark SQL data source tables that have been accessed. */
  protected[sql] val cachedDataSourceTables = {
    val cacheLoader = new CacheLoader[QualifiedTableName, LogicalPlan]() {
      override def load(in: QualifiedTableName): LogicalPlan = {
        logDebug(s"Creating new cached data source for $in")
        val table = in.getTable(client)

        def schemaStringFromParts: Option[String] = {
          table.properties.get("spark.sql.sources.schema.numParts").map { v =>
            val parts = (0 until v.toInt).map { index =>
              val partProp = s"spark.sql.sources.schema.part.$index"
              table.properties.get(partProp) match {
                case Some(part) => part
                case None => throw new AnalysisException("Could not read " +
                    "schema from metastore because it is corrupted (missing " +
                    s"part $index of the schema, $v parts are expected).")
              }
            }
            // Stick all parts back to a single schema string.
            parts.mkString
          }
        }

        // Originally, we used spark.sql.sources.schema to store the schema
        // of a data source table. After SPARK-6024, this flag was removed.
        // Still need to support the deprecated property.
        val schemaString = table.properties.get("spark.sql.sources.schema")
            .orElse(schemaStringFromParts)

        val userSpecifiedSchema =
          schemaString.map(s => DataType.fromJson(s).asInstanceOf[StructType])

        val partitionColumns = table.partitionColumns.map(_.name)
        val options = table.serdeProperties

        val resolvedRelation = ResolvedDataSource(context,
          userSpecifiedSchema, partitionColumns.toArray,
          table.properties("spark.sql.sources.provider"), options)

        LogicalRelation(resolvedRelation.relation)
      }
    }

    CacheBuilder.newBuilder().maximumSize(1000).build(cacheLoader)
  }

  def processTableIdentifier(tableIdentifier: String): String = {
    if (conf.caseSensitiveAnalysis) {
      tableIdentifier
    } else {
      Utils.normalizeId(tableIdentifier)
    }
  }

  override def processTableIdentifier(tableIdentifier: Seq[String]) = {
    if (conf.caseSensitiveAnalysis) {
      tableIdentifier
    } else {
      tableIdentifier.map(Utils.normalizeId)
    }
  }

  def newQualifiedTableName(dbName: String,
      tableName: String): QualifiedTableName = {
    QualifiedTableName(processTableIdentifier(dbName),
      processTableIdentifier(tableName)).checkAndSetIsCurrentDB(
          client.currentDatabase)
  }

  def newQualifiedTableName(tableIdent: String): QualifiedTableName = {
    val tableName = processTableIdentifier(tableIdent)
    val dotIndex = tableName.indexOf('.')
    if (dotIndex > 0 && tableName.indexOf('.', dotIndex + 1) > 0) {
      QualifiedTableName(tableName.substring(0, dotIndex),
        tableName.substring(dotIndex + 1)).checkAndSetIsCurrentDB(
            client.currentDatabase)
    } else {
      QualifiedTableName(client.currentDatabase, tableName).setIsCurrentDB(true)
    }
  }

  def newQualifiedTableName(tableIdent: Seq[String]): QualifiedTableName = {
    val fullName = processTableIdentifier(tableIdent)
    var isCurrentDB = true
    val dbName = fullName.lift(fullName.size - 3).map { name =>
      isCurrentDB = name == client.currentDatabase
      name
    }.getOrElse(client.currentDatabase)
    val tableName = if (fullName.length > 1) {
      fullName(fullName.size - 2) + '.' + fullName.last
    } else {
      fullName.last
    }
    QualifiedTableName(dbName, tableName).setIsCurrentDB(isCurrentDB)
  }

  override def refreshTable(dbName: String, tableName: String): Unit = {
    // refreshTable does not eagerly reload the cache. It just invalidates
    // the cache. it is better at here to invalidate the cache to avoid
    // confusing warning logs from the cache loader (e.g. cannot find data
    // source provider, which is only defined for data source table).
    invalidateTable(dbName, tableName)
  }

  def invalidateTable(dbName: String, tableName: String): Unit = {
    cachedDataSourceTables.invalidate(newQualifiedTableName(dbName, tableName))
  }

  override def unregisterAllTables(): Unit = {
    tables.clear()
    topKStructures.clear()
  }

  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {
    val qualifiedTable = newQualifiedTableName(tableIdentifier)
    if (tables.contains(qualifiedTable)) {
      context.truncateTable(qualifiedTable.qualifiedName)
      tables -= qualifiedTable
    } else if (topKStructures.contains(qualifiedTable)) {
      topKStructures -= qualifiedTable
    }
  }

  def lookupRelation(tableName: QualifiedTableName,
      alias: Option[String]): LogicalPlan = {
    val plan = tables.getOrElse(tableName,
      tableName.getTableOption(client) match {
        case Some(table) =>
          table.tableType match {
            case ExternalTable => cachedDataSourceTables(tableName)

            case SampleTable => null

            case TopKTable => null

            case VirtualView =>
              val viewText = table.viewText
                  .getOrElse(sys.error("Invalid view without text."))
              context.parseSql(viewText)

            case other =>
              throw new IllegalStateException(s"Unsupported table type $other")
          }

        case None =>
          throw new AnalysisException(s"Table Not Found: $tableName")
      })
    // If an alias was specified by the lookup, wrap the plan in a
    // sub-query so that attributes are properly qualified with this alias
    Subquery(alias.getOrElse(tableName.qualifiedName), plan)
  }

  def lookupRelation(tableIdentifier: String): LogicalPlan = {
    lookupRelation(newQualifiedTableName(tableIdentifier), None)
  }

  override def lookupRelation(tableIdentifier: Seq[String],
      alias: Option[String]): LogicalPlan = {
    lookupRelation(newQualifiedTableName(tableIdentifier), alias)
  }

  override def tableExists(tableIdentifier: Seq[String]): Boolean = {
    val qualifiedTable = newQualifiedTableName(tableIdentifier)
    tables.contains(qualifiedTable) ||
        qualifiedTable.getTableOption(client).isDefined
  }

  // TODO: SW:
  override def registerTable(tableIdentifier: Seq[String],
      plan: LogicalPlan): Unit = {
    tables += (newQualifiedTableName(tableIdentifier) -> plan)
  }

  def registerSampleTable(tableIdent: String, schema: StructType,
      samplingOptions: Map[String, Any], df: Option[SampleDataFrame] = None,
      streamTable: Option[QualifiedTableName] = None,
      jdbcSource: Option[Map[String, String]] = None): SampleDataFrame = {
    require(tableIdent != null && tableIdent.length > 0,
      "registerSampleTable: expected non-empty table name")

    val qualifiedTable = newQualifiedTableName(tableIdent)

    if (tables.contains(qualifiedTable)) {
      throw new IllegalStateException(
        s"A structure with name $qualifiedTable is already defined")
    }

    val tableName = qualifiedTable.qualifiedName
    // add or overwrite existing name attribute
    val opts = Utils.normalizeOptions(samplingOptions)
        .filterKeys(_ != "name") + ("name" -> tableName)

    // update the options in any provided StratifiedSample LogicalPlan
    df foreach (_.logicalPlan.options = opts)
    // create new StratifiedSample LogicalPlan if none was passed
    // (currently for streaming case)
    val sampleDF = df.getOrElse {
      val plan: LogicalRDD = LogicalRDD(schema.toAttributes,
        new DummyRDD(context))(context)
      val newDF = new SampleDataFrame(context,
        StratifiedSample(opts, plan, streamTable)())
      val tableOpt = Some(tableName)
      jdbcSource match {
        case None => context.cacheManager.cacheQuery(newDF, tableOpt)

        case Some(jdbcOptions) =>
          val externalStore = new JDBCSourceAsStore(jdbcOptions)
          createExternalTableForCachedBatches(tableName, externalStore)
          context.cacheManager.cacheQuery_ext(newDF, tableOpt, externalStore)
      }
      newDF
    }
    tables.put(qualifiedTable, sampleDF.logicalPlan)
    sampleDF
  }

  def registerTopK(tableIdent: String, streamTableIdent: String,
      schema: StructType, topkOptions: Map[String, Any], rdd: RDD[(Int, TopK)]): Unit = {
    val qualifiedTable = newQualifiedTableName(tableIdent)
    val streamTable = newQualifiedTableName(streamTableIdent)

    if (topKStructures.contains(qualifiedTable)) {
      throw new IllegalStateException(
        s"A structure with name $qualifiedTable is already defined")
    }

    topKStructures.put(qualifiedTable, TopKWrapper(qualifiedTable, topkOptions,
      schema, Some(streamTable)) -> rdd)
  }

  def registerAndInsertIntoExternalStore(df: DataFrame, tableIdent: String,
      schema: StructType, jdbcSource: Map[String, String]): Unit = {
    require(tableIdent != null && tableIdent.length > 0,
      "registerAndInsertIntoExternalStore: expected non-empty table name")

    val qualifiedTable = newQualifiedTableName(tableIdent)
    val externalStore = new JDBCSourceAsStore(jdbcSource)
    createExternalTableForCachedBatches(qualifiedTable.qualifiedName,
      externalStore)
    tables.put(qualifiedTable, df.logicalPlan)
    context.cacheManager.cacheQuery_ext(df, Some(qualifiedTable.qualifiedName),
      externalStore)
  }

  def createTable(externalStore: ExternalStore, tableStr: String,
      tableName: String, dropIfExists: Boolean) = {
    val isLoner = context.isLoner

    val rdd = new DummyRDD(context) {

      override def compute(split: Partition,
          taskContext: TaskContext): Iterator[Row] = {
        GemFireXDDialect.init()
        DriverRegistry.register(externalStore.driver)
        JdbcDialects.get(externalStore.url) match {
          case d: JdbcExtendedDialect =>
            val extraProps = d.extraCreateTableProperties(isLoner).propertyNames()
            while (extraProps.hasMoreElements) {
              val p = extraProps.nextElement()
              if (externalStore.connProps.get(p) != null) {
                sys.error(s"Master specific property $p " +
                    "shouldn't exist here in Executors")
              }
            }
        }

        val conn = ExternalStoreUtils.getConnection(externalStore.url,
          externalStore.connProps)
        conn.close()
        ExternalStoreUtils.getConnectionType(externalStore.url) match {
          case ConnectionType.Embedded =>
            val cf = CacheFactory.getAnyInstance
            if (cf.getRegion(tableName) == null) {
              val rf = cf.createRegionFactory()
              //val rf = cf.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT)
              val paf = new PartitionAttributesFactory[UUIDRegionKey, CachedBatch]()
              paf.setPartitionResolver(new UUIDKeyResolver)
              rf.setPartitionAttributes(paf.create())
              try {
                val pr = rf.create(tableName)
                PartitionRegionHelper.assignBucketsToPartitions(pr)
              } catch {
                case ree: RegionExistsException => // ignore
              }
            }
          case _ => Nil
        }
        Iterator.empty
      }

      override protected def getPartitions: Array[Partition] = {
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
    JdbcDialects.get(externalStore.url) match {
      case d: JdbcExtendedDialect =>
        connProps.putAll(d.extraCreateTableProperties(isLoner))
    }

    externalStore.tryExecute(tableName, {
      case conn =>
        val statement = conn.createStatement()
        try {
          // TODO: [sumedh] if exists should come from dialect
          if (dropIfExists) {
            statement.execute(s"drop table if exists $tableName")
          }
          statement.execute(tableStr)
          // TODO: [sumedh] below should come from dialect
          statement.execute(s"call sys.CREATE_ALL_BUCKETS('$tableName')")

        } finally {
          statement.close()
        }
    })
  }

  private def createExternalTableForCachedBatches(tableName: String,
      externalStore: ExternalStore): Unit = {
    require(tableName != null && tableName.length > 0,
      "registerAndInsertIntoExternalStore: expected non-empty table name")

    //val tableName = processTableIdentifier(tableIdent)
    val (primarykey, partitionStrategy) = ExternalStoreUtils.getConnectionType(externalStore.url) match {
      case ConnectionType.Embedded =>
        (s"constraint ${tableName}_bucketCheck check (bucketId != -1), primary key (uuid, bucketId)",
            "partition by column (bucketId)")
      // TODO: [sumedh] Neeraj, the partition clause should come from JdbcDialect or something
      case _ => ("primary key (uuid)", "partition by primary key")
    }

    createTable(externalStore, s"create table $tableName (uuid varchar(36) " +
        s"not null, bucketId integer, cachedBatch Blob not null, $primarykey) " +
        s"$partitionStrategy", tableName, dropIfExists = true)
  }

  /** tableName is assumed to be pre-normalized with processTableIdentifier */
  private[sql] def getStreamTableRelation[T](
      tableIdentifier: String): StreamRelation[T] = {
    getStreamTableRelation(newQualifiedTableName(tableIdentifier))
  }

  /** tableName is assumed to be pre-normalized with processTableIdentifier */
  private[sql] def getStreamTableRelation[T](
      tableName: QualifiedTableName): StreamRelation[T] = {
    val plan: LogicalPlan = tables.getOrElse(tableName,
      throw new IllegalStateException(s"Plan for stream $tableName not found"))

    plan match {
      case LogicalRelation(sr: StreamRelation[T]) => sr
      case _ => throw new IllegalStateException(
        s"StreamRelation was expected for $tableName but got $plan")
    }
  }

  override def getTables(dbIdent: Option[String]): Seq[(String, Boolean)] = {
    val dbName = dbIdent.map(processTableIdentifier)
        .getOrElse(client.currentDatabase)
    tables.collect {
      case (name, _) if name.dbName == dbName => (name.qualifiedName, true)
    }.toSeq ++ client.listTables(dbName).map((_, false))
  }
}

/** A fully qualified identifier for a table (i.e., dbName.schema.tableName) */
case class QualifiedTableName(dbName: String, qualifiedName: String) {

  @transient private[this] var _isCurrentDB: Boolean = _
  @transient private[this] var _table: Option[HiveTable] = None

  private[hive] def checkAndSetIsCurrentDB(currentDB: String) = {
    _isCurrentDB = currentDB == dbName
    this
  }

  private[hive] def setIsCurrentDB(isCurrentDB: Boolean) = {
    _isCurrentDB = isCurrentDB
    this
  }

  def getTableOption(client: ClientInterface) = _table.orElse {
    _table = client.getTableOption(dbName, qualifiedName)
    _table
  }

  def getTable(client: ClientInterface) =
    getTableOption(client).getOrElse(throw new AnalysisException(
      s"Table Not Found: $qualifiedName (in database: $dbName)"))

  override def toString: String =
    if (_isCurrentDB) qualifiedName else dbName + '.' + qualifiedName
}

private[sql] case object SampleTable extends TableType {
  override val name = "SAMPLE_TABLE"
}

private[sql] case object TopKTable extends TableType {
  override val name = "TOPK_TABLE"
}
