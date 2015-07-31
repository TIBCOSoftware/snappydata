package org.apache.spark.sql

import java.sql.Connection

import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection
import org.apache.spark.sql.columnar.{ConnectionType, ExternalStoreUtils}
import org.apache.spark.sql.execution.row.{GemFireXDDialect, JdbcExtendedDialect}
import org.apache.spark.sql.jdbc.{DriverRegistry, JdbcDialects}
import org.apache.spark.sql.store.{ExternalStore, CachedBatchPartitioner}
import org.apache.spark.sql.store.impl.JDBCSourceAsStore

import scala.collection.mutable
import scala.language.implicitConversions

import org.apache.spark.{TaskContext, Partition, Logging}
import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.analysis.SimpleCatalog
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}
import org.apache.spark.sql.collection.{ExecutorLocalPartition, Utils}
import org.apache.spark.sql.execution.{LogicalRDD, StratifiedSample, TopKWrapper}
import org.apache.spark.sql.sources.LogicalRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.StreamRelation

/**
 * Catalog primarily tracking stream/topK tables and returning LogicalPlan
 * to materialize these entities.
 *
 * Created by Soubhik on 5/13/15.
 */
final class SnappyStoreCatalog(snappyContext: SnappyContext,
    override val conf: CatalystConf)
    extends SimpleCatalog(conf) with Logging {

  protected val currentDatabase: String = "snappydata"

  val topKStructures = new mutable.HashMap[String, TopKWrapper]()

  def processTableIdentifier(tableIdentifier: String): String = {
    if (conf.caseSensitiveAnalysis) {
      tableIdentifier
    } else {
      Utils.normalizeId(tableIdentifier)
    }
  }

  override def processTableIdentifier(
      tableIdentifier: Seq[String]): Seq[String] = {
    if (conf.caseSensitiveAnalysis) {
      tableIdentifier
    } else {
      tableIdentifier.map(Utils.normalizeId)
    }
  }

  override def unregisterAllTables(): Unit = {
    super.unregisterAllTables()
    topKStructures.clear()
  }

  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    val tblName = getDbTableName(tableIdent)
    if (tables.contains(tblName)) {
      snappyContext.truncateTable(tblName)
      tables -= tblName
    } else if (topKStructures.contains(tblName)) {
      topKStructures -= tblName
    }
  }

  def lookupRelation(tableIdentifier: String): LogicalPlan = {
    val tblName = processTableIdentifier(tableIdentifier)
    tables.getOrElse(tblName, sys.error(s"Table Not Found: $tblName"))
  }

  override def lookupRelation(tableIdentifier: Seq[String],
      alias: Option[String]): LogicalPlan = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    val tableName = getDbTableName(tableIdent)
    val table = tables.getOrElse(tableName,
      sys.error(s"Table Not Found: $tableName"))

    // If an alias was specified by the lookup, wrap the plan in a
    // sub-query so that attributes are properly qualified with this alias.
    alias.map(Subquery(_, table)).getOrElse(table)
  }

  def registerSampleTable(schema: StructType, tableIdent: String,
      samplingOptions: Map[String, Any], df: Option[SampleDataFrame] = None,
      streamTableIdent: Option[String] = None,
      jdbcSource: Option[Map[String, String]] = None): SampleDataFrame = {
    require(tableIdent != null && tableIdent.length > 0,
      "registerSampleTable: expected non-empty table name")

    val tableName = processTableIdentifier(tableIdent)

    if (tables.contains(tableName)) {
      throw new IllegalStateException(
        s"A structure with name $tableName is already defined")
    }

    // add or overwrite existing name attribute
    val opts = Utils.normalizeOptions(samplingOptions)
        .filterKeys(_ != "name") + ("name" -> tableName)

    // update the options in any provided StratifiedSample LogicalPlan
    df foreach (_.logicalPlan.options = opts)
    // create new StratifiedSample LogicalPlan if none was passed
    // (currently for streaming case)
    val sampleDF = df.getOrElse {
      val plan: LogicalRDD = LogicalRDD(schema.toAttributes,
        new DummyRDD(snappyContext))(snappyContext)
      val streamTable = streamTableIdent.map(processTableIdentifier)
      val newDF = new SampleDataFrame(snappyContext,
        StratifiedSample(opts, plan, streamTable)())
      if (jdbcSource.isEmpty) {
        snappyContext.cacheManager.cacheQuery(newDF, Some(tableName))
      } else {
        val externalStore = new JDBCSourceAsStore(jdbcSource.get);
        createExternalTableForCachedBatches(tableName, externalStore)
        snappyContext.cacheManager.cacheQuery_ext(newDF, Some(tableName), externalStore)
      }
      newDF
    }
    tables.put(tableName, sampleDF.logicalPlan)
    sampleDF
  }

  def registerTopK(tableIdent: String, streamTableIdent: String,
      schema: StructType, topkOptions: Map[String, Any]): Unit = {
    val tableName = processTableIdentifier(tableIdent)
    val streamTableName = processTableIdentifier(streamTableIdent)

    if (topKStructures.contains(tableName)) {
      throw new IllegalStateException(
        s"A structure with name $tableName is already defined")
    }

    topKStructures.put(tableName, TopKWrapper(tableName, topkOptions, schema,
      Some(streamTableName)))
  }

  def registerAndInsertIntoExternalStore(df: DataFrame, tableIdent: String,
      schema: StructType, jdbcSource: Map[String, String]): Unit = {
    require(tableIdent != null && tableIdent.length > 0,
      "registerAndInsertIntoExternalStore: expected non-empty table name")

    val tableName = processTableIdentifier(tableIdent)
    val externalStore = new JDBCSourceAsStore(jdbcSource);
    createExternalTableForCachedBatches(tableName, externalStore)
    tables.put(tableName, df.logicalPlan)
    snappyContext.cacheManager.cacheQuery_ext(df, Some(tableName), externalStore)
  }


  def createTable(externalStore: ExternalStore, tableStr: String, tableName: String, dropIfExists: Boolean) = {
    val rdd = new DummyRDD(snappyContext) {
      override def compute(split: Partition, taskContext: TaskContext): Iterator[Row] = {
        GemFireXDDialect.init()
        DriverRegistry.register(externalStore.driver)
        JdbcDialects.get(externalStore.url) match {
          case d: JdbcExtendedDialect =>
            for (p <- d.extraCreateTableProperties(context).propertyNames()) {
              if (externalStore.connProps.getProperty(p) != null) {
                sys.error(s"Master specific property ${p} shouldn't exist here in Executors")
              }
            }
        }

        val conn = ExternalStoreUtils.getConnection(externalStore.url, externalStore.connProps)
        conn.close()
        Iterator.empty
      }

      override protected def getPartitions: Array[Partition] = {
        val partitions = new Array[Partition](1000)
        for (p <- 0 until 1000) {
          partitions(p) = new ExecutorLocalPartition(p, null)
        }
        partitions
      }
    }

    rdd.collect

    //val tableName = processTableIdentifier(tableIdent)
    //val (url, driver, poolProps, connProps, hikariCP) = ExternalStoreUtils.validateAndGetAllProps(jdbcSource)
    val connProps = externalStore.connProps
    JdbcDialects.get(externalStore.url) match {
      case d: JdbcExtendedDialect =>
        connProps.putAll(d.extraCreateTableProperties(snappyContext.sparkContext))
    }

    externalStore.tryExecute(tableName, {
      case conn => {
        val statement = conn.createStatement();
        try {
          if (dropIfExists) {
            statement.execute(s"drop table if exists $tableName")
          }
          statement.execute(tableStr)
          statement.execute(s"call sys.CREATE_ALL_BUCKETS('$tableName')")

        } finally {
          statement.close()
        }
      }
    })
  }

  private def createExternalTableForCachedBatches(tableName: String,
      externalStore: ExternalStore): Unit = {
    require(tableName != null && tableName.length > 0,
      "registerAndInsertIntoExternalStore: expected non-empty table name")

    val (primarykey, partitionStrategy) = ExternalStoreUtils.getConnectionType(externalStore.url) match {
      case ConnectionType.Embedded => {
        (s"constraint ${tableName}_bucketCheck check (bucketId != -1), primary key (uuid, bucketId)",
          "partition by column (bucketId)")
      }
      case _ => ("primary key (uuid)", "partition by primary key")
    }

    createTable(externalStore, s"create table $tableName (uuid varchar(36) not null, bucketId integer, cachedBatch Blob not null," +
                      s"$primarykey" +
                      s") $partitionStrategy", tableName, true)
  }

  /** tableName is assumed to be pre-normalized with processTableIdentifier */
  private[sql] def getStreamTableRelation[T](
      tableName: String): StreamRelation[T] = {
    val plan: LogicalPlan = tables.getOrElse(tableName,
      throw new IllegalStateException("Plan for stream not found"))

    plan match {
      case LogicalRelation(sr: StreamRelation[T]) => sr
      case _ => throw new IllegalStateException("StreamRelation was expected")
    }
  }

  override def getTables(dbName: Option[String]): Seq[(String, Boolean)] = {
    super.getTables(dbName)
  }

  override def refreshTable(dbName: String, tableName: String): Unit = {
    throw new NotImplementedError()
  }

  override def tableExists(tableIdentifier: Seq[String]): Boolean = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    val tableName = getDbTableName(tableIdent)
    tables.contains(tableName)
  }
}
