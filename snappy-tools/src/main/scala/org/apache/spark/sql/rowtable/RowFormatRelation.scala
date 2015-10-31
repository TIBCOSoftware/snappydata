package org.apache.spark.sql.rowtable


import java.sql.Connection
import java.util.Properties

import scala.util.control.NonFatal

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.PartitionedRegion
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.{GfxdPartitionByExpressionResolver, GfxdListPartitionResolver}

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, SnappyCoarseGrainedSchedulerBackend}
import org.apache.spark.scheduler.local.LocalBackend
import org.apache.spark.sql._
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.columnar.{ConnectionType, ExternalStoreUtils}
import org.apache.spark.sql.execution.datasources.CaseInsensitiveMap

import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.jdbc.JdbcDialects

import org.apache.spark.sql.row.{MutableRelationProvider, JDBCMutableRelation}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.util.StoreUtils
import org.apache.spark.sql.store.{StoreProperties, MembershipAccumulator, StoreInitRDD, StoreRDD}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.BlockManagerId

import org.apache.spark.{AccumulatorParam, Partition}

import scala.collection.mutable

/**
 * A LogicalPlan implementation for an Snappy row table whose contents
 * are retrieved using a JDBC URL or DataSource.
 */
class RowFormatRelation(
                         override val url: String,
                         override val table: String,
                         override val provider: String,
                         preservepartitions: Boolean,
                         mode: SaveMode,
                         userSpecifiedString: String,
                         parts: Array[Partition],
                         _poolProps: Map[String, String],
                         override val connProperties: Properties,
                         override val hikariCP: Boolean,
                         override val origOptions: Map[String, String],
                         blockMap : Map[InternalDistributedMember, BlockManagerId],
                         @transient override val sqlContext: SQLContext)
  extends JDBCMutableRelation(url,
    table,
    provider,
    mode,
    userSpecifiedString,
    parts,
    _poolProps,
    connProperties,
    hikariCP,
    origOptions,
    sqlContext) {

  lazy val connectionType = ExternalStoreUtils.getConnectionType(url)

  val connFunctor = JDBCMutableRelation.getConnector(table, driver, poolProperties,
    connProperties, hikariCP)

  override def buildScan(requiredColumns: Array[String],
      filters: Array[Filter]): RDD[Row] = {
    connectionType match {
      case ConnectionType.Embedded =>
        new RowFormatScanRDD(
          sqlContext.sparkContext,
          connFunctor,
          JDBCMutableRelation.pruneSchema(schemaFields, requiredColumns),
          table,
          requiredColumns,
          filters,
          parts,
          blockMap,
          connProperties
        ).asInstanceOf[RDD[Row]]

      case _ => super.buildScan(requiredColumns, filters)
    }
  }

}



final class DefaultSource
  extends MutableRelationProvider
  {


  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], schema: String) = {
    val parameters = new mutable.HashMap[String, String]
    parameters ++= options

    val connProps = new Properties()
    val sc = sqlContext.sparkContext

    val (url, driver, poolProps, _connProps, hikariCP) =
      ExternalStoreUtils.validateAndGetAllProps(sc, options)


    val dbtableProp = JdbcExtendedUtils.DBTABLE_PROPERTY
    val table = parameters.remove(dbtableProp)
        .getOrElse(sys.error(s"Option '$dbtableProp' not specified"))
    parameters.remove(JdbcExtendedUtils.ALLOW_EXISTING_PROPERTY)
    parameters.remove(JdbcExtendedUtils.SCHEMA_PROPERTY)
    parameters.remove("serialization.format")



    val preservepartitions = parameters.remove("preservepartitions")
    val dialect = JdbcDialects.get(url)
    val coptions = new CaseInsensitiveMap(parameters.toMap)
    val ddlExtension = StoreUtils.ddlExtensionString(coptions)
    val schemaExtension = s"$schema $ddlExtension"

    val blockMap = StoreUtils.initStore(sc, url, connProps)


    dialect match {// The driver if not a loner should be an accesor only
      case d: JdbcExtendedDialect =>
        connProps.putAll(d.extraCreateTableProperties(SnappyContext(sc).isLoner))
    }

    new RowFormatRelation(url,
      SnappyStoreHiveCatalog.processTableIdentifier(table, sqlContext.conf),
      getClass.getCanonicalName,
      preservepartitions.getOrElse("false").toBoolean,
      mode,
      schemaExtension,
      Seq.empty.toArray,
      poolProps,
      connProps,
      hikariCP,
      options,
      blockMap,
      sqlContext)
  }

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String], schema: StructType) = {
    val SNAPPY_STORE_JDBC_URL = sqlContext.sparkContext.getConf.get(StoreProperties.SNAPPY_STORE_JDBC_URL, "")
    val dialect = JdbcDialects.get(SNAPPY_STORE_JDBC_URL)
    val schemaString = JdbcExtendedUtils.schemaString(schema, dialect)

    val allowExisting = options.get(JdbcExtendedUtils
        .ALLOW_EXISTING_PROPERTY).exists(_.toBoolean)
    val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
    createRelation(sqlContext, mode, options, schemaString)
  }

}

