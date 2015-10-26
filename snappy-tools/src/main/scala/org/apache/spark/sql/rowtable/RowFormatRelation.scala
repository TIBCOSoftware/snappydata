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
import org.apache.spark.sql.store.{MembershipAccumulator, StoreInitRDD, StoreRDD}
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

  override def insert(data: DataFrame): Unit = {
    connectionType match {
      case ConnectionType.Embedded =>
        val storeRDD = new StoreRDD(
          sqlContext.sparkContext,
          data.rdd,
          table,
          connFunctor,
          schema,
          preservepartitions,
          blockMap
        )
        val storeDF = sqlContext.createDataFrame(storeRDD, schema)
        JdbcUtils.saveTable(storeDF, url, table, connProperties)
      case _ => super.insert(data, false)
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

    val url = parameters.remove("url")
        .getOrElse(sys.error("JDBC URL option 'url' not specified"))



    val dbtableProp = JdbcExtendedUtils.DBTABLE_PROPERTY
    val table = parameters.remove(dbtableProp)
        .getOrElse(sys.error(s"Option '$dbtableProp' not specified"))
    val driver = parameters.remove("driver")


    val poolImpl = parameters.remove("poolimpl")
    val poolProperties = parameters.remove("poolproperties")
    // remove ALLOW_EXISTING property, if remaining
    parameters.remove(JdbcExtendedUtils.ALLOW_EXISTING_PROPERTY)
    parameters.remove(JdbcExtendedUtils.SCHEMA_PROPERTY)

    parameters.remove("serialization.format")

    driver.foreach(DriverRegistry.register)


    val preservepartitions = parameters.remove("preservepartitions")
    val dialect = JdbcDialects.get(url)
    val coptions = new CaseInsensitiveMap(parameters.toMap)
    val ddlExtension = StoreUtils.ddlExtensionString(coptions)

    val schemaExtension = s"$schema $ddlExtension"

    val hikariCP = poolImpl.map(Utils.normalizeId) match {
      case Some("hikari") => true
      case Some("tomcat") => false
      case Some(p) =>
        throw new IllegalArgumentException("RowFormatRelation: " +
            s"unsupported pool implementation '$p' " +
            s"(supported values: tomcat, hikari)")
      case None => false
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



    val sc = sqlContext.sparkContext


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



}

