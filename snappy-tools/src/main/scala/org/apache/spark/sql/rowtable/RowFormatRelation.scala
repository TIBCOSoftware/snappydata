package org.apache.spark.sql.rowtable


import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.cluster.SnappyCoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.local.LocalBackend
import org.apache.spark.sql._
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.columnar.{ConnectionType, ExternalStoreUtils}

import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog

import org.apache.spark.sql.row.{MutableRelationProvider, JDBCMutableRelation}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.{StoreInitRDD, StoreRDD}

import org.apache.spark.{ Partition}

import scala.collection.mutable

/**
 * A LogicalPlan implementation for an Snappy row table whose contents
 * are retrieved using a JDBC URL or DataSource.
 */
class RowFormatRelation(
                         override val url: String,
                         override val table: String,
                         override val provider: String,
                         partitionColumn: Option[String],
                         buckets: Int,
                         preservepartitions: Boolean,
                         mode: SaveMode,
                         userSpecifiedString: String,
                         parts: Array[Partition],
                         _poolProps: Map[String, String],
                         override val connProperties: Properties,
                         override val hikariCP: Boolean,
                         override val origOptions: Map[String, String],
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

  override def buildScan(requiredColumns: Array[String],
                         filters: Array[Filter]): RDD[Row] = {
    connectionType match {
      case ConnectionType.Embedded =>
        new RowFormatScanRDD(
          sqlContext.sparkContext,
          JDBCMutableRelation.getConnector(table, driver, poolProperties,
            connProperties, hikariCP),
          JDBCMutableRelation.pruneSchema(schemaFields, requiredColumns),
          table,
          requiredColumns,
          filters,
          parts,
          connProperties).asInstanceOf[RDD[Row]]

      case ConnectionType.Net => // TODO Non-Embedded RDD will come here
        new JDBCRDD(
          sqlContext.sparkContext,
          JDBCMutableRelation.getConnector(table, driver, poolProperties,
            connProperties, hikariCP),
          JDBCMutableRelation.pruneSchema(schemaFields, requiredColumns),
          table,
          requiredColumns,
          filters,
          parts,
          connProperties).asInstanceOf[RDD[Row]]
    }
  }

  override def insert(data: DataFrame): Unit = {
    val storeRDD = new StoreRDD(
      sqlContext.sparkContext,
      data.rdd,
      table,
      JDBCMutableRelation.getConnector(table, driver, poolProperties,
        connProperties, hikariCP),
      schema,
      partitionColumn,
      preservepartitions,
      buckets
    )
    val storeDF = sqlContext.createDataFrame(storeRDD, schema)
    JdbcUtils.saveTable(storeDF, url, table, connProperties)
  }
}



final class DefaultSource
  extends MutableRelationProvider
  {

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
                              options: Map[String, String], schema: String) = {
    val parameters = new mutable.HashMap[String, String]
    parameters ++= options

    val url = parameters.remove("url")
      .getOrElse(sys.error("JDBC URL option 'url' not specified"))
    val dbtableProp = JdbcExtendedUtils.DBTABLE_PROPERTY
    val table = parameters.remove(dbtableProp)
      .getOrElse(sys.error(s"Option '$dbtableProp' not specified"))
    val driver = parameters.remove("driver")
    val poolImpl = parameters.remove("poolimpl")
    val poolProperties = parameters.remove("poolproperties")
    val partitionColumn = parameters.remove("partitioncolumn")
    val preservepartitions = parameters.remove("preservepartitions")
    var buckets = parameters.remove("buckets")
    // remove ALLOW_EXISTING property, if remaining
    parameters.remove(JdbcExtendedUtils.ALLOW_EXISTING_PROPERTY)
    parameters.remove(JdbcExtendedUtils.SCHEMA_PROPERTY)

    parameters.remove("serialization.format")

    driver.foreach(DriverRegistry.register)

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

    if(partitionColumn.isDefined) {
      if(!buckets.isDefined){
        buckets = Option("113")
      }
    }else{
      buckets = Option("0")
    }

    val connProps = new Properties()
    parameters.foreach(kv => connProps.setProperty(kv._1, kv._2))

    val sc = sqlContext.sparkContext
    val runStoreInit = sc.schedulerBackend match {
      case snb: SnappyCoarseGrainedSchedulerBackend => false
      case lb : LocalBackend => false
      case _ => true
    }
    if(runStoreInit){
      new StoreInitRDD(sc, url, connProps).collect()
    }


    new RowFormatRelation(url,
      SnappyStoreHiveCatalog.processTableIdentifier(table, sqlContext.conf),
      getClass.getCanonicalName,
      partitionColumn,
      buckets.get.toInt,
      preservepartitions.getOrElse("false").toBoolean,
      mode,
      schema,
      Seq.empty.toArray,
      poolProps,
      connProps,
      hikariCP,
      options,
      sqlContext)
  }
}
