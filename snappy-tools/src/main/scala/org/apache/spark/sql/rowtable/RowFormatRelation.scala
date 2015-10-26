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

  val PARTITION_BY = "PARTITION_BY"
  val BUCKETS = "BUCKETS"
  val COLOCATE_WITH = "COLOCATE_WITH"
  val REDUNDANCY = "REDUNDANCY"
  val RECOVERYDELAY = "RECOVERYDELAY"
  val MAXPARTSIZE = "MAXPARTSIZE"
  val EVICTION_BY = "EVICTION_BY"
  val PERSISTENT = "PERSISTENT"
  val SERVER_GROUPS = "SERVER_GROUPS"
  val OFFHEAP = "OFFHEAP"

  val GEM_PARTITION_BY = "PARTITION BY"
  val GEM_BUCKETS = "BUCKETS"
  val GEM_COLOCATE_WITH = "COLOCATE WITH"
  val GEM_REDUNDANCY = "REDUNDANCY"
  val GEM_RECOVERYDELAY = "RECOVERYDELAY"
  val GEM_MAXPARTSIZE = "MAXPARTSIZE"
  val GEM_EVICTION_BY = "EVICTION BY"
  val GEM_PERSISTENT = "PERSISTENT"
  val GEM_SERVER_GROUPS = "SERVER GROUPS"
  val GEM_OFFHEAP = "OFFHEAP"
  val PRIMARY_KEY = "PRIMARY KEY"

  val EMPTY_STRING = ""

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
    val ddlExtension = ddlExtensionString(coptions)

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

  def appendClause(sb : mutable.StringBuilder, getClause : () => String) : Unit = {
       val clause = getClause.apply()
       if(!clause.isEmpty){
         sb.append(s"$clause " )
       }
  }

  def ddlExtensionString(parameters: Map[String, String]): String = {
    val sb = new StringBuilder()

    val options = new CaseInsensitiveMap(parameters)

    options.keySet.foreach { prop =>
      prop.toUpperCase match {
        case PARTITION_BY | BUCKETS | COLOCATE_WITH | REDUNDANCY | RECOVERYDELAY | MAXPARTSIZE | EVICTION_BY | PERSISTENT | SERVER_GROUPS | OFFHEAP => // Do nothing. Allowed values
        case _ => throw new IllegalArgumentException(
          s"Illegal property $prop while creating table")
      }
    }

    appendClause(sb, () => {
      val partitionby = options.getOrElse(PARTITION_BY, EMPTY_STRING)
      if (partitionby.isEmpty){
        EMPTY_STRING
      }else{
        val parclause =
          if (partitionby.equals(PRIMARY_KEY)) {
            PRIMARY_KEY
          } else {
            s"COLUMN ($partitionby)"
          }
        s"$GEM_PARTITION_BY $parclause"
      }

    })

    appendClause(sb, () => {
      if (options.get(BUCKETS).isDefined) {
        s"$GEM_BUCKETS ${options.get(BUCKETS).get}"
      } else {
        EMPTY_STRING
      }
    })


    appendClause(sb, () => {
      if (options.get(COLOCATE_WITH).isDefined) {
        s"$GEM_COLOCATE_WITH ${options.get(COLOCATE_WITH).get}"
      } else {
        EMPTY_STRING
      }
    })

    appendClause(sb, () => {
      if (options.get(REDUNDANCY).isDefined) {
        s"$GEM_REDUNDANCY ${options.get(REDUNDANCY).get}"
      } else {
        EMPTY_STRING
      }
    })


    appendClause(sb, () => {
      if (options.get(RECOVERYDELAY).isDefined) {
        s"$GEM_RECOVERYDELAY ${options.get(RECOVERYDELAY).get}"
      } else {
        EMPTY_STRING
      }
    })

    appendClause(sb, () => {
      if (options.get(MAXPARTSIZE).isDefined) {
        s"$GEM_MAXPARTSIZE ${options.get(MAXPARTSIZE).get}"
      } else {
        EMPTY_STRING
      }
    })

    appendClause(sb, () => {
      if (options.get(EVICTION_BY).isDefined) {
        s"$GEM_EVICTION_BY ${options.get(EVICTION_BY).get}"
      } else {
        EMPTY_STRING
      }
    })

    appendClause(sb, () => {
      if (options.get(PERSISTENT).isDefined) {
        s"$GEM_PERSISTENT ${options.get(PERSISTENT).get}"
      } else {
        EMPTY_STRING
      }
    })

    appendClause(sb, () => {
      if (options.get(SERVER_GROUPS).isDefined) {
        s"$GEM_SERVER_GROUPS ${options.get(SERVER_GROUPS).get}"
      } else {
        EMPTY_STRING
      }
    })

    appendClause(sb, () => {
      if (options.get(OFFHEAP).isDefined) {
        s"$GEM_OFFHEAP ${options.get(OFFHEAP).get}"
      } else {
        EMPTY_STRING
      }
    })

    sb.toString()
  }

}

