package org.apache.spark.sql.rowtable

import java.util.Properties

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.PartitionedRegion
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionByExpressionResolver

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.columnar.{ConnectionType, ExternalStoreUtils}

import org.apache.spark.sql.execution.PartitionedDataSourceScan

import org.apache.spark.sql.execution.datasources.CaseInsensitiveMap

import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.row.{GemFireXDDialect, JDBCMutableRelation}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.StoreFunctions._
import org.apache.spark.sql.store.util.StoreUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.BlockManagerId

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
    blockMap: Map[InternalDistributedMember, BlockManagerId],
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
      sqlContext) with PartitionedDataSourceScan {

  lazy val connectionType = ExternalStoreUtils.getConnectionType(url)

  override def buildScan(requiredColumns: Array[String],
      filters: Array[Filter]): RDD[Row] = {
    connectionType match {
      case ConnectionType.Embedded =>
        new RowFormatScanRDD(
          sqlContext.sparkContext,
          connector,
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


  /**
   * We need to set num partitions just to cheat Exchange of Spark.
   * This partition is not used for actual scan operator which depends on the
   * actual RDD.
   * Spark ClusteredDistribution is pretty simplistic to consider numShufflePartitions for
   * its partitioning scheme as Spark always uses shuffle.
   * Ideally it should consider child Spark plans partitioner.
   *
   */
  override def numPartitions: Int = {
    executeWithConnection(connector, {
      case conn =>
        val tableSchema = conn.getSchema
        val resolvedName = StoreUtils.lookupName(table, tableSchema)
        val region = Misc.getRegionForTable(resolvedName, true)
        if (region.isInstanceOf[PartitionedRegion]) {
/*          val par = region.asInstanceOf[PartitionedRegion]
          par.getTotalNumberOfBuckets*/
          sqlContext.conf.numShufflePartitions
        } else {
          1
        }
    })
  }
  override def partitionColumns: Seq[String] = {
    executeWithConnection(connector, {
      case conn => val tableSchema = conn.getSchema
        val resolvedName = StoreUtils.lookupName(table, tableSchema)
        val region = Misc.getRegionForTable(resolvedName, true)
        val partitionColumn = if (region.isInstanceOf[PartitionedRegion]) {
          val region = Misc.getRegionForTable(resolvedName, true).asInstanceOf[PartitionedRegion]
          val resolver = region.getPartitionResolver.asInstanceOf[GfxdPartitionByExpressionResolver]
          val parColumn = resolver.getColumnNames
          parColumn.toSeq
        } else {
          Seq.empty[String]
        }
        partitionColumn
    })
  }
}

final class DefaultSource extends MutableRelationProvider {

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], schema: String) = {
    val parameters = new CaseInsensitiveMutableHashMap(options)
    val table = ExternalStoreUtils.removeInternalProps(parameters)

    val ddlExtension = StoreUtils.ddlExtensionString(parameters)
    val schemaExtension = s"$schema $ddlExtension"
    val preservePartitions = parameters.remove("preservepartitions")
    val sc = sqlContext.sparkContext

    val (url, _, poolProps, connProps, hikariCP) =
      ExternalStoreUtils.validateAndGetAllProps(sc, parameters)

    val dialect = JdbcDialects.get(url)
    val blockMap =
      dialect match {
        case GemFireXDDialect => StoreUtils.initStore(sc, url, connProps)
        case _ => Map.empty[InternalDistributedMember, BlockManagerId]
      }

    dialect match {
      // The driver if not a loner should be an accesor only
      case d: JdbcExtendedDialect =>
        connProps.putAll(d.extraDriverProperties(Utils.isLoner(sc)))
      case _ =>
    }

    new RowFormatRelation(url,
      SnappyStoreHiveCatalog.processTableIdentifier(table, sqlContext.conf),
      getClass.getCanonicalName,
      preservePartitions.exists(_.toBoolean),
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
