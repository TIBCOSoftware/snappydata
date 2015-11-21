package org.apache.spark.sql.rowtable

import java.util.Properties

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.columnar.{ConnectionType, ExternalStoreUtils}
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.row.{GemFireXDDialect, JDBCMutableRelation}
import org.apache.spark.sql.sources._
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
      sqlContext) {

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
}

final class DefaultSource extends MutableRelationProvider {

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], schema: String) = {
    val parameters = new CaseInsensitiveMutableHashMap(options)
    val table = StoreUtils.removeInternalProps(parameters)

    val ddlExtension = StoreUtils.ddlExtensionString(parameters)
    val schemaExtension = s"$schema $ddlExtension"
    val preservePartitions = parameters.remove("preservepartitions")
    val sc = sqlContext.sparkContext

    val (url, _, poolProps, connProps, hikariCP) =
      ExternalStoreUtils.validateAndGetAllProps(sc, parameters.toMap)

    val dialect = JdbcDialects.get(url)
    val blockMap =
      dialect match {
        case GemFireXDDialect => StoreUtils.initStore(sc, url, connProps)
        case _ => Map.empty[InternalDistributedMember, BlockManagerId]
      }

    dialect match {
      // The driver if not a loner should be an accesor only
      case d: JdbcExtendedDialect =>
        connProps.putAll(d.extraCreateTableProperties(Utils.isLoner(sc)))
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

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String], schema: StructType) = {
    val (url, _, _, _, _) =
      ExternalStoreUtils.validateAndGetAllProps(sqlContext.sparkContext, options)
    val dialect = JdbcDialects.get(url)
    val schemaString = JdbcExtendedUtils.schemaString(schema, dialect)

    val allowExisting = options.get(JdbcExtendedUtils
        .ALLOW_EXISTING_PROPERTY).exists(_.toBoolean)
    val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
    createRelation(sqlContext, mode, options, schemaString)
  }
}
