package org.apache.spark.sql.columntable

import java.nio.ByteBuffer
import java.util.Properties

import scala.collection.mutable

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember

import org.apache.spark.{SparkContext, Partition}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, CatalystTypeConverters}
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.columnar.{ExternalStoreUtils, ColumnarRelationProvider, JDBCAppendableRelation}
import org.apache.spark.sql.execution.datasources.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCRelation, JDBCPartitioningInfo, DriverRegistry}
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.row.MutableRelationProvider
import org.apache.spark.sql.sources.{JdbcExtendedUtils, Filter, BaseRelation}
import org.apache.spark.sql.store.impl.JDBCSourceAsColumnarStore
import org.apache.spark.sql.store.util.StoreUtils
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.store.ExternalStore
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.BlockManagerId

/**
 * Created by rishim on 29/10/15.
 */
class ColumnFormatRelation(
    override val url: String,
    override val table: String,
    override val provider: String,
    override val mode: SaveMode,
    userSchema: StructType,
    parts: Array[Partition],
    _poolProps: Map[String, String],
    override val connProperties: Properties,
    override val hikariCP: Boolean,
    override val origOptions: Map[String, String],
    override val externalStore: ExternalStore,
    @transient override val sqlContext: SQLContext
    ) extends JDBCAppendableRelation(
  url,
  table,
  provider,
  mode,
  userSchema,
  parts,
  _poolProps,
  connProperties,
  hikariCP,
  origOptions,
  externalStore,
  sqlContext
)() {


}

final class DefaultSource
    extends ColumnarRelationProvider {

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, options: Map[String, String], schema: StructType) = {
    val connProps = new Properties()

    val parameters = new mutable.HashMap[String, String]
    parameters ++= options

    val sc = sqlContext.sparkContext
    val dbtableProp = JdbcExtendedUtils.DBTABLE_PROPERTY
    val table = parameters.remove(dbtableProp)
        .getOrElse(sys.error(s"Option '$dbtableProp' not specified"))
    parameters.remove(JdbcExtendedUtils.ALLOW_EXISTING_PROPERTY)
    parameters.remove(JdbcExtendedUtils.SCHEMA_PROPERTY)
    parameters.remove("serialization.format")

    val (url, driver, poolProps, _connProps, hikariCP) =
      ExternalStoreUtils.validateAndGetAllProps(sc, options)

    val preservepartitions = parameters.remove("preservepartitions")
    val dialect = JdbcDialects.get(url)

    // Remove all added options before ddl extenison
    val coptions = new CaseInsensitiveMap(parameters.toMap)
    val ddlExtension = StoreUtils.ddlExtensionString(coptions)
    val schemaExtension = s"$schema $ddlExtension"



    val externalStore = getExternalSource(sc, url, driver, poolProps, connProps, hikariCP)

    new ColumnFormatRelation(url,
      SnappyStoreHiveCatalog.processTableIdentifier(table, sqlContext.conf),
      getClass.getCanonicalName, mode, schema, Seq.empty.toArray,
      poolProps, connProps, hikariCP, options, externalStore, sqlContext)
  }

  override def getExternalSource(sc: SparkContext, url: String,
      driver: String,
      poolProps: Map[String, String],
      connProps: Properties,
      hikariCP: Boolean): ExternalStore = {
    val blockMap = StoreUtils.initStore(sc, url, connProps)
    new JDBCSourceAsColumnarStore(url, driver, poolProps, connProps, hikariCP, blockMap)
  }

}
