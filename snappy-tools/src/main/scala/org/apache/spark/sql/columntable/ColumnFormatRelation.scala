package org.apache.spark.sql.columntable

import java.nio.ByteBuffer
import java.util.Properties

import scala.collection.mutable

import org.apache.spark.Partition
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
import org.apache.spark.sql.store.util.StoreUtils
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.store.ExternalStore
import org.apache.spark.sql.types.StructType

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

    val url = sqlContext.sparkContext.getConf.get("snappy.store.jdbc.url")
    val driver = "com.pivotal.gemfirexd.jdbc.EmbeddedDriver"



    val dbtableProp = JdbcExtendedUtils.DBTABLE_PROPERTY
    val table = parameters.remove(dbtableProp)
        .getOrElse(sys.error(s"Option '$dbtableProp' not specified"))

    // remove ALLOW_EXISTING property, if remaining
    parameters.remove(JdbcExtendedUtils.ALLOW_EXISTING_PROPERTY)
    parameters.remove(JdbcExtendedUtils.SCHEMA_PROPERTY)
    parameters.remove("serialization.format")

    val (_url, _driver, _poolProps, _connProps, _hikariCP) =
      ExternalStoreUtils.validateAndGetAllProps(options, Option(url), Option(driver))

    val preservepartitions = parameters.remove("preservepartitions")
    val dialect = JdbcDialects.get(url)

    // Remove all added options before ddl extenison
    val coptions = new CaseInsensitiveMap(parameters.toMap)
    val ddlExtension = StoreUtils.ddlExtensionString(coptions)
    val schemaExtension = s"$schema $ddlExtension"

    val externalStore = getExternalTable(_url, _driver, _poolProps, connProps, _hikariCP)

    new ColumnFormatRelation(url,
      SnappyStoreHiveCatalog.processTableIdentifier(table, sqlContext.conf),
      getClass.getCanonicalName, mode, schema, Seq.empty.toArray,
      _poolProps, connProps, _hikariCP, options, externalStore, sqlContext)
  }

  override def getExternalTable(url : String,
      driver : Option[String],
      poolProps: Map[String, String],
      connProps : Properties,
      hikariCP : Boolean): ExternalStore = {
    val externalSource = "org.apache.spark.sql.store.impl.JDBCSourceAsColumnarStore"
    val constructor = Class.forName(externalSource).getConstructors()(0)
    return constructor.newInstance(url, driver, poolProps, connProps, new java.lang.Boolean(hikariCP)).asInstanceOf[ExternalStore]
  }

}
