package org.apache.spark.sql.columntable

import java.util.Properties

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember

import org.apache.spark.sql.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.columnar.{ColumnarRelationProvider, ExternalStoreUtils, JDBCAppendableRelation}
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.row.GemFireXDDialect
import org.apache.spark.sql.store.{ExternalStore, StoreUtils}
import org.apache.spark.sql.store.impl.JDBCSourceAsColumnarStore
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.{Partition, SparkContext}

/**
 * Created by rishim on 29/10/15.
 * This class acts as a DataSource provider for column format tables provided Snappy. It uses GemFireXD as actual datastore to physically locate the tables.
 * Column tables can be used for storing data in columnar compressed format.
 * A example usage is given below.
 *
 * val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createExternalTable(tableName, "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    This provider scans underlying tables in parallel and is aware of the data partition.
    It does not introduces a shuffle if simple table query is fired.
    One can insert a single or multiple rows into this table as well as do a bulk insert by a Spark DataFrame.
    Bulk insert example is shown above.

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
    ) extends JDBCAppendableRelation(url, table, provider, mode, userSchema,
      parts, _poolProps, connProperties, hikariCP, origOptions, externalStore, sqlContext)() {
}

final class DefaultSource extends ColumnarRelationProvider {

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], schema: StructType) = {
    val parameters = new CaseInsensitiveMutableHashMap(options)

    val partitionColumn = parameters.remove("partitioncolumn")
    val lowerBound = parameters.remove("lowerbound")
    val upperBound = parameters.remove("upperbound")
    val numPartitions = parameters.remove("numpartitions")

    val table = ExternalStoreUtils.removeInternalProps(parameters)
    val sc = sqlContext.sparkContext
    //val ddlExtension = StoreUtils.ddlExtensionString(parameters)
    //val preservepartitions = parameters.remove("preservepartitions")
    val (url, driver, poolProps, connProps, hikariCP) =
      ExternalStoreUtils.validateAndGetAllProps(sc, parameters)

    //val dialect = JdbcDialects.get(url)
    //val schemaExtension = s"$schema $ddlExtension"

    val externalStore = getExternalSource(sc, url, driver, poolProps,
      connProps, hikariCP)

    new ColumnFormatRelation(url,
      SnappyStoreHiveCatalog.processTableIdentifier(table, sqlContext.conf),
      getClass.getCanonicalName, mode, schema,getPartitionInfo(parameters),
      poolProps, connProps, hikariCP, options, externalStore, sqlContext)
  }

  override def getExternalSource(sc: SparkContext, url: String,
      driver: String,
      poolProps: Map[String, String],
      connProps: Properties,
      hikariCP: Boolean): ExternalStore = {

    val dialect = JdbcDialects.get(url)
    val blockMap =
      dialect match {
        case GemFireXDDialect => StoreUtils.initStore(sc, url, connProps)
        case _ => Map.empty[InternalDistributedMember, BlockManagerId]
      }
    new JDBCSourceAsColumnarStore(url, driver, poolProps, connProps, hikariCP, blockMap)
  }
}
