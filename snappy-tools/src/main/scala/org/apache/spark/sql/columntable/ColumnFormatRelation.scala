package org.apache.spark.sql.columntable

import java.util.Properties

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.PartitionedRegion
import com.pivotal.gemfirexd.internal.engine.Misc
import org.apache.spark.Partition
import org.apache.spark.sql.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.columnar.{ColumnarRelationProvider, ExternalStoreUtils, JDBCAppendableRelation}
import org.apache.spark.sql.execution.PartitionedDataSourceScan
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.row.GemFireXDDialect
import org.apache.spark.sql.sources.JdbcExtendedDialect
import org.apache.spark.sql.store.impl.JDBCSourceAsColumnarStore
import org.apache.spark.sql.store.{ExternalStore, StoreRDD, StoreUtils}
import org.apache.spark.sql.store.StoreFunctions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.storage.BlockManagerId

/**
 * Created by rishim on 29/10/15.
 * This class acts as a DataSource provider for column format tables provided Snappy.
 * It uses GemFireXD as actual datastore to physically locate the tables.
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
    ddlExtensionForShadowTable: String,
    poolProps: Map[String, String],
    override val connProperties: Properties,
    override val hikariCP: Boolean,
    override val origOptions: Map[String, String],
    override val externalStore: ExternalStore,
    blockMap: Map[InternalDistributedMember, BlockManagerId],
    partitioningColumns: Seq[String],
    @transient override val sqlContext: SQLContext
    ) extends JDBCAppendableRelation(url, table, provider, mode, userSchema,
        Array[Partition](), poolProps, connProperties, hikariCP, origOptions, externalStore, sqlContext)()
     with PartitionedDataSourceScan {

  override def toString: String = s"ColumnFormatRelation[$table]"

  override def insert(df: DataFrame, overwrite: Boolean = true): Unit = {
    if (!partitioningColumns.isEmpty) {
      val rdd = new StoreRDD(sqlContext.sparkContext,
        df.rdd,
        table,
        connector,
        schema,
        false,
        blockMap,
        partitioningColumns
      )
      super.insert(rdd, df, overwrite)
    } else {
      super.insert(df, overwrite)
    }
  }

  override def numPartitions: Int = {
    executeWithConnection(connector, {
      case conn => val tableSchema = conn.getSchema
        val resolvedName = StoreUtils.lookupName(table, tableSchema)
        val region = Misc.getRegionForTable(resolvedName, true).asInstanceOf[PartitionedRegion]
        region.getTotalNumberOfBuckets
    })
  }

  override def partitionColumns: Seq[String] = {
    partitioningColumns
  }

  override def createExternalTableForCachedBatches(tableName: String,
      externalStore: ExternalStore): Unit = {
    require(tableName != null && tableName.length > 0,
      "createExternalTableForCachedBatches: expected non-empty table name")

    val (primarykey, partitionStrategy) = dialect match {
      // The driver if not a loner should be an accessor only
      case d: JdbcExtendedDialect =>
        (s"constraint ${tableName}_bucketCheck check (bucketId != -1), " +
            "primary key (uuid, bucketId) ", d.getPartitionByClause("bucketId"))
      case _ => ("primary key (uuid)", "") //TODO. How to get primary key contraint from each DB
    }
    val colocationClause = s"" // To be Filled in by Suranjan

    createTable(externalStore, s"create table $tableName (uuid varchar(36) " +
        "not null, bucketId integer not null, numRows integer not null, " +
        "stats blob, " + userSchema.fields.map(structField => columnPrefix +
        structField.name + " blob").mkString(" ", ",", " ") +
        s", $primarykey) $partitionStrategy $colocationClause $ddlExtensionForShadowTable",
      tableName, dropIfExists = false)
  }
}

final class DefaultSource extends ColumnarRelationProvider {

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], schema: StructType) = {
    val parameters = new CaseInsensitiveMutableHashMap(options)

    val table = ExternalStoreUtils.removeInternalProps(parameters)
    val sc = sqlContext.sparkContext
    val partitioningColumn = StoreUtils.getPartitioningColumn(parameters)
    val ddlExtension = StoreUtils.ddlExtensionStringForColumnTable(parameters)
    //val preservepartitions = parameters.remove("preservepartitions")
    val (url, driver, poolProps, connProps, hikariCP) =
      ExternalStoreUtils.validateAndGetAllProps(sc, parameters)

    //val schemaExtension = s"$schema $ddlExtension"

    val dialect = JdbcDialects.get(url)
    val blockMap =
      dialect match {
        case GemFireXDDialect => StoreUtils.initStore(sc, url, connProps)
        case _ => Map.empty[InternalDistributedMember, BlockManagerId]
      }

    val externalStore = new JDBCSourceAsColumnarStore(url, driver, poolProps,
      connProps, hikariCP, blockMap)

    new ColumnFormatRelation(url,
      SnappyStoreHiveCatalog.processTableIdentifier(table, sqlContext.conf),
      getClass.getCanonicalName,
      mode,
      schema,
      ddlExtension,
      poolProps,
      connProps,
      hikariCP,
      options,
      externalStore,
      blockMap,
      partitioningColumn,
      sqlContext)
  }

}
