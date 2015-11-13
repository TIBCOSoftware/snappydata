package org.apache.spark.sql.columntable

import java.util.Properties

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, CatalystTypeConverters}
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow
import org.apache.spark.sql.collection.UUIDRegionKey
import org.apache.spark.sql.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.columnar.{ConnectionType, ColumnAccessor, ColumnType, CachedBatch, ColumnarRelationProvider, ExternalStoreUtils, JDBCAppendableRelation}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.row.{JDBCMutableRelation, GemFireXDDialect}
import org.apache.spark.sql.rowtable.RowFormatScanRDD
import org.apache.spark.sql.sources.{Filter, JdbcExtendedUtils}
import org.apache.spark.sql.store.ExternalStore
import org.apache.spark.sql.store.impl.JDBCSourceAsColumnarStore
import org.apache.spark.sql.store.util.StoreUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.storage.{StorageLevel, BlockManagerId}
import org.apache.spark.{Logging, Partition, SparkContext}
import java.nio.ByteBuffer

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
    schemaExtensions: String,
    parts: Array[Partition],
    _poolProps: Map[String, String],
    override val connProperties: Properties,
    override val hikariCP: Boolean,
    override val origOptions: Map[String, String],
    override val externalStore: ExternalStore,
    blockMap: Map[InternalDistributedMember, BlockManagerId],
    @transient override val sqlContext: SQLContext
    )
    (private var uuidList: ArrayBuffer[RDD[UUIDRegionKey]] = new ArrayBuffer[RDD[UUIDRegionKey]]()
        )
    extends JDBCAppendableRelation(url, table, provider, mode, userSchema, schemaExtensions, parts, _poolProps, connProperties, hikariCP, origOptions, externalStore, sqlContext)() {

  // TODO: Suranjan currently doesn't apply any filters.
  // will see that later.
  override def buildScan(requiredColumns: Array[String],
      filters: Array[Filter]): RDD[Row] = {

    def cachedColumnBuffers: RDD[CachedBatch] = readLock {
      externalStore.getCachedBatchRDD(table+shadowTableNamePrefix, requiredColumns.map(column => columnPrefix + column), uuidList,
        sqlContext.sparkContext)
    }

    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    val colRDD = cachedColumnBuffers.mapPartitions { cachedBatchIterator =>
      // Find the ordinals and data types of the requested columns.  If none are requested, use the
      // narrowest (the field with minimum default element size).
      val (requestedColumnIndices, requestedColumnDataTypes) = if (requiredColumns.isEmpty) {
        val (narrowestOrdinal, narrowestDataType) =
          schema.fields.zipWithIndex.map { case (a, ordinal) =>
            ordinal -> a.dataType
          } minBy { case (_, dataType) =>
            ColumnType(dataType).defaultSize
          }
        Seq(narrowestOrdinal) -> Seq(narrowestDataType)
      } else {
        requiredColumns.map { a =>
          schema.getFieldIndex(a).get -> schema(a).dataType
        }.unzip
      }
      val nextRow = new SpecificMutableRow(requestedColumnDataTypes)
      def cachedBatchesToRows(cacheBatches: Iterator[CachedBatch]): Iterator[Row] = {
        val rows = cacheBatches.flatMap { cachedBatch =>
          // Build column accessors
          val columnAccessors = requestedColumnIndices.map { batchColumnIndex =>
            ColumnAccessor(
              schema.fields(batchColumnIndex).dataType,
              ByteBuffer.wrap(cachedBatch.buffers(batchColumnIndex)))
          }
          // Extract rows via column accessors
          new Iterator[InternalRow] {
            private[this] val rowLen = nextRow.numFields

            override def next(): InternalRow = {
              var i = 0
              while (i < rowLen) {
                columnAccessors(i).extractTo(nextRow, i)
                i += 1
              }
              if (requiredColumns.isEmpty) InternalRow.empty else nextRow
            }

            override def hasNext: Boolean = columnAccessors(0).hasNext
          }
        }
        rows.map(converter(_).asInstanceOf[Row])
      }
      cachedBatchesToRows(cachedBatchIterator)
    }

    colRDD.union(connectionType match {
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
        ).map(converter(_).asInstanceOf[Row])

      case _ => super.buildScan(requiredColumns, filters)
    })
  }

  lazy val connectionType = ExternalStoreUtils.getConnectionType(url)

  val connFunctor = JDBCAppendableRelation.getConnector(table, driver, _poolProps,
    connProperties, hikariCP)
}


object ColumnFormatRelation extends Logging with StoreCallback {
  // register the call backs with the JDBCSource so that
  // bucket region can insert into the column table

  def registerStoreCallbacks(table: String, schema: StructType, externalStore: ExternalStore) = {
    // if already registered don't register
    StoreCallbacksImpl.registerExternalStoreAndSchema(table.toUpperCase, schema, externalStore)
  }
}

final class DefaultSource
    extends ColumnarRelationProvider {

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, options: Map[String, String], schema: StructType) = {
    val parameters = new CaseInsensitiveMutableHashMap(options)

    val table = StoreUtils.removeInternalProps(parameters)

    val sc = sqlContext.sparkContext
    val ddlExtension = StoreUtils.ddlExtensionString(parameters)
    val preservepartitions = parameters.remove("preservepartitions")
    val (url, driver, poolProps, connProps, hikariCP) =
      ExternalStoreUtils.validateAndGetAllProps(sc, parameters.toMap)

    val dialect = JdbcDialects.get(url)
    val blockMap =
      dialect match {
        case GemFireXDDialect => StoreUtils.initStore(sc, url, connProps)
        case _ => Map.empty[InternalDistributedMember, BlockManagerId]
      }
    val schemaString = JdbcExtendedUtils.schemaString(schema, dialect)
    val schemaExtension = s"$schemaString $ddlExtension"

    val externalStore = getExternalSource(sc, url, driver, poolProps, connProps, hikariCP)
    ColumnFormatRelation.registerStoreCallbacks(table, schema, externalStore)

    new ColumnFormatRelation(url,
      SnappyStoreHiveCatalog.processTableIdentifier(table, sqlContext.conf),
      getClass.getCanonicalName, mode, schema, schemaExtension, Seq.empty.toArray,
      poolProps, connProps, hikariCP, options, externalStore, blockMap, sqlContext)()
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
