package org.apache.spark.sql.columntable

import java.util.UUID

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.pivotal.gemfirexd.internal.engine.access.heap.MemHeapScanController
import com.pivotal.gemfirexd.internal.engine.store.AbstractCompactExecRow
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow
import com.pivotal.gemfirexd.internal.iapi.store.access.ScanController

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.collection.UUIDRegionKey
import org.apache.spark.sql.columnar.{CachedBatch, CachedBatchHolder, ColumnBuilder, ColumnType}
import org.apache.spark.sql.store.ExternalStore
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Created by skumar on 5/11/15.
 */

class CachedBatchCreator(
    val tableName: String,
    val schema: StructType,
    val externalStore: ExternalStore,
    val columnBatchSize: Int,
    val useCompression: Boolean) {

  type JDBCConversion = (SpecificMutableRow, ExecRow, Int) => Unit

  def createInternalRow(execRow: ExecRow, mutableRow: SpecificMutableRow): InternalRow = {
    var i = 0
    while (i < schema.length) {
      val pos = i + 1
      schema.fields(i).dataType match {
        case BooleanType => mutableRow.setBoolean(i, execRow.getColumn(pos).getBoolean())
        case DateType =>
          // DateTimeUtils.fromJavaDate does not handle null value, so we need to check it.
          val dateVal = execRow.getColumn(pos).getDate(null)
          if (dateVal != null) {
            mutableRow.setInt(i, DateTimeUtils.fromJavaDate(dateVal))
          } else {
            mutableRow.update(i, null)
          }
        // When connecting with Oracle DB through JDBC, the precision and scale of BigDecimal
        // object returned by ResultSet.getBigDecimal is not correctly matched to the table
        // schema reported by ResultSetMetaData.getPrecision and ResultSetMetaData.getScale.
        // If inserting values like 19999 into a column with NUMBER(12, 2) type, you get through
        // a BigDecimal object with scale as 0. But the dataframe schema has correct type as
        // DecimalType(12, 2). Thus, after saving the dataframe into parquet file and then
        // retrieve it, you will get wrong result 199.99.
        // So it is needed to set precision and scale for Decimal based on JDBC metadata.
        case DecimalType.Fixed(p, s) =>
          val decimalVal = execRow.getColumn(pos).typeToBigDecimal()
          if (decimalVal == null) {
            mutableRow.update(i, null)
          } else {
            mutableRow.update(i, Decimal(decimalVal, p, s))
          }
        case DoubleType => mutableRow.setDouble(i, execRow.getColumn(pos).getDouble())
        case FloatType => mutableRow.setFloat(i, execRow.getColumn(pos).getFloat())
        case IntegerType => mutableRow.setInt(i, execRow.getColumn(pos).getInt())
        case LongType =>
          if (schema.fields(i).metadata.contains("binarylong")) {
            val bytes = execRow.getColumn(pos).getBytes()
            var ans = 0L
            var j = 0
            while (j < bytes.size) {
              ans = 256 * ans + (255 & bytes(j))
              j = j + 1;
            }
            mutableRow.setLong(i, ans)
          } else {
            mutableRow.setLong(i, execRow.getColumn(pos).getLong())
          }
        // TODO(davies): use getBytes for better performance, if the encoding is UTF-8
        case StringType => mutableRow.update(i,
          UTF8String.fromString(execRow.getColumn(pos).getString()))
        case TimestampType =>
          val t = execRow.getColumn(pos).getTimestamp(null)
          if (t != null) {
            mutableRow.setLong(i, DateTimeUtils.fromJavaTimestamp(t))
          } else {
            mutableRow.update(i, null)
          }
        case BinaryType => mutableRow.update(i, execRow.getColumn(pos).getBytes())
        case _ => throw new IllegalArgumentException(s"Unsupported field ${schema.fields(i)}")
      }
      if (execRow.getColumn(pos).isNull) {
        mutableRow.setNullAt(i)
      }
      i = i + 1
    }
    mutableRow
  }

  def createAndStoreBatch(sc: ScanController, row: AbstractCompactExecRow,
      batchID: UUID, bucketID: Int): mutable.HashSet[Any] = {

    def uuidBatchAggregate(accumulated: ArrayBuffer[UUIDRegionKey],
        batch: CachedBatch): ArrayBuffer[UUIDRegionKey] = {
      val uuid = externalStore.storeCachedBatch(tableName , batch, bucketID, Option(batchID))
      accumulated += uuid
    }

    def columnBuilders = schema.map {
      attribute =>
        val columnType = ColumnType(attribute.dataType)
        val initialBufferSize = columnType.defaultSize * columnBatchSize
        ColumnBuilder(attribute.dataType, initialBufferSize,
          attribute.name, useCompression)
    }.toArray

    // adding one variable so that only one cached batch is created
    val holder = new CachedBatchHolder(columnBuilders, 0, Integer.MAX_VALUE, schema,
      new ArrayBuffer[UUIDRegionKey](1), uuidBatchAggregate)

    val batches = holder.asInstanceOf[CachedBatchHolder[ArrayBuffer[Serializable]]]
    val memHeapScanController = sc.asInstanceOf[MemHeapScanController]
    memHeapScanController.setAddRegionAndKey
    val keySet = new mutable.HashSet[Any]
    val mutableRow = new SpecificMutableRow(schema.fields.map(x => x.dataType))
    try {
      while (memHeapScanController.fetchNext(row)) {
        batches.appendRow((), createInternalRow(row, mutableRow))
        keySet.add(row.getAllRegionAndKeyInfo.first().getKey)
      }
      batches.forceEndOfBatch
      keySet
    } finally {
      sc.close();
    }
  }
}
