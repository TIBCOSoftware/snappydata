/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.sql.execution.row

import java.sql.{Connection, ResultSet, Statement}
import java.util.GregorianCalendar

import com.gemstone.gemfire.internal.cache.{CacheDistributionAdvisee, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.store.AbstractCompactExecRow
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedResultSet

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecificMutableRow, UnsafeArrayData,
  UnsafeMapData, UnsafeRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.collection.MultiBucketExecutorPartition
import org.apache.spark.sql.execution.columnar.{ExternalStoreUtils, ResultSetIterator}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD
import org.apache.spark.sql.execution.{CompactExecRowToMutableRow, ConnectionPool}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.mutable.ArrayBuffer

/**
 * A scanner RDD which is very specific to Snappy store row tables.
 * This scans row tables in parallel unlike Spark's inbuilt JDBCRDD.
 * Most of the code is copy of JDBCRDD. We had to copy a lot of stuffs
 * as JDBCRDD has a lot of methods as private.
 */
class RowFormatScanRDD(_sc: SparkContext,
    getConnection: () => Connection,
    schema: StructType,
    tableName: String,
    isPartitioned: Boolean,
    columns: Array[String],
    connProperties: ConnectionProperties,
    filters: Array[Filter] = Array.empty[Filter],
    partitions: Array[Partition] = Array.empty[Partition])
    extends JDBCRDD(_sc, getConnection, schema, tableName, columns,
      filters, partitions, connProperties.url,
      connProperties.executorConnProps) {

  protected var filterWhereArgs: ArrayBuffer[Any] = _
  /**
   * `filters`, but as a WHERE clause suitable for injection into a SQL query.
   */
  protected val filterWhereClause: String = {
    val numFilters = filters.length
    if (numFilters > 0) {
      val sb = new StringBuilder().append(" WHERE ")
      val args = new ArrayBuffer[Any](numFilters)
      val initLen = sb.length
      filters.foreach { s =>
        compileFilter(s, sb, args, sb.length > initLen)
      }
      if (args.nonEmpty) {
        filterWhereArgs = args
        sb.toString()
      } else ""
    } else ""
  }

  // below should exactly match ExternalStoreUtils.handledFilter
  private def compileFilter(f: Filter, sb: StringBuilder,
      args: ArrayBuffer[Any], addAnd: Boolean): Unit = f match {
    case EqualTo(col, value) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append(col).append(" = ?")
      args += value
    case LessThan(col, value) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append(col).append(" < ?")
      args += value
    case GreaterThan(col, value) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append(col).append(" > ?")
      args += value
    case LessThanOrEqual(col, value) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append(col).append(" <= ?")
      args += value
    case GreaterThanOrEqual(col, value) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append(col).append(" >= ?")
      args += value
    case StringStartsWith(col, value) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append(col).append(" LIKE ?")
      args += (value + '%')
    case In(col, values) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append(col).append(" IN (")
      (1 until values.length).foreach(v => sb.append("?,"))
      sb.append("?)")
      args ++= values
    case And(left, right) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append('(')
      compileFilter(left, sb, args, addAnd = false)
      sb.append(") AND (")
      compileFilter(right, sb, args, addAnd = false)
      sb.append(')')
    case Or(left, right) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append('(')
      compileFilter(left, sb, args, addAnd = false)
      sb.append(") OR (")
      compileFilter(right, sb, args, addAnd = false)
      sb.append(')')
    case _ => // no filter pushdown
  }

  /**
   * `columns`, but as a String suitable for injection into a SQL query.
   */
  protected val columnList: String = {
    if (columns.length > 0) {
      val sb = new StringBuilder()
      columns.foreach { s =>
        if (sb.nonEmpty) sb.append(',')
        sb.append(s)
      }
      sb.toString()
    } else "1"
  }

  def computeResultSet(
      thePart: Partition): (Connection, Statement, ResultSet) = {
    val conn = getConnection()

    if (isPartitioned) {
      val ps = conn.prepareStatement(
        "call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION(?, ?)")
      try {
        ps.setString(1, tableName)
        val partition = thePart.asInstanceOf[MultiBucketExecutorPartition]
        var bucketString = ""
        partition.buckets.foreach(bucket => {
          bucketString = bucketString + bucket + ","
        })
        ps.setString(2, bucketString.substring(0, bucketString.length - 1))
        ps.executeUpdate()
      } finally {
        ps.close()
      }
    }

    val sqlText = s"SELECT $columnList FROM $tableName$filterWhereClause"
    val args = filterWhereArgs
    val stmt = conn.prepareStatement(sqlText)
    if (args ne null) {
      ExternalStoreUtils.setStatementParameters(stmt, args)
    }
    val fetchSize = connProperties.executorConnProps.getProperty("fetchSize")
    if (fetchSize ne null) {
      stmt.setFetchSize(fetchSize.toInt)
    }

    val rs = stmt.executeQuery()
    /* (hangs for some reason)
    // setup context stack for lightWeightNext calls
    val rs = stmt.executeQuery().asInstanceOf[EmbedResultSet]
    val embedConn = stmt.getConnection.asInstanceOf[EmbedConnection]
    val lcc = embedConn.getLanguageConnectionContext
    embedConn.getTR.setupContextStack()
    rs.pushStatementContext(lcc, true)
    */
    (conn, stmt, rs)
  }

  /**
   * Runs the SQL query against the JDBC driver.
   */
  override def compute(thePart: Partition,
      context: TaskContext): Iterator[InternalRow] = {
    val (conn, stmt, rs) = computeResultSet(thePart)
    val itr = new InternalRowIteratorOnRS(conn, stmt, rs, context, schema)
    // move once to next at the start (or close if no result available)
    itr.moveNext()
    // switch to optimized iterator for CompactExecRows
    rs match {
      case r: EmbedResultSet =>
        if (itr.hasNext && r.currentRow.isInstanceOf[AbstractCompactExecRow]) {
          // use the optimized iterator
          new CompactExecRowIteratorOnRS(conn, stmt, r, context, schema)
        } else {
          itr
        }
      case _ => itr
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[MultiBucketExecutorPartition].hostExecutorIds
  }

  override def getPartitions: Array[Partition] = {
    val conn = ConnectionPool.getPoolConnection(tableName,
      connProperties.dialect, connProperties.poolProps,
      connProperties.connProps, connProperties.hikariCP)
    try {
      val tableSchema = conn.getSchema
      val resolvedName = ExternalStoreUtils.lookupName(tableName, tableSchema)
      Misc.getRegionForTable(resolvedName, true)
          .asInstanceOf[CacheDistributionAdvisee] match {
        case pr: PartitionedRegion =>
          StoreUtils.getPartitionsPartitionedTable(sparkContext, pr)
        case dr =>
          StoreUtils.getPartitionsReplicatedTable(sparkContext, dr)
      }
    } finally {
      conn.close()
    }
  }
}

final class InternalRowIteratorOnRS(conn: Connection,
    stmt: Statement, rs: ResultSet, context: TaskContext, schema: StructType)
    extends ResultSetIterator[InternalRow](conn, stmt, rs, context) {

  private[this] lazy val defaultCal = new GregorianCalendar()

  private[this] val dataTypes: ArrayBuffer[DataType] =
    new ArrayBuffer[DataType](schema.length)

  private[this] val fieldTypes = StoreUtils.mapCatalystTypes(schema, dataTypes)

  private lazy val unsafeproj = UnsafeProjection.create(schema.map(_.dataType).toArray)

  private[this] val mutableRow = new SpecificMutableRow(dataTypes)

  override def next(): InternalRow = {
    var i = 0
    while (i < schema.length) {
      val pos = i + 1
      fieldTypes(i) match {
        case StoreUtils.STRING_TYPE =>
          val v = rs.getString(pos)
          if (v != null) {
            mutableRow.update(i, UTF8String.fromString(v))
          } else {
            mutableRow.setNullAt(i)
          }
        case StoreUtils.INT_TYPE =>
          val v = rs.getInt(pos)
          if (v != 0 || !rs.wasNull()) {
            mutableRow.setInt(i, v)
          } else {
            mutableRow.setNullAt(i)
          }
        case StoreUtils.LONG_TYPE =>
          val v = rs.getLong(pos)
          if (v != 0L || !rs.wasNull()) {
            mutableRow.setLong(i, v)
          } else {
            mutableRow.setNullAt(i)
          }
        case StoreUtils.BINARY_LONG_TYPE =>
          val bytes = rs.getBytes(pos)
          if (bytes != null) {
            var v = 0L
            var j = 0
            val numBytes = bytes.size
            while (j < numBytes) {
              v = 256 * v + (255 & bytes(j))
              j = j + 1
            }
            mutableRow.setLong(i, v)
          } else {
            mutableRow.setNullAt(i)
          }
        case StoreUtils.SHORT_TYPE =>
          val v = rs.getShort(pos)
          if (v != 0 || !rs.wasNull()) {
            mutableRow.setShort(i, v)
          } else {
            mutableRow.setNullAt(i)
          }
        case StoreUtils.BYTE_TYPE =>
          val v = rs.getByte(pos)
          if (v != 0 || !rs.wasNull()) {
            mutableRow.setByte(i, v)
          } else {
            mutableRow.setNullAt(i)
          }
        case StoreUtils.BOOLEAN_TYPE =>
          val v = rs.getBoolean(pos)
          if (!rs.wasNull()) {
            mutableRow.setBoolean(i, v)
          } else {
            mutableRow.setNullAt(i)
          }
        case StoreUtils.DECIMAL_TYPE =>
          // When connecting with Oracle DB through JDBC, the precision and
          // scale of BigDecimal object returned by ResultSet.getBigDecimal
          // is not correctly matched to the table schema reported by
          // ResultSetMetaData.getPrecision and ResultSetMetaData.getScale.
          // If inserting values like 19999 into a column with NUMBER(12, 2)
          // type, you get through a BigDecimal object with scale as 0. But the
          // dataframe schema has correct type as DecimalType(12, 2).
          // Thus, after saving the dataframe into parquet file and then
          // retrieve it, you will get wrong result 199.99. So it is needed
          // to set precision and scale for Decimal based on JDBC metadata.
          val v = rs.getBigDecimal(pos)
          if (v != null) {
            val d = schema.fields(i).dataType.asInstanceOf[DecimalType]
            mutableRow.update(i, Decimal(v, d.precision, d.scale))
          } else {
            mutableRow.setNullAt(i)
          }
        case StoreUtils.DOUBLE_TYPE =>
          val v = rs.getDouble(pos)
          if (!rs.wasNull()) {
            mutableRow.setDouble(i, v)
          } else {
            mutableRow.setNullAt(i)
          }
        case StoreUtils.FLOAT_TYPE =>
          val v = rs.getFloat(pos)
          if (!rs.wasNull()) {
            mutableRow.setFloat(i, v)
          } else {
            mutableRow.setNullAt(i)
          }
        case StoreUtils.DATE_TYPE =>
          val cal = this.defaultCal
          cal.clear()
          val v = rs.getDate(pos, cal)
          if (v != null) {
            mutableRow.setInt(i, DateTimeUtils.fromJavaDate(v))
          } else {
            mutableRow.setNullAt(i)
          }
        case StoreUtils.TIMESTAMP_TYPE =>
          val cal = this.defaultCal
          cal.clear()
          val v = rs.getTimestamp(pos, cal)
          if (v != null) {
            mutableRow.setLong(i, DateTimeUtils.fromJavaTimestamp(v))
          } else {
            mutableRow.setNullAt(i)
          }
        case StoreUtils.BINARY_TYPE =>
          val v = rs.getBytes(pos)
          if (v != null) {
            mutableRow.update(i, v)
          } else {
            mutableRow.setNullAt(i)
          }
        case StoreUtils.ARRAY_TYPE =>
          val v = rs.getBytes(pos)
          if (v != null) {
            val array = new UnsafeArrayData
            array.pointTo(v, Platform.BYTE_ARRAY_OFFSET, v.length)
            mutableRow.update(i, array)
          } else {
            mutableRow.setNullAt(i)
          }
        case StoreUtils.MAP_TYPE =>
          val v = rs.getBytes(pos)
          if (v != null) {
            val map = new UnsafeMapData
            map.pointTo(v, Platform.BYTE_ARRAY_OFFSET, v.length)
            mutableRow.update(i, map)
          } else {
            mutableRow.setNullAt(i)
          }
        case StoreUtils.STRUCT_TYPE =>
          val v = rs.getBytes(pos)
          if (v != null) {
            val s = schema.fields(i).dataType.asInstanceOf[StructType]
            val row = new UnsafeRow(s.fields.length)
            row.pointTo(v, Platform.BYTE_ARRAY_OFFSET, v.length)
            mutableRow.update(i, row)
          } else {
            mutableRow.setNullAt(i)
          }
        case _ => throw new IllegalArgumentException(
          s"Unsupported field ${schema.fields(i)}")
      }
      i = i + 1
    }
    moveNext()
    unsafeproj.apply(mutableRow)
  }
}

final class CompactExecRowIteratorOnRS(conn: Connection,
    stmt: Statement, ers: EmbedResultSet, context: TaskContext,
    override val schema: StructType)
    extends ResultSetIterator[InternalRow](conn, stmt, ers, context)
    with CompactExecRowToMutableRow {

  private lazy val unsafeproj = UnsafeProjection.create(schema.map(_.dataType).toArray)

  private[this] val mutableRow = new SpecificMutableRow(dataTypes)

  override def next(): InternalRow = {
    val result = createInternalRow(
      ers.currentRow.asInstanceOf[AbstractCompactExecRow], mutableRow)
    moveNext()
    unsafeproj.apply(result)
  }
}
