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

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.PartitionedRegion
import com.pivotal.gemfirexd.internal.engine.Misc
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecificMutableRow, UnsafeArrayData, UnsafeMapData, UnsafeRow}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.collection.MultiBucketExecutorPartition
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.columnar.{ExternalStoreUtils, ResultSetIterator}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.types._
import org.apache.spark.storage.BlockManagerId
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
class RowFormatScanRDD(@transient sc: SparkContext,
    getConnection: () => Connection,
    schema: StructType,
    tableName: String,
    columns: Array[String],
    connProperties: ConnectionProperties,
    filters: Array[Filter] = Array.empty[Filter],
    partitions: Array[Partition] = Array.empty[Partition],
    blockMap: Map[InternalDistributedMember, BlockManagerId] =
    Map.empty[InternalDistributedMember, BlockManagerId])
    extends JDBCRDD(sc, getConnection, schema, tableName, columns,
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
      filters.foreach { s =>
        compileFilter(s, sb, args, sb.length > 7)
      }
      // if no filter added return empty
      if (args.nonEmpty) {
        filterWhereArgs = args
        sb.toString()
      } else ""
    } else ""
  }

  // TODO: needs to be updated to use unhandledFilters of Spark 1.6.0

  private def compileFilter(f: Filter, sb: StringBuilder,
      args: ArrayBuffer[Any], addAnd: Boolean): Unit = f match {
    case EqualTo(attr, value) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append(attr).append(" = ?")
      args += value
    case LessThan(attr, value) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append(attr).append(" < ?")
      args += value
    case GreaterThan(attr, value) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append(attr).append(" > ?")
      args += value
    case LessThanOrEqual(attr, value) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append(attr).append(" <= ?")
      args += value
    case GreaterThanOrEqual(attr, value) =>
      if (addAnd) {
        sb.append(" AND ")
      }
      sb.append(attr).append(" >= ?")
      args += value
    case _ => // no filter
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

    val resolvedName = StoreUtils.lookupName(tableName, conn.getSchema)
    val region = Misc.getRegionForTable(resolvedName, true)

    if (region.isInstanceOf[PartitionedRegion]) {
      val ps = conn.prepareStatement(
        "call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION(?, ?)")
      ps.setString(1, resolvedName)
      val partition = thePart.asInstanceOf[MultiBucketExecutorPartition]
      var bucketString = ""
      partition.buckets.foreach( bucket => {
        bucketString = bucketString + bucket + ","
      })
      ps.setString(2, bucketString.substring(0, bucketString.length-1))
      ps.executeUpdate()
      ps.close()
    }

    val sqlText = s"SELECT $columnList FROM $resolvedName$filterWhereClause"
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
    new InternalRowIteratorOnRS(conn, stmt, rs, context, schema)
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
      val resolvedName = StoreUtils.lookupName(tableName, tableSchema)
      val region = Misc.getRegionForTable(resolvedName, true)
      if (region.isInstanceOf[PartitionedRegion]) {
        // StoreUtils.getPartitionsPartitionedTable(sc, tableName, tableSchema, blockMap)
        StoreUtils.getPartitions(sc, tableName, tableSchema, blockMap)
      } else {
        StoreUtils.getPartitionsReplicatedTable(sc, resolvedName, tableSchema, blockMap)
      }
    } finally {
      conn.close()
    }
  }
}

final class InternalRowIteratorOnRS(conn: Connection,
    stmt: Statement, rs: ResultSet, context: TaskContext, schema: StructType)
    extends ResultSetIterator[InternalRow](conn, stmt, rs, context) {

  private[this] val types = schema.fields.map(_.dataType)
  private[this] val mutableRow = new SpecificMutableRow(types)

  override def getNextValue(rs: ResultSet): InternalRow = {
    var i = 0
    while (i < types.length) {
      val pos = i + 1
      types(i) match {
        case StringType =>
          // TODO: can use direct bytes from CompactExecRows
          mutableRow.update(i, UTF8String.fromString(rs.getString(pos)))
        case IntegerType =>
          val iv = rs.getInt(pos)
          if (iv != 0 || !rs.wasNull()) {
            mutableRow.setInt(i, iv)
          } else {
            mutableRow.setNullAt(i)
          }
        case LongType =>
          if (schema.fields(i).metadata.contains("binarylong")) {
            val bytes = rs.getBytes(pos)
            if (bytes ne null) {
              var lv = 0L
              var j = 0
              while (j < bytes.size) {
                lv = 256 * lv + (255 & bytes(j))
                j = j + 1
              }
              mutableRow.setLong(i, lv)
            } else {
              mutableRow.setNullAt(i)
            }
          } else {
            val lv = rs.getLong(pos)
            if (lv != 0L || !rs.wasNull()) {
              mutableRow.setLong(i, lv)
            } else {
              mutableRow.setNullAt(i)
            }
          }
        case DoubleType =>
          val dv = rs.getDouble(pos)
          if (!rs.wasNull()) {
            mutableRow.setDouble(i, dv)
          } else {
            mutableRow.setNullAt(i)
          }
        case FloatType =>
          val fv = rs.getFloat(pos)
          if (!rs.wasNull()) {
            mutableRow.setFloat(i, fv)
          } else {
            mutableRow.setNullAt(i)
          }
        case BooleanType =>
          val bv = rs.getBoolean(pos)
          if (bv || !rs.wasNull()) {
            mutableRow.setBoolean(i, bv)
          } else {
            mutableRow.setNullAt(i)
          }
        // When connecting with Oracle DB through JDBC, the precision and
        // scale of BigDecimal object returned by ResultSet.getBigDecimal
        // is not correctly matched to the table schema reported by
        // ResultSetMetaData.getPrecision and ResultSetMetaData.getScale.
        // If inserting values like 19999 into a column with NUMBER(12, 2)
        // type, you get through a BigDecimal object with scale as 0.
        // But the dataframe schema has correct type as DecimalType(12, 2).
        // Thus, after saving the dataframe into parquet file and then
        // retrieve it, you will get wrong result 199.99.
        // So it is needed to set precision and scale for Decimal
        // based on JDBC metadata.
        case DecimalType.Fixed(p, s) =>
          val decimalVal = rs.getBigDecimal(pos)
          if (decimalVal ne null) {
            mutableRow.update(i, Decimal(decimalVal, p, s))
          } else {
            mutableRow.setNullAt(i)
          }
        case TimestampType =>
          val t = rs.getTimestamp(pos)
          if (t ne null) {
            mutableRow.setLong(i, DateTimeUtils.fromJavaTimestamp(t))
          } else {
            mutableRow.setNullAt(i)
          }
        case DateType =>
          // DateTimeUtils.fromJavaDate does not handle null value, so we need to check it.
          val dateVal = rs.getDate(pos)
          if (dateVal ne null) {
            mutableRow.setInt(i, DateTimeUtils.fromJavaDate(dateVal))
          } else {
            mutableRow.setNullAt(i)
          }
        case BinaryType => mutableRow.update(i, rs.getBytes(pos))
        case _: ArrayType =>
          val bytes = rs.getBytes(pos)
          val array = new UnsafeArrayData
          array.pointTo(bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length)
          mutableRow.update(i, array)
        case _: MapType =>
          val bytes = rs.getBytes(pos)
          val map = new UnsafeMapData
          map.pointTo(bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length)
          mutableRow.update(i, map)
        case s: StructType =>
          val bytes = rs.getBytes(pos)
          val row = new UnsafeRow
          row.pointTo(bytes, Platform.BYTE_ARRAY_OFFSET,
            s.fields.length, bytes.length)
          mutableRow.update(i, row)
        case _ => throw new IllegalArgumentException(
          s"Unsupported field ${schema.fields(i)}")
      }
      i += 1
    }
    mutableRow
  }
}
