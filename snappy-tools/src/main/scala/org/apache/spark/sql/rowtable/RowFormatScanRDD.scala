/*
 * Copyright (c) 2010-2016 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql.rowtable

import java.sql.{Connection, ResultSet, Statement}
import java.util.Properties

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.PartitionedRegion
import com.pivotal.gemfirexd.internal.engine.Misc

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.collection.MultiExecutorLocalPartition
import org.apache.spark.sql.columnar.{ConnectionProperties, ExternalStoreUtils}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.StoreFunctions._
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.types._
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}

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
    connectionProperties: ConnectionProperties,
    filters: Array[Filter] = Array.empty[Filter],
    partitions: Array[Partition] = Array.empty[Partition],
    blockMap: Map[InternalDistributedMember, BlockManagerId] =
    Map.empty[InternalDistributedMember, BlockManagerId],
    properties: Properties = new Properties())
    extends JDBCRDD(sc, getConnection, schema, tableName, columns,
      filters, partitions, properties) {

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
        if (sb.length > 7) {
          sb.append(" AND ")
        }
        compileFilter(s, sb, args)
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
      args: ArrayBuffer[Any]): Unit = f match {
    case EqualTo(attr, value) =>
      sb.append(attr).append(" = ?")
      args += value
    case LessThan(attr, value) =>
      sb.append(attr).append(" < ?")
      args += value
    case GreaterThan(attr, value) =>
      sb.append(attr).append(" > ?")
      args += value
    case LessThanOrEqual(attr, value) =>
      sb.append(attr).append(" <= ?")
      args += value
    case GreaterThanOrEqual(attr, value) =>
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
      ps.setInt(2, thePart.index)
      ps.executeUpdate()
      ps.close()
    }

    val sqlText = s"SELECT $columnList FROM $resolvedName$filterWhereClause"
    val args = filterWhereArgs
    val stmt = conn.prepareStatement(sqlText)
    if (args ne null) {
      ExternalStoreUtils.setStatementParameters(stmt, args)
    }
    val fetchSize = properties.getProperty("fetchSize")
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
    new InternalRowIteratorOnRS(conn, stmt, rs, context,
      schema).asInstanceOf[Iterator[InternalRow]]
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[MultiExecutorLocalPartition].hostExecutorIds
  }

  override def getPartitions: Array[Partition] = {
    executeWithConnection(getConnection, {
      case conn =>
        val tableSchema = conn.getSchema
        val resolvedName = StoreUtils.lookupName(tableName, tableSchema)
        val region = Misc.getRegionForTable(resolvedName, true)
        if (region.isInstanceOf[PartitionedRegion]) {
          StoreUtils.getPartitionsPartitionedTable(sc, tableName, tableSchema, blockMap)
        } else {
          StoreUtils.getPartitionsReplicatedTable(sc, resolvedName, tableSchema, blockMap)
        }
    })
  }
}

final class InternalRowIteratorOnRS(conn: Connection,
    stmt: Statement, rs: ResultSet, context: TaskContext,
    schema: StructType) extends Iterator[InternalRow] with Logging {

  private[this] val types = schema.fields.map(_.dataType)
  private[this] val mutableRow = new SpecificMutableRow(types)

  private[this] var hasNextValue = true
  private[this] var nextValue: InternalRow = _

  context.addTaskCompletionListener { context => close() }

  def getNext: InternalRow = {
    if (rs.next() /* rs.lightWeightNext() */) {
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
          case _ => throw new IllegalArgumentException(
            s"Unsupported field ${schema.fields(i)}")
        }
        i = i + 1
      }
      mutableRow
    } else {
      null.asInstanceOf[InternalRow]
    }
  }

  def close() {
    if (!hasNextValue) return

    try {
      // GfxdConnectionWrapper.restoreContextStack(stmt, rs)
      // rs.lightWeightClose()
      rs.close()
    } catch {
      case e: Exception => logWarning("Exception closing resultSet", e)
    }
    try {
      stmt.close()
    } catch {
      case e: Exception => logWarning("Exception closing statement", e)
    }
    try {
      conn.close()
      logDebug("closed connection for task " + context.partitionId())
    } catch {
      case e: Exception => logWarning("Exception closing connection", e)
    }
    hasNextValue = false
  }

  override def hasNext: Boolean = {
    if (hasNextValue) {
      if (nextValue eq null) {
        try {
          nextValue = getNext
        } catch {
          case NonFatal(e) =>
            logWarning("Exception iterating resultSet", e)
        } finally {
          if (nextValue eq null) {
            close()
          }
        }
        hasNextValue
      } else {
        true
      }
    } else {
      false
    }
  }

  override def next(): InternalRow = {
    val v = nextValue
    if (v ne null) {
      nextValue = null
      v
    } else if (hasNext) {
      val v = nextValue
      nextValue = null
      v
    } else {
      throw new NoSuchElementException("End of stream")
    }
  }
}
