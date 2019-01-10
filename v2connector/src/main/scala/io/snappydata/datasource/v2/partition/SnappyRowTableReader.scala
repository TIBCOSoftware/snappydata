/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
package io.snappydata.datasource.v2.partition

import java.io.IOException
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Collections

import scala.collection.mutable.ArrayBuffer

import io.snappydata.datasource.v2.driver.{QueryConstructs, SnappyTableMetaData}
import io.snappydata.thrift.StatementAttrs
import io.snappydata.thrift.internal.ClientStatement

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.execution.columnar.SharedExternalStoreUtils
import org.apache.spark.sql.sources.JdbcExtendedUtils.quotedName
import org.apache.spark.sql.sources.v2.reader.DataReader
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

/**
 *  Actually fetches the data on executors
 *
 * @param bucketId          bucketId for which this factory is created
 * @param tableMetaData     metadata of the table being scanned
 * @param queryConstructs   contains projections and filters
 */
class SnappyRowTableReader(val bucketId: Int,
    tableMetaData: SnappyTableMetaData, queryConstructs: QueryConstructs)
    extends DataReader[Row] {

  private lazy val conn = jdbcConnection()
  private var preparedStatement: PreparedStatement = _
  private var resultSet: ResultSet = _
  private lazy val resultColumnCount = resultSet.getMetaData.getColumnCount

  initiateScan()

  def initiateScan(): Unit = {
    setLocalBucketScan()
    prepareScanStatement()
  }

  private def jdbcConnection(): Connection = {
    // from bucketToServerMapping get the collection of hosts where the bucket exists
    // (each element in hostsAndURLs ArrayBuffer is in the form of a tuple (host, jdbcURL))
    val hostsAndURLs: ArrayBuffer[(String, String)] = if (tableMetaData.bucketCount == 0) {
      tableMetaData.bucketToServerMapping.head.apply(0)
    } else {
      tableMetaData.bucketToServerMapping.get(bucketId)
    }
    val connectionURL = hostsAndURLs(0)._2
    DriverManager.getConnection(connectionURL)
  }

  private def setLocalBucketScan(): Unit = {
    val statement = conn.createStatement()

    val thriftConn = statement match {
      case clientStmt: ClientStatement =>
        val clientConn = clientStmt.getConnection
        if (tableMetaData.bucketCount > 0) { // partitioned table
          clientConn.setCommonStatementAttributes(ClientStatement.setLocalExecutionBucketIds(
            new StatementAttrs(), Collections.singleton(Int.box(bucketId)),
            tableMetaData.tableName, true))
        }
        clientConn
      case _ => null
    }

    // TODO: handle case of DRDA driver that is when thriftConn = null
  }

  private def prepareScanStatement(): Unit = {
    val columnList = queryConstructs.projections.fieldNames.mkString(",")

    val filterWhereClause = if (queryConstructs.whereClause ne null) {
      queryConstructs.whereClause
    } else {
      ""
    }

    val sqlText = s"SELECT $columnList FROM" +
        s" ${quotedName(tableMetaData.tableName)}$filterWhereClause"

    preparedStatement = conn.prepareStatement(sqlText)
    if (queryConstructs.whereClauseArgs ne null) {
      SharedExternalStoreUtils.setStatementParameters(preparedStatement,
        queryConstructs.whereClauseArgs)
    }
    resultSet = preparedStatement.executeQuery()
  }

  /**
   * Proceed to next record, returns false if there is no more records.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   *
   * @throws IOException if failure happens during disk/network IO like reading files.
   */
  override def next(): Boolean = {
    // For the first cut we are assuming that we will get entire data
    // in the first call of this method
    // We will have to think about breaking into chunks if the data size
    // too huge to handle in one fetch
    // Check the current smart connector code, to see how row buffers and column
    // batches are brought and how filters and column projections are pushed.
    // We can exactly mirror the smart connector implementation
    // We decode and form a row. We will use our decoder classes
    // which are to be moved to a new package. This package needs to be present in
    // the classpath
    resultSet.next()
  }

  /**
   * Return the current record. This method should return same value until `next` is called.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   */
  override def get(): Row = {
    val values = new Array[Any](resultColumnCount)
    for(index <- 0 until resultColumnCount) {
      values(index) = resultSet.getObject(index + 1)
    }
    new GenericRowWithSchema(values, queryConstructs.projections)
  }

  /**
   * Returns the current record in the result set as a ColumnarBatch
   * @return ColumnarBatch of one row
   */
  def getAsColumnarBatch(): ColumnarBatch = {
    val columnVectors = new Array[ColumnVector](resultColumnCount)
    for(index <- 0 until resultColumnCount) {
      columnVectors(index) = new JDBCResultSetColumnVector(
        queryConstructs.projections.fields(index).dataType, resultSet, index + 1)
    }
    val columnarBatch = new ColumnarBatch(columnVectors)
    columnarBatch.setNumRows(1)
    columnarBatch
  }

  override def close(): Unit = {
    if (resultSet != null) resultSet.close()
    if (preparedStatement != null) preparedStatement.close()
    if (conn != null) conn.close()
  }

}
