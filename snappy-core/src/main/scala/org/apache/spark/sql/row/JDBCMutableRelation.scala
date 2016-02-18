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
package org.apache.spark.sql.row

import java.sql.Connection
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.jdbc._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.{Logging, Partition}

/**
 * A LogicalPlan implementation for an external row table whose contents
 * are retrieved using a JDBC URL or DataSource.
 */
class JDBCMutableRelation(
    val url: String,
    val table: String,
    val provider: String,
    mode: SaveMode,
    userSpecifiedString: String,
    val parts: Array[Partition],
    val poolProperties: Map[String, String],
    val connProperties: Properties,
    val hikariCP: Boolean,
    val origOptions: Map[String, String],
    @transient override val sqlContext: SQLContext)
    extends BaseRelation
    with PrunedFilteredScan
    with InsertableRelation
    with RowInsertableRelation
    with UpdatableRelation
    with DeletableRelation
    with DestroyRelation
    with IndexableRelation
    with Logging {

  override val needConversion: Boolean = false

  val driver = DriverRegistry.getDriverClassName(url)

  final val dialect = JdbcDialects.get(url)

  // create table in external store once upfront
  var tableSchema: String = _

  override final lazy val schema: StructType =
    JDBCRDD.resolveTable(url, table, connProperties)

  final lazy val schemaFields = Utils.schemaFields(schema)

  def createTable(mode: SaveMode): String = {
    var conn: Connection = null
    try {
      conn = ExternalStoreUtils.getConnection(url, connProperties,
        dialect, isLoner = Utils.isLoner(sqlContext.sparkContext))
      logInfo("Applying DDL : "+ url + " connproperties " + connProperties)

      var tableExists = JdbcExtendedUtils.tableExists(table, conn,
        dialect, sqlContext)
      val tableSchema = JdbcExtendedUtils.getCurrentSchema(conn, dialect)
      if (mode == SaveMode.Ignore && tableExists) {
        dialect match {
          case d: JdbcExtendedDialect => d.initializeTable(table,
            sqlContext.conf.caseSensitiveAnalysis, conn)
          case _ => // Do Nothing
        }
        return tableSchema
      }

      if (mode == SaveMode.ErrorIfExists && tableExists) {
        sys.error(s"Table $table already exists.")
      }

      if (mode == SaveMode.Overwrite && tableExists) {
        // truncate the table if possible
        val truncate = dialect match {
          case MySQLDialect | PostgresDialect => s"TRUNCATE TABLE $table"
          case d: JdbcExtendedDialect => d.truncateTable(table)
          case _ => ""
        }
        if (truncate != null && truncate.length > 0) {
          JdbcExtendedUtils.executeUpdate(truncate, conn)
        } else {
          JdbcUtils.dropTable(conn, table)
          tableExists = false
        }
      }

      // Create the table if the table didn't exist.
      if (!tableExists) {
        val sql = s"CREATE TABLE $table $userSpecifiedString"
        logInfo("Applying DDL : " + sql)
        JdbcExtendedUtils.executeUpdate(sql, conn)
        dialect match {
          case d: JdbcExtendedDialect => d.initializeTable(table,
            sqlContext.conf.caseSensitiveAnalysis, conn)
          case _ => // Do Nothing
        }
      }
      tableSchema
    } catch {
      case sqle: java.sql.SQLException =>
        if (sqle.getMessage.contains("No suitable driver found")) {
          throw new AnalysisException(s"${sqle.getMessage}\n" +
              "Ensure that the 'driver' option is set appropriately and " +
              "the driver jars available (--jars option in spark-submit).")
        } else {
          throw sqle
        }
    } finally {
      if (conn != null) {
        conn.close()
      }
    }
  }

  final lazy val connector = ExternalStoreUtils.getConnector(table, driver,
    dialect, poolProperties, connProperties, hikariCP)

  override def buildScan(requiredColumns: Array[String],
      filters: Array[Filter]): RDD[Row] = {
    new JDBCRDD(
      sqlContext.sparkContext,
      connector,
      ExternalStoreUtils.pruneSchema(schemaFields, requiredColumns),
      table,
      requiredColumns,
      filters,
      parts,
      url,
      connProperties).asInstanceOf[RDD[Row]]
  }

  final lazy val rowInsertStr = ExternalStoreUtils.getInsertString(table, schema)

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    insert(data, if (overwrite) SaveMode.Overwrite else SaveMode.Append)
  }

  def insert(data: DataFrame, mode: SaveMode): Unit = {
    createTable(mode)
    insert(data)
  }

  def insert(data: DataFrame): Unit = {
    JdbcUtils.saveTable(data, url, table, connProperties)
  }

  // TODO: SW: should below all be executed from driver or some random executor?
  // at least the insert can be split into batches and modelled as an RDD
 // TODO: Suranjan common code in  ColumnFormatRelation too
  override def insert(rows: Seq[Row]): Int = {

    val numRows = rows.length
    if (numRows == 0) {
      throw new IllegalArgumentException(
        "JDBCUpdatableRelation.insert: no rows provided")
    }
    val connection = ConnectionPool.getPoolConnection(table, None, dialect,
      poolProperties, connProperties, hikariCP)
    try {
      val stmt = connection.prepareStatement(rowInsertStr)
      var result = 0
      if (numRows > 1) {
        for (row <- rows) {
          ExternalStoreUtils.setStatementParameters(stmt, schema.fields,
            row, dialect)
          stmt.addBatch()
        }
        val insertCounts = stmt.executeBatch()
        result = insertCounts.length
      } else {
        ExternalStoreUtils.setStatementParameters(stmt, schema.fields,
          rows.head, dialect)
        result = stmt.executeUpdate()
      }
      stmt.close()
      result
    } finally {
      connection.close()
    }
  }

  override def executeUpdate(sql: String): Int = {
    val connection = ConnectionPool.getPoolConnection(table, None, dialect,
      poolProperties, connProperties, hikariCP)
    try {
      val stmt = connection.prepareStatement(sql)
      val result = stmt.executeUpdate()
      stmt.close()
      result
    } finally {
      connection.close()
    }
  }

  override def update(filterExpr: String, newColumnValues: Row,
      updateColumns: Seq[String]): Int = {
    val ncols = updateColumns.length
    if (ncols == 0) {
      throw new IllegalArgumentException(
        "JDBCUpdatableRelation.update: no columns provided")
    }
    val setFields = new Array[StructField](ncols)
    var index = 0
    // not using loop over index below because incoming Seq[...]
    // may not have efficient index lookup
    updateColumns.foreach { col =>
      setFields(index) = schemaFields.getOrElse(col, schemaFields.getOrElse(
        col, throw new AnalysisException(
          "JDBCUpdatableRelation: Cannot resolve column name " +
              s""""$col" among (${schema.fieldNames.mkString(", ")})""")))
      index += 1
    }
    val connection = ConnectionPool.getPoolConnection(table, None, dialect,
      poolProperties, connProperties, hikariCP)
    try {
      val setStr = updateColumns.mkString("SET ", "=?, ", "=?")
      val whereStr =
        if (filterExpr == null || filterExpr.isEmpty) ""
        else " WHERE " + filterExpr
      val stmt = connection.prepareStatement(s"UPDATE $table $setStr$whereStr")
      ExternalStoreUtils.setStatementParameters(stmt, setFields,
        newColumnValues, dialect)
      val result = stmt.executeUpdate()
      stmt.close()
      result
    } finally {
      connection.close()
    }
  }

  override def delete(filterExpr: String): Int = {
    val connection = ConnectionPool.getPoolConnection(table, None, dialect,
      poolProperties, connProperties, hikariCP)
    try {
      val whereStr =
        if (filterExpr == null || filterExpr.isEmpty) ""
        else "WHERE " + filterExpr
      val stmt = connection.prepareStatement(s"DELETE FROM $table $whereStr")
      val result = stmt.executeUpdate()
      stmt.close()
      result
    } finally {
      connection.close()
    }
  }

  override def destroy(ifExists: Boolean): Unit = {
    // drop the external table using a non-pool connection
    val conn = ExternalStoreUtils.getConnection(url, connProperties,
      dialect, isLoner = Utils.isLoner(sqlContext.sparkContext))
    try {
      // clean up the connection pool on executors first
      Utils.mapExecutors(sqlContext,
        JDBCMutableRelation.removePool(table)).count()
      // then on the driver
      JDBCMutableRelation.removePool(table)
    } finally {
      try {
        JdbcExtendedUtils.dropTable(conn, table, dialect, sqlContext, ifExists)
      } finally {
        conn.close()
      }
    }
  }

  def truncate(): Unit = {
    val conn = ExternalStoreUtils.getConnection(url, connProperties,
      dialect, isLoner = Utils.isLoner(sqlContext.sparkContext))
    try {
      JdbcExtendedUtils.truncateTable(conn, table, dialect)
    }
    finally {
      conn.close()
    }
  }

  override def createIndex(tableName: String, sql: String): Unit = {
    var conn: Connection = null
    try {
      conn = ExternalStoreUtils.getConnection(url, connProperties,
        dialect, isLoner = Utils.isLoner(sqlContext.sparkContext))
      val tableExists = JdbcExtendedUtils.tableExists(tableName, conn,
        dialect, sqlContext)

      // Create the Index if the table exist.
      if (tableExists) {
        JdbcExtendedUtils.executeUpdate(sql, conn)
      } else {
        throw new AnalysisException(s"Base table $table does not exist.")
      }
    } catch {
      case sqle: java.sql.SQLException =>
        if (sqle.getMessage.contains("No suitable driver found")) {
          throw new AnalysisException(s"${sqle.getMessage}\n" +
              "Ensure that the 'driver' option is set appropriately and " +
              "the driver jars available (--jars option in spark-submit).")
        } else {
          throw sqle
        }
    } finally {
      if (conn != null) {
        conn.close()
      }
    }
  }
}

object JDBCMutableRelation extends Logging {

  private def removePool(table: String): () => Iterator[Unit] = () => {
    ConnectionPool.removePoolReference(table)
    Iterator.empty
  }
}

final class DefaultSource extends MutableRelationProvider
