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
package org.apache.spark.sql.row

import java.sql.Connection

import scala.collection.JavaConverters._

import io.snappydata.SnappyTableStatsProviderService

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortDirection}
import org.apache.spark.sql.catalyst.plans.logical.OverwriteOptions
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.execution.row.{RowDeleteExec, RowInsertExec, RowUpdateExec}
import org.apache.spark.sql.execution.sources.StoreDataSourceStrategy.translateToFilter
import org.apache.spark.sql.execution.{ConnectionPool, SparkPlan}
import org.apache.spark.sql.internal.ColumnTableBulkOps
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.sources.JdbcExtendedUtils.quotedName
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.CodeGeneration
import org.apache.spark.sql.types._
import org.apache.spark.{Logging, Partition}

/**
 * A LogicalPlan implementation for an external row table whose contents
 * are retrieved using a JDBC URL or DataSource.
 */
abstract case class JDBCMutableRelation(
    connProperties: ConnectionProperties,
    table: String,
    mode: SaveMode,
    userSpecifiedString: String,
    parts: Array[Partition],
    origOptions: CaseInsensitiveMap,
    @transient override val sqlContext: SQLContext)
    extends BaseRelation
    with PrunedUnsafeFilteredScan
    with InsertableRelation
    with PlanInsertableRelation
    with RowInsertableRelation
    with UpdatableRelation
    with DeletableRelation
    with DestroyRelation
    with IndexableRelation
    with AlterableRelation
    with NativeTableRowLevelSecurityRelation
    with Logging {

  override val needConversion: Boolean = false

  override def sizeInBytes: Long = {
    SnappyTableStatsProviderService.getService.getTableStatsFromService(table) match {
      case Some(s) => s.getTotalSize
      case None => super.sizeInBytes
    }
  }

  val driver: String = Utils.registerDriverUrl(connProperties.url)

  override protected final def dialect: JdbcDialect = connProperties.dialect

  override protected final def isRowTable: Boolean = true

  override final def resolvedName: String = table

  override val (schemaName: String, tableName: String) =
    JdbcExtendedUtils.getTableWithSchema(table, conn = null, Some(sqlContext.sparkSession))

  private[sql] var tableCreated: Boolean = _

  final lazy val schemaFields: scala.collection.Map[String, StructField] =
    Utils.schemaFields(schema)

  def partitionExpressions(relation: LogicalRelation): Seq[Expression]

  def numBuckets: Int

  def isPartitioned: Boolean

  override protected final val connFactory: () => Connection =
    JdbcUtils.createConnectionFactory(new JDBCOptions(connProperties.url, table,
      connProperties.connProps.asScala.toMap))

  refreshTableSchema(invalidateCached = false, fetchFromStore = false)

  override protected def createActualTables(conn: Connection): Unit = {
    // quote the table name e.g. for reserved keywords or special chars
    val sql = s"CREATE TABLE ${quotedName(resolvedName)} $userSpecifiedString"
    val pass = connProperties.connProps.remove(com.pivotal.gemfirexd.Attribute.PASSWORD_ATTR)
    logInfo(s"Applying DDL (url=${connProperties.url}; " +
        s"props=${connProperties.connProps}): $sql")
    if (pass != null) {
      connProperties.connProps.setProperty(com.pivotal.gemfirexd.Attribute.PASSWORD_ATTR,
        pass.asInstanceOf[String])
    }
    JdbcExtendedUtils.executeUpdate(sql, conn)
    tableCreated = true
    dialect match {
      case d: JdbcExtendedDialect => d.initializeTable(resolvedName,
        sqlContext.conf.caseSensitiveAnalysis, conn)
      case _ => // Do Nothing
    }
    refreshTableSchema(invalidateCached = true, fetchFromStore = true)
  }

  override def buildUnsafeScan(requiredColumns: Array[String],
      filters: Array[Expression]): (RDD[Any], Seq[RDD[InternalRow]]) = {
    val jdbcOptions = new JDBCOptions(connProperties.url,
      table, connProperties.executorConnProps.asScala.toMap)

    val rdd = JDBCRDD.scanTable(
      sqlContext.sparkContext,
      schema,
      requiredColumns,
      filters.flatMap(translateToFilter),
      parts, jdbcOptions).asInstanceOf[RDD[Any]]
    (rdd, Nil)
  }

  final lazy val rowInsertStr: String = JdbcExtendedUtils.getInsertOrPutString(
    table, schema, putInto = false)

  override def getInsertPlan(relation: LogicalRelation,
      child: SparkPlan): SparkPlan = {
    RowInsertExec(child, putInto = false, partitionColumns,
      partitionExpressions(relation), numBuckets, isPartitioned, schema, Some(this),
      onExecutor = false, table, connProperties)
  }

  /**
   * Get a spark plan to update rows in the relation. The result of SparkPlan
   * execution should be a count of number of updated rows.
   */
  override def getUpdatePlan(relation: LogicalRelation, child: SparkPlan,
      updateColumns: Seq[Attribute], updateExpressions: Seq[Expression],
      keyColumns: Seq[Attribute]): SparkPlan = {
    RowUpdateExec(child, resolvedName, partitionColumns, partitionExpressions(relation),
      numBuckets, isPartitioned, schema, Some(this), updateColumns, updateExpressions,
      keyColumns, connProperties, onExecutor = false)
  }

  /**
   * Get a spark plan to delete rows the relation. The result of SparkPlan
   * execution should be a count of number of updated rows.
   */
  override def getDeletePlan(relation: LogicalRelation, child: SparkPlan,
      keyColumns: Seq[Attribute]): SparkPlan = {
    RowDeleteExec(child, resolvedName, partitionColumns, partitionExpressions(relation),
      numBuckets, isPartitioned, schema, Some(this), keyColumns, connProperties,
      onExecutor = false)
  }

  /**
   * Get the "key" columns for the table that need to be projected out by
   * UPDATE and DELETE operations for affecting the selected rows.
   */
  override def getKeyColumns: Seq[String] = {
    val keyColumns = relationInfo.pkCols
    // if partitioning columns are different from primary key then add those
    if (keyColumns.length > 0) {
      keyColumns ++ partitionColumns.filter(!keyColumns.contains(_))
    } else Nil
  }

  /** Get primary keys of the row table */
  override def getPrimaryKeyColumns(session: SnappySession): Seq[String] = relationInfo.pkCols

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    // use the Insert plan for best performance
    // that will use the getInsertPlan above (in StoreStrategy)
    sqlContext.sessionState.executePlan(
      new Insert(
        table = LogicalRelation(this),
        partition = Map.empty[String, Option[String]],
        child = data.logicalPlan,
        OverwriteOptions(overwrite),
        ifNotExists = false)).toRdd
  }

  override def insert(rows: Seq[Row]): Int = {
    val numRows = rows.length
    if (numRows == 0) {
      throw new IllegalArgumentException(
        "JDBCUpdatableRelation.insert: no rows provided")
    }
    val connProps = connProperties.connProps
    val batchSize = connProps.getProperty("batchsize", "1000").toInt
    // use bulk insert using insert plan for large number of rows
    if (numRows > (batchSize * 4)) {
      ColumnTableBulkOps.bulkInsertOrPut(rows, sqlContext.sparkSession, schema,
        table, putInto = false)
    } else {
      val connection = ConnectionPool.getPoolConnection(table, dialect,
        connProperties.poolProps, connProps, connProperties.hikariCP)
      try {
        val stmt = connection.prepareStatement(rowInsertStr)
        val result = CodeGeneration.executeUpdate(table, stmt,
          rows, numRows > 1, batchSize, schema.fields, dialect)
        stmt.close()
        result
      } finally {
        connection.commit()
        connection.close()
      }
    }
  }

  override def executeUpdate(sql: String, defaultSchema: String): Int = {
    val connection = ConnectionPool.getPoolConnection(table, dialect,
      connProperties.poolProps, connProperties.connProps,
      connProperties.hikariCP)
    var currentSchema: String = null
    try {
      if (defaultSchema ne null) {
        currentSchema = connection.getSchema
        if (defaultSchema != currentSchema) {
          connection.setSchema(defaultSchema)
        }
      }
      val stmt = connection.prepareStatement(sql)
      val result = stmt.executeUpdate()
      stmt.close()
      result
    } finally {
      if (currentSchema ne null) connection.setSchema(currentSchema)
      connection.commit()
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
    updateColumns.foreach { c =>
      setFields(index) = schemaFields.getOrElse(c, throw new AnalysisException(
        "JDBCUpdatableRelation: Cannot resolve column name " +
            s""""$c" among (${schema.fieldNames.mkString(", ")})"""))
      index += 1
    }
    val connection = ConnectionPool.getPoolConnection(table, dialect,
      connProperties.poolProps, connProperties.connProps,
      connProperties.hikariCP)
    try {
      val setStr = updateColumns.mkString("SET ", "=?, ", "=?")
      val whereStr = if (filterExpr == null || filterExpr.isEmpty) ""
      else " WHERE " + filterExpr
      val updateSql = s"UPDATE $table $setStr$whereStr"
      val stmt = connection.prepareStatement(updateSql)
      val result = CodeGeneration.executeUpdate(updateSql, stmt,
        newColumnValues, setFields, dialect)
      stmt.close()
      result
    } finally {
      connection.commit()
      connection.close()
    }
  }

  override def delete(filterExpr: String): Int = {
    val connection = ConnectionPool.getPoolConnection(table, dialect,
      connProperties.poolProps, connProperties.connProps,
      connProperties.hikariCP)
    try {
      val whereStr =
        if (filterExpr == null || filterExpr.isEmpty) ""
        else "WHERE " + filterExpr
      val stmt = connection.prepareStatement(s"DELETE FROM $table $whereStr")
      val result = stmt.executeUpdate()
      stmt.close()
      result
    } finally {
      connection.commit()
      connection.close()
    }
  }

  override def destroy(ifExists: Boolean): Unit = {
    // drop the external table using a non-pool connection
    val conn = connFactory()
    try {
      // clean up the connection pool and caches
      ExternalStoreUtils.removeCachedObjects(sqlContext, table)
    } finally {
      try {
        JdbcExtendedUtils.dropTable(conn, table, dialect, sqlContext, ifExists)
      } finally {
        conn.commit()
        conn.close()
      }
    }
  }

  def truncate(): Unit = {
    val conn = connFactory()
    try {
      JdbcExtendedUtils.truncateTable(conn, table, dialect)
    }
    finally {
      conn.commit()
      conn.close()
    }
  }

  protected def constructCreateIndexSQL(indexName: String,
      baseTable: String,
      indexColumns: Seq[(String, Option[SortDirection])],
      options: Map[String, String]): String = ""

  override def createIndex(indexIdent: TableIdentifier,
      tableIdent: TableIdentifier,
      indexColumns: Seq[(String, Option[SortDirection])],
      options: Map[String, String]): Unit = {
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    val sql = constructCreateIndexSQL(indexIdent.unquotedString, tableIdent.unquotedString,
      indexColumns, options)

    // Create the Index if the table exists.
    if (schema.nonEmpty) {
      executeUpdate(sql, JdbcExtendedUtils.toUpperCase(session.getCurrentSchema))
    } else {
      throw new AnalysisException(s"Base table $table does not exist.")
    }
  }

  override def dropIndex(indexIdent: TableIdentifier, tableIdent: TableIdentifier,
      ifExists: Boolean): Unit = {
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    val ifExistsStr = if (ifExists) " IF EXISTS" else ""
    executeUpdate(s"DROP INDEX$ifExistsStr ${quotedName(tableIdent.unquotedString)}",
      JdbcExtendedUtils.toUpperCase(session.getCurrentSchema))
  }

  private def getDataType(column: StructField): String = {
    val dataType: String = dialect match {
      case d: JdbcExtendedDialect =>
        val jd = d.getJDBCType(column.dataType, column.metadata)
        jd match {
          case Some(x) => x.databaseTypeDefinition
          case _ => column.dataType.simpleString
        }
      case _ => column.dataType.simpleString
    }
    dataType
  }

  override def alterTable(tableIdent: TableIdentifier,
      isAddColumn: Boolean, column: StructField, defaultValue: Option[String],
      referentialAction: String): Unit = {
    val conn = connFactory()
    try {
      val columnName = JdbcExtendedUtils.toUpperCase(column.name)
      val sql = if (isAddColumn) {
        val defaultColumnValue = defaultValue match {
          case Some(v) =>
            val defaultString = column.dataType match {
              case StringType | DateType | TimestampType => s" default '$v'"
              case _ => s" default $v"
            }
            defaultString
          case None => ""
        }

        val nullable = if (column.nullable) "" else " NOT NULL"
        s"""alter table ${quotedName(table)}
           | add column "$columnName"
           |  ${getDataType(column)}$nullable$defaultColumnValue""".stripMargin
      } else {
        s"""alter table ${quotedName(table)} drop column "$columnName" $referentialAction"""
      }
      if (schema.nonEmpty) {
        JdbcExtendedUtils.executeUpdate(sql, conn)
        // refresh the schema in the relation object
        refreshTableSchema(invalidateCached = true, fetchFromStore = true)
      } else {
        throw new TableNotFoundException(schemaName, tableName)
      }
    } catch {
      case se: java.sql.SQLException =>
        if (se.getMessage.contains("No suitable driver found")) {
          throw new AnalysisException(s"${se.getMessage}\n" +
              "Ensure that the 'driver' option is set appropriately and " +
              "the driver jars available (--jars option in spark-submit).")
        } else {
          throw se
        }
    } finally {
      conn.commit()
      conn.close()
    }
  }
}
