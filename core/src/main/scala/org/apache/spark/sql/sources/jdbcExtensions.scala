/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql.sources

import java.sql.Connection
import java.util.Properties

import scala.collection.{mutable, Map => SMap}
import scala.util.control.NonFatal

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OverwriteOptions}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.sources.JdbcExtendedUtils.quotedName
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Row, SQLContext, SaveMode, SnappySession, SparkSession}

/**
 * Some extensions to `JdbcDialect` used by Snappy implementation.
 */
abstract class JdbcExtendedDialect extends JdbcDialect {

  /** Query string to check for existence of a table */
  def tableExists(table: String, conn: Connection, context: SQLContext): Boolean

  /**
   * Retrieve the jdbc / sql type for a given datatype.
   *
   * @param dataType The datatype (e.g. [[StringType]])
   * @param md       The metadata
   * @return The new JdbcType if there is an override for this DataType
   */
  def getJDBCType(dataType: DataType, md: Metadata): Option[JdbcType] =
    getJDBCType(dataType)

  /** Create a new schema. */
  def createSchema(schemaName: String, conn: Connection): Unit

  /**
   * Get the DDL to truncate a table, or null/empty
   * if truncate is not supported.
   */
  def truncateTable(tableName: String): String = s"TRUNCATE TABLE ${quotedName(tableName)}"

  def dropTable(tableName: String, conn: Connection, context: SQLContext,
      ifExists: Boolean): Unit

  def initializeTable(tableName: String, caseSensitive: Boolean,
      conn: Connection): Unit = {
  }

  def addExtraDriverProperties(isLoner: Boolean, props: Properties): Unit = {
  }

  def getPartitionByClause(col: String): String
}

object JdbcExtendedUtils extends Logging {

  // "dbtable" lower case since some other code including Spark's depends on it
  val DBTABLE_PROPERTY = "dbtable"

  val SCHEMADDL_PROPERTY = "SCHEMADDL"
  val ALLOW_EXISTING_PROPERTY = "ALLOWEXISTING"
  val BASETABLE_PROPERTY = "BASETABLE"

  // internal properties will be stored as Hive table parameters
  val TABLETYPE_PROPERTY = "EXTERNAL_SNAPPY"

  def executeUpdate(sql: String, conn: Connection): Unit = {
    val stmt = conn.createStatement()
    try {
      stmt.executeUpdate(sql)
    } finally {
      stmt.close()
    }
  }

  /**
   * Compute the schema string for this RDD.
   */
  def schemaString(schema: StructType, dialect: JdbcDialect): String = {
    val jdbcType: (DataType, Metadata) => Option[JdbcType] = dialect match {
      case ed: JdbcExtendedDialect => ed.getJDBCType
      case _ => (dataType, _) => dialect.getJDBCType(dataType)
    }
    val sb = new StringBuilder()
    schema.fields.foreach { field =>
      val dataType = Utils.getSQLDataType(field.dataType)
      val typeString: String =
        jdbcType(dataType, field.metadata).map(_.databaseTypeDefinition).getOrElse(
          dataType match {
            case IntegerType => "INTEGER"
            case LongType => "BIGINT"
            case DoubleType => "DOUBLE PRECISION"
            case FloatType => "REAL"
            case ShortType => "INTEGER"
            case ByteType => "BYTE"
            case BooleanType => "BIT(1)"
            case StringType => "TEXT"
            case BinaryType => "BLOB"
            case TimestampType => "TIMESTAMP"
            case DateType => "DATE"
            case DecimalType.Fixed(precision, scale) =>
              s"DECIMAL($precision,$scale)"
            case _ => throw new IllegalArgumentException(
              s"Don't know how to save $field to JDBC")
          })
      sb.append(s""", "${field.name}" $typeString""")
      if (!field.nullable) sb.append(" NOT NULL")
    }
    if (sb.length < 2) "" else "(".concat(sb.substring(2)).concat(")")
  }

  def addSplitProperty(value: String, propertyName: String,
      options: SMap[String, String]): SMap[String, String] = {
    // split the string into parts for size limitation in hive metastore table
    val parts = value.grouped(3500).toSeq
    val opts = options match {
      case m: mutable.Map[String, String] => m
      case _ =>
        val m = new mutable.HashMap[String, String]
        m ++= options
        m
    }
    opts += (s"$propertyName.numParts" -> parts.size.toString)
    parts.zipWithIndex.foreach { case (part, index) =>
      opts += (s"$propertyName.part.$index" -> part)
    }
    opts
  }

  def readSplitProperty(propertyName: String,
      options: SMap[String, String]): Option[String] = {
    // read the split schema DDL string from hive metastore table parameters
    options.get(s"$propertyName.numParts") map { numParts =>
      (0 until numParts.toInt).map { index =>
        val partProp = s"$propertyName.part.$index"
        options.get(partProp) match {
          case Some(part) => part
          case None => throw new AnalysisException("Could not read " +
              s"$propertyName from metastore because it is corrupted " +
              s"(missing part $index, $numParts parts expected).")
        }
        // Stick all parts back to a single schema string.
      }.mkString
    }
  }

  def getTableWithSchema(table: String, conn: Connection,
      session: Option[() => SnappySession] = None): (String, String) = {
    val dotIndex = table.indexOf('.')
    val schemaName = if (dotIndex > 0) {
      table.substring(0, dotIndex)
    } else {
      // get the current schema
      if (session.isDefined) session.get().getCurrentSchema else conn.getSchema
    }
    val tableName = if (dotIndex > 0) table.substring(dotIndex + 1) else table
    (schemaName, tableName)
  }

  def tableExistsInMetaData(table: String, conn: Connection,
      dialect: JdbcDialect): Boolean = {
    val (schemaName, tableName) = getTableWithSchema(table, conn)
    try {
      // using the JDBC meta-data API
      val rs = conn.getMetaData.getTables(null, schemaName, tableName, null)
      rs.next()
    } catch {
      case _: java.sql.SQLException => false
    }
  }

  def createSchema(schemaName: String, conn: Connection,
      dialect: JdbcDialect): Unit = {
    dialect match {
      case d: JdbcExtendedDialect => d.createSchema(schemaName, conn)
      case _ => // ignore
    }
  }

  /**
   * Returns true if the table already exists in the JDBC database.
   */
  def tableExists(table: String, conn: Connection, dialect: JdbcDialect,
      context: SQLContext): Boolean = {
    dialect match {
      case d: JdbcExtendedDialect => d.tableExists(table, conn, context)

      case _ =>
        try {
          tableExistsInMetaData(table, conn, dialect)
        } catch {
          case NonFatal(_) =>
            val stmt = conn.createStatement()
            val quotedTable = quotedName(table)
            // try LIMIT clause, then FETCH FIRST and lastly COUNT
            val testQueries = Array(s"SELECT 1 FROM $quotedTable LIMIT 1",
              s"SELECT 1 FROM $quotedTable FETCH FIRST ROW ONLY",
              s"SELECT COUNT(1) FROM $quotedTable")
            for (q <- testQueries) {
              try {
                val rs = stmt.executeQuery(q)
                rs.next()
                rs.close()
                stmt.close()
                // return is not very efficient but then this code
                // is not performance sensitive
                return true
              } catch {
                case NonFatal(_) => // continue
              }
            }
            false
        }
    }
  }

  def dropTable(conn: Connection, tableName: String, dialect: JdbcDialect,
      context: SQLContext, ifExists: Boolean): Unit = {
    dialect match {
      case d: JdbcExtendedDialect =>
        d.dropTable(tableName, conn, context, ifExists)
      case _ =>
        if (!ifExists || tableExists(tableName, conn, dialect, context)) {
          JdbcExtendedUtils.executeUpdate(s"DROP TABLE ${quotedName(tableName)}", conn)
        }
    }
  }

  def truncateTable(conn: Connection, tableName: String, dialect: JdbcDialect): Unit = {
    dialect match {
      case d: JdbcExtendedDialect =>
        JdbcExtendedUtils.executeUpdate(d.truncateTable(tableName), conn)
      case _ =>
        JdbcExtendedUtils.executeUpdate(s"TRUNCATE TABLE ${quotedName(tableName)}", conn)
    }
  }

  /** get the table name in SQL quoted form e.g. "APP"."TABLE1" */
  def quotedName(table: String, escapeQuotes: Boolean = false): String = {
    val (schema, tableName) = getTableWithSchema(table, null)
    if (escapeQuotes) {
      val sb = new java.lang.StringBuilder(schema.length + tableName.length + 9)
      sb.append("\\\"").append(schema).append("\\\".\\\"")
          .append(tableName).append("\\\"").toString
    } else {
      val sb = new java.lang.StringBuilder(schema.length + tableName.length + 5)
      sb.append('"').append(schema).append("\".\"").append(tableName).append('"').toString
    }
  }

  /**
   * Create a [[DataSource]] for an external DataSource schema DDL
   * string specification.
   */
  def externalResolvedDataSource(
      snappySession: SnappySession,
      schemaString: String,
      provider: String,
      mode: SaveMode,
      options: Map[String, String],
      data: Option[LogicalPlan] = None): BaseRelation = {
    val dataSource = DataSource(snappySession, className = provider)
    val clazz: Class[_] = dataSource.providingClass
    val relation = clazz.newInstance() match {

      case dataSource: ExternalSchemaRelationProvider =>
        // add schemaString as separate property for Hive persistence
        dataSource.createRelation(snappySession.snappyContext, mode,
          new CaseInsensitiveMap(JdbcExtendedUtils.addSplitProperty(
            schemaString, JdbcExtendedUtils.SCHEMADDL_PROPERTY, options).toMap),
          schemaString, data)

      case _ => throw new AnalysisException(
        s"${clazz.getCanonicalName} is not an ExternalSchemaRelationProvider.")
    }
    relation
  }

  /**
   * Returns the SQL for prepare to insert or put rows into a table.
   */
  def getInsertOrPutString(table: String, rddSchema: StructType,
      putInto: Boolean, escapeQuotes: Boolean = false): String = {
    val sql = new StringBuilder()
    val tableName = quotedName(table, escapeQuotes)
    if (putInto) {
      sql.append("PUT INTO ").append(tableName).append(" (")
    } else {
      sql.append("INSERT INTO ").append(tableName).append(" (")
    }
    var fieldsLeft = rddSchema.fields.length
    rddSchema.fields.foreach { field =>
      if (escapeQuotes) {
        sql.append("""\"""").append(field.name).append("""\"""")
      } else {
        sql.append('"').append(field.name).append('"')
      }
      if (fieldsLeft > 1) sql.append(',') else sql.append(')')
      fieldsLeft -= 1
    }
    sql.append(" VALUES (")
    fieldsLeft = rddSchema.fields.length
    while (fieldsLeft > 0) {
      sql.append('?')
      if (fieldsLeft > 1) sql.append(',') else sql.append(')')
      fieldsLeft -= 1
    }
    sql.toString()
  }

  /**
   * Returns the SQL for creating the WHERE clause for a set of columns.
   */
  def fillColumnsClause(sql: StringBuilder, fields: Seq[String],
      escapeQuotes: Boolean = false, separator: String = " AND "): Unit = {
    var fieldsLeft = fields.length
    fields.foreach { field =>
      if (escapeQuotes) {
        sql.append("""\"""").append(field).append("""\"""")
      } else {
        sql.append('"').append(field).append('"')
      }
      sql.append("=?")
      if (fieldsLeft > 1) sql.append(separator)
      fieldsLeft -= 1
    }
  }

  def bulkInsertOrPut(rows: Seq[Row], sparkSession: SparkSession,
      schema: StructType, resolvedName: String, putInto: Boolean): Int = {
    val session = sparkSession.asInstanceOf[SnappySession]
    val sessionState = session.sessionState
    val tableIdent = sessionState.sqlParser.parseTableIdentifier(resolvedName)
    val encoder = RowEncoder(schema)
    val ds = session.internalCreateDataFrame(session.sparkContext.parallelize(
      rows.map(encoder.toRow)), schema)
    val plan = if (putInto) {
      PutIntoTable(
        table = UnresolvedRelation(tableIdent),
        child = ds.logicalPlan)
    } else {
      new Insert(
        table = UnresolvedRelation(tableIdent),
        partition = Map.empty[String, Option[String]],
        child = ds.logicalPlan,
        overwrite = OverwriteOptions(enabled = false),
        ifNotExists = false)
    }
    session.sessionState.executePlan(plan).executedPlan.executeCollect()
        // always expect to create a TableInsertExec
        .foldLeft(0)(_ + _.getInt(0))
  }
}
