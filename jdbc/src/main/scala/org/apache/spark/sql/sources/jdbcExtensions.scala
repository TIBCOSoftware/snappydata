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
package org.apache.spark.sql.sources

import java.lang.reflect.{Field, Method}
import java.sql.{Connection, ResultSet, ResultSetMetaData, Types}
import java.util.Properties

import scala.annotation.tailrec
import scala.collection.{mutable, Map => SMap}
import scala.util.control.NonFatal

import com.pivotal.gemfirexd.Attribute
import io.snappydata.Constant

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.sources.JdbcExtendedUtils.quotedName
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, DataFrameWriter, SQLContext, SnappyDataBaseDialect, SnappyDataPoolDialect, SparkSession}

/**
 * Some extensions to `JdbcDialect` used by Snappy implementation.
 */
abstract class JdbcExtendedDialect extends JdbcDialect {

  /** Query string to check for existence of a table */
  def tableExists(table: String, conn: Connection, context: SQLContext): Boolean

  /**
   * Retrieve the jdbc / sql type for a given datatype.
   *
   * @param dataType The datatype (e.g. StringType)
   * @param md       The metadata
   * @return The new JdbcType if there is an override for this DataType
   */
  def getJDBCType(dataType: DataType, md: Metadata): Option[JdbcType]

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

  val DUMMY_TABLE_NAME: String = "SYSDUMMY1"
  val DUMMY_TABLE_QUALIFIED_NAME: String = "SYSIBM." + DUMMY_TABLE_NAME
  val EMPTY_SCHEMA: StructType = StructType(Nil)

  def executeUpdate(sql: String, conn: Connection): Unit = {
    val stmt = conn.createStatement()
    try {
      stmt.executeUpdate(sql)
    } finally {
      stmt.close()
    }
  }

  @tailrec
  def getSQLDataType(dataType: DataType): DataType = dataType match {
    case udt: UserDefinedType[_] => getSQLDataType(udt.sqlType)
    case _ => dataType
  }

  /**
   * Get [[JdbcType]] for given [[DataType]] and [[Metadata]].
   */
  def getJdbcType(dt: DataType, md: Metadata, dialect: JdbcDialect): JdbcType = {
    (dialect match {
      case d: JdbcExtendedDialect => d.getJDBCType(dt, md)
      case _ => dialect.getJDBCType(dt)
    }) match {
      case Some(d) => d
      case None => JdbcUtils.getCommonJDBCType(dt) match {
        case Some(d) => d
        case None =>
          throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.simpleString}")
      }
    }
  }

  def getCatalystType(dialect: JdbcDialect, jdbcType: Int, typeName: String, size: Int,
      scale: Int, md: MetadataBuilder, session: Option[SparkSession]): DataType = {
    (dialect match {
      case snappyDialect: SnappyDataBaseDialect =>
        snappyDialect.getCatalystType(jdbcType, typeName, size, md, session)
      case _ => dialect.getCatalystType(jdbcType, typeName, size, md)
    }) match {
      case Some(d) => d
      case None => getCatalystTypeMethod.invoke(JdbcUtils, Int.box(jdbcType),
        Int.box(size), Int.box(scale),
        Boolean.box(isSigned(jdbcType))).asInstanceOf[DataType]
    }
  }

  /**
   * Convert column names to upper-case for snappy-store or lower-case for spark.
   */
  def normalizeType(dataType: DataType, forSpark: Boolean): DataType = dataType match {
    case a: ArrayType => a.copy(elementType = normalizeType(a.elementType, forSpark))
    case m: MapType => m.copy(keyType = normalizeType(m.keyType, forSpark),
      valueType = normalizeType(m.valueType, forSpark))
    case s: StructType => StructType(s.fields.map(f =>
      f.copy(name = if (forSpark) toLowerCase(f.name) else toUpperCase(f.name),
        dataType = normalizeType(f.dataType, forSpark))))
    case _ => dataType
  }

  def normalizeSchema(schema: StructType, forSpark: Boolean = true): StructType =
    normalizeType(schema, forSpark).asInstanceOf[StructType]

  /**
   * Compute the schema string for this RDD. This is different from JdbcUtils.schemaString
   * in having its own getJdbcType that can honour metadata for VARCHAR/CHAR types.
   */
  def schemaString(schema: StructType, dialect: JdbcDialect): String = {
    val sb = new StringBuilder()
    normalizeSchema(schema, forSpark = false).foreach { field =>
      val jdbcType = getJdbcType(field.dataType, field.metadata, dialect)
      sb.append(s""", "${field.name}" ${jdbcType.databaseTypeDefinition}""")
      if (!field.nullable) sb.append(" NOT NULL")
    }
    if (sb.length < 2) "" else "(".concat(sb.substring(2)).concat(")")
  }

  def addSplitProperty(value: String, propertyName: String,
      options: SMap[String, String], threshold: Int): SMap[String, String] = {
    // split the string into parts for size limitation in hive metastore table
    val parts = value.grouped(threshold).toSeq
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
    val params = options match {
      case _: CaseInsensitiveMap => options
      case _ if options.getClass.getName.contains("CaseInsensitiveMutableHashMap") => options
      case _ => new CaseInsensitiveMap(options.toMap)
    }
    // read the split schema DDL string from hive metastore table parameters
    params.get(s"$propertyName.numParts") map { numParts =>
      (0 until numParts.toInt).map { index =>
        val partProp = s"$propertyName.part.$index"
        params.get(partProp) match {
          case Some(part) => part
          case None => throw new AnalysisException("Could not read " +
              s"$propertyName from metastore because it is corrupted " +
              s"(missing part $index, $numParts parts expected).")
        }
        // Stick all parts back to a single schema string.
      }.mkString
    }
  }

  // when forSpark=false then values will be converted to upper-case for snappy-store
  def getTableWithSchema(table: String, conn: Connection,
      session: Option[SparkSession], forSpark: Boolean = true): (String, String) = {
    val dotIndex = table.indexOf('.')
    val schemaName = if (dotIndex > 0) {
      table.substring(0, dotIndex)
    } else session match {
      case None if conn != null => conn.getSchema
      // get the current schema
      case Some(s) => s.catalog.currentDatabase
      case None => ""
    }
    val tableName = if (dotIndex > 0) table.substring(dotIndex + 1) else table
    // hive meta-store is case-insensitive so convert to upper-case for snappy-store
    if (forSpark) {
      (toLowerCase(if (!schemaName.isEmpty) schemaName else schemaName), toLowerCase(tableName))
    }
    else (toUpperCase(if (!schemaName.isEmpty) schemaName else schemaName), toUpperCase(tableName))
  }

  private def getTableMetadataResultSet(schemaName: String, tableName: String,
      conn: Connection): ResultSet = {
    // using the JDBC meta-data API
    conn.getMetaData.getTables(null, schemaName, tableName, null)
  }

  private lazy val getCatalystTypeMethod: Method = {
    val m = JdbcUtils.getClass.getDeclaredMethod(
      "org$apache$spark$sql$execution$datasources$jdbc$JdbcUtils$$getCatalystType",
      classOf[Int], classOf[Int], classOf[Int], classOf[Boolean])
    m.setAccessible(true)
    m
  }

  /**
   * Is the given JDBC type (java.sql.Types) data type signed.
   */
  def isSigned(typeId: Int): Boolean = {
    typeId == Types.INTEGER || typeId == Types.FLOAT || typeId == Types.DECIMAL ||
        typeId == Types.SMALLINT || typeId == Types.BIGINT || typeId == Types.TINYINT ||
        typeId == Types.NUMERIC || typeId == Types.REAL || typeId == Types.DOUBLE
  }

  def getTableSchema(schemaName: String, tableName: String, conn: Connection,
      session: Option[SparkSession]): StructType = {
    val rs = conn.getMetaData.getColumns(null, toUpperCase(schemaName),
      toUpperCase(tableName), null)
    if (rs.next()) {
      val cols = new mutable.ArrayBuffer[StructField]()
      do {
        // COLUMN_NAME
        val columnName = rs.getString(4)
        // DATA_TYPE
        val jdbcType = rs.getInt(5)
        // TYPE_NAME
        val typeName = rs.getString(6)
        // COLUMN_SIZE
        val size = rs.getInt(7)
        // DECIMAL_DIGITS
        val scale = rs.getInt(9)
        // NULLABLE
        val nullable = rs.getInt(11) != ResultSetMetaData.columnNoNulls
        val metadataBuilder = new MetadataBuilder()
            .putString("name", columnName).putLong("scale", scale)
        val columnType = getCatalystType(SnappyDataPoolDialect, jdbcType, typeName,
          size, scale, metadataBuilder, session)
        cols += StructField(columnName, columnType, nullable, metadataBuilder.build())
      } while (rs.next())
      normalizeSchema(StructType(cols))
    } else EMPTY_SCHEMA
  }

  def tableExistsInMetaData(schemaName: String, tableName: String,
      conn: Connection, skipType: String = ""): Boolean = {
    try {
      // using the JDBC meta-data API
      val rs = getTableMetadataResultSet(schemaName, tableName, conn)
      if (skipType.isEmpty) {
        rs.next()
      } else {
        while (rs.next()) {
          if (rs.getString(4) != skipType) return true
        }
        false
      }
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
          val (schemaName, tableName) = getTableWithSchema(table, conn, None, forSpark = false)
          tableExistsInMetaData(schemaName, tableName, conn)
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

  def isRowLevelSecurityEnabled(table: String, conn: Connection, dialect: JdbcDialect,
      context: SQLContext): Boolean = {
    val (schemaName, tableName) = getTableWithSchema(table, conn, None, forSpark = false)
    val q = s"select 1 from sys.systables s where s.tablename = '$tableName' and " +
        s" s.tableschemaname = '$schemaName' and s.rowlevelsecurityenabled = true "
    conn.createStatement().executeQuery(q).next()
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
    // always expect fully qualified name
    val (schema, tableName) = getTableWithSchema(table, conn = null, None, forSpark = false)
    if (escapeQuotes) {
      val sb = new java.lang.StringBuilder(schema.length + tableName.length + 9)
      if (!schema.isEmpty) {
        sb.append("\\\"").append(schema).append("\\\".")
      }
      sb.append("\\\"").append(tableName).append("\\\"").toString
    } else {
      val sb = new java.lang.StringBuilder(schema.length + tableName.length + 5)
      if (!schema.isEmpty) {
        sb.append('"').append(schema).append("\".")
      }
      sb.append('"').append(tableName).append('"').toString
    }
  }

  def toLowerCase(k: String): String = k.toLowerCase(java.util.Locale.ROOT)

  def toUpperCase(k: String): String = k.toUpperCase(java.util.Locale.ROOT)

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
      val columnName = toUpperCase(field.name)
      if (escapeQuotes) {
        sql.append("""\"""").append(columnName).append("""\"""")
      } else {
        sql.append('"').append(columnName).append('"')
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
        sql.append("""\"""").append(toUpperCase(field)).append("""\"""")
      } else {
        sql.append('"').append(toUpperCase(field)).append('"')
      }
      sql.append("=?")
      if (fieldsLeft > 1) sql.append(separator)
      fieldsLeft -= 1
    }
  }

  def defaultPoolURL(session: SparkSession): String = {
    val sparkProp = s"${Constant.SPARK_PREFIX}${Constant.CONNECTION_PROPERTY}"
    val conf = session.conf
    val hostPort = conf.getOption(sparkProp) match {
      case Some(c) => c
      case None => conf.getOption(Constant.CONNECTION_PROPERTY) match {
        case Some(c) => c
        case None => throw new IllegalStateException(
          s"Neither $sparkProp nor ${Constant.CONNECTION_PROPERTY} set for SnappyData connect")
      }
    }
    s"${Constant.POOLED_THIN_CLIENT_URL}$hostPort"
  }

  val PREFIXES: Array[String] = Array(
    Constant.STORE_PROPERTY_PREFIX,
    Constant.SPARK_STORE_PREFIX,
    Constant.PROPERTY_PREFIX,
    Constant.SPARK_SNAPPY_PREFIX)

  /** set the user/password in the property bag if not present */
  private[sql] def fillUserPassword(properties: SMap[String, String],
      session: SparkSession): SMap[String, String] = {
    var props = properties
    val conf = session.conf
    PREFIXES.find(p => conf.contains(p + Attribute.USERNAME_ATTR)) match {
      case None =>
      case Some(prefix) =>
        if (!props.contains(Attribute.USERNAME_ATTR) &&
            !props.contains(Attribute.USERNAME_ALT_ATTR)) {
          props += (Attribute.USERNAME_ATTR -> conf.get(prefix + Attribute.USERNAME_ATTR))
          conf.getOption(prefix + Attribute.PASSWORD_ATTR) match {
            case Some(password) => props += (Attribute.PASSWORD_ATTR -> password)
            case None =>
          }
        }
    }
    props
  }

  private[sql] lazy val writerDfField: Field = {
    val f = try {
      classOf[DataFrameWriter[_]].getDeclaredField(
        "org$apache$spark$sql$DataFrameWriter$$df")
    } catch {
      case _: Exception => classOf[DataFrameWriter[_]].getDeclaredField("df")
    }
    f.setAccessible(true)
    f
  }
}
