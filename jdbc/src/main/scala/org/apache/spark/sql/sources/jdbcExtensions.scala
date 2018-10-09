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

import java.lang.reflect.Method
import java.nio.ByteBuffer
import java.sql.{Connection, ResultSet, ResultSetMetaData, Types}
import java.util.Properties

import scala.annotation.tailrec
import scala.collection.{mutable, Map => SMap}
import scala.util.control.NonFatal

import com.gemstone.gemfire.internal.shared.ClientSharedUtils

import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.sources.JdbcExtendedUtils.quotedName
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, SQLContext, SnappyDataPoolDialect, SparkSession}
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkEnv}

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

  // "dbtable" lower case since some other code including Spark's depends on it
  val DBTABLE_PROPERTY = "dbtable"

  val SCHEMADDL_PROPERTY = "SCHEMADDL"
  val ALLOW_EXISTING_PROPERTY = "ALLOWEXISTING"
  val BASETABLE_PROPERTY = "BASETABLE"

  // internal properties will be stored as Hive table parameters
  val TABLETYPE_PROPERTY = "EXTERNAL_SNAPPY"

  val DUMMY_TABLE_NAME: String = "SYSDUMMY1"
  val DUMMY_TABLE_QUALIFIED_NAME: String = "SYSIBM." + DUMMY_TABLE_NAME

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

  /**
   * Compute the schema string for this RDD.
   */
  def schemaString(schema: StructType, dialect: JdbcDialect): String = {
    val sb = new StringBuilder()
    schema.foreach { field =>
      val jdbcType = getJdbcType(field.dataType, field.metadata, dialect)
      sb.append(s""", "${field.name}" ${jdbcType.databaseTypeDefinition}""")
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
      session: Option[() => SparkSession] = None): (String, String) = {
    val dotIndex = table.indexOf('.')
    val schemaName = if (dotIndex > 0) {
      table.substring(0, dotIndex)
    } else session match {
      case None => conn.getSchema
      // get the current schema
      case Some(s) => s().catalog.currentDatabase
    }
    val tableName = if (dotIndex > 0) table.substring(dotIndex + 1) else table
    (schemaName, tableName)
  }

  private def getTableMetadataResultSet(table: String, conn: Connection): ResultSet = {
    val (schemaName, tableName) = getTableWithSchema(table, conn)
    // using the JDBC meta-data API
    conn.getMetaData.getTables(null, schemaName, tableName, null)
  }

  protected lazy val getCatalystTypeMethod: Method = {
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

  def getTableSchema(schemaName: String, tableName: String, conn: Connection): Seq[StructField] = {
    val rs = conn.getMetaData.getColumns(null, schemaName, tableName, null)
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
        val columnType = SnappyDataPoolDialect.getCatalystType(jdbcType, typeName,
          size, metadataBuilder) match {
          case Some(t) => t
          case None =>
            if (jdbcType == Types.JAVA_OBJECT) {
              // try to get class for the typeName else fallback to Object
              val userClass = try {
                Utils.classForName(typeName).asInstanceOf[Class[AnyRef]]
              } catch {
                case _: Throwable => classOf[AnyRef]
              }
              new JavaObjectType(userClass)
            } else {
              getCatalystTypeMethod.invoke(JdbcUtils, Int.box(jdbcType),
                Int.box(size), Int.box(scale),
                Boolean.box(isSigned(jdbcType))).asInstanceOf[DataType]
            }
        }
        cols += StructField(columnName, columnType, nullable, metadataBuilder.build())
      } while (rs.next())
      cols
    } else Nil
  }

  def tableExistsInMetaData(table: String, conn: Connection): Boolean = {
    try {
      // using the JDBC meta-data API
      val rs = getTableMetadataResultSet(table, conn)
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
          tableExistsInMetaData(table, conn)
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
    val (schemaName, tableName) = getTableWithSchema(table, conn)
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

  def toLowerCase(k: String): String = k.toLowerCase(java.util.Locale.ENGLISH)

  def toUpperCase(k: String): String = k.toUpperCase(java.util.Locale.ENGLISH)

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
}

final class JavaObjectType(override val userClass: java.lang.Class[AnyRef])
    extends UserDefinedType[AnyRef] {

  override def typeName: String = userClass.getName

  override def sqlType: DataType = BinaryType

  override def serialize(obj: AnyRef): Any = {
    val serializer = SparkEnv.get.serializer.newInstance()
    ClientSharedUtils.toBytes(serializer.serialize(obj))
  }

  override def deserialize(datum: Any): AnyRef = {
    val serializer = SparkEnv.get.serializer.newInstance()
    serializer.deserialize(ByteBuffer.wrap(datum.asInstanceOf[Array[Byte]]))
  }
}
