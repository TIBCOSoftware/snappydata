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
package org.apache.spark.sql

import java.sql.{Connection, Types}

import com.pivotal.gemfirexd.internal.shared.common.reference.{JDBC40Translation, Limits}
import io.snappydata.Constant

import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.JdbcType
import org.apache.spark.sql.sources.JdbcExtendedUtils.quotedName
import org.apache.spark.sql.sources.{JdbcExtendedDialect, JdbcExtendedUtils}
import org.apache.spark.sql.types._

/**
 * Base implementation of various dialect implementations for SnappyData.
 */
abstract class SnappyDataBaseDialect extends JdbcExtendedDialect {

  override def tableExists(table: String, conn: Connection,
      context: SQLContext): Boolean = {
    if (table.equalsIgnoreCase("SYSIBM.SYSDUMMY1")) return true
    val session = context.sparkSession
    val (schemaName, tableName) = JdbcExtendedUtils.getTableWithSchema(
      table, conn, Some(() => session))
    session.catalog.tableExists(schemaName, tableName)
  }

  override def getCatalystType(sqlType: Int, typeName: String,
      size: Int, md: MetadataBuilder): Option[DataType] = sqlType match {
    case Types.FLOAT if typeName.equalsIgnoreCase("float") => Some(DoubleType)
    case Types.REAL if typeName.equalsIgnoreCase("real") => Some(FloatType)
    case Types.TINYINT | Types.SMALLINT => Some(ShortType)
    case Types.VARCHAR if size > 0 =>
      md.putLong(Constant.CHAR_TYPE_SIZE_PROP, size)
      md.putString(Constant.CHAR_TYPE_BASE_PROP, "VARCHAR")
      Some(StringType)
    case Types.CHAR if size > 0 =>
      md.putLong(Constant.CHAR_TYPE_SIZE_PROP, size)
      md.putString(Constant.CHAR_TYPE_BASE_PROP, "CHAR")
      Some(StringType)
    // complex types are sent back as JSON strings by default
    case Types.ARRAY | Types.STRUCT | JDBC40Translation.MAP | JDBC40Translation.JSON =>
      Some(StringType)
    case Types.BIT if size > 1 => Some(BinaryType)
    case _ => None
  }

  /**
   * See SPARK-10101 issue for similar problem. If the PR raised is
   * merged we can update VARCHAR handling here accordingly.
   */
  def getColumnMetadata(dt: DataType, md: Metadata,
      forSchema: Boolean): (DataType, String, Int, Int, Int) = {
    val (dataType, typeName) = dt match {
      case u: UserDefinedType[_] =>
        (JdbcExtendedUtils.getSQLDataType(u.sqlType), Some(u.userClass.getName))
      case t => (t, None)
    }
    def getTypeName(name: String): String = typeName match {
      case Some(n) => n
      case None => name
    }
    dataType match {
      case StringType =>
        if ((md ne null) && md.contains(Constant.CHAR_TYPE_SIZE_PROP) &&
            md.contains(Constant.CHAR_TYPE_BASE_PROP)) {
          val size = math.min(md.getLong(Constant.CHAR_TYPE_SIZE_PROP), Int.MaxValue).toInt
          md.getString(Constant.CHAR_TYPE_BASE_PROP) match {
            case "CHAR" =>
              (dataType, s"${getTypeName("CHAR")}($size)", Types.CHAR, size, -1)
            case "VARCHAR" =>
              (dataType, s"${getTypeName("VARCHAR")}($size)", Types.VARCHAR, size, -1)
            case _ =>
              (dataType, getTypeName("STRING"), Types.CLOB, Limits.DB2_LOB_MAXWIDTH, -1)
          }
        } else {
          (dataType, getTypeName("STRING"), Types.CLOB, Limits.DB2_LOB_MAXWIDTH, -1)
        }
      case BooleanType => (dataType, getTypeName("BOOLEAN"), Types.BOOLEAN, 1, 0)
      case ByteType | ShortType => (dataType, getTypeName("SMALLINT"), Types.SMALLINT, 5, 0)
      case d: DecimalType =>
        (dataType, s"${getTypeName("DECIMAL")}(${d.precision},${d.scale})", Types.DECIMAL,
            d.precision, d.scale)
      case BinaryType => (dataType, getTypeName("BLOB"), Types.BLOB, Limits.DB2_LOB_MAXWIDTH, -1)
      case _: ArrayType | _: MapType | _: StructType if forSchema =>
        (dataType, getTypeName("BLOB"), Types.BLOB, Limits.DB2_LOB_MAXWIDTH, -1)
      case a: ArrayType =>
        (a, s"${getTypeName("ARRAY")}<${getColumnMetadata(a.elementType, null, forSchema)._2}>",
            Types.ARRAY, -1, -1)
      case m: MapType =>
        val keyTypeName = getColumnMetadata(m.keyType, null, forSchema)._2
        val valueTypeName = getColumnMetadata(m.valueType, null, forSchema)._2
        (m, s"${getTypeName("MAP")}<$keyTypeName,$valueTypeName>", JDBC40Translation.MAP, -1, -1)
      case s: StructType =>
        (s, getTypeName("STRUCT") + s.map(f => getColumnMetadata(f.dataType, md, forSchema)._2)
            .mkString("<", ",", ">"), Types.STRUCT, -1, -1)
      case NullType => (dataType, getTypeName("NULL"), Types.NULL, 0, -1)
      case d =>
        val scale = if (d.isInstanceOf[NumericType]) 0 else -1
        JdbcUtils.getCommonJDBCType(d) match {
          case Some(t) =>
            (d, getTypeName(t.databaseTypeDefinition), t.jdbcNullType, -1, scale)
          case None =>
            (d, getTypeName(d.simpleString), Types.OTHER, -1, scale)
        }
    }
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = getJDBCType(dt, null)

  /**
   * Look SPARK-10101 issue for similar problem. If the PR raised is
   * ever merged we can remove this method here.
   */
  override def getJDBCType(dt: DataType, md: Metadata): Option[JdbcType] = {
    getColumnMetadata(dt, md, forSchema = true) match {
      case (_, _, Types.OTHER, _, _) => None
      case (_, name, jdbcType, _, _) => Some(JdbcType(name, jdbcType))
    }
  }

  override def getTableExistsQuery(table: String): String = {
    s"SELECT 1 FROM ${quotedName(table)} FETCH FIRST ROW ONLY"
  }

  override def getSchemaQuery(table: String): String = {
    s"SELECT * FROM ${quotedName(table)} FETCH FIRST ROW ONLY"
  }

  override def createSchema(schemaName: String, conn: Connection): Unit = {
    JdbcExtendedUtils.executeUpdate("CREATE SCHEMA " + schemaName, conn)
  }

  override def dropTable(tableName: String, conn: Connection,
      context: SQLContext, ifExists: Boolean): Unit = {
    if (ifExists) {
      JdbcExtendedUtils.executeUpdate(s"DROP TABLE IF EXISTS ${quotedName(tableName)}", conn)
    } else {
      JdbcExtendedUtils.executeUpdate(s"DROP TABLE ${quotedName(tableName)}", conn)
    }
  }

  override def initializeTable(tableName: String, caseSensitive: Boolean,
      conn: Connection): Unit = {
    val dotIndex = tableName.indexOf('.')
    val (schema, table) = if (dotIndex > 0) {
      (tableName.substring(0, dotIndex), tableName.substring(dotIndex + 1))
    } else {
      (Constant.DEFAULT_SCHEMA, tableName)
    }
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("select datapolicy from sys.systables where " +
        s"tableName='$table' and tableschemaname='$schema'")
    val result = if (rs.next()) rs.getString(1) else null
    rs.close()
    stmt.close()
    if ("PARTITION".equalsIgnoreCase(result) ||
        "PERSISTENT_PARTITION".equalsIgnoreCase(result)) {

      JdbcExtendedUtils.executeUpdate(
        s"call sys.CREATE_ALL_BUCKETS('$tableName')", conn)
    }
  }

  override def getPartitionByClause(col: String): String =
    s"partition by column($col)"
}
