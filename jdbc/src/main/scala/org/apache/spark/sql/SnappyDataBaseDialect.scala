/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

import java.nio.ByteBuffer
import java.sql.{Connection, Types}

import com.gemstone.gemfire.internal.shared.ClientSharedUtils
import com.pivotal.gemfirexd.internal.shared.common.reference.JDBC40Translation
import com.pivotal.gemfirexd.internal.shared.common.reference.Limits.{DB2_LOB_MAXWIDTH => LOB_MAXWIDTH}
import io.snappydata.Constant

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.parser.AbstractSqlParser
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.JdbcType
import org.apache.spark.sql.sources.JdbcExtendedUtils.quotedName
import org.apache.spark.sql.sources.{JdbcExtendedDialect, JdbcExtendedUtils}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * Base implementation of various dialect implementations for SnappyData.
 */
abstract class SnappyDataBaseDialect extends JdbcExtendedDialect {

  override def tableExists(table: String, conn: Connection,
      context: SQLContext): Boolean = {
    if (table.equalsIgnoreCase(JdbcExtendedUtils.DUMMY_TABLE_QUALIFIED_NAME)) return true
    val session = context.sparkSession
    val (schemaName, tableName) = JdbcExtendedUtils.getTableWithSchema(
      table, conn, Some(session))
    session.catalog.tableExists(schemaName, tableName)
  }

  override def getCatalystType(sqlType: Int, typeName: String,
      size: Int, md: MetadataBuilder): Option[DataType] = {
    getCatalystType(sqlType, typeName, size, md, None)
  }

  def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder,
      session: Option[SparkSession]): Option[DataType] = sqlType match {
    case Types.FLOAT =>
      if ("float".equalsIgnoreCase(typeName)) Some(DoubleType) else Some(FloatType)
    case Types.REAL =>
      if ("real".equalsIgnoreCase(typeName)) Some(FloatType) else Some(DoubleType)
    case Types.TINYINT | Types.SMALLINT => Some(ShortType)
    case Types.VARCHAR if size > 0 =>
      md.putLong(Constant.CHAR_TYPE_SIZE_PROP, size)
      md.putString(Constant.CHAR_TYPE_BASE_PROP, "VARCHAR")
      Some(StringType)
    case Types.CHAR if size > 0 =>
      md.putLong(Constant.CHAR_TYPE_SIZE_PROP, size)
      md.putString(Constant.CHAR_TYPE_BASE_PROP, "CHAR")
      Some(StringType)
    case Types.CLOB | JDBC40Translation.JSON =>
      val base = JdbcExtendedUtils.toUpperCase(typeName) match {
        case "CLOB" | "JSON" => "CLOB"
        case _ => "STRING"
      }
      md.putString(Constant.CHAR_TYPE_BASE_PROP, base)
      Some(StringType)
    case Types.BIT if size > 1 => Some(BinaryType)
    // translate complex types by running the parser on the type string
    case Types.ARRAY | JDBC40Translation.MAP | Types.STRUCT =>
      val sparkSession = session match {
        case Some(s) => s
        case None => SparkSession.builder().getOrCreate()
      }
      Some(sparkSession.sessionState.sqlParser
          .asInstanceOf[AbstractSqlParser].parseDataType(typeName))
    case Types.JAVA_OBJECT => // used by some system tables and VTIs
      // try to get class for the typeName else fallback to Object
      val userClass = try {
        Utils.classForName(typeName).asInstanceOf[Class[AnyRef]]
      } catch {
        case _: Throwable => classOf[AnyRef]
      }
      Some(new JavaObjectType(userClass))
    case _ => None
  }

  /**
   * Get JDBC metadata for a catalyst column representation.
   *
   * See SPARK-10101 issue for similar problem. If the PR raised is
   * merged we can update VARCHAR handling here accordingly.
   * [UPDATE] related PRs have been merged and SPARK-10101 closed but
   * it is only for passing through full types in CREATE TABLE and not
   * when reading unlike what is reauired for SnappyData.
   *
   * @param dt           the DataType of the column
   * @param md           any additional Metadata for the column
   * @param forTableDefn if true then the type name string returned will be suitable for column
   *                     definition in CREATE TABLE etc; the differences being that it will
   *                     include size definitions for CHAR/VARCHAR and complex types will be
   *                     mapped to underlying storage format rather than for display (i.e. BLOB)
   */
  def getJDBCMetadata(dt: DataType, md: Metadata,
      forTableDefn: Boolean): (DataType, String, Int, Int, Int) = {
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
        if ((md ne null) && md.contains(Constant.CHAR_TYPE_BASE_PROP)) {
          val size = if (md.contains(Constant.CHAR_TYPE_SIZE_PROP)) {
            math.min(md.getLong(Constant.CHAR_TYPE_SIZE_PROP), Int.MaxValue).toInt
          } else LOB_MAXWIDTH
          // skip size specification in the name when forTableDefn=false
          lazy val sizeSuffix = if (forTableDefn) s"($size)" else ""
          md.getString(Constant.CHAR_TYPE_BASE_PROP) match {
            case "CHAR" =>
              (dataType, s"${getTypeName("CHAR")}$sizeSuffix", Types.CHAR, size, -1)
            case "VARCHAR" =>
              (dataType, s"${getTypeName("VARCHAR")}$sizeSuffix", Types.VARCHAR, size, -1)
            case "CLOB" =>
              (dataType, getTypeName("CLOB"), Types.CLOB, LOB_MAXWIDTH, -1)
            case _ =>
              (dataType, getTypeName("STRING"), Types.CLOB, LOB_MAXWIDTH, -1)
          }
        } else {
          (dataType, getTypeName("STRING"), Types.CLOB, LOB_MAXWIDTH, -1)
        }
      case BooleanType => (dataType, getTypeName("BOOLEAN"), Types.BOOLEAN, 1, 0)
      case ByteType | ShortType => (dataType, getTypeName("SMALLINT"), Types.SMALLINT, 5, 0)
      case d: DecimalType =>
        (dataType, s"${getTypeName("DECIMAL")}(${d.precision},${d.scale})", Types.DECIMAL,
            d.precision, d.scale)
      case BinaryType => (dataType, getTypeName("BLOB"), Types.BLOB, LOB_MAXWIDTH, -1)
      case _: ArrayType | _: MapType | _: StructType if forTableDefn =>
        (dataType, getTypeName("BLOB"), Types.BLOB, LOB_MAXWIDTH, -1)
      case a: ArrayType =>
        (a, s"${getTypeName("ARRAY")}<${getJDBCMetadata(a.elementType, null, forTableDefn)._2}>",
            Types.ARRAY, LOB_MAXWIDTH, -1)
      case m: MapType =>
        val keyType = getJDBCMetadata(m.keyType, null, forTableDefn)._2
        val valueType = getJDBCMetadata(m.valueType, null, forTableDefn)._2
        (m, s"${getTypeName("MAP")}<$keyType,$valueType>", JDBC40Translation.MAP, LOB_MAXWIDTH, -1)
      case s: StructType =>
        (s, getTypeName("STRUCT") + s.map(f => f.name + ':' + getJDBCMetadata(f.dataType,
          md, forTableDefn)._2).mkString("<", ",", ">"), Types.STRUCT, LOB_MAXWIDTH, -1)
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
    getJDBCMetadata(dt, md, forTableDefn = true) match {
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
      (conn.getSchema, tableName)
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
