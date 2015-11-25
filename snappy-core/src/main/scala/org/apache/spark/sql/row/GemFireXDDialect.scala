package org.apache.spark.sql.row

import java.sql.{Connection, Types}
import java.util.Properties

import scala.util.control.NonFatal

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.collection.Utils._
import org.apache.spark.sql.jdbc.{JdbcDialects, JdbcType}
import org.apache.spark.sql.sources.{JdbcExtendedDialect, JdbcExtendedUtils}
import org.apache.spark.sql.types._

/**
 * Default dialect for GemFireXD >= 1.4.0.
 * Contains specific type conversions to and from Spark SQL catalyst types.
 */
@DeveloperApi
case object GemFireXDDialect extends GemFireXDBaseDialect {

  // register the dialect
  JdbcDialects.registerDialect(GemFireXDDialect)

  def canHandle(url: String): Boolean =
      (url.startsWith("jdbc:gemfirexd:") ||
      url.startsWith("jdbc:snappydata:")) &&
      !url.startsWith("jdbc:gemfirexd://") &&
      !url.startsWith("jdbc:snappydata://")

  override def extraDriverProperties(isLoner: Boolean): Properties = {
    isLoner match {
      case true => new Properties
      case false =>
        val props = new Properties()
        props.setProperty("host-data", "false")
        props
    }
  }

  override def getPartitionByClause(col : String): String = s"partition by column($col)"
}

/**
 * Default dialect for GemFireXD >= 1.4.0.
 * Contains specific type conversions to and from Spark SQL catalyst types.
 */
@DeveloperApi
case object GemFireXDClientDialect extends GemFireXDBaseDialect {

  // register the dialect
  JdbcDialects.registerDialect(GemFireXDClientDialect)

  def canHandle(url: String): Boolean =
      url.startsWith("jdbc:gemfirexd://") ||
      url.startsWith("jdbc:snappydata://")

  override def getPartitionByClause(col : String): String = s"partition by column($col)"
}

abstract class GemFireXDBaseDialect extends JdbcExtendedDialect {

  def init(): Unit = {
    // do nothing; just forces one-time invocation of various registerDialects
    GemFireXDDialect.getClass
    GemFireXDClientDialect.getClass
  }

  protected val bitTypeName = "bit".normalize
  protected val floatTypeName = "float".normalize
  protected val realTypeName = "real".normalize

  override def getCatalystType(sqlType: Int, typeName: String,
                      size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.FLOAT && typeName.normalize.equals(floatTypeName)) {
      Some(DoubleType)
    } else if (sqlType == Types.REAL &&
      typeName.normalize.equals(realTypeName)) {
      Some(FloatType)
    } else if (sqlType == Types.BIT && size > 1 &&
      typeName.normalize.equals(bitTypeName)) {
      Some(BinaryType)
    } else None
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Some(JdbcType("CLOB", java.sql.Types.CLOB))
    case BinaryType => Some(JdbcType("BLOB", java.sql.Types.BLOB))
    case BooleanType => Some(JdbcType("BOOLEAN", java.sql.Types.BOOLEAN))
    // TODO: check if this should be INTEGER for GemFireXD for below two
    case ByteType => Some(JdbcType("SMALLINT", java.sql.Types.INTEGER))
    case ShortType => Some(JdbcType("SMALLINT", java.sql.Types.INTEGER))
    case DecimalType.Fixed(precision, scale) =>
      Some(JdbcType(s"DECIMAL($precision,$scale)", java.sql.Types.DECIMAL))
    case _ => None
  }

  override def getCurrentSchema(conn: Connection): String = {
    try {
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery("VALUES CURRENT SCHEMA")
      val result = if (rs.next()) rs.getString(1) else null
      rs.close()
      stmt.close()
      result
    } catch {
      case NonFatal(e) => null
    }
  }

  override def dropTable(tableName: String, conn: Connection,
      context: SQLContext, ifExists: Boolean): Unit = {
    if (ifExists) {
      JdbcExtendedUtils.executeUpdate(s"DROP TABLE IF EXISTS $tableName", conn)
    } else {
      JdbcExtendedUtils.executeUpdate(s"DROP TABLE $tableName", conn)
    }
  }

  override def initializeTable(tableName: String, caseSensitive: Boolean,
      conn: Connection): Unit = {
    val stmt = conn.createStatement()
    // TODO: need to use quoted names with caseSensitive=true in create & query
    /*
    val table = if (caseSensitive) tableName
    else Utils.normalizeIdUpperCase(tableName)
    */
    val table = Utils.normalizeIdUpperCase(tableName)
    val rs = stmt.executeQuery("select datapolicy from sys.systables where " +
        s"tablename='$table'")
    val result = if (rs.next()) rs.getString(1) else null
    rs.close()
    stmt.close()
    if ("PARTITION".equalsIgnoreCase(result) ||
        "PERSISTENT_PARTITION".equalsIgnoreCase(result)) {
      JdbcExtendedUtils.executeUpdate(
        s"call sys.CREATE_ALL_BUCKETS('$table')", conn)
    }
  }
}
