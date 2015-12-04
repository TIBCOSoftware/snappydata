package org.apache.spark.sql.row

import java.sql.{Connection, Types}
import java.util.Properties

import scala.util.control.NonFatal

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.SQLContext
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
  protected val varcharTypeName = "varchar".normalize

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
    } else if (sqlType == Types.VARCHAR && size > 1 &&
        typeName.normalize.equals(varcharTypeName)) {
      md.putLong("maxlength", size)
      Some(StringType)
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

  /**
   * Look SPARK-10101 issue for similar problem. If the PR raised is
   * ever merged we can remove this method here.
   */
  override def getJDBCType(dt: DataType, md: Metadata): Option[JdbcType] = dt match {
    case StringType => {
      if (md.contains("maxlength")) {
        Some(JdbcType(s"VARCHAR(${md.getLong("maxlength")})", java.sql.Types.VARCHAR))
      } else {
        Some(JdbcType("CLOB", java.sql.Types.CLOB))
      }
    }
    case _ => getJDBCType(dt)
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

  override def initializeTable(tableName: String, conn: Connection): Unit = {
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(s"select datapolicy from sys.systables where tablename='${tableName.toUpperCase}'")
      val result = if (rs.next()) rs.getString(1) else null
      if(result.equalsIgnoreCase("PARTITION") || result.equalsIgnoreCase("PERSISTENT_PARTITION")){
        JdbcExtendedUtils.executeUpdate(
          s"call sys.CREATE_ALL_BUCKETS('$tableName')", conn)
      }
      rs.close()
      stmt.close()
  }
}
