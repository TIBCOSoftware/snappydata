package org.apache.spark.sql.execution.row

import java.sql.Types
import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler.local.LocalBackend
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.collection.Utils._
import org.apache.spark.sql.jdbc.{JdbcDialects, JdbcType}
import org.apache.spark.sql.types._

/**
 * Default dialect for GemFireXD >= 1.4.0.
 * Contains specific type conversions to and from Spark SQL catalyst types.
 */
@DeveloperApi
case object GemFireXDDialect extends GemFireXDBaseDialect {

  // register the dialect
  JdbcDialects.registerDialect(GemFireXDDialect)

  def canHandle(url: String): Boolean = url.startsWith("jdbc:gemfirexd:") &&
    !url.startsWith("jdbc:gemfirexd://")

  override def extraCreateTableProperties(sparkContext: SparkContext): Properties = {
    sparkContext.schedulerBackend match {
      case lb: LocalBackend => new Properties()
      case _ =>
        val props = new Properties()
        props.setProperty("host-data", "false")
        props
    }
  }
}

/**
 * Default dialect for GemFireXD >= 1.4.0.
 * Contains specific type conversions to and from Spark SQL catalyst types.
 */
@DeveloperApi
case object GemFireXDClientDialect extends GemFireXDBaseDialect {

  // register the dialect
  JdbcDialects.registerDialect(GemFireXDClientDialect)

  def canHandle(url: String): Boolean = url.startsWith("jdbc:gemfirexd://")
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
    case DecimalType.Unlimited =>
      // GemFireXD supports maximum precision of 127
      Some(JdbcType("DECIMAL(127,63)", java.sql.Types.DECIMAL))
    case _ => None
  }
}
