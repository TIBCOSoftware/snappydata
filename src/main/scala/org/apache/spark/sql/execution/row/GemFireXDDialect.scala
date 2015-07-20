package org.apache.spark.sql.execution.row

import java.sql.Types

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.collection.Utils._
import org.apache.spark.sql.jdbc.{JdbcDialects, JdbcType}
import org.apache.spark.sql.types._

/**
 * Default dialect for GemFireXD >= 1.4.0.
 * Contains specific type conversions to and from Spark SQL catalyst types.
 */
@DeveloperApi
case object GemFireXDDialect extends JdbcExtendedDialect {

  // register the dialect
  JdbcDialects.registerDialect(GemFireXDDialect)

  def init(): Unit = {
    // do nothing; just forces one-time invocation of registerDialect
  }

  def canHandle(url: String): Boolean = url.startsWith("jdbc:gemfirexd:")

  private val bitTypeName = "bit".normalize
  private val floatTypeName = "float".normalize
  private val realTypeName = "real".normalize

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
