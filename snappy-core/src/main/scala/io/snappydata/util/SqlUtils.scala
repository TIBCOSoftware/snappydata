package io.snappydata.util

import org.apache.spark.sql.types._

object SqlUtils {
  def getInternalType(dataType: DataType): Class[_] = {
    dataType match {
      case ByteType => classOf[scala.Byte]
      case IntegerType => classOf[scala.Int]
      case DoubleType => classOf[scala.Double]
      //case "numeric" => org.apache.spark.sql.types.DoubleType
      //case "character" => org.apache.spark.sql.types.StringType
      case StringType => classOf[String]
      //case "binary" => org.apache.spark.sql.types.BinaryType
      //case "raw" => org.apache.spark.sql.types.BinaryType
      //case "logical" => org.apache.spark.sql.types.BooleanType
      //case "boolean" => org.apache.spark.sql.types.BooleanType
      //case "timestamp" => org.apache.spark.sql.types.TimestampType
      //case "date" => org.apache.spark.sql.types.DateType
      case _ => throw new IllegalArgumentException(s"Invaid type $dataType")
    }
  }

}