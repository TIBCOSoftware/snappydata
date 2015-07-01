package org.apache.spark.sql.sources

import scala.util.control.NonFatal

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * Optimized cast for a column in a row to double.
 */
trait CastDouble {

  def doubleColumnType: DataType

  protected final val castType: Int = doubleColumnType match {
    case IntegerType => 0
    case DoubleType => 1
    case LongType => 2
    case FloatType => 3
    case ShortType => 4
    case ByteType => 5
    case DecimalType.Fixed(_, _) | DecimalType.Unlimited => 6
    case x: NumericType => 7
    case other => sys.error(s"Type $other does not support cast to double")
  }

  protected final val numericCast: Numeric[Any] = doubleColumnType match {
    case x: NumericType => x.numeric.asInstanceOf[Numeric[Any]]
    case _ => null
  }

  final def toDouble(row: Row, ordinal: Int, default: Double): Double = {
    try {
      castType match {
        case 0 =>
          val v = row.getInt(ordinal)
          if (v != 0 || !row.isNullAt(ordinal)) v else default
        case 1 =>
          val v = row.getDouble(ordinal)
          if (v != 0 || !row.isNullAt(ordinal)) v else default
        case 2 =>
          val v = row.getLong(ordinal)
          if (v != 0 || !row.isNullAt(ordinal)) v else default
        case 3 =>
          val v = row.getFloat(ordinal)
          if (v != 0 || !row.isNullAt(ordinal)) v else default
        case 4 =>
          val v = row.getShort(ordinal)
          if (v != 0 || !row.isNullAt(ordinal)) v else default
        case 5 =>
          val v = row.getByte(ordinal)
          if (v != 0 || !row.isNullAt(ordinal)) v else default
        case 6 =>
          val v = row(ordinal)
          if (v != null) v.asInstanceOf[Decimal].toDouble else default
        case 7 =>
          val v = row(ordinal)
          if (v != null) numericCast.toDouble(v) else default
      }
    } catch {
      case NonFatal(e) => if (row.isNullAt(ordinal)) default else throw e
      case t: Throwable => throw t
    }
  }

  /** cast a non-null value to double */
  final def toDouble(v: Any): Double = {
    castType match {
      case 0 => v.asInstanceOf[Int]
      case 1 => v.asInstanceOf[Double]
      case 2 => v.asInstanceOf[Long]
      case 3 => v.asInstanceOf[Float]
      case 4 => v.asInstanceOf[Short]
      case 5 => v.asInstanceOf[Byte]
      case 6 => v.asInstanceOf[Decimal].toDouble
      case 7 => numericCast.toDouble(v)
    }
  }
}
