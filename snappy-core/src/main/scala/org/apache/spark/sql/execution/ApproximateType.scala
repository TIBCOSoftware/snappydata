package org.apache.spark.sql.execution

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class ApproximateType extends UserDefinedType[Approximate]  {
  /** Underlying storage type for this UDT */
  override def sqlType: DataType = ApproximateType.internalType

  override def serialize(obj: Any): Any =
    obj match {
      case approx: Approximate => Row(approx.lowerBound, approx.estimate, approx.max,
        approx.probabilityWithinBounds)
    }

  private[sql] val ordering = new Ordering[Row] {
    override def compare(x: Row, y: Row) = {
      val xx = deserialize(x)
      val yy = deserialize(y)
      xx.compare(yy)
    }
  }

  override def userClass: java.lang.Class[Approximate] = classOf[Approximate]

  /** Convert a SQL datum to the user type */
  override def deserialize(datum: Any): Approximate =
    datum match {
      case row: Row =>
        new Approximate(row.getAs[Long](0), row.getAs[Long](1),
          row.getAs[Long](2), row.getAs[Double](3))
    }

}

object ApproximateType extends ApproximateType {
  private val internalType = createSQLType

  private def createSQLType: DataType = {
    StructType(
      StructField("lowerBound", LongType, false) ::
        StructField("estimate", LongType, false) ::
        StructField("max", LongType, false) ::
        StructField("confidence", DoubleType, false) :: Nil)
  }
}