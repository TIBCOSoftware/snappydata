package org.apache.spark.sql.sampling

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.columntable.ColumnFormatRelation

/**
 * Created by rishim on 21/12/15.
 */

private[sql] trait StratifiedSamplingRelation{

  def identifier() : String

  def insert(df: DataFrame, overwrite: Boolean, sample : Boolean): Unit

  def samplingOptions() : Map[String,Any]

}
private[sql] trait StratifiedSamplingColumnRelation extends StratifiedSamplingRelation{

  self: ColumnFormatRelation =>

  override def identifier() : String = table

  override def samplingOptions = Map(
    "qcs" -> "UniqueCarrier,YearI,MonthI", "fraction" -> 0.03,
    "strataReservoirSize" -> "50")

  override def insert(df: DataFrame, overwrite: Boolean, sample : Boolean): Unit = {
    insert(df, overwrite)
  }
}


