package org.apache.spark.sql.sampling

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.columnar.JDBCAppendableRelation
import org.apache.spark.sql.execution.StratifiedSampler
import org.apache.spark.sql.snappy._

/**
 * Created by rishim on 21/12/15.
 */
private[sql] trait StratifiedSamplingColumnRelation {

  self: JDBCAppendableRelation =>

  def samplingOptions = Map(
    "qcs" -> "UniqueCarrier,YearI,MonthI", "fraction" -> 0.03,
    "strataReservoirSize" -> "50")

/*  def insert(df: DataFrame, overwrite: Boolean = true): Unit = {
    val columnBatchSize = sqlContext.conf.columnBatchSize

    val cached = df.rdd.mapPartitionsPreserveWithIndex({ case (split, rowIterator) =>
      val s = StratifiedSampler(samplingOptions, Array.emptyIntArray,
        table, columnBatchSize, df.schema, true)

      val converter = CatalystTypeConverters.createToCatalystConverter(df.schema)
      s.sample(rowIterator.map(converter(_).asInstanceOf[InternalRow]), true)
      s.iterator
    }, true)

    val df1 = sqlContext.internalCreateDataFrame(cached, df.schema)
    insert(df1, overwrite)
  }*/
}


