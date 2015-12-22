package io.snappydata.examples

import com.typesafe.config.Config
import org.apache.spark.sql.{SaveMode, DataFrame, SnappySQLJob}
import spark.jobserver.{SparkJobValid, SparkJobValidation}
import org.apache.spark.sql.snappy._

object AirlineDataJob extends SnappySQLJob {

  override def runJob(snc: C, jobConfig: Config): Any = {

    val colTableName = "airline"
    val rowTableName = "airlineref"
    val sampleTableName = "airlineSampled"

    // Get the tables that were created using sql scripts via snappy-shell
    val airlineDF: DataFrame = snc.table(colTableName)
    val airlineCodeDF: DataFrame = snc.table(rowTableName)

    // Data Frame query to get Averag ARR_DELAY with Description from column and row table.
    val result1 = airlineDF.join(airlineCodeDF, airlineDF.col("UniqueCarrier").
      equalTo(airlineCodeDF("CODE"))).groupBy(airlineDF("UniqueCarrier"),
      airlineDF("YearI"), airlineDF("MonthI"), airlineCodeDF("DESCRIPTION")).
      agg("ArrDelay" -> "avg", "FlightNum" -> "count")

    // Create a sample table
    val samples = airlineDF.stratifiedSample(Map("qcs" -> "UniqueCarrier", "fraction" -> 0.01))
    samples.write.mode(SaveMode.Append).format("column").
      options(Map[String, String]()).saveAsTable(sampleTableName)

    // Todo :Run query with ERROR ESTIMATE once the bug SNAP-274 is fixed.
    val start = System.currentTimeMillis
    val result2 = snc.sql(s"select AVG(ArrDelay),UniqueCarrier,YearI,MonthI " +
      s"from ${sampleTableName} group by UniqueCarrier,YearI,MonthI ")

    val end = System.currentTimeMillis
    val totalTime = (end - start)

    val start1 = System.currentTimeMillis
    val result3 = snc.sql(s"select AVG(ArrDelay) as AVGDELAY, UniqueCarrier, YearI, " +
      s"MonthI from ${colTableName} group by UniqueCarrier,YearI,MonthI")
    val end1 = System.currentTimeMillis

    val totalTime1 = (end1 - start1)
    Map("Result of Average ARR_DELAY" -> result1.collect(),
      "Duration in ms on SAMPLED table " -> totalTime1, "SampleTable Result: " -> result2.collect(),
      "Duration in ms on AIRLINE table" -> totalTime, "AirlineTable Result: " -> result3.collect()
    )
  }

  override def validate(sc: C, config: Config): SparkJobValidation = {
    SparkJobValid
  }
}
